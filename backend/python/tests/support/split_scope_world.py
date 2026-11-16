"""One world of split-file points, written the way the vector store writes them, and the questions asked of it.

Shared by the unit test (in-memory store) and the real-store test (Qdrant, OpenSearch, Redis, pgvector), so
both check the same thing: a write to one segment never reaches another.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

from app.modules.indexing.embed_resume import EmbedAttempt
from app.modules.indexing.split_state import PartInfo
from app.modules.transformers.vectorstore import VectorStore, deterministic_point_id
from app.modules.transformers.write_scope import WriteScope
from app.services.vector_db.const.const import PAYLOAD_KEYWORD_INDEXES
from app.services.vector_db.membership import vector_point_payload
from app.services.vector_db.models import CollectionConfig, DistanceMetric, VectorPoint
from tests.support.vector_db import make_collection_registry

if TYPE_CHECKING:
    from app.services.vector_db.interface.vector_db import IVectorDBService

ORG = "5f0c1d2e3a4b5c6d7e8f9a0b"
V = str(uuid.uuid5(uuid.NAMESPACE_URL, "split-world/V"))
OTHER = str(uuid.uuid5(uuid.NAMESPACE_URL, "split-world/other-file"))
GEN = str(uuid.uuid5(uuid.NAMESPACE_URL, "split-world/gen"))
BLOCKS = ("b0", "b1", "b2")  # the same block ids in every segment: only the segment tells them apart
DIM = 8


def part_vrid(k: int) -> str:
    return str(uuid.uuid5(uuid.UUID(GEN), str(k)))


MANIFEST = WriteScope.manifest(V, GEN)
PART1 = WriteScope.part(PartInfo("n1", ORG, V, GEN, 1, 1, 500, 0, 1, "app", part_vrid(1)))
PART2 = WriteScope.part(PartInfo("n2", ORG, V, GEN, 2, 501, 900, 500, 1, "app", part_vrid(2)))
UNSPLIT = WriteScope(OTHER, OTHER)
SCOPES = {"manifest": MANIFEST, "part1": PART1, "part2": PART2, "other": UNSPLIT}


def points_of(scope: WriteScope, *, rev: str = "rev-1", slot: int = 0, blocks: tuple[str, ...] = BLOCKS) -> list[VectorPoint]:
    out = []
    for i, block in enumerate(blocks):
        content = {"virtualRecordId": scope.content_vrid, "blockId": block, "blockIndex": i, "orgId": ORG, "isBlock": True,
                   "isBlockGroup": False, "contentRev": rev, "embedKey": "key-1"}
        text = f"{scope.content_vrid} {block}"
        dense = [1.0 if j == (i + slot) % DIM else 0.0 for j in range(DIM)]
        out.append(VectorPoint(id=deterministic_point_id(content, text), dense_vector=dense,
                               payload=vector_point_payload(scope.payload(content), text)))
    return out


def store_over(service: IVectorDBService) -> VectorStore:
    graph = AsyncMock()
    graph.get_document = AsyncMock(return_value={"_key": "r"})
    return VectorStore(logger=MagicMock(), config_service=AsyncMock(), graph_provider=graph,
                       collection_registry=make_collection_registry(), vector_db_service=service)


async def seed(service: IVectorDBService, collection: str) -> None:
    for slot, scope in enumerate(SCOPES.values()):
        await service.upsert_points(collection, points_of(scope, slot=slot))


async def count(service: IVectorDBService, collection: str, **must: object) -> int:
    return await service.count_points(collection, await service.filter_collection(must={"orgId": ORG, **must}))


async def census(service: IVectorDBService, collection: str, *, expected: int | None = None, wait_s: float = 45.0) -> dict[str, int]:
    """Points per segment. With *expected* total, waits for a store that makes writes visible late (OpenSearch)."""
    async def read() -> dict[str, int]:
        # A store that makes writes visible at its refresh interval (OpenSearch: 30 s here) is asked to show what it has
        # acknowledged, so the census reads the state after the write and not the one before the next refresh.
        await service.make_searchable(collection)
        return {name: await count(service, collection, virtualRecordId=scope.read_vrid, **({"segmentVirtualRecordId": scope.segment}
                if scope.segment else {})) for name, scope in SCOPES.items()}
    deadline = asyncio.get_event_loop().time() + wait_s
    while True:
        got = await read()
        if expected is None or sum(got.values()) == expected or asyncio.get_event_loop().time() > deadline:
            return got
        await asyncio.sleep(0.5)


FULL = {"manifest": 3, "part1": 3, "part2": 3, "other": 3}


async def fresh(service: IVectorDBService, collection: str) -> None:
    await service.create_collection(collection, CollectionConfig(embedding_size=DIM, distance_metric=DistanceMetric.COSINE))
    for field_name, schema in PAYLOAD_KEYWORD_INDEXES:
        try:
            await service.create_index(collection, field_name, schema)
        except Exception as e:
            if "exist" not in str(e).lower():
                raise
    await seed(service, collection)
    assert await census(service, collection, expected=12) == FULL


async def part_delete(service: IVectorDBService, collection: str) -> dict[str, int]:
    await store_over(service).delete_embeddings(PART1.content_vrid, collection, org_id=ORG, scope=PART1)
    return await census(service, collection, expected=9)


async def part_purge(service: IVectorDBService, collection: str) -> dict[str, int]:
    """The purge of a retired generation deletes a part's points by the file's id and the part's segment (the
    points carry the file's id as their own, so the part's id alone matches nothing)."""
    from app.services.vector_db.membership import delete_segment_points

    await delete_segment_points(service, [collection], ORG, V, PART1.content_vrid)
    return await census(service, collection, expected=9)


async def purge_by_the_part_id_alone(service: IVectorDBService, collection: str) -> dict[str, int]:
    """What the purge did before: filtering by the part's id as the virtual record id deletes nothing."""
    await service.delete_points(collection, await service.filter_collection(must={"orgId": ORG, "virtualRecordId": PART1.content_vrid}))
    return await census(service, collection, expected=12)


async def part_block_delete(service: IVectorDBService, collection: str) -> dict[str, int]:
    await store_over(service).delete_blocks_by_ids({"b0", "b1"}, PART1.content_vrid, collection, org_id=ORG, scope=PART1)
    return await census(service, collection, expected=10)


async def manifest_delete(service: IVectorDBService, collection: str) -> dict[str, int]:
    await store_over(service).delete_embeddings(V, collection, org_id=ORG, scope=MANIFEST)
    return await census(service, collection, expected=9)


async def end_of_run_cleanup(service: IVectorDBService, collection: str) -> dict[str, int]:
    """A resumed part run drops its own points of another revision, and only its own."""
    for scope in (PART1, PART2):
        await service.upsert_points(collection, points_of(scope, rev="rev-old", blocks=("old0",)))
    assert sum((await census(service, collection, expected=14)).values()) == 14
    wanted = [p.id for p in points_of(PART1)]
    attempt = EmbedAttempt(delivery_id="d", rev="rev-1", vrid=PART1.content_vrid)
    await store_over(service)._fence_and_verify(attempt, "key-1", wanted, "rec", PART1.content_vrid, collection, ORG, PART1)
    return await census(service, collection, expected=13)


async def file_level_delete(service: IVectorDBService, collection: str) -> dict[str, int]:
    """A delete by the file's vrid (trash, delete, connector purge) removes the manifest and every part."""
    await service.delete_points(collection, await service.filter_collection(must={"orgId": ORG, "virtualRecordId": V}))
    return await census(service, collection, expected=3)


def _whole_file_point(name: str) -> VectorPoint:
    """A point of the file indexed whole by a build that knows nothing of parts: the file's id, no segment."""
    return VectorPoint(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"split-world/{name}")), dense_vector=[1.0] + [0.0] * (DIM - 1),
        payload=vector_point_payload({"virtualRecordId": V, "blockId": name, "blockIndex": 0, "orgId": ORG, "isBlock": True,
                                      "isBlockGroup": False}, "a whole-file point"),
    )


async def repeated_and_concurrent_attempts(service: IVectorDBService, collection: str) -> dict[str, int]:
    """A second attempt for a file whose generation another attempt is indexing (it repeats, or runs at the same time)
    deletes the whole-file points outside the generation's segments and nothing inside them."""
    from app.modules.indexing.run import IndexingPipeline

    pipeline = IndexingPipeline.__new__(IndexingPipeline)
    pipeline.vector_db_service = service
    pipeline.collection_registry = MagicMock()
    pipeline.collection_registry.resolve_write_collection = MagicMock(return_value=collection)
    segments = [V, part_vrid(1), part_vrid(2)]
    await service.upsert_points(collection, [_whole_file_point("whole-1"), _whole_file_point("whole-2")])
    await service.make_searchable(collection)
    assert await count(service, collection, virtualRecordId=V) == 11, "the file's own points and two whole-file points"

    async def attempt() -> None:
        await pipeline.delete_points_outside_segments(MagicMock(), ORG, V, segments)

    await attempt()
    await asyncio.gather(attempt(), attempt(), attempt())
    await attempt()
    got = await census(service, collection, expected=12)
    await service.make_searchable(collection)
    assert await count(service, collection, virtualRecordId=V) == 9, "only the whole-file points went"
    return got


EXPECT = {
    part_delete: {"manifest": 3, "part1": 0, "part2": 3, "other": 3},
    part_purge: {"manifest": 3, "part1": 0, "part2": 3, "other": 3},
    purge_by_the_part_id_alone: FULL,
    part_block_delete: {"manifest": 3, "part1": 1, "part2": 3, "other": 3},
    manifest_delete: {"manifest": 0, "part1": 3, "part2": 3, "other": 3},
    end_of_run_cleanup: {"manifest": 3, "part1": 3, "part2": 4, "other": 3},
    file_level_delete: {"manifest": 0, "part1": 0, "part2": 0, "other": 3},
    repeated_and_concurrent_attempts: FULL,
}


async def rolled_back(service: IVectorDBService, collection: str) -> dict[str, int]:
    """``SplitRollback`` over this world's points: the graph is the in-memory fake, the vector store is the real one."""
    from app.modules.indexing.split_purge import GenerationPurger
    from app.modules.indexing.split_rollback import SplitRollback
    from app.modules.indexing.split_state import (
        SPLITS,
        SplitRow,
        SplitState,
        SplitStore,
        part_specs,
    )
    from app.services.vector_db.membership import delete_segment_points
    from tests.support.split_fakes import FakeGraph, FakeLeases, FakePublisher

    graph = FakeGraph()
    graph.put("records", "holder", {"orgId": ORG, "connectorId": "app", "virtualRecordId": V, "recordName": "big.pdf",
                                     "mimeType": "application/pdf", "isDeleted": False})
    specs = part_specs(GEN, [(1, 500, 501), (501, 900, 900)], ocr=False)
    assert [s.vrid for s in specs] == [part_vrid(1), part_vrid(2)], "the world's parts are this row's parts"
    row = SplitRow(v=V, org_id=ORG, gen=GEN, parts=specs, state=SplitState.INDEXING_PARTS, content_rev="rev-1")
    graph.put(SPLITS, V, row.to_doc())

    class Resources:
        async def delete_points_and_stored_record(self, org_id: str, root: str, part: str) -> None:
            await delete_segment_points(service, [collection], org_id, root, part)

        async def delete_pdf(self, org_id: str, storage_document_id: str) -> None:
            return None

    async def manifest_points(org_id: str, v: str) -> None:
        await delete_segment_points(service, [collection], org_id, v, v)

    store = SplitStore(graph)
    report = await SplitRollback(
        graph=graph, store=store, purger=GenerationPurger(graph, store, FakeLeases(), Resources()), publisher=FakePublisher(),
        manifest_points=manifest_points, apply=True,
    ).run_once()
    assert (report.unsplit, report.requeued) == (1, 1)
    return await census(service, collection, expected=3)


async def stray_points(service: IVectorDBService, collection: str) -> tuple[bool, bool]:
    """The roll-forward heal's question, asked of the real store: (before, after) one whole-file point is written beside
    the segments. The whole point is what a build that knows nothing of parts writes; it has no segment field."""
    from app.modules.indexing.run import IndexingPipeline

    pipeline = IndexingPipeline.__new__(IndexingPipeline)
    pipeline.vector_db_service = service
    pipeline.collection_registry = MagicMock()
    pipeline.collection_registry.resolve_write_collection = MagicMock(return_value=collection)
    segments = [V, part_vrid(1), part_vrid(2)]

    async def ask() -> bool:
        await service.make_searchable(collection)
        return await pipeline.has_points_outside_segments(MagicMock(), ORG, V, segments)

    before = await ask()
    whole = VectorPoint(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, "split-world/whole-file-point")), dense_vector=[1.0] + [0.0] * (DIM - 1),
        payload=vector_point_payload({"virtualRecordId": V, "blockId": "whole", "blockIndex": 0, "orgId": ORG, "isBlock": True,
                                      "isBlockGroup": False}, "written by a build that knows nothing of parts"),
    )
    await service.upsert_points(collection, [whole])
    return before, await ask()
