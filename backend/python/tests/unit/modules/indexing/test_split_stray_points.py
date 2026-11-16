# ruff: noqa: F811
"""The only evidence that an older build wrote a split file whole is a point under the file's id that belongs to no
segment of its live generation.

A restore, a ``vectorDbOnly`` re-embed, a vector rebuild and a copy of the file all leave the row alone and give the
parent a new ``lastIndexTimestamp``; none of them writes a point outside a segment, so none may cut the file again,
with splitting on or off. The check is one bounded read, never a count.
"""
from __future__ import annotations

import dataclasses
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing.run import IndexingPipeline
from app.modules.indexing.split_state import PartInfo, SplitState
from app.modules.indexing.split_wiring import _WholeFile
from app.modules.transformers.write_scope import WriteScope, use_live_generation
from app.services.vector_db.membership import vector_point_payload
from app.services.vector_db.models import VectorPoint
from tests.support.in_memory_vector_db import InMemoryVectorDBService
from tests.unit.modules.indexing.test_split_seam import ORG, Rig, V, rig  # noqa: F401  (fixture)

COLLECTION = "records"


def _points(scope: WriteScope, tag: str, *, org: str = ORG, vrid: str | None = None, n: int = 2) -> list[VectorPoint]:
    out = []
    for i in range(n):
        content = {"virtualRecordId": vrid or scope.content_vrid, "blockId": f"{tag}-{i}", "blockIndex": i, "orgId": org,
                   "isBlock": True, "isBlockGroup": False}
        payload = scope.payload(content) if vrid is None else content
        out.append(VectorPoint(id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{tag}/{org}/{i}")), dense_vector=[1.0] + [0.0] * 1023,
                               payload=vector_point_payload(payload, f"{tag} {i}")))
    return out


def _pipeline(service: InMemoryVectorDBService) -> IndexingPipeline:
    pipeline = IndexingPipeline.__new__(IndexingPipeline)
    pipeline.vector_db_service = service
    pipeline.logger = MagicMock()
    pipeline.collection_registry = MagicMock()
    pipeline.collection_registry.resolve_write_collection = MagicMock(return_value=COLLECTION)
    return pipeline


async def _split_file(rig: Rig, service: InMemoryVectorDBService) -> tuple[Any, list[WriteScope]]:
    """A completed split file whose points are written the way this build writes them: the manifest and each part."""
    await rig.seam.split(rig.request())
    for node in rig.parts():
        rig.graph.docs["records"][node["_key"]]["indexingStatus"] = ProgressStatus.COMPLETED.value
    row = await rig.store.get(V)
    assert await rig.store.roll_up(row, done=3, failed=0, state=SplitState.COMPLETED)
    row = await rig.store.get(V)
    scopes = [WriteScope.manifest(row.v, row.gen)] + [
        WriteScope.part(PartInfo.of(spec, v=row.v, gen=row.gen, org_id=ORG, owner_connector_id="conn-1")) for spec in row.parts
    ]
    await service.create_collection(COLLECTION)
    for scope in scopes:
        await service.upsert_points(COLLECTION, _points(scope, scope.segment or "m"))
    return row, scopes


def _segments(row: Any) -> list[str]:  # noqa: ANN401
    return [row.v, *(p.vrid for p in row.parts)]


@pytest.mark.asyncio
class TestTheCheckIsOneBoundedExistenceRead:
    async def test_a_file_whose_every_point_is_in_a_segment_has_no_stray_points(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        row, _ = await _split_file(rig, service)

        assert await _pipeline(service).has_points_outside_segments(MagicMock(), ORG, V, _segments(row)) is False

    async def test_one_whole_file_point_beside_the_segments_is_stray(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        row, _ = await _split_file(rig, service)
        await service.upsert_points(COLLECTION, _points(WriteScope(V, V), "whole", vrid=V, n=1))

        assert await _pipeline(service).has_points_outside_segments(MagicMock(), ORG, V, _segments(row)) is True

    async def test_a_point_of_another_generation_is_stray(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        row, _ = await _split_file(rig, service)
        old = WriteScope.part(PartInfo("old", ORG, V, "old-gen", 1, 1, 10, 0, 1, "conn-1", "old-part-vrid"))
        await service.upsert_points(COLLECTION, _points(old, "old", n=1))

        assert await _pipeline(service).has_points_outside_segments(MagicMock(), ORG, V, _segments(row)) is True

    async def test_another_file_and_another_organisation_are_not_this_files_points(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        row, _ = await _split_file(rig, service)
        other_file = WriteScope("another-file", "another-file")
        await service.upsert_points(COLLECTION, _points(other_file, "other-file", vrid="another-file"))
        await service.upsert_points(COLLECTION, _points(WriteScope(V, V), "other-org", org="org-2", vrid=V))

        assert await _pipeline(service).has_points_outside_segments(MagicMock(), ORG, V, _segments(row)) is False

    async def test_it_reads_one_point_and_counts_nothing(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        row, _ = await _split_file(rig, service)
        await service.upsert_points(COLLECTION, _points(WriteScope(V, V), "whole", vrid=V, n=50))
        reads: list[int] = []
        scroll, count = service.scroll, service.count_points

        async def spy_scroll(collection_name: str, scroll_filter: Any, limit: int, *a: Any, **kw: Any) -> Any:  # noqa: ANN401
            reads.append(limit)
            return await scroll(collection_name, scroll_filter, limit, *a, **kw)

        async def no_count(*a: Any, **kw: Any) -> int:  # noqa: ANN401
            raise AssertionError("the file's points are counted")

        service.scroll, service.count_points = spy_scroll, no_count  # type: ignore[method-assign]
        assert await _pipeline(service).has_points_outside_segments(MagicMock(), ORG, V, _segments(row)) is True
        assert reads == [1]
        service.scroll, service.count_points = scroll, count  # type: ignore[method-assign]


@pytest.mark.asyncio
class TestNoWritePathOfThisBuildLooksLikeAnOlderBuild:
    """Every write of the parent goes through ``WriteScope.of_record`` (restore, ``vectorDbOnly``, rebuild, the
    manifest itself); a copy of the file writes no points. Whatever they write is in a segment, the parent's
    timestamp is new, and the file is not cut again."""

    @pytest.fixture(autouse=True)
    def _live_generation(self, rig: Rig):  # noqa: ANN202
        async def live(v: str) -> str | None:
            row = await rig.store.get(v)
            return row.gen if row is not None else None

        use_live_generation(live)
        yield
        use_live_generation(None)

    @pytest.mark.parametrize("path", ["restore", "vector_db_only", "vector_rebuild", "dedup_copy"])
    @pytest.mark.parametrize("splitting_on", [True, False])
    async def test_the_path_does_not_cut_a_healthy_split_file_again(
        self, rig: Rig, path: str, splitting_on: bool,
    ) -> None:
        service = InMemoryVectorDBService()
        row, _ = await _split_file(rig, service)
        gen = row.gen
        if path != "dedup_copy":  # the copy attaches to the file and writes no point
            record = SimpleNamespace(virtual_record_id=V, mime_type="application/pdf", extension="pdf")
            scope = await WriteScope.of_record(record)
            assert scope.segment == V and scope.gen == gen, "the parent's re-embed writes into the manifest segment"
            await service.delete_points(COLLECTION, await service.filter_collection(
                must={"orgId": ORG, "virtualRecordId": V, "segmentVirtualRecordId": V}))
            await service.upsert_points(COLLECTION, _points(scope, path))
        rig.seam._whole_file = _WholeFile(_pipeline(service))  # the real check over the real points
        parent = {**rig.graph.docs["records"]["parent-1"], "lastIndexTimestamp": row.updated_at_ms + 7_200_000}

        outcome = await rig.seam.split(dataclasses.replace(rig.request(), parent=parent, may_heal=splitting_on))

        assert outcome.reused_generation and (await rig.store.get(V)).gen == gen
        assert len(rig.cutter.calls) == 1, "a healthy split file was cut again"

    async def test_an_older_builds_whole_file_write_is_found_and_cut_again_only_while_splitting_is_on(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        row, _ = await _split_file(rig, service)
        await service.upsert_points(COLLECTION, _points(WriteScope(V, V), "older", vrid=V, n=3))
        rig.seam._whole_file = _WholeFile(_pipeline(service))
        # the heal never runs when splitting is off, so the control must show it does run when it is on
        off = await rig.seam.split(dataclasses.replace(rig.request(), may_heal=False))
        assert off.reused_generation and len(rig.cutter.calls) == 1

        on = await rig.seam.split(rig.request())

        assert not on.reused_generation and on.row.gen != row.gen and len(rig.cutter.calls) == 2
