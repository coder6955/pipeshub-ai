"""The whole life of a split file on a real graph store: split, part gate, ``roots_of``, the parked-while-off
walk, trash and restore, re-split purge and the orphan sweep, through the product code (``ParentSplitSeam``, ``GenerationPurger``,
``SplitSweep``, ``check_part``, ``part_records_pages``). Only the cutter, uploader, publisher, leases and the vector/blob resources
are fakes. Shared by the Neo4j/ArangoDB suite and the PostgreSQL one.

A fake of the graph hid that ``get_records_by_virtual_record_id`` answers record keys, not documents: this scenario is the check.
"""
from __future__ import annotations

import uuid
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING

from app.config.constants.arangodb import (
    CollectionNames,
    Connectors,
    OriginTypes,
    ProgressStatus,
)
from app.models.entities import FileRecord, RecordType
from app.modules.indexing.part_gate import PartContext, PartStop, check_part
from app.modules.indexing.split_purge import FILE_REMOVED_REASON, GenerationPurger
from app.modules.indexing.split_seam import ParentSplitSeam, SplitRequest
from app.modules.indexing.split_state import (
    SplitState,
    SplitStore,
    clear_part_cache,
    part_info,
    part_records_pages,
)
from app.modules.indexing.split_sweep import SplitSweep
from app.modules.parsers.pdf.split.config import SplitConfig
from app.utils.user_errors import CONNECTOR_OFF
from tests.support.split_fakes import (
    FakeLeases,
    FakePublisher,
    FakeResources,
    FakeUploader,
    fake_cutter,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value
RANGES = [(1, 500, 501), (501, 1000, 1001), (1001, 1138, 1138)]
OWNER = "conn-owner"
NOW = 1_700_000_000_000


def file_record(record_id: str, org: str, connector_id: str = OWNER) -> FileRecord:
    return FileRecord(
        id=record_id, org_id=org, record_name="big.pdf", record_type=RecordType.FILE, external_record_id=f"ext-{record_id}",
        version=0, origin=OriginTypes.CONNECTOR, connector_name=Connectors.GOOGLE_DRIVE, connector_id=connector_id,
        mime_type="application/pdf", is_file=True, extension="pdf", created_at=NOW, updated_at=NOW,
        source_created_at=NOW, source_updated_at=NOW,
    )


async def run_lifecycle(
    graph: IGraphDBProvider, org: str, *,
    org_scope: Callable[[str], AbstractContextManager] = lambda _org: nullcontext(),
    sweep_scope: Callable[[], AbstractContextManager] = nullcontext,
) -> None:
    clear_part_cache()
    parent_id, v = str(uuid.uuid4()), str(uuid.uuid4())
    store, leases, resources, publisher = SplitStore(graph), FakeLeases(), FakeResources(), FakePublisher()
    purger = GenerationPurger(graph, store, leases, resources)
    seam = ParentSplitSeam(
        graph=graph, store=store, purger=purger, cutter=fake_cutter(RANGES), uploader=FakeUploader(), publisher=publisher,
        whole_file=resources, config=SplitConfig,
    )
    sweep = SplitSweep(
        graph=graph, store=store, purger=purger, queue_parts=seam.queue_parts, parts_missing_files=seam.parts_missing_files, lease_ms=0,
    )

    with org_scope(org):
        await graph.batch_upsert_records([file_record(parent_id, org)])
        await graph.update_node(parent_id, RECORDS, {"virtualRecordId": v})
        parent = {**(await graph.get_document(parent_id, RECORDS)), "_key": parent_id}

        def request(rev: str) -> SplitRequest:
            return SplitRequest(parent=parent, v=v, org_id=org, content=b"%PDF", content_rev=rev, page_count=1138, needs_ocr=False)

        # The vrid lookup answers keys on every store.
        assert await graph.get_records_by_virtual_record_id(v, raise_on_error=True) == [parent_id]

        # Split: the row reaches INDEXING_PARTS and every part is queued under the owner the part document names.
        old = (await seam.split(request("rev-1"))).row
        row = await store.get(v)
        assert row is not None and row.state is SplitState.INDEXING_PARTS, row
        assert len(publisher.sent) == 3
        assert {payload["connectorId"] for _, _, payload, _ in publisher.sent} == {OWNER}
        for spec in old.parts:
            node = await graph.get_document(spec.node_key, RECORDS)
            assert node["indexingStatus"] == ProgressStatus.QUEUED.value and node["storageDocumentId"]

        # The part gate and the part-to-file map.
        node = {**(await graph.get_document(old.parts[0].node_key, RECORDS)), "_key": old.parts[0].node_key}
        info = await part_info(node, graph)
        assert isinstance(await check_part(graph, store, info), PartContext)
        assert await store.roots_of([p.vrid for p in old.parts]) == {p.vrid: v for p in old.parts}

        # Parked while the owner was off: the walk reads only those parts, found by their owner.
        for spec in old.parts[:2]:
            assert await graph.update_node_fields_if_match(
                spec.node_key, RECORDS, {"indexingStatus": ProgressStatus.AUTO_INDEX_OFF.value, "reason": CONNECTOR_OFF},
                {"virtualRecordId": spec.vrid},
            )
        walked = [
            r["_key"] async for page in part_records_pages(
                graph, OWNER, org, {"indexingStatus": ProgressStatus.AUTO_INDEX_OFF.value, "reason": CONNECTOR_OFF}, page_size=1,
            ) for r in page
        ]
        assert sorted(walked) == sorted(s.node_key for s in old.parts[:2])
        assert [
            r async for page in part_records_pages(graph, "conn-other", org, {"indexingStatus": ProgressStatus.AUTO_INDEX_OFF.value}) for r in page
        ] == []
        for spec in old.parts[:2]:
            await graph.update_node_fields_if_match(
                spec.node_key, RECORDS, {"indexingStatus": ProgressStatus.QUEUED.value, "reason": None}, {"virtualRecordId": spec.vrid},
            )

        # Trash and restore: the gate stops the parts while nothing live holds the file, the sweep keeps them, and
        # brings them back once the file is live again.
        assert await graph.update_node_fields_if_match(parent_id, RECORDS, {"isDeleted": True}, {"virtualRecordId": v})
        assert await graph.get_records_by_virtual_record_id(v, raise_on_error=True) == []
        assert isinstance(await check_part(graph, store, info), PartStop)
        for spec in old.parts:
            await graph.update_node_fields_if_match(
                spec.node_key, RECORDS, {"indexingStatus": ProgressStatus.AUTO_INDEX_OFF.value, "reason": FILE_REMOVED_REASON},
                {"virtualRecordId": spec.vrid},
            )
        sent = len(publisher.sent)
    with sweep_scope():
        await sweep.run_once()
    with org_scope(org):
        assert (await store.get(v)) is not None, "a trashed file keeps its parts"
        assert len(publisher.sent) == sent
        assert await graph.update_node_fields_if_match(parent_id, RECORDS, {"isDeleted": False}, {"virtualRecordId": v})
    with sweep_scope():
        await sweep.run_once()
    with org_scope(org):
        for spec in old.parts:
            assert (await graph.get_document(spec.node_key, RECORDS))["indexingStatus"] == ProgressStatus.QUEUED.value
        assert len(publisher.sent) == sent + 3

        # A changed file replaces the generation and purges the old parts.
        new = (await seam.split(request("rev-2"))).row
        assert new.gen != old.gen
        for spec in old.parts:
            assert await graph.get_document(spec.node_key, RECORDS) is None
            assert await store.part(spec.node_key) is None
        assert isinstance(await check_part(graph, store, info), PartStop)
        assert (await store.get(v)).state is SplitState.INDEXING_PARTS  # type: ignore[union-attr]

        # The file is deleted: the orphan sweep retires the row and the parts.
        await graph.delete_records_and_relations(parent_id, hard_delete=True)
    with sweep_scope():
        await sweep.run_once()
    with org_scope(org):
        assert await store.get(v) is None
        for spec in new.parts:
            assert await graph.get_document(spec.node_key, RECORDS) is None
        assert sorted(set(resources.points_deleted)) == sorted(p.vrid for p in (*old.parts, *new.parts))
