"""Two connectors hold the same split file; the first one's connector is deleted. Run on a real graph store through
the product code (``ParentSplitSeam``, ``SplitSweep``, ``rehome_parts_owned_by``, ``reindex_failed_parts``). Only the cutter,
uploader, publisher, leases and the vector/blob resources are fakes. Shared by the Neo4j/ArangoDB suite and the PostgreSQL one.

The parts belong to the content: while any copy holds the file they stay, and the connector they answer to is the one
that still holds it, so "reindex failed" from that connector finds and re-queues the failed parts.
"""
from __future__ import annotations

import uuid
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing.split_owner import resolve_owner_connector
from app.modules.indexing.split_purge import GenerationPurger
from app.modules.indexing.split_reindex import failed_parts, reindex_failed_parts
from app.modules.indexing.split_rollup import SplitRollup
from app.modules.indexing.split_seam import ParentSplitSeam, SplitRequest
from app.modules.indexing.split_state import SplitState, SplitStore, clear_part_cache
from app.modules.indexing.split_sweep import SplitSweep
from app.modules.parsers.pdf.split.config import SplitConfig
from tests.integration.graph_db.split_seam_world import RANGES, RECORDS, file_record
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

FIRST, SECOND = "conn-first", "conn-second"


class _Producer:
    """The ``send_message`` producer ``reindex_failed_parts`` talks to."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send_message(self, topic: str, message: dict[str, Any], key: str | None = None) -> bool:
        self.sent.append(message)
        return True


async def run_rehome(
    graph: IGraphDBProvider, org: str, *,
    org_scope: Callable[[str], AbstractContextManager] = lambda _org: nullcontext(),
    sweep_scope: Callable[[], AbstractContextManager] = nullcontext,
) -> None:
    clear_part_cache()
    first_id, second_id, v = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    store, resources = SplitStore(graph), FakeResources()
    purger = GenerationPurger(graph, store, FakeLeases(), resources)
    seam = ParentSplitSeam(
        graph=graph, store=store, purger=purger, cutter=fake_cutter(RANGES), uploader=FakeUploader(), publisher=FakePublisher(),
        whole_file=resources, config=SplitConfig,
    )
    sweep = SplitSweep(
        graph=graph, store=store, purger=purger, queue_parts=seam.queue_parts, parts_missing_files=seam.parts_missing_files, lease_ms=0,
    )

    with org_scope(org):
        # The same content in two connectors: the first copy is the older one and splits it.
        await graph.batch_upsert_records([file_record(first_id, org, FIRST)])
        await graph.update_node(first_id, RECORDS, {"virtualRecordId": v, "createdAtTimestamp": 1_000})
        await graph.batch_upsert_records([file_record(second_id, org, SECOND)])
        await graph.update_node(second_id, RECORDS, {"virtualRecordId": v, "createdAtTimestamp": 2_000})
        parent = {**(await graph.get_document(first_id, RECORDS)), "_key": first_id}
        row = (await seam.split(SplitRequest(
            parent=parent, v=v, org_id=org, content=b"%PDF", content_rev="rev-1", page_count=1138, needs_ocr=False,
        ))).row
        assert {(await store.part(s.node_key)).owner_connector_id for s in row.parts} == {FIRST}

        # One part failed; the file is PARTIAL.
        failed = row.parts[1]
        for spec in row.parts:
            status = ProgressStatus.FAILED.value if spec is failed else ProgressStatus.COMPLETED.value
            await graph.update_node(spec.node_key, RECORDS, {"indexingStatus": status})
        assert await store.roll_up((await store.get(v)), done=2, failed=1, state=SplitState.PARTIAL)  # type: ignore[arg-type]

        # While both copies live, the second connector owns no part: nothing to retry from it.
        assert await failed_parts(graph, store, org, SECOND, [ProgressStatus.FAILED.value]) == []

        # The first copy's connector is deleted.
        await graph.delete_records_and_relations(first_id, hard_delete=True)
        assert await resolve_owner_connector(graph, v, FIRST) == SECOND, "the surviving holder is the owner"
        assert await failed_parts(graph, store, org, SECOND, [ProgressStatus.FAILED.value]) == [], "not re-homed yet"

    with org_scope(org):
        assert await sweep.rehome_owned_by(org, FIRST) == 1
    with sweep_scope():
        report = await sweep.run_once()
    assert report.orphans_purged == 0, "a copy still holds the file: nothing is purged"

    with org_scope(org):
        assert (await store.get(v)) is not None, "the split row stays"
        for spec in row.parts:
            assert await graph.get_document(spec.node_key, RECORDS) is not None, "every part record stays"
            assert (await store.part(spec.node_key)).owner_connector_id == SECOND
        assert resources.points_deleted == [] and resources.pdfs_deleted == []
        found = await failed_parts(graph, store, org, SECOND, [ProgressStatus.FAILED.value])
        assert [f["_key"] for f in found] == [failed.node_key]
        producer = _Producer()
        files = await reindex_failed_parts(
            graph, store, producer, org_id=org, connector_id=SECOND, statuses=[ProgressStatus.FAILED.value],
            rollup=SplitRollup(graph, store),
        )
        assert files == 1
        assert [m["payload"]["recordId"] for m in producer.sent] == [failed.node_key]
        assert producer.sent[0]["payload"]["connectorId"] == SECOND
        assert (await graph.get_document(failed.node_key, RECORDS))["indexingStatus"] == ProgressStatus.QUEUED.value
