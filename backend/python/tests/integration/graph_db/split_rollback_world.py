"""Un-splitting every file before a downgrade, on a real graph store: ``SplitRollback`` through the product code
(``ParentSplitSeam``, ``GenerationPurger``). Only the cutter, uploader, publisher, leases and the vector/blob resources are
fakes. Shared by the Neo4j/ArangoDB suite and the PostgreSQL one."""
from __future__ import annotations

import contextlib
import uuid
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING

from app.modules.indexing.split_purge import GenerationPurger
from app.modules.indexing.split_rollback import SplitRollback
from app.modules.indexing.split_seam import ParentSplitSeam, SplitRequest
from app.modules.indexing.split_state import SplitStore, clear_part_cache
from app.modules.parsers.pdf.split.config import SplitConfig
from tests.integration.graph_db.split_seam_world import (
    OWNER,
    RANGES,
    RECORDS,
    file_record,
)
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


async def run_rollback(
    graph: IGraphDBProvider, org: str, *,
    org_scope: Callable[[str], AbstractContextManager] = lambda _org: nullcontext(),
    sweep_scope: Callable[[], AbstractContextManager] = nullcontext,
) -> None:
    clear_part_cache()
    record_id, v = str(uuid.uuid4()), str(uuid.uuid4())
    store, leases, resources, publisher = SplitStore(graph), FakeLeases(), FakeResources(), FakePublisher()
    purger = GenerationPurger(graph, store, leases, resources)
    seam = ParentSplitSeam(
        graph=graph, store=store, purger=purger, cutter=fake_cutter(RANGES), uploader=FakeUploader(), publisher=publisher,
        whole_file=resources, config=SplitConfig,
    )
    manifest: list[tuple[str, str]] = []

    async def manifest_points(org_id: str, vrid: str) -> None:
        manifest.append((org_id, vrid))

    def rollback(*, apply: bool) -> SplitRollback:
        return SplitRollback(
            graph=graph, store=store, purger=purger, publisher=publisher, manifest_points=manifest_points,
            apply=apply, org_id=org, splitting_enabled=False,
        )

    async def run(*, apply: bool):  # noqa: ANN202
        with sweep_scope():  # the recovery loop reads every organisation's rows
            return await rollback(apply=apply).run_once()

    with org_scope(org):
        await graph.batch_upsert_records([file_record(record_id, org, OWNER)])
        await graph.update_node(record_id, RECORDS, {"virtualRecordId": v})
        parent = {**(await graph.get_document(record_id, RECORDS)), "_key": record_id}
        old = (await seam.split(SplitRequest(
            parent=parent, v=v, org_id=org, content=b"%PDF", content_rev="rev-1", page_count=1138, needs_ocr=False,
        ))).row
    queued_before = len(publisher.sent)

    dry = await run(apply=False)
    assert (dry.files, dry.unsplit) == (1, 0), "a dry run changes nothing"
    with org_scope(org):
        assert await store.get(v) is not None and len(publisher.sent) == queued_before

    done = await run(apply=True)
    assert (done.files, done.unsplit, done.requeued, done.busy) == (1, 1, 1, 0)
    with org_scope(org):
        assert await store.get(v) is None, "the split row is gone"
        for spec in old.parts:
            assert await graph.get_document(spec.node_key, RECORDS) is None, "every part record is gone"
            assert await store.part(spec.node_key) is None, "and its part document"
        assert await graph.get_document(record_id, RECORDS) is not None, "the file itself stays"
    assert sorted(resources.points_deleted) == sorted(s.vrid for s in old.parts)
    assert len(resources.pdfs_deleted) == 3 and manifest == [(org, v)]
    sent = publisher.sent[queued_before:]
    assert [(e, k, p["forceReindex"]) for _t, e, p, k in sent] == [("reindexRecord", record_id, True)]

    again = await run(apply=True)
    assert again.done and len(publisher.sent) == queued_before + 1, "idempotent: nothing more to do, nothing sent twice"


async def run_poisoned_pages(
    graph: IGraphDBProvider, org: str, *,
    org_scope: Callable[[str], AbstractContextManager] = lambda _org: nullcontext(),
    sweep_scope: Callable[[], AbstractContextManager] = nullcontext,
) -> None:
    """One row this build cannot read sits in the middle of a page of split files, and another organisation's file sits
    in front of them. Rollback un-splits every readable file of the organisation, counts the unreadable row as failed,
    and says split files remain; the sweep's orphan step still retires the deleted file behind the bad row. The row is
    stored the way a newer build might: a plan entry that has no page range."""
    from app.modules.indexing.split_state import SPLITS
    from app.modules.indexing.split_sweep import SplitSweep

    clear_part_cache()
    store, leases, resources, publisher = SplitStore(graph), FakeLeases(), FakeResources(), FakePublisher()
    purger = GenerationPurger(graph, store, leases, resources)
    seam = ParentSplitSeam(
        graph=graph, store=store, purger=purger, cutter=fake_cutter(RANGES), uploader=FakeUploader(), publisher=publisher,
        whole_file=resources, config=SplitConfig,
    )
    prefix = uuid.uuid4().hex[:8]
    first, bad, third, gone = (f"{prefix}-{n}" for n in ("1-first", "2-bad", "3-third", "4-gone"))
    records: dict[str, str] = {}

    async def split(v: str) -> None:
        record_id = str(uuid.uuid4())
        records[v] = record_id
        await graph.batch_upsert_records([file_record(record_id, org, OWNER)])
        await graph.update_node(record_id, RECORDS, {"virtualRecordId": v})
        parent = {**(await graph.get_document(record_id, RECORDS)), "_key": record_id}
        await seam.split(SplitRequest(parent=parent, v=v, org_id=org, content=b"%PDF", content_rev="rev-1", page_count=1138, needs_ocr=False))

    with org_scope(org):
        await split(first)
        await split(third)
        await split(gone)
        await graph.create_node_if_absent(bad, SPLITS, {
            "orgId": org, "gen": "g-bad", "plan": [{"k": 1}], "total": 1, "done": 0, "failed": 0, "state": "COMPLETED",
            "epoch": 0, "updatedAtMs": 1,
        })
        await graph.delete_nodes([records[gone]], RECORDS)  # its file is deleted: only the orphan step retires its parts

    async def manifest_points(org_id: str, vrid: str) -> None:
        return None

    sweep = SplitSweep(
        graph=graph, store=store, purger=purger, queue_parts=seam.queue_parts, parts_missing_files=seam.parts_missing_files,
        lease_ms=0, page_size=2,
    )
    with sweep_scope():
        for _ in range(3):
            with contextlib.suppress(Exception):  # a pass may report the bad row; it must still reach the others
                await sweep.run_once()
    with org_scope(org):
        assert await store.get(gone) is None, "the deleted file behind the unreadable row has no parts left"
        assert await store.get(first) is not None and await store.get(third) is not None, "files that have a holder keep theirs"

    rollback = SplitRollback(
        graph=graph, store=store, purger=purger, publisher=publisher, manifest_points=manifest_points, apply=True,
        org_id=org, splitting_enabled=False, page_size=2,
    )
    with sweep_scope():
        reports = [await rollback.run_once() for _ in range(3)]
    assert sum(r.unsplit for r in reports) == 2, "both readable files are un-split, the one in front of the bad row and the one behind"
    assert all(not r.done for r in reports) and sum(r.failed for r in reports) >= 1, "the unreadable row keeps the rollback open"
    with org_scope(org):
        assert await store.get(first) is None and await store.get(third) is None
