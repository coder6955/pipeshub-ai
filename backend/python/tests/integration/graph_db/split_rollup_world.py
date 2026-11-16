"""One roll-up race, run against whichever graph store the caller hands it (real stores).

35 parts end at the same moment, each ending calls the roll-up; three of them failed and are retried while the
others' roll-ups are still running; a heal pass runs last. Whatever the interleaving of the store's own reads and
writes, the row must end equal to the recount of the part records.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import CollectionNames, ProgressStatus
from app.modules.indexing.split_rollup import (
    DONE_STATUSES,
    FAILED_STATUSES,
    Outcome,
    SplitRollup,
)
from app.modules.indexing.split_state import (
    PartInfo,
    SplitRow,
    SplitState,
    SplitStore,
    derive_gen,
    part_specs,
)
from tests.integration.graph_db.test_content_splits_real_backends import NOW, _part

if TYPE_CHECKING:
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value
PARTS = 35
FAILING = (4, 17, 30)


class Driven:
    def __init__(self) -> None:
        self.rows: list[SplitRow] = []

    async def __call__(self, row: SplitRow) -> None:
        self.rows.append(row)


async def end_stages(graph: IGraphDBProvider, info: PartInfo, org: str) -> Any:  # noqa: ANN401
    """Returns the part's ``entities`` job: what the stage worker hands the settle hook."""
    """The part's own ``classify`` and ``entities`` have ended: real stage states on the store (the parent's stages
    run only after them, D-28), and the revision they belong to on the part record."""
    from app.modules.pipeline.models import Priority, StageJob, StageState
    from app.modules.pipeline.state import GraphStageStateStore
    from app.services.resource_governor.models import ParseTier

    rev = f"rev-{info.k}"
    assert await graph.update_node(info.key, RECORDS, {"contentRev": rev})
    states = GraphStageStateStore(graph)
    for stage in ("classify", "entities"):
        job = StageJob.model_validate({
            "stage": stage, "stage_version": 1, "org_id": org, "virtual_record_id": info.vrid, "rev": rev,
            "record_ids": (info.key,), "connector_id": "conn-owner", "tier": ParseTier.LIGHT, "priority": Priority.BULK,
            "trigger": "embed", "text_digest": "t", "blocks_digest": "b", "text_chars": 1, "has_tables": False, "has_images": False,
        })
        await states.create_if_absent(StageState.from_job(job, ProgressStatus.COMPLETED, NOW))
    return job


async def seed(graph: IGraphDBProvider, org: str) -> tuple[SplitStore, SplitRow, list[PartInfo]]:
    v = f"root-{uuid.uuid4().hex}"
    gen = derive_gen(v, "rev", "digest")
    specs = part_specs(gen, [(1 + 10 * i, 10 * (i + 1), 10 * (i + 1) + 1) for i in range(PARTS)], ocr=False)
    store = SplitStore(graph, clock_ms=lambda: NOW)
    row = SplitRow(v=v, org_id=org, gen=gen, parts=specs, state=SplitState.INDEXING_PARTS, content_rev="rev", plan_digest="digest")
    assert await store.create(row)
    records = []
    for spec in specs:
        record = _part(v, gen, spec.k, org=org)
        records.append({**record, "id": spec.node_key, "virtualRecordId": spec.vrid})
    assert await graph.batch_upsert_nodes(records, RECORDS)
    infos = [PartInfo.of(s, v=v, gen=gen, org_id=org, owner_connector_id="conn-owner") for s in specs]
    await store.put_parts(infos)
    return store, row, infos


async def recount(graph: IGraphDBProvider, row: SplitRow) -> tuple[int, int]:
    docs = await graph.get_nodes_by_field_in(
        RECORDS, "virtualRecordId", [p.vrid for p in row.parts], ["virtualRecordId", "indexingStatus"], raise_on_error=True,
    )
    statuses = [d["indexingStatus"] for d in docs]
    assert len(statuses) == len(row.parts)
    return sum(s in DONE_STATUSES for s in statuses), sum(s in FAILED_STATUSES for s in statuses)


async def race(graph: IGraphDBProvider, org: str) -> dict[str, Any]:
    store, row, infos = await seed(graph, org)
    driven = Driven()
    rollup = SplitRollup(graph, store, driven)

    async def end(info: PartInfo, status: str) -> Outcome:
        assert await graph.update_node(info.key, RECORDS, {"indexingStatus": status})
        if status == ProgressStatus.COMPLETED.value:
            await end_stages(graph, info, org)
        return await rollup.on_part_terminal(info)

    first = await asyncio.gather(*(
        end(info, ProgressStatus.FAILED.value if info.k in FAILING else ProgressStatus.COMPLETED.value) for info in infos
    ))
    stored = await store.get(row.v)
    after_first = (stored.done, stored.failed, stored.state)

    healed_first = await rollup.recount(row.v)
    stored = await store.get(row.v)
    partial = (stored.done, stored.failed, stored.state)
    truth_partial = await recount(graph, row)

    # The retry: the file is indexing again, and the failed parts finish while a late roll-up of another part runs.
    assert await rollup.reopen(row.v, row.gen)
    retried = [i for i in infos if i.k in FAILING]
    second = await asyncio.gather(*(end(i, ProgressStatus.COMPLETED.value) for i in retried), rollup.recount(row.v))
    healed_second = await rollup.recount(row.v)
    final = await store.get(row.v)
    return {
        "first_outcomes": first, "after_first": after_first, "partial": partial, "truth_partial": truth_partial,
        "healed_first": healed_first, "second_outcomes": second, "healed_second": healed_second,
        "final": (final.done, final.failed, final.state), "truth_final": await recount(graph, row),
        "driven": len(driven.rows), "epoch": final.epoch,
    }


def check(result: dict[str, Any]) -> None:
    if Outcome.GAVE_UP not in result["first_outcomes"]:
        # Every roll-up got its write in: no heal was needed, and what the last writer stored is the recount.
        assert result["after_first"][:2] == (PARTS - len(FAILING), len(FAILING))
    assert result["partial"][:2] == result["truth_partial"] == (PARTS - len(FAILING), len(FAILING))
    assert result["partial"][2] is SplitState.PARTIAL
    assert result["final"][:2] == result["truth_final"] == (PARTS, 0)
    assert result["final"][2] is SplitState.COMPLETED
    assert result["driven"] >= 2, "the parent's stages were driven once for the partial end and again after the retry"


async def stale_snapshot(graph: IGraphDBProvider, org: str) -> tuple[tuple[int, int], tuple[int, int, SplitState]]:
    """A roll-up counts one part done and stalls; every other part ends and a second roll-up writes the full count;
    the first resumes. Its stale count must lose, whatever the store."""
    store, row, infos = await seed(graph, org)
    rollup = SplitRollup(graph, store, Driven())
    assert await graph.update_node(infos[0].key, RECORDS, {"indexingStatus": ProgressStatus.COMPLETED.value})
    stalled, resume = asyncio.Event(), asyncio.Event()
    real = graph.get_nodes_by_field_in
    calls = 0

    async def stall_the_first_recount(*args: Any, **kw: Any) -> list[dict]:  # noqa: ANN401
        nonlocal calls
        calls += 1
        found = await real(*args, **kw)
        if calls == 1:
            stalled.set()
            await resume.wait()
        return found

    graph.get_nodes_by_field_in = stall_the_first_recount  # type: ignore[method-assign]
    try:
        early = asyncio.create_task(rollup.recount(row.v))
        await stalled.wait()
        for info in infos[1:]:
            assert await graph.update_node(info.key, RECORDS, {"indexingStatus": ProgressStatus.COMPLETED.value})
        assert await rollup.recount(row.v) is Outcome.WRITTEN
        resume.set()
        await early
    finally:
        graph.get_nodes_by_field_in = real  # type: ignore[method-assign]
    final = await store.get(row.v)
    return await recount(graph, row), (final.done, final.failed, final.state)


async def drive_waits_for_the_parts_stages(graph: IGraphDBProvider, org: str) -> list[int]:
    """D-28 on the real store: all parts indexed drives nothing; the stage states of every part but the last drive
    nothing; the last stage ending drives once; a repeat, and a second process (a fresh roll-up), drive nothing.
    Returns the number of drives after each step."""
    store, row, infos = await seed(graph, org)
    driven = Driven()
    rollup = SplitRollup(graph, store, driven)
    seen: list[int] = []
    for info in infos:
        assert await graph.update_node(info.key, RECORDS, {"indexingStatus": ProgressStatus.COMPLETED.value})
        await rollup.on_part_terminal(info)
    assert (await store.get(row.v)).state is SplitState.COMPLETED
    seen.append(len(driven.rows))
    for info in infos[:-1]:
        await end_stages(graph, info, org)
        await rollup.on_part_stage_settled(info.vrid)
    seen.append(len(driven.rows))
    await end_stages(graph, infos[-1], org)
    await rollup.on_part_stage_settled(infos[-1].vrid)
    seen.append(len(driven.rows))
    await rollup.on_part_stage_settled(infos[-1].vrid)
    await SplitRollup(graph, store, driven).on_part_stage_settled(infos[0].vrid)
    seen.append(len(driven.rows))
    return seen


async def token_round_trip(graph: IGraphDBProvider, org: str) -> tuple[str, str, str]:
    """The drive token on the closed ``contentSplits`` schema: a new row reads back with none, a roll-up write carries
    one, another write may clear it. The write is refused by a store whose schema does not declare the field."""
    store, row, _infos = await seed(graph, org)
    first = (await store.get(row.v)).redrive_token
    assert await store.roll_up(row, done=0, failed=0, state=SplitState.INDEXING_PARTS, redriveToken="abc123")
    claimed = await store.get(row.v)
    assert await store.roll_up(claimed, done=0, failed=0, state=SplitState.INDEXING_PARTS, redriveToken="")
    return first, claimed.redrive_token, (await store.get(row.v)).redrive_token


async def stale_drive_claim_is_reclaimed(graph: IGraphDBProvider, org: str) -> list[int]:
    """A drive claim left by a process that died (token and claim time on the row, no drive) is reclaimed by the heal
    path after the bound and driven once; before the bound, and after the drive ended, nothing is driven. Returns the
    number of drives after each step: young claim, stale claim, repeat, a finished claim aged by hours."""
    store, row, infos = await seed(graph, org)
    driven = Driven()
    clock = [10_000_000]
    rollup = SplitRollup(graph, store, driven, clock_ms=lambda: clock[0])
    for info in infos:
        assert await graph.update_node(info.key, RECORDS, {"indexingStatus": ProgressStatus.COMPLETED.value})
        await end_stages(graph, info, org)
    # The counts are written (the file is COMPLETED) with the claim a dead process left.
    await rollup.recount(row.v)
    # recount drove it; clear that to model the crash: the token stays, the drive never ran.
    driven.rows.clear()
    settled = await store.get(row.v)
    assert await store.roll_up(
        settled, done=settled.done, failed=settled.failed, state=settled.state,
        redriveToken="dead", redriveAtMs=clock[0],
    )
    seen: list[int] = []
    clock[0] += 10 * 60_000
    await rollup.reclaim_stale(await store.get(row.v))
    seen.append(len(driven.rows))
    clock[0] += 25 * 60_000
    held = await store.get(row.v)
    assert held.redrive_token == "dead" and held.redrive_at_ms > 0
    await rollup.reclaim_stale(held)
    seen.append(len(driven.rows))
    await rollup.reclaim_stale(await store.get(row.v))
    seen.append(len(driven.rows))
    clock[0] += 5 * 60 * 60_000
    await rollup.reclaim_stale(await store.get(row.v))
    seen.append(len(driven.rows))
    return seen


async def retried_part_drives_the_parent_once(graph: IGraphDBProvider, org: str) -> list[int]:
    """A part fails, is retried, completes, and its stages end: the parent's stages are driven again, exactly once.

    The jobs carry the owner connector, as every production job does (never the hidden parts connector). Returns the
    drives after: the first settle (one part failed), the retry's reopening, the retried part's indexed write, the
    stage hook of that part, a repeat of the hook, and a heal pass of a fresh process."""
    from app.modules.indexing.split_rollup import part_stage_settled, use_rollup

    store, row, infos = await seed(graph, org)
    driven = Driven()
    rollup = SplitRollup(graph, store, driven)
    use_rollup(rollup)
    try:
        failing = infos[1]
        for info in infos:
            status = ProgressStatus.FAILED.value if info is failing else ProgressStatus.COMPLETED.value
            assert await graph.update_node(info.key, RECORDS, {"indexingStatus": status})
            if info is not failing:
                await end_stages(graph, info, org)
            await rollup.on_part_terminal(info)
        assert (await store.get(row.v)).state is SplitState.PARTIAL
        seen = [len(driven.rows)]
        assert await rollup.reopen(row.v, row.gen)
        assert (await store.get(row.v)).redrive_token == "", "reopening the file gives the drive claim back"
        assert await graph.update_node(failing.key, RECORDS, {"indexingStatus": ProgressStatus.QUEUED.value})
        seen.append(len(driven.rows))
        assert await graph.update_node(failing.key, RECORDS, {"indexingStatus": ProgressStatus.COMPLETED.value})
        await rollup.on_part_terminal(failing)
        seen.append(len(driven.rows))
        job = await end_stages(graph, failing, org)
        await part_stage_settled(job)
        seen.append(len(driven.rows))
        await part_stage_settled(job)
        seen.append(len(driven.rows))
        await SplitRollup(graph, store, driven).drive(row.v)
        seen.append(len(driven.rows))
        return seen
    finally:
        use_rollup(None)
