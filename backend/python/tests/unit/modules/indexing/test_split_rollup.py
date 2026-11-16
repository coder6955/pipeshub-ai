"""The file's progress rolls up from its parts correctly under concurrency.

The model is the algorithm of design §11.5: a part's roll-up reads the row, recounts, and writes with a
compare-and-set on ``epoch``. The scripted interleavings below make every read and write of the roll-up a
point where another writer may run, and the final row must equal the recount.
"""
from __future__ import annotations

import asyncio
import random
import re
from pathlib import Path
from typing import Any

import pytest

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing import split_rollup
from app.modules.indexing.split_rollup import (
    DONE_STATUSES,
    FAILED_STATUSES,
    TERMINAL_STATUSES,
    Outcome,
    SplitRollup,
    part_terminal,
    use_rollup,
)
from app.modules.indexing.split_state import (
    SplitRow,
    SplitState,
    SplitStore,
    clear_part_cache,
    part_info,
)
from app.modules.transformers.write_scope import use_live_generation
from tests.support.split_fakes import FakeGraph
from tests.unit.modules.indexing import test_split_seam as seam_tests
from tests.unit.modules.indexing.test_split_seam import ORG, OWNER, Rig, V

APP = Path(__file__).resolve().parents[4] / "app"
MANY = [(1 + 10 * i, 10 * (i + 1), 10 * (i + 1) + 1) for i in range(35)]


class Racy(FakeGraph):
    """Yields to the other writers around every read and write of the roll-up, in an order the seed picks."""

    def __init__(self, rng: random.Random) -> None:
        super().__init__()
        self.rng = rng

    async def _race(self) -> None:
        for _ in range(self.rng.randint(0, 12)):
            await asyncio.sleep(0)

    async def get_document(self, key: str, collection: str, **kw: object) -> dict[str, Any] | None:
        await self._race()
        return await super().get_document(key, collection, **kw)

    async def get_nodes_by_field_in(self, *args: Any, **kw: Any) -> list[dict[str, Any]]:  # noqa: ANN401
        await self._race()
        found = await super().get_nodes_by_field_in(*args, **kw)
        await self._race()
        return found

    async def update_node_fields_if_match(self, *args: Any, **kw: Any) -> bool:  # noqa: ANN401
        await self._race()
        return await super().update_node_fields_if_match(*args, **kw)


class Driven:
    def __init__(self) -> None:
        self.rows: list[SplitRow] = []

    async def __call__(self, row: SplitRow) -> None:
        self.rows.append(row)


async def _rig(ranges: list[tuple[int, int, int]] | None = None, *, graph: FakeGraph | None = None) -> tuple[Rig, SplitRow]:
    clear_part_cache()
    seam_tests.NOW[0] = 1_000_000
    rig = Rig(ranges) if ranges else Rig()
    if graph is not None:
        graph.docs = rig.graph.docs
        rig.graph = graph
        rig.store = SplitStore(graph, clock_ms=lambda: 1_000_000)
    outcome = await rig.seam.split(rig.request())
    return rig, outcome.row


def _finish(rig: Rig, row: SplitRow, k: int, status: str) -> str:
    key = row.parts[k].node_key
    rig.graph.docs["records"][key]["indexingStatus"] = status
    return key


def _stages(rig: Rig, row: SplitRow, *ks: int, status: str = "COMPLETED", attempt: int = 1) -> None:
    """The part's own ``classify`` and ``entities`` have ended (what its stage worker leaves behind)."""
    from app.modules.pipeline.models import stage_state_key

    for k in ks:
        spec = row.parts[k]
        rig.graph.docs["records"][spec.node_key]["contentRev"] = f"rev-{k}"
        for stage in ("classify", "entities"):
            rig.graph.states[stage_state_key(spec.vrid, f"rev-{k}", stage)] = {"status": status, "attempt": attempt}


def _rollup(rig: Rig, driven: Driven | None = None, **kw: Any) -> SplitRollup:  # noqa: ANN401
    async def no_wait(_s: float) -> None:
        await asyncio.sleep(0)

    return SplitRollup(rig.graph, rig.store, driven, sleep=no_wait, jitter=lambda: 0.0, **kw)


class TestTerminalSet:
    def test_the_terminal_grouping_is_pinned_against_progress_status(self) -> None:
        waiting = {
            ProgressStatus.NOT_STARTED, ProgressStatus.PAUSED, ProgressStatus.IN_PROGRESS, ProgressStatus.AUTO_INDEX_OFF,
            ProgressStatus.ENABLE_MULTIMODAL_MODELS, ProgressStatus.QUEUED, ProgressStatus.SKIPPED,
        }
        terminal = {
            ProgressStatus.COMPLETED, ProgressStatus.EMPTY, ProgressStatus.FAILED, ProgressStatus.PASSWORD_PROTECTED,
            ProgressStatus.FILE_TYPE_NOT_SUPPORTED,
        }
        assert waiting | terminal == set(ProgressStatus), "a new ProgressStatus must be placed in one group here"
        assert not waiting & terminal
        assert TERMINAL_STATUSES == {s.value for s in terminal}
        assert DONE_STATUSES | FAILED_STATUSES == TERMINAL_STATUSES
        assert not DONE_STATUSES & FAILED_STATUSES


@pytest.mark.asyncio
class TestRecount:
    async def test_progress_is_counted_and_the_file_completes_when_every_part_is(self) -> None:
        rig, row = await _rig()
        driven = Driven()
        rollup = _rollup(rig, driven)

        _finish(rig, row, 0, "COMPLETED")
        assert await rollup.recount(V) is Outcome.WRITTEN
        mid = await rig.store.get(V)
        assert (mid.done, mid.failed, mid.state, mid.epoch) == (1, 0, SplitState.INDEXING_PARTS, 2)
        assert driven.rows == []

        _finish(rig, row, 1, "EMPTY")
        _finish(rig, row, 2, "COMPLETED")
        assert await rollup.recount(V) is Outcome.WRITTEN
        end = await rig.store.get(V)
        assert (end.done, end.failed, end.state) == (3, 0, SplitState.COMPLETED)
        assert driven.rows == [], "the last part's indexed write drives nothing: its own stages have not ended"

        _stages(rig, row, 0, 2)
        assert await rollup.drive(V) is True
        assert len(driven.rows) == 1, "the parent's stages are driven once the parts' own stages have ended"

    async def test_the_file_becomes_searchable_once_when_its_roll_up_completes(self) -> None:
        rig, row = await _rig()
        seen: list[str] = []

        async def searchable(r: SplitRow) -> None:
            seen.append(r.v)

        rollup = _rollup(rig, on_searchable=searchable)
        _finish(rig, row, 0, "COMPLETED")
        await rollup.recount(V)
        assert seen == [], "a file with parts still running is not searchable yet"
        _finish(rig, row, 1, "COMPLETED")
        _finish(rig, row, 2, "COMPLETED")
        await rollup.recount(V)
        await rollup.recount(V)
        assert seen == [V]

    async def test_a_failing_notification_does_not_undo_the_roll_up(self) -> None:
        rig, row = await _rig()

        async def broken(_r: SplitRow) -> None:
            raise RuntimeError("cache down")

        for k in range(3):
            _finish(rig, row, k, "COMPLETED")
        assert await _rollup(rig, on_searchable=broken).recount(V) is Outcome.WRITTEN
        assert (await rig.store.get(V)).state is SplitState.COMPLETED

    async def test_a_failed_part_makes_the_file_partial_and_all_failed_makes_it_failed(self) -> None:
        rig, row = await _rig()
        rollup = _rollup(rig)
        _finish(rig, row, 0, "COMPLETED")
        _finish(rig, row, 1, "FAILED")
        _finish(rig, row, 2, "PASSWORD_PROTECTED")
        await rollup.recount(V)
        partial = await rig.store.get(V)
        assert (partial.done, partial.failed, partial.state) == (1, 2, SplitState.PARTIAL)

        _finish(rig, row, 0, "FILE_TYPE_NOT_SUPPORTED")
        await rollup.recount(V)
        assert (await rig.store.get(V)).state is SplitState.FAILED

    async def test_waiting_statuses_never_count(self) -> None:
        rig, row = await _rig()
        for k, status in enumerate(("AUTO_INDEX_OFF", "PAUSED", "QUEUED")):
            _finish(rig, row, k, status)
        assert await _rollup(rig).recount(V) is Outcome.UNCHANGED
        stored = await rig.store.get(V)
        assert (stored.done, stored.failed, stored.state) == (0, 0, SplitState.INDEXING_PARTS)

    async def test_an_unchanged_recount_writes_nothing(self) -> None:
        rig, row = await _rig()
        rollup = _rollup(rig)
        _finish(rig, row, 0, "COMPLETED")
        await rollup.recount(V)
        epoch = (await rig.store.get(V)).epoch
        assert await rollup.recount(V) is Outcome.UNCHANGED
        assert (await rig.store.get(V)).epoch == epoch

    async def test_a_part_of_another_generation_or_a_purging_file_changes_nothing(self) -> None:
        rig, row = await _rig()
        rollup = _rollup(rig)
        _finish(rig, row, 0, "COMPLETED")
        assert await rollup.recount(V, gen="an-older-generation") is Outcome.STALE
        assert (await rig.store.get(V)).done == 0
        await rig.store.move(V, row.gen, SplitState.INDEXING_PARTS, SplitState.PURGING)
        assert await rollup.recount(V, gen=row.gen) is Outcome.STALE
        assert await rollup.recount("no-such-file") is Outcome.STALE

    async def test_the_state_stays_splitting_until_the_parent_has_queued_every_part(self) -> None:
        rig, row = await _rig()
        await rig.store.move(V, row.gen, SplitState.INDEXING_PARTS, SplitState.SPLITTING)
        _finish(rig, row, 0, "COMPLETED")
        await _rollup(rig).recount(V)
        stored = await rig.store.get(V)
        assert (stored.done, stored.state) == (1, SplitState.SPLITTING)

    async def test_a_retry_that_completes_a_part_drives_the_parent_again(self) -> None:
        rig, row = await _rig()
        driven = Driven()
        rollup = _rollup(rig, driven)
        for k, status in enumerate(("COMPLETED", "FAILED", "COMPLETED")):
            _finish(rig, row, k, status)
        _stages(rig, row, 0, 2)
        await rollup.recount(V)
        assert (await rig.store.get(V)).state is SplitState.PARTIAL
        assert len(driven.rows) == 1, "a failed part runs no stages, so it is not waited for"

        assert await rollup.reopen(V, row.gen)
        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS
        _finish(rig, row, 1, "COMPLETED")
        await rollup.recount(V)
        assert (await rig.store.get(V)).state is SplitState.COMPLETED
        assert len(driven.rows) == 1, "the retried part has not ended its stages yet"
        _stages(rig, row, 1)
        assert await rollup.on_part_stage_settled(row.parts[1].vrid) is True
        assert len(driven.rows) == 2, "its stage completion re-drives classify and entities"

    async def test_a_drive_that_fails_gives_its_claim_back_for_the_next_trigger(self) -> None:
        rig, row = await _rig()

        async def broken(_row: SplitRow) -> None:
            raise RuntimeError("broker down")

        rollup = _rollup(rig, broken)
        for k in range(3):
            _finish(rig, row, k, "COMPLETED")
        _stages(rig, row, 0, 1, 2)
        assert await rollup.recount(V) is Outcome.WRITTEN, "the failed drive does not undo or raise out of the count"
        stored = await rig.store.get(V)
        assert stored.state is SplitState.COMPLETED and stored.redrive_token == "", "the claim was given back"

        driven = Driven()
        assert await _rollup(rig, driven).drive(V) is True
        assert len(driven.rows) == 1


@pytest.mark.asyncio
class TestConcurrency:
    """35 parts end at once; each end triggers a roll-up that other roll-ups interleave with."""

    @staticmethod
    async def _run(seed: int, tries: int) -> tuple[Rig, SplitRow, list[Outcome]]:
        rng = random.Random(seed)
        rig, row = await _rig(MANY, graph=Racy(rng))
        rollup = _rollup(rig, tries=tries)
        statuses = [rng.choice(["COMPLETED", "COMPLETED", "COMPLETED", "FAILED", "EMPTY"]) for _ in row.parts]
        outcomes: list[Outcome] = []

        async def end(k: int) -> None:
            for _ in range(rng.randint(0, 6)):
                await asyncio.sleep(0)
            _finish(rig, row, k, statuses[k])
            info = await rig.store.part(row.parts[k].node_key)
            outcomes.append(await rollup.on_part_terminal(info))

        await asyncio.gather(*(end(k) for k in range(len(row.parts))))
        return rig, row, outcomes

    @staticmethod
    async def _truth(rig: Rig, row: SplitRow) -> tuple[int, int]:
        statuses = [rig.graph.docs["records"][p.node_key]["indexingStatus"] for p in row.parts]
        return sum(s in DONE_STATUSES for s in statuses), sum(s in FAILED_STATUSES for s in statuses)

    @pytest.mark.parametrize("seed", range(40))
    async def test_every_run_ends_with_the_stored_state_equal_to_the_recount(self, seed: int) -> None:
        rig, row, outcomes = await self._run(seed, tries=split_rollup.MAX_TRIES)
        done, failed = await self._truth(rig, row)
        if Outcome.GAVE_UP in outcomes:
            # A roll-up that ran out of tries leaves its part to the heal sweep: one more recount.
            await _rollup(rig).recount(V)
        stored = await rig.store.get(V)
        assert (stored.done, stored.failed) == (done, failed)
        assert stored.state is (SplitState.COMPLETED if not failed else SplitState.PARTIAL if done else SplitState.FAILED)

    @pytest.mark.parametrize("seed", range(10))
    async def test_with_enough_tries_no_heal_is_ever_needed(self, seed: int) -> None:
        rig, row, outcomes = await self._run(seed, tries=500)
        done, failed = await self._truth(rig, row)
        assert Outcome.GAVE_UP not in outcomes
        stored = await rig.store.get(V)
        assert (stored.done, stored.failed) == (done, failed)

    async def test_a_roll_up_that_recounted_early_cannot_overwrite_a_later_one(self) -> None:
        """A reads one part done and stalls; the rest finish and B writes the full count; A must not land its stale one."""
        rig, row = await _rig()
        _finish(rig, row, 0, "COMPLETED")
        stalled, resume = asyncio.Event(), asyncio.Event()
        real = rig.graph.get_nodes_by_field_in
        calls = 0

        async def stall_the_first_recount(*args: Any, **kw: Any) -> list[dict[str, Any]]:  # noqa: ANN401
            nonlocal calls
            calls += 1
            found = await real(*args, **kw)
            if calls == 1:
                stalled.set()
                await resume.wait()
            return found

        rig.graph.get_nodes_by_field_in = stall_the_first_recount  # type: ignore[method-assign]
        rollup = _rollup(rig)
        early = asyncio.create_task(rollup.recount(V))
        await stalled.wait()
        _finish(rig, row, 1, "COMPLETED")
        _finish(rig, row, 2, "COMPLETED")
        assert await rollup.recount(V) is Outcome.WRITTEN
        resume.set()
        await early

        final = await rig.store.get(V)
        assert (final.done, final.state) == (3, SplitState.COMPLETED)

    async def test_a_roll_up_that_read_the_row_before_a_purge_cannot_overwrite_purging(self) -> None:
        """A state move takes the next epoch and a roll-up writes only against the state it read, so both orders fail the
        stale writer: a purge's ``PURGING`` is never put back to ``COMPLETED`` by a recount that read before it."""
        rig, row = await _rig()
        for k in range(3):
            _finish(rig, row, k, "COMPLETED")
        stale = await rig.store.get(V)
        assert stale is not None
        assert await rig.store.move(V, row.gen, (SplitState.SPLITTING, SplitState.INDEXING_PARTS), SplitState.PURGING)

        assert await rig.store.roll_up(stale, done=3, failed=0, state=SplitState.COMPLETED) is False

        held = await rig.store.get(V)
        assert held is not None and held.state is SplitState.PURGING and held.epoch == stale.epoch + 1

    async def test_a_roll_up_that_won_first_makes_a_purge_that_read_before_it_move_again(self) -> None:
        rig, row = await _rig()
        for k in range(3):
            _finish(rig, row, k, "COMPLETED")
        before = await rig.store.get(V)
        assert before is not None
        assert await rig.store.roll_up(before, done=3, failed=0, state=SplitState.COMPLETED)

        # the move reads the row it moves, so it lands on the roll-up's write instead of overwriting it blindly
        assert await rig.store.move(V, row.gen, (SplitState.INDEXING_PARTS, SplitState.COMPLETED), SplitState.PURGING)
        held = await rig.store.get(V)
        assert held is not None and held.state is SplitState.PURGING and held.epoch == before.epoch + 2

    async def test_without_the_epoch_compare_the_same_script_loses_the_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def last_writer_wins(self: SplitStore, row: SplitRow, *, done: int, failed: int, state: SplitState) -> bool:
            return await self._graph.update_node_fields_if_match(
                row.v, "contentSplits", {"done": done, "failed": failed, "state": state.value, "epoch": row.epoch + 1},
                {"gen": row.gen},
            )

        monkeypatch.setattr(SplitStore, "roll_up", last_writer_wins)
        with pytest.raises(AssertionError):
            await self.test_a_roll_up_that_recounted_early_cannot_overwrite_a_later_one()


@pytest.mark.asyncio
class TestHook:
    async def test_the_hook_ignores_what_is_not_a_part_or_not_terminal_and_never_raises(self) -> None:
        rig, row = await _rig()
        seen: list[str] = []

        class Spy:
            async def on_part_terminal(self, info: Any) -> None:  # noqa: ANN401
                seen.append(info.key)

        use_rollup(Spy())  # type: ignore[arg-type]
        try:
            part = rig.parts()[0]
            await part_terminal(part, "QUEUED")
            await part_terminal({"_key": "x", "connectorId": OWNER}, "COMPLETED")
            assert seen == []
            await part_terminal(part, ProgressStatus.COMPLETED)
            await part_terminal(part, "FAILED")
            assert seen == [part["_key"]] * 2

            class Boom:
                async def on_part_terminal(self, info: Any) -> None:  # noqa: ANN401
                    raise RuntimeError("store down")

            use_rollup(Boom())  # type: ignore[arg-type]
            await part_terminal(part, "COMPLETED")
        finally:
            use_rollup(None)

    async def test_the_hook_is_what_the_cached_part_document_resolves(self) -> None:
        rig, _row = await _rig()
        info = await part_info(rig.parts()[0], rig.graph)
        assert info is not None
        assert (info.root, info.k) == (V, 1)


class TestEveryTerminalWriterCallsTheHook:
    """The writers of a terminal ``indexingStatus`` (design §2.8). A new one must call ``part_terminal`` or be listed
    here with the reason it does not count."""

    CALLERS = {
        "modules/transformers/sink_orchestrator.py": 1,  # _update_indexing_status: COMPLETED
        "modules/transformers/pipeline.py": 1,  # EMPTY of an empty parse
        "events/events.py": 2,  # mark_record_status, the in-process EMPTY write
        "events/processor.py": 1,  # _mark_record: the legacy pipeline's EMPTY, FAILED, FILE_TYPE_NOT_SUPPORTED
        "services/messaging/kafka/handlers/record.py": 1,  # __update_document_status: handler FAILED, finally, dead letter, recovery, rate limit
    }

    @pytest.mark.parametrize("path", sorted(CALLERS))
    def test_each_writer_file_calls_part_terminal_where_it_writes(self, path: str) -> None:
        source = (APP / path).read_text()
        assert len(re.findall(r"await part_terminal\(", source)) == self.CALLERS[path]

    def test_no_other_module_writes_a_terminal_indexing_status_without_listing_itself(self) -> None:
        allowed = {
            # Stage and sweep writers that do not end a part's own indexing, or that act on a record that cannot be a part.
            "modules/pipeline", "modules/indexing/split_", "modules/indexing/record_republish.py",
            "modules/indexing/parse_now_marker.py", "modules/indexing/connector_off_events.py",
            "modules/indexing/duplicate_reconcile.py", "modules/indexing/class_units_backfill.py",
            "modules/indexing/embed_resume.py", "events/dedup.py", "migrations/", "scripts/",
        }
        pattern = re.compile(r'"indexingStatus"\s*:\s*ProgressStatus\.(?:COMPLETED|EMPTY|FAILED|PASSWORD_PROTECTED|FILE_TYPE_NOT_SUPPORTED)')
        found = {
            str(p.relative_to(APP)) for p in APP.rglob("*.py")
            if "connectors/sources" not in str(p) and pattern.search(p.read_text())
        } - set(self.CALLERS)
        unexplained = {f for f in found if not any(f.startswith(a) for a in allowed)}
        assert not unexplained, f"writes a terminal indexingStatus without calling part_terminal: {sorted(unexplained)}"


@pytest.mark.asyncio
class TestHealSweep:
    async def test_the_sweep_recounts_a_file_no_roll_up_has_written_for_two_minutes(self) -> None:
        rig, row = await _rig()
        driven = Driven()
        rig.sweep._rollup = _rollup(rig, driven)
        for k in range(3):
            _finish(rig, row, k, "COMPLETED")
        _stages(rig, row, 0, 1, 2)

        seam_tests.NOW[0] += 60_000
        assert (await rig.sweep.run_once()).healed == 0, "a file that rolled up a minute ago is left alone"
        seam_tests.NOW[0] += 61_000
        report = await rig.sweep.run_once()

        assert report.healed == 1
        assert (await rig.store.get(V)).state is SplitState.COMPLETED
        assert len(driven.rows) == 1
        assert ORG


@pytest.mark.asyncio
class TestRetryOfAFile:
    async def test_only_the_failed_parts_are_republished_and_the_file_is_indexing_again(self) -> None:
        from app.modules.indexing.split_reindex import retry_failed_parts_of

        rig, row = await _rig()
        driven = Driven()
        rollup = _rollup(rig, driven)
        for k, status in enumerate(("COMPLETED", "FAILED", "PASSWORD_PROTECTED")):
            _finish(rig, row, k, status)
        _stages(rig, row, 0)
        await rollup.recount(V)
        assert (await rig.store.get(V)).state is SplitState.PARTIAL
        sent_before = len(rig.publisher.sent)

        queued = await retry_failed_parts_of(rig.graph, rig.store, rig.publisher, rollup, V, owner_connector_id=OWNER)

        assert queued == 2
        sent = rig.publisher.sent[sent_before:]
        assert [key for _t, _e, _p, key in sent] == [row.parts[1].node_key, row.parts[2].node_key]
        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS
        assert [rig.graph.docs["records"][p.node_key]["indexingStatus"] for p in row.parts] == ["COMPLETED", "QUEUED", "QUEUED"]

        _finish(rig, row, 1, "COMPLETED")
        _finish(rig, row, 2, "COMPLETED")
        _stages(rig, row, 1, 2)
        await rollup.recount(V)
        assert (await rig.store.get(V)).state is SplitState.COMPLETED
        assert len(driven.rows) == 2

    async def test_a_file_with_no_live_generation_has_nothing_to_retry(self) -> None:
        from app.modules.indexing.split_reindex import retry_failed_parts_of

        rig, _row = await _rig()
        assert await retry_failed_parts_of(rig.graph, rig.store, rig.publisher, _rollup(rig), "no-such-file", owner_connector_id=OWNER) == 0


@pytest.mark.asyncio
async def test_the_process_wiring_builds_the_roll_up_the_writers_reach_and_the_sweep_heals_with_it() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from app.modules.indexing.split_wiring import build_split_runtime
    from tests.support.split_fakes import FakeLeases

    rig, row = await _rig()
    records = rig.graph.docs["records"]
    rig.graph.get_records_by_virtual_record_id = AsyncMock(  # type: ignore[method-assign]
        side_effect=lambda vrid, **_kw: [k for k, d in records.items() if d.get("virtualRecordId") == vrid]
    )
    ingress = MagicMock()
    ingress.redrive = AsyncMock(return_value=["job"])
    runtime = build_split_runtime(
        graph=rig.graph, pipeline=MagicMock(), blob_storage=MagicMock(), leases=FakeLeases(), publisher=rig.publisher,
        lease_seconds=120.0, ingress=ingress,
    )
    try:
        for k in range(3):
            _finish(rig, row, k, "COMPLETED")
        _stages(rig, row, 0, 1, 2)
        await part_terminal(rig.parts()[2], "COMPLETED")
        assert (await runtime.store.get(V)).state is SplitState.COMPLETED
        ingress.redrive.assert_awaited_once()
        assert ingress.redrive.await_args.args[2] == ("classify", "entities")
        assert ingress.redrive.await_args.kwargs == {"force": False}
        assert runtime.sweep._rollup is runtime.rollup
    finally:
        use_rollup(None)
        use_live_generation(None)
