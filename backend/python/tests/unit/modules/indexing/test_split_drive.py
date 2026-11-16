"""When the parent of a split file runs its own ``classify`` and ``entities``.

The parent's stages read what its parts' stages produced, so they run once every part of the live generation has ended
its own ``classify`` and ``entities`` (a part that failed or is empty runs none, so it is not waited for). The trigger is
a part's stage completion (the stage worker's settle path), the recount that settles the file, and the heal sweep; never
the last part's indexed write alone. A fake stage runtime below settles the parts' stages in the order the live stack
showed (the indexed writes of all parts first, the LLM stages of the last part about ten seconds later), and the parent's
stages are modelled with the run-time input digest, so a repeated drive that changes nothing costs no model call.
"""
from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING, Any

import pytest

from app.models.blocks import SemanticMetadata
from app.modules.indexing.split_inputs import classify_input, entities_input
from app.modules.indexing.split_reindex import retry_failed_parts_of
from app.modules.indexing.split_rollup import (
    Outcome,
    SplitRollup,
    part_output_replaced,
    part_stage_settled,
    part_terminal,
    use_rollup,
)
from app.modules.indexing.split_state import SplitRow, SplitState
from app.modules.named_entities.domain.models import NamedEntityExtraction
from app.modules.pipeline.models import Priority, StageJob, stage_state_key
from app.services.resource_governor.models import ParseTier
from tests.unit.modules.indexing.test_split_seam import NOW, ORG, OWNER, V
from tests.unit.modules.pipeline.test_split_stages import _entity, _Graph, _split_world

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class ParentModel:
    """The parent's two stages as the runtime runs them: each reads its input at run time and costs a model call only
    when the input's digest is not the one it last completed with (the stored fingerprint)."""

    def __init__(self, graph: _Graph) -> None:
        self.graph = graph
        self.drives = 0
        self.model_calls = 0
        self.last: dict[str, str] = {}
        self.inputs: list[tuple[str, str]] = []

    async def __call__(self, row: SplitRow) -> None:
        self.drives += 1
        classify = await classify_input(self.graph, row, "big.pdf")
        entities = await entities_input(self.graph, row)
        for stage, digest, seen in (("classify", classify.digest, classify.text), ("entities", entities.digest, "")):
            if self.last.get(stage) != digest:
                self.last[stage] = digest
                self.model_calls += 1
                self.inputs.append((stage, seen))


class StageRuntime:
    """What the stage worker does to a part's stage: the state is terminal first, then its settle hook runs."""

    def __init__(self, graph: _Graph, row: SplitRow) -> None:
        self.graph, self.row = graph, row
        self.attempts: dict[str, int] = {}

    def settle_state(self, k: int, stage: str, *, status: str = "COMPLETED", summary: str | None = None, mentions: int = 1) -> None:
        spec = self.row.parts[k - 1]
        key = stage_state_key(spec.vrid, f"rev-{k}", stage)
        self.attempts[key] = self.attempts.get(key, 0) + 1
        output = (
            SemanticMetadata(summary=summary or f"summary of part {k}").model_dump_json() if stage == "classify"
            else NamedEntityExtraction(entities=[_entity(f"person-{k}", *range(mentions))]).model_dump_json()
        )
        self.graph.states[key] = {"status": status, "attempt": self.attempts[key], "output": output}

    def job(self, k: int, stage: str) -> StageJob:
        return StageJob.model_validate({
            "stage": stage, "stage_version": 1, "org_id": ORG, "virtual_record_id": self.row.parts[k - 1].vrid, "rev": f"rev-{k}",
            "record_ids": (self.row.parts[k - 1].node_key,), "connector_id": OWNER, "tier": ParseTier.LIGHT,
            "priority": Priority.BULK, "trigger": "embed", "text_digest": "t", "blocks_digest": "b", "text_chars": 10,
            "has_tables": False, "has_images": False,
        })

    async def finish(self, k: int, stage: str, **kw: Any) -> None:  # noqa: ANN401
        self.settle_state(k, stage, **kw)
        await part_stage_settled(self.job(k, stage))


class World:
    def __init__(self, rig: Any, graph: _Graph, row: SplitRow) -> None:  # noqa: ANN401
        self.rig, self.graph, self.row = rig, graph, row
        self.model = ParentModel(graph)

        async def no_wait(_s: float) -> None:
            await asyncio.sleep(0)

        self.rollup = SplitRollup(graph, rig.store, self.model, sleep=no_wait, jitter=lambda: 0.0, clock_ms=lambda: NOW[0])
        self.runtime = StageRuntime(graph, row)
        rig.sweep._rollup = self.rollup
        use_rollup(self.rollup)

    def index(self, k: int, status: str = "COMPLETED") -> dict[str, Any]:
        record = self.graph.docs["records"][self.row.parts[k - 1].node_key]
        record["indexingStatus"] = status
        return record

    async def indexed(self, k: int, status: str = "COMPLETED") -> None:
        await part_terminal(self.index(k, status), status)

    async def state(self) -> SplitRow:
        row = await self.rig.store.get(V)
        assert row is not None
        return row


@pytest.fixture
async def world() -> AsyncIterator[World]:
    rig, graph = await _split_world()
    built = World(rig, graph, await rig.store.get(V))
    yield built
    use_rollup(None)


@pytest.mark.asyncio
class TestTheLiveOrder:
    async def test_the_parents_stages_wait_for_the_last_parts_own_stages_and_then_run_once(self, world: World) -> None:
        w = world
        # The live order: every part is indexed within a second, then the LLM stages come back one by one.
        for k in (1, 2, 3):
            await w.indexed(k)
        assert (await w.state()).state is SplitState.COMPLETED
        assert w.model.drives == 0, "the last part's indexed write must not drive the parent"

        await w.runtime.finish(1, "entities")
        await w.runtime.finish(1, "classify")
        await w.runtime.finish(2, "entities")
        await w.runtime.finish(3, "entities")
        await w.runtime.finish(2, "classify")
        assert w.model.drives == 0, "part 3's classify is still running"

        await w.runtime.finish(3, "classify")
        assert (w.model.drives, w.model.model_calls) == (1, 2)
        classify_text = dict(w.model.inputs)["classify"]
        assert all(f"summary of part {k}" in classify_text for k in (1, 2, 3)), "the last part's summary is in the input"
        entities = await entities_input(w.graph, await w.state())
        assert sorted(e.norm_key for e in entities.extraction.entities) == ["person-1", "person-2", "person-3"]

    async def test_a_completed_extraction_of_a_part_drives_the_finished_file_again(self, world: World) -> None:
        w = world
        for k in (1, 2, 3):
            await w.indexed(k)
            await w.runtime.finish(k, "entities")
            await w.runtime.finish(k, "classify")
        assert (w.model.drives, w.model.model_calls) == (1, 2)
        assert not await w.rollup.drive(V), "the parts ended as they had: nothing to drive"

        # The re-extraction of part 2 finished what it left unread and replaced its kept output (the token of how the parts
        # ended, which names statuses and attempts and not outputs, does not change).
        w.runtime.settle_state(2, "entities", mentions=3)
        w.runtime.attempts.clear()
        await part_output_replaced(w.row.parts[1].vrid)

        assert (w.model.drives, w.model.model_calls) == (2, 3), "the file's entities merge ran again, with the new input"
        assert (await w.state()).redrive_at_ms == 0

    async def test_a_part_that_is_not_in_a_live_split_drives_nothing(self, world: World) -> None:
        await part_output_replaced("unrelated-vrid")
        await part_output_replaced(None)
        assert world.model.drives == 0

    async def test_a_part_whose_stage_failed_still_counts_as_ended(self, world: World) -> None:
        w = world
        for k in (1, 2, 3):
            await w.indexed(k)
        for k in (1, 2):
            await w.runtime.finish(k, "entities")
            await w.runtime.finish(k, "classify")
        await w.runtime.finish(3, "entities", status="FAILED")
        assert w.model.drives == 0
        await w.runtime.finish(3, "classify", status="SKIPPED")
        assert w.model.drives == 1

    async def test_a_failed_part_runs_no_stages_and_is_not_waited_for(self, world: World) -> None:
        w = world
        await w.indexed(1)
        await w.indexed(2, "FAILED")
        await w.runtime.finish(1, "entities")
        await w.runtime.finish(1, "classify")
        assert w.model.drives == 0, "part 3 has not even been indexed"
        await w.indexed(3, "EMPTY")
        assert (await w.state()).state is SplitState.PARTIAL
        assert w.model.drives == 1, "the last part to end was one that runs no stage: its own end drives"

    async def test_a_part_still_indexing_blocks_the_drive_even_when_every_other_stage_ended(self, world: World) -> None:
        w = world
        await w.indexed(1)
        await w.indexed(2)
        for k in (1, 2):
            await w.runtime.finish(k, "entities")
            await w.runtime.finish(k, "classify")
        assert w.model.drives == 0
        await w.indexed(3)
        await w.runtime.finish(3, "entities")
        await w.runtime.finish(3, "classify")
        assert w.model.drives == 1

    async def test_the_hook_ignores_other_stages_ordinary_files_and_parts_of_a_replaced_generation(self, world: World) -> None:
        w = world
        for k in (1, 2, 3):
            await w.indexed(k)
            await w.runtime.finish(k, "entities")
        before = w.model.drives
        ordinary = w.runtime.job(3, "classify").model_copy(update={"connector_id": "conn-1"})
        await part_stage_settled(ordinary)
        other_stage = w.runtime.job(3, "summary-embed") if False else w.runtime.job(3, "classify").model_copy(update={"stage": "embed"})
        await part_stage_settled(other_stage)
        assert w.model.drives == before == 0


@pytest.mark.asyncio
class TestRepeatsCostNothing:
    async def test_duplicate_and_concurrent_triggers_drive_once_and_make_no_extra_model_call(self, world: World) -> None:
        w = world
        for k in (1, 2, 3):
            await w.indexed(k)
        for k in (1, 2, 3):
            w.runtime.settle_state(k, "entities")
            w.runtime.settle_state(k, "classify")
        # Redelivered hooks, the settle hook of every stage at once, a heal pass, the recount of every part.
        await asyncio.gather(
            *(part_stage_settled(w.runtime.job(k, s)) for k in (1, 2, 3) for s in ("classify", "entities") for _ in range(3)),
            *(w.indexed(k) for k in (1, 2, 3)),
        )
        assert (w.model.drives, w.model.model_calls) == (1, 2)
        stored = await w.state()
        assert stored.redrive_token

        from tests.unit.modules.indexing import test_split_seam as seam_tests

        seam_tests.NOW[0] += 3 * 60_000
        report = await w.rig.sweep.run_once()
        assert report.healed == 0
        assert (w.model.drives, w.model.model_calls) == (1, 2), "the heal sweep leaves a driven file alone"

    async def test_a_drive_the_claim_lets_through_twice_still_costs_no_model_call(self, world: World) -> None:
        """Another process drives the same state (the claim is per process-visible state, but a second claimant
        after a released claim is possible): the run-time digest makes it free."""
        w = world
        for k in (1, 2, 3):
            await w.indexed(k)
            w.runtime.settle_state(k, "entities")
            w.runtime.settle_state(k, "classify")
        assert await w.rollup.drive(V) is True
        row = await w.state()
        await w.rig.store.roll_up(row, done=row.done, failed=row.failed, state=row.state, redriveToken="")
        assert await w.rollup.drive(V) is True
        assert (w.model.drives, w.model.model_calls) == (2, 2)

    async def test_a_part_that_runs_again_with_a_new_output_drives_the_parent_once_more(self, world: World) -> None:
        w = world
        for k in (1, 2, 3):
            await w.indexed(k)
            w.runtime.settle_state(k, "entities")
            w.runtime.settle_state(k, "classify")
        assert await w.rollup.drive(V) is True
        await w.runtime.finish(2, "classify", summary="a better summary")
        assert (w.model.drives, w.model.model_calls) == (2, 3), "one drive, and only the classification read something new"
        await w.runtime.finish(2, "classify", summary="a better summary")
        assert w.model.drives == 3 and w.model.model_calls == 3, "an equal output costs nothing"


@pytest.mark.asyncio
class TestARetry:
    async def _settled_partial(self, w: World) -> None:
        await w.indexed(1)
        await w.indexed(2, "FAILED")
        await w.indexed(3)
        for k in (1, 3):
            await w.runtime.finish(k, "entities")
            await w.runtime.finish(k, "classify")
        assert (await w.state()).state is SplitState.PARTIAL and w.model.drives == 1

    async def test_a_retried_part_drives_the_parent_again_only_after_its_own_stages(self, world: World) -> None:
        w = world
        await self._settled_partial(w)
        queued = await retry_failed_parts_of(w.graph, w.rig.store, w.rig.publisher, w.rollup, V, owner_connector_id=OWNER)
        assert queued == 1 and (await w.state()).state is SplitState.INDEXING_PARTS
        await w.indexed(2)
        assert w.model.drives == 1
        await w.runtime.finish(2, "entities")
        assert w.model.drives == 1
        await w.runtime.finish(2, "classify")
        assert (w.model.drives, (await w.state()).state) == (2, SplitState.COMPLETED)

    async def test_with_the_stage_hook_lost_the_sweep_drives_the_retried_file_once(self, world: World) -> None:
        """A part fails, is retried and completes, and the hook that tells the roll-up its stages ended is lost: the
        retry cleared the file's drive claim, so the sweep drives the parent's stages again, exactly once."""
        from app.modules.indexing.split_rollup import HEAL_AFTER_MS

        w = world
        await self._settled_partial(w)
        assert w.model.drives == 1 and (await w.state()).redrive_token, "the first drive left its once-only mark"
        await retry_failed_parts_of(w.graph, w.rig.store, w.rig.publisher, w.rollup, V, owner_connector_id=OWNER)
        assert not (await w.state()).redrive_token, "reopening the file gives the claim back"
        await w.indexed(2)
        w.runtime.settle_state(2, "entities")  # the stages end, but nobody is told
        w.runtime.settle_state(2, "classify")
        NOW[0] += 2 * HEAL_AFTER_MS

        await w.rig.sweep.run_once()
        await w.rig.sweep.run_once()

        assert w.model.drives == 2, "driven again after the retry, and only once"
        assert (await w.state()).state is SplitState.COMPLETED

    async def test_a_stale_recount_during_a_retry_drives_nothing(self, world: World) -> None:
        """a trigger that read the settled file before the retry reopened it must not drive from that view."""
        w = world
        await self._settled_partial(w)
        stale = await w.state()
        # The trigger has read the row (settled, old epoch) ...
        assert stale.state is SplitState.PARTIAL
        # ... when the retry reopens the file and queues the part.
        await retry_failed_parts_of(w.graph, w.rig.store, w.rig.publisher, w.rollup, V, owner_connector_id=OWNER)
        assert await w.rig.store.roll_up(stale, done=stale.done, failed=stale.failed, state=stale.state, redriveToken="x") is False
        assert w.model.drives == 1
        for _ in range(3):
            assert await w.rollup.drive(V) is False, "a QUEUED part is not ended"
        assert w.model.drives == 1

    async def test_a_retry_does_not_overwrite_a_part_that_already_ended(self, world: World) -> None:
        """the retry's status write is a compare-and-set FAILED -> QUEUED made before the event goes out."""
        w = world
        await self._settled_partial(w)
        key = w.row.parts[1].node_key
        sent_when: list[str] = []
        original = w.rig.publisher.send_event

        async def quick_consumer(topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> bool:
            sent = await original(topic, event_type, payload, key=key)
            # The consumer ends the part (a deterministic quick failure) the moment the event is out.
            sent_when.append(w.graph.docs["records"][w.row.parts[1].node_key]["indexingStatus"])
            w.graph.docs["records"][w.row.parts[1].node_key]["indexingStatus"] = "PASSWORD_PROTECTED"
            return sent

        w.rig.publisher.send_event = quick_consumer  # type: ignore[method-assign]
        await retry_failed_parts_of(w.graph, w.rig.store, w.rig.publisher, w.rollup, V, owner_connector_id=OWNER)
        assert sent_when == ["QUEUED"], "the part is QUEUED before the event is published"
        assert w.graph.docs["records"][key]["indexingStatus"] == "PASSWORD_PROTECTED"
        await w.rollup.recount(V)
        assert (await w.state()).state is SplitState.PARTIAL, "the file settles again"

    async def test_a_part_that_moved_on_before_the_retry_claimed_it_is_not_queued_or_published(self, world: World) -> None:
        w = world
        await self._settled_partial(w)
        sent_before = len(w.rig.publisher.sent)
        original_read = w.graph.get_nodes_by_field_in

        async def read_then_complete(collection: str, field: str, values: list[Any], *a: object, **kw: object) -> list[dict[str, Any]]:
            docs = await original_read(collection, field, values, *a, **kw)
            if collection == "records" and w.row.parts[1].vrid in values:
                w.graph.docs["records"][w.row.parts[1].node_key]["indexingStatus"] = "COMPLETED"  # another retry finished it meanwhile
            return docs

        w.graph.get_nodes_by_field_in = read_then_complete  # type: ignore[method-assign]
        assert await retry_failed_parts_of(w.graph, w.rig.store, w.rig.publisher, w.rollup, V, owner_connector_id=OWNER) == 0
        assert len(w.rig.publisher.sent) == sent_before
        assert w.graph.docs["records"][w.row.parts[1].node_key]["indexingStatus"] == "COMPLETED"

    async def test_a_part_the_broker_refuses_goes_back_to_failed_with_its_reason(self, world: World) -> None:
        w = world
        await self._settled_partial(w)
        record = w.graph.docs["records"][w.row.parts[1].node_key]
        record["reason"] = "page 3 unreadable"

        async def refuse(*_a: object, **_k: object) -> bool:
            return False

        w.rig.publisher.send_event = refuse  # type: ignore[method-assign]
        assert await retry_failed_parts_of(w.graph, w.rig.store, w.rig.publisher, w.rollup, V, owner_connector_id=OWNER) == 0
        assert (record["indexingStatus"], record["reason"]) == ("FAILED", "page 3 unreadable")


class Racy(_Graph):
    def __init__(self, rng: random.Random) -> None:
        super().__init__()
        self.rng = rng

    async def _race(self) -> None:
        for _ in range(self.rng.randint(0, 6)):
            await asyncio.sleep(0)

    async def get_document(self, key: str, collection: str, **kw: object) -> dict[str, Any] | None:
        await self._race()
        return await super().get_document(key, collection, **kw)

    async def update_node_fields_if_match(self, *args: Any, **kw: Any) -> bool:  # noqa: ANN401
        await self._race()
        return await super().update_node_fields_if_match(*args, **kw)

    async def get_nodes_by_field_in(self, *args: Any, **kw: Any) -> list[dict[str, Any]]:  # noqa: ANN401
        await self._race()
        return await super().get_nodes_by_field_in(*args, **kw)

    async def stage_state_get(self, key: str) -> dict[str, Any] | None:
        await self._race()
        return await super().stage_state_get(key)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(20))
async def test_at_most_one_drive_per_transition_however_the_writers_interleave(seed: int) -> None:
    """35 parts end in a seeded interleaving, each followed by its two stages' hooks, a heal pass in between."""
    from tests.unit.modules.indexing import test_split_seam as seam_tests
    from tests.unit.modules.indexing.test_split_seam import Rig

    rng = random.Random(seed)
    seam_tests.NOW[0] = 1_000_000
    from app.modules.indexing.split_state import clear_part_cache

    clear_part_cache()
    ranges = [(1 + 10 * i, 10 * (i + 1), 10 * (i + 1) + 1) for i in range(35)]
    rig = Rig(ranges)
    graph = Racy(rng)
    graph.docs = rig.graph.docs
    rig.graph = graph
    from app.modules.indexing.split_state import SplitStore

    rig.store = SplitStore(graph, clock_ms=lambda: seam_tests.NOW[0])
    row = (await rig.seam.split(rig.request())).row
    for spec in row.parts:
        graph.docs["records"][spec.node_key]["contentRev"] = f"rev-{spec.k}"
    w = World(rig, graph, row)

    async def one_part(k: int) -> None:
        for _ in range(rng.randint(0, 20)):
            await asyncio.sleep(0)
        await w.indexed(k)
        stages = ["classify", "entities"]
        rng.shuffle(stages)
        for stage in stages:
            for _ in range(rng.randint(0, 20)):
                await asyncio.sleep(0)
            await w.runtime.finish(k, stage)

    try:
        await asyncio.gather(*(one_part(k) for k in range(1, 36)), w.rollup.recount(V), w.rollup.drive(V))
        await w.rollup.recount(V)
        assert (await w.state()).state is SplitState.COMPLETED
        assert w.model.model_calls == 2
        assert w.model.drives == 1, f"seed {seed}: one drive for one transition into all-ended, not one per writer"
    finally:
        use_rollup(None)


@pytest.mark.asyncio
async def test_a_failed_drive_gives_its_claim_back_and_the_heal_sweep_drives_the_file(world: World) -> None:
    w = world
    for k in (1, 2, 3):
        await w.indexed(k)
        w.runtime.settle_state(k, "entities")
        w.runtime.settle_state(k, "classify")
    broken = [True]
    real = w.rollup._redrive

    async def flaky(row: SplitRow) -> None:
        if broken[0]:
            raise RuntimeError("broker down")
        await real(row)

    w.rollup._redrive = flaky
    assert await w.rollup.drive(V) is False
    assert (await w.state()).redrive_token == "", "the claim was given back"
    broken[0] = False

    from tests.unit.modules.indexing import test_split_seam as seam_tests

    seam_tests.NOW[0] += 3 * 60_000
    await w.rig.sweep.run_once()
    assert (w.model.drives, (await w.state()).redrive_token != "") == (1, True)


@pytest.mark.asyncio
async def test_the_recount_that_settles_a_file_whose_parts_have_ended_their_stages_drives_it(world: World) -> None:
    """A trigger lost (the stage hook never ran): the part's terminal write or the heal recount settles the file and
    finds every stage ended, so it drives."""
    w = world
    for k in (1, 2, 3):
        w.index(k)
        w.runtime.settle_state(k, "entities")
        w.runtime.settle_state(k, "classify")
    assert await w.rollup.recount(V, gen=w.row.gen) is Outcome.WRITTEN
    assert w.model.drives == 1


@pytest.mark.asyncio
async def test_a_drive_claim_left_by_a_process_that_died_is_reclaimed_after_the_bound_and_driven_once(world: World) -> None:
    """A crash between the claim and the end of the drive leaves the token set (a process death is not an exception, so
    nothing gives the claim back). The heal sweep waits out the bound, then drives the file, once."""
    w = world
    for k in (1, 2, 3):
        await w.indexed(k)
        w.runtime.settle_state(k, "entities")
        w.runtime.settle_state(k, "classify")
    real = w.rollup._redrive

    async def dies(_row: SplitRow) -> None:
        raise asyncio.CancelledError  # the process is gone: no release, no finish

    w.rollup._redrive = dies
    with pytest.raises(asyncio.CancelledError):
        await w.rollup.drive(V)
    w.rollup._redrive = real
    held = await w.state()
    assert held.redrive_token and held.redrive_at_ms == NOW[0], "the claim is on the row, with its time"
    assert w.model.drives == 0

    NOW[0] += 10 * 60_000
    await w.rig.sweep.run_once()
    assert w.model.drives == 0, "a claim younger than the bound may still belong to a live drive"

    NOW[0] += 25 * 60_000
    await w.rig.sweep.run_once()
    assert (w.model.drives, w.model.model_calls) == (1, 2)
    done = await w.state()
    assert done.redrive_token and done.redrive_at_ms == 0, "the reclaimed drive ended and said so"

    NOW[0] += 5 * 60 * 60_000
    await w.rig.sweep.run_once()
    await w.rig.sweep.run_once()
    assert w.model.drives == 1, "a finished drive is never reclaimed, however old"


@pytest.mark.asyncio
async def test_two_sweepers_reclaiming_the_same_stale_claim_drive_once(world: World) -> None:
    w = world
    for k in (1, 2, 3):
        await w.indexed(k)
        w.runtime.settle_state(k, "entities")
        w.runtime.settle_state(k, "classify")
    row = await w.state()
    assert await w.rig.store.roll_up(
        row, done=row.done, failed=row.failed, state=row.state, redriveToken="dead", redriveAtMs=NOW[0],
    )
    NOW[0] += 31 * 60_000
    stale = await w.state()
    results = await asyncio.gather(w.rollup.reclaim_stale(stale), w.rollup.reclaim_stale(stale))
    assert sorted(results) == [False, True]
    assert w.model.drives == 1


@pytest.mark.asyncio
async def test_a_redrive_that_dispatches_nothing_gives_its_claim_back_and_the_next_drive_runs(world: World) -> None:
    """The runtime holds no state for the revision, so ``redrive`` answers None: the drive did not happen, and keeping
    the claim would make the file look driven for ever."""
    from app.modules.indexing.split_inputs import ParentRedrive

    w = world
    answers: list[list[str] | None] = [None, ["j1", "j2"]]
    seen: list[tuple[str, tuple[str, ...]]] = []

    class Ingress:
        async def redrive(self, v: str | None, rev: str | None, stages: Any, *, force: bool = True) -> list[str] | None:  # noqa: ANN401
            seen.append((v or "", tuple(stages)))
            return answers.pop(0)

    redrive = ParentRedrive(w.graph, lambda: Ingress())
    rollup = SplitRollup(w.graph, w.rig.store, redrive, sleep=lambda _s: asyncio.sleep(0), jitter=lambda: 0.0, clock_ms=lambda: NOW[0])
    use_rollup(rollup)
    await w.indexed(1)
    await w.indexed(2)
    w.index(3)
    for k in (1, 2, 3):
        w.runtime.settle_state(k, "entities")
        w.runtime.settle_state(k, "classify")

    await rollup.recount(V)  # settles the file and tries to drive it: the drive never raises, it gives the claim back
    assert len(seen) == 1 and not (await w.state()).redrive_token, "the claim went back"

    assert await rollup.drive(V) is True
    assert len(seen) == 2 and (await w.state()).redrive_token, "the second drive dispatched and kept its mark"
