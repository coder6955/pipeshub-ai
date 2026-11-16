# ruff: noqa: F811
"""D-27: an event for one split file (KB reindex, connector reindex_records) retries only its failed parts.

Before the fix the record-level "Retry failed parts" did nothing for a KB file (nonce "") and re-cut every part of a
connector file (forceReindex gives nonce = delivery id). The decision is one function the seam calls first.
"""
from __future__ import annotations

import dataclasses

import pytest

from app.modules.indexing.split_reindex import ReindexAction, single_file_reindex_action
from app.modules.indexing.split_state import SplitRow, SplitState, part_specs
from tests.unit.modules.indexing.test_split_seam import (  # noqa: F401  (fixture)
    Rig,
    V,
    rig,
)


async def _make_state(rig: Rig, statuses: tuple[str, str, str], state: SplitState) -> tuple[str, str]:
    outcome = await rig.seam.split(rig.request())
    parts = rig.parts()
    for part, status in zip(parts, statuses, strict=True):
        rig.graph.docs["records"][part["_key"]]["indexingStatus"] = status
    failed = sum(s in ("FAILED", "PASSWORD_PROTECTED") for s in statuses)
    row = await rig.store.get(V)
    assert await rig.store.roll_up(row, done=3 - failed, failed=failed, state=state)
    rig.publisher.sent.clear()
    return outcome.row.gen, parts[1]["_key"]


async def _partial(rig: Rig) -> tuple[str, str]:
    return await _make_state(rig, ("COMPLETED", "FAILED", "COMPLETED"), SplitState.PARTIAL)


@pytest.mark.asyncio
class TestSingleFileReindex:
    async def test_kb_file_retry_republishes_exactly_the_failed_part(self, rig: Rig) -> None:
        gen, failed_key = await _partial(rig)

        await rig.seam.split(rig.request())  # KB upload: newRecord without forceReindex, nonce ""

        assert [key for (_t, _e, _p, key) in rig.publisher.sent] == [failed_key]
        row = await rig.store.get(V)
        assert row.gen == gen
        assert row.state is SplitState.INDEXING_PARTS
        assert rig.graph.docs["records"][failed_key]["indexingStatus"] == "QUEUED"
        assert len(rig.cutter.calls) == 1, "no re-cut"

    async def test_connector_file_retry_republishes_only_the_failed_part_of_the_same_generation(self, rig: Rig) -> None:
        gen, failed_key = await _partial(rig)

        await rig.seam.split(rig.request(nonce="delivery-7"))  # reindex_records: forceReindex, nonce = delivery id

        row = await rig.store.get(V)
        assert row.gen == gen
        assert [key for (_t, _e, _p, key) in rig.publisher.sent] == [failed_key]
        assert len(rig.cutter.calls) == 1

    async def test_a_failed_file_retries_its_failed_parts(self, rig: Rig) -> None:
        gen, failed_key = await _make_state(rig, ("FAILED", "FAILED", "FAILED"), SplitState.FAILED)

        await rig.seam.split(rig.request(nonce="delivery-8"))

        assert len(rig.publisher.sent) == 3
        assert (await rig.store.get(V)).gen == gen

    async def test_a_completed_file_is_left_alone_without_force(self, rig: Rig) -> None:
        gen, _ = await _make_state(rig, ("COMPLETED", "COMPLETED", "COMPLETED"), SplitState.COMPLETED)

        outcome = await rig.seam.split(rig.request())

        assert outcome.reused_generation
        assert rig.publisher.sent == []
        row = await rig.store.get(V)
        assert (row.gen, row.state) == (gen, SplitState.COMPLETED)

    async def test_a_completed_file_gets_a_new_generation_on_force(self, rig: Rig) -> None:
        gen, _ = await _make_state(rig, ("COMPLETED", "COMPLETED", "COMPLETED"), SplitState.COMPLETED)

        await rig.seam.split(rig.request(nonce="delivery-9"))

        assert (await rig.store.get(V)).gen != gen

    async def test_a_completed_file_whose_content_changed_gets_a_new_generation(self, rig: Rig) -> None:
        gen, _ = await _make_state(rig, ("COMPLETED", "COMPLETED", "COMPLETED"), SplitState.COMPLETED)

        await rig.seam.split(rig.request(rev="rev-2"))

        assert (await rig.store.get(V)).gen != gen

    async def test_a_partial_file_whose_content_changed_is_cut_again(self, rig: Rig) -> None:
        gen, _ = await _partial(rig)

        await rig.seam.split(rig.request(rev="rev-2"))

        assert (await rig.store.get(V)).gen != gen

    async def test_an_event_without_a_nonce_continues_a_forced_generation_that_is_still_running(self, rig: Rig) -> None:
        """A stale-recovery republish or a connector update of the same content carries no nonce: it must not retire,
        purge and re-cut the generation a force reindex started."""
        forced = (await rig.seam.split(rig.request(nonce="delivery-1"))).row
        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS
        sent = len(rig.publisher.sent)

        outcome = await rig.seam.split(rig.request())

        assert outcome.reused_generation and outcome.row.gen == forced.gen
        assert (await rig.store.get(V)).gen == forced.gen
        assert len(rig.cutter.calls) == 1, "no re-cut"
        assert rig.resources.points_deleted == [] and rig.resources.pdfs_deleted == [], "nothing purged"
        assert len(rig.publisher.sent) == sent, "parts already queued are not queued again"

    async def test_a_new_forced_reindex_replaces_a_generation_that_is_still_running(self, rig: Rig) -> None:
        first = (await rig.seam.split(rig.request(nonce="delivery-1"))).row

        second = (await rig.seam.split(rig.request(nonce="delivery-2"))).row

        assert second.gen != first.gen
        assert len(rig.resources.points_deleted) == first.total

    async def test_the_same_forced_event_redelivered_continues_its_generation(self, rig: Rig) -> None:
        first = (await rig.seam.split(rig.request(nonce="delivery-1"))).row

        again = await rig.seam.split(rig.request(nonce="delivery-1"))

        assert again.reused_generation and again.row.gen == first.gen


def _row(state: SplitState, rev: str = "r", nonce: str = "") -> SplitRow:
    return SplitRow(
        v="v", org_id="o", gen="g", parts=part_specs("g", [(1, 500, 501)], ocr=None), state=state, content_rev=rev, nonce=nonce,
    )


@pytest.mark.parametrize(
    ("row", "nonce", "rev", "expected"),
    [
        (None, "", "r", ReindexAction.CONTINUE),
        (_row(SplitState.PARTIAL), "", "r", ReindexAction.RETRY_FAILED),
        (_row(SplitState.FAILED), "d1", "r", ReindexAction.RETRY_FAILED),
        (_row(SplitState.PARTIAL), "", "other", ReindexAction.CONTINUE),
        (_row(SplitState.COMPLETED), "", "r", ReindexAction.NOTHING),
        (_row(SplitState.COMPLETED, nonce="d1"), "d1", "r", ReindexAction.NOTHING),
        (_row(SplitState.COMPLETED), "d2", "r", ReindexAction.CONTINUE),
        (_row(SplitState.COMPLETED), "", "other", ReindexAction.CONTINUE),
        (_row(SplitState.SPLITTING), "", "r", ReindexAction.CONTINUE),
        (_row(SplitState.INDEXING_PARTS), "d3", "r", ReindexAction.CONTINUE),
        (_row(SplitState.PURGING), "", "r", ReindexAction.CONTINUE),
    ],
)
def test_the_decision_table(row: SplitRow | None, nonce: str, rev: str, expected: ReindexAction) -> None:
    assert single_file_reindex_action(row, content_rev=rev, nonce=nonce) is expected


@pytest.mark.asyncio
class TestRollForwardAfterARollback:
    """An older build indexes a split file whole (a rollback). On roll-forward the next index event must split it again
    instead of reading the COMPLETED row as "nothing to do"."""

    async def _completed(self, rig: Rig) -> str:
        gen, _ = await _make_state(rig, ("COMPLETED", "COMPLETED", "COMPLETED"), SplitState.COMPLETED)
        return gen

    async def test_a_file_indexed_long_after_its_row_completed_is_not_split_again(self, rig: Rig) -> None:
        """A restore, a ``vectorDbOnly`` re-embed and a vector rebuild write the parent's COMPLETED status with a new
        ``lastIndexTimestamp`` and leave the row alone: the timestamp says nothing about who wrote the points."""
        gen = await self._completed(rig)
        row = await rig.store.get(V)
        request = rig.request()
        parent = {**request.parent, "lastIndexTimestamp": row.updated_at_ms + 3_600_000}

        outcome = await rig.seam.split(dataclasses.replace(request, parent=parent))

        assert outcome.reused_generation and (await rig.store.get(V)).gen == gen
        assert len(rig.cutter.calls) == 1, "a healthy split file was cut again"

    async def test_a_file_whose_points_include_ones_outside_every_segment_is_split_again(self, rig: Rig) -> None:
        gen = await self._completed(rig)
        rig.resources.stray_points = True

        await rig.seam.split(rig.request())

        assert (await rig.store.get(V)).gen != gen

    async def test_an_unchanged_split_file_is_left_alone(self, rig: Rig) -> None:
        gen = await self._completed(rig)
        row = await rig.store.get(V)
        request = rig.request()
        # Indexed within the grace of the completion: the parent attempt's own manifest, not an older build.
        parent = {**request.parent, "lastIndexTimestamp": row.updated_at_ms + 1_000}

        outcome = await rig.seam.split(dataclasses.replace(request, parent=parent))

        assert outcome.reused_generation and (await rig.store.get(V)).gen == gen
        assert len(rig.cutter.calls) == 1

    async def test_the_healed_generation_continues_on_the_next_plain_event(self, rig: Rig) -> None:
        await self._completed(rig)
        rig.resources.stray_points = True
        healed = (await rig.seam.split(rig.request())).row
        rig.resources.stray_points = False

        again = await rig.seam.split(rig.request())

        assert again.row.gen == healed.gen and again.reused_generation

    async def test_with_splitting_switched_off_a_file_with_stray_points_is_never_cut_again(self, rig: Rig) -> None:
        """The flag governs making new splits only, so the roll-forward heal (which makes one) stays out of it."""
        gen = await self._completed(rig)
        rig.resources.stray_points = True

        outcome = await rig.seam.split(dataclasses.replace(rig.request(), may_heal=False))

        assert outcome.reused_generation and (await rig.store.get(V)).gen == gen
        assert len(rig.cutter.calls) == 1



@pytest.mark.parametrize(
    ("event", "delivery", "expected"),
    [
        ({"forceReindex": True, "forceReindexAt": 1700}, "d1", "1700:d1"),
        ({"forceReindex": True}, "d1", "0:d1"),
        ({"forceReindex": True, "forceReindexAt": "soon"}, "d1", "0:d1"),
        ({"forceReindex": True, "forceReindexAt": True}, "d1", "0:d1"),
        ({"forceReindex": False, "forceReindexAt": 1700}, "d1", ""),
        ({"forceReindex": True, "forceReindexAt": 1700}, None, ""),
        ({}, "d1", ""),
    ],
)
def test_a_forced_event_names_its_request_time_and_delivery(event: dict, delivery: str | None, expected: str) -> None:
    from app.modules.indexing.split_reindex import forced_nonce

    assert forced_nonce(event, delivery) == expected


@pytest.mark.parametrize(
    ("nonce", "current", "replaces"),
    [
        ("", "200:d2", False),  # a plain event continues
        ("200:d2", "200:d2", False),  # the request that made the generation
        ("300:d3", "200:d2", True),  # a newer forced request
        ("100:d1", "200:d2", False),  # a redelivery of an older one
        ("200:d9", "200:d2", False),  # the same instant: not told apart, so the running one stands
        ("0:d9", "0:d2", True),  # senders that stamp no time: as before, a different delivery replaces
        ("0:d9", "", True),  # nothing forced made the running generation
        ("d9", "d2", True),  # an old-shape nonce
        ("healed:g1", "500:d2", True),  # the roll-forward heal always replaces
    ],
)
def test_only_a_newer_forced_request_replaces_a_generation(nonce: str, current: str, replaces: bool) -> None:
    from app.modules.indexing.split_reindex import supersedes

    assert supersedes(nonce, current) is replaces


@pytest.mark.asyncio
class TestARedeliveredOlderForcedEventLeavesTheNewerGeneration:
    async def test_while_the_generation_is_running(self, rig: Rig) -> None:
        first = (await rig.seam.split(rig.request(nonce="100:d1"))).row
        replaced = (await rig.seam.split(rig.request(nonce="200:d2"))).row
        assert replaced.gen != first.gen and len(rig.cutter.calls) == 2

        stale = await rig.seam.split(rig.request(nonce="100:d1"))

        assert stale.row.gen == replaced.gen and stale.reused_generation and len(rig.cutter.calls) == 2

    async def test_once_the_generation_is_completed(self, rig: Rig) -> None:
        await rig.seam.split(rig.request(nonce="100:d1"))
        newer = (await rig.seam.split(rig.request(nonce="200:d2"))).row
        for node in rig.parts():
            rig.graph.docs["records"][node["_key"]]["indexingStatus"] = "COMPLETED"
        assert await rig.store.roll_up(await rig.store.get(V), done=3, failed=0, state=SplitState.COMPLETED)

        stale = await rig.seam.split(rig.request(nonce="100:d1"))
        later = await rig.seam.split(rig.request(nonce="300:d3"))

        assert stale.row.gen == newer.gen and stale.reused_generation
        assert later.row.gen != newer.gen and len(rig.cutter.calls) == 3
