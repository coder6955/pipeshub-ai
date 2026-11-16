"""One bad row never stops the split sweep, and no row starves behind others that change nothing.

Review findings of the large-PDF split pull request: a file trashed while its row was ``SPLITTING`` raised out of
``run_once`` on every pass (so purges, orphans and the heal never ran for any organisation), and the heal pass read
the same first page of rows for ever.
"""
from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing.split_rollup import HEAL_AFTER_MS, SplitRollup
from app.modules.indexing.split_state import SplitState, clear_part_cache
from app.modules.indexing.split_sweep import SplitSweep
from tests.unit.modules.indexing.test_split_seam import NOW, ORG, OWNER, Rig


@pytest.fixture
def rig() -> Rig:
    NOW[0] = 1_000_000
    clear_part_cache()
    return Rig()


async def _split_file(rig: Rig, v: str, key: str) -> Any:  # noqa: ANN401
    """Another file of the same organisation, split through the same seam."""
    rig.graph.put("records", key, {
        "orgId": ORG, "connectorId": OWNER, "virtualRecordId": v, "recordName": f"{v}.pdf",
        "indexingStatus": "IN_PROGRESS", "mimeType": "application/pdf",
    })
    request = dataclasses.replace(rig.request(), parent=rig.graph.docs["records"][key], v=v)
    return (await rig.seam.split(request)).row


def _sweep(rig: Rig, *, page_size: int, rollup: SplitRollup | None = None, queue: Any = None) -> SplitSweep:  # noqa: ANN401
    return SplitSweep(
        graph=rig.graph, store=rig.store, purger=rig.purger, queue_parts=queue or rig.seam.queue_parts,
        parts_missing_files=rig.seam.parts_missing_files, lease_ms=120_000, clock_ms=lambda: NOW[0], page_size=page_size,
        rollup=rollup,
    )


async def _unqueue(rig: Rig, v: str, state: SplitState = SplitState.SPLITTING) -> None:
    """Put a file back as a parent that crashed after cutting: parts not queued, the row still SPLITTING."""
    row = await rig.store.get(v)
    assert row is not None
    for spec in row.parts:
        rig.graph.docs["records"][spec.node_key]["indexingStatus"] = ProgressStatus.NOT_STARTED.value
    rig.graph.docs["contentSplits"][v]["state"] = state.value


@pytest.mark.asyncio
class TestOneBadRowDoesNotStopThePass:
    async def test_a_file_trashed_while_splitting_waits_and_the_files_behind_it_are_queued(self, rig: Rig) -> None:
        await _split_file(rig, "a-trashed", "rec-a")
        await _split_file(rig, "z-live", "rec-z")
        await _unqueue(rig, "a-trashed")
        await _unqueue(rig, "z-live")
        rig.graph.docs["records"]["rec-a"]["isDeleted"] = True  # in the trash: no live holder, but not gone
        NOW[0] += 200_000
        sweep = _sweep(rig, page_size=1)

        first = await sweep.run_once()  # before the fix: raises "no live record holds", every pass, for every org
        second = await sweep.run_once()

        assert first.requeued == 0 and second.requeued == 1, "the live file behind the trashed one is queued on the next page"
        z_parts = [d for d in rig.graph.docs["records"].values() if d.get("virtualRecordId", "").startswith("v:") or d.get("connectorId", "").startswith("pdfparts_")]
        assert any(d["indexingStatus"] == ProgressStatus.QUEUED.value for d in z_parts)
        trashed = rig.graph.docs["contentSplits"]["a-trashed"]
        assert trashed["state"] == SplitState.SPLITTING.value and trashed["updatedAtMs"] == NOW[0], "the waiting row is touched"

    async def test_a_row_whose_queueing_fails_is_touched_and_the_rest_of_the_pass_still_runs(self, rig: Rig) -> None:
        await _split_file(rig, "a-broker", "rec-a")
        await _unqueue(rig, "a-broker")
        # A file already deleted: its parts are retired by the orphan step that runs after the failing one.
        await _split_file(rig, "b-gone", "rec-b")
        rig.graph.docs["records"].pop("rec-b")
        NOW[0] += 200_000
        calls: list[str] = []

        async def refusing(row: Any) -> None:  # noqa: ANN401
            calls.append(row.v)
            raise RuntimeError("the broker refused")

        report = await _sweep(rig, page_size=10, queue=refusing).run_once()

        assert calls == ["a-broker"]
        assert report.orphans_purged >= 1 and await rig.store.get("b-gone") is None
        assert rig.graph.docs["contentSplits"]["a-broker"]["updatedAtMs"] == NOW[0]

    async def test_a_file_deleted_while_its_row_is_splitting_has_its_parts_retired(self, rig: Rig) -> None:
        row = await _split_file(rig, "a-gone", "rec-a")
        await _unqueue(rig, "a-gone")
        rig.graph.docs["records"].pop("rec-a")
        NOW[0] += 200_000

        report = await _sweep(rig, page_size=10).run_once()

        assert report.orphans_purged == 1
        assert await rig.store.get("a-gone") is None
        assert not [s for s in row.parts if s.node_key in rig.graph.docs["records"]]

    async def test_a_failing_step_does_not_stop_the_steps_behind_it_and_is_raised_at_the_end(self, rig: Rig) -> None:
        await _split_file(rig, "a-gone", "rec-a")
        rig.graph.docs["records"].pop("rec-a")
        sweep = _sweep(rig, page_size=10)

        async def broken(_cutoff: int) -> int:
            raise ConnectionError("graph down")

        sweep._finish_splitting = broken  # type: ignore[method-assign]
        with pytest.raises(ConnectionError):
            await sweep.run_once()

        assert await rig.store.get("a-gone") is None, "the orphan step ran although the first step failed"


@pytest.mark.asyncio
class TestHealReachesEveryRow:
    async def test_a_file_behind_a_full_page_of_unchanged_ones_is_recounted_within_two_passes(self, rig: Rig) -> None:
        # v1 and v2: parts still running (a recount changes nothing). v3: every part completed, roll-up lost.
        for n in (1, 2, 3):
            await _split_file(rig, f"v{n}", f"rec-{n}")
        for spec in (await rig.store.get("v3")).parts:  # type: ignore[union-attr]
            rig.graph.docs["records"][spec.node_key]["indexingStatus"] = ProgressStatus.COMPLETED.value
        NOW[0] += 2 * HEAL_AFTER_MS
        sweep = _sweep(rig, page_size=2, rollup=SplitRollup(rig.graph, rig.store, clock_ms=lambda: NOW[0]))

        first = await sweep.run_once()  # before the fix this page was the same on every pass
        second = await sweep.run_once()

        assert first.healed + second.healed == 1
        assert (await rig.store.get("v3")).state is SplitState.COMPLETED  # type: ignore[union-attr]


@pytest.mark.asyncio
class TestEveryStepPagesByKey:
    """A row whose own bookkeeping cannot be written (its touch fails) stays at the front of its step's window. The
    step's cursor is what lets the rows behind it be reached; without it the same first page comes back for ever."""

    async def test_finish_splitting_reaches_live_files_behind_a_full_page_of_rows_that_stay(self, rig: Rig) -> None:
        stuck = [f"a{n:03d}" for n in range(4)]
        live = [f"z{n:03d}" for n in range(3)]
        for v in stuck + live:
            await _split_file(rig, v, f"rec-{v}")
            await _unqueue(rig, v)
        for v in stuck:
            rig.graph.docs["records"][f"rec-{v}"]["isDeleted"] = True  # trashed: waits, and its touch is lost below
        NOW[0] += 200_000
        real_touch = rig.store.touch

        async def lost_touch(v: str, gen: str) -> bool:
            if v in stuck:
                raise RuntimeError("the store did not take the write")
            return await real_touch(v, gen)

        rig.store.touch = lost_touch  # type: ignore[method-assign]
        sweep = _sweep(rig, page_size=4)

        queued = sum([(await sweep.run_once()).requeued for _ in range(3)])

        assert queued == len(live), "the live files behind four rows that never leave the window were never reached"

    async def test_resume_purge_reaches_a_row_behind_a_full_page_of_rows_that_stay(self, rig: Rig) -> None:
        stuck = [f"a{n:03d}" for n in range(4)]
        for v in [*stuck, "z-free"]:
            row = await _split_file(rig, v, f"rec-{v}")
            rig.graph.docs["contentSplits"][v]["state"] = SplitState.PURGING.value
            if v in stuck:
                rig.leases.held.add(row.parts[0].node_key)  # a part is still being indexed: the purge waits
        NOW[0] += 200_000
        real_touch = rig.store.touch

        async def lost_touch(v: str, gen: str) -> bool:
            if v in stuck:
                raise RuntimeError("the store did not take the write")
            return await real_touch(v, gen)

        rig.store.touch = lost_touch  # type: ignore[method-assign]
        sweep = _sweep(rig, page_size=4)

        resumed = 0
        for _ in range(3):
            resumed += (await sweep.run_once()).purges_resumed
        assert await rig.store.get("z-free") is None, f"the free file behind the waiting ones was never purged (resumed {resumed})"


@pytest.mark.asyncio
class TestAMalformedRowIsSkippedNotFatal:
    async def test_the_orphan_step_reaches_the_deleted_file_behind_a_row_it_cannot_read(self, rig: Rig) -> None:
        await _split_file(rig, "z-gone", "rec-z")
        rig.graph.docs["records"].pop("rec-z")
        rig.graph.put("contentSplits", "a-bad", {"orgId": ORG, "state": SplitState.COMPLETED.value, "plan": [], "updatedAtMs": 1})
        sweep = _sweep(rig, page_size=10)

        for _ in range(2):
            await sweep.run_once()

        assert await rig.store.get("z-gone") is None, "the deleted file behind the malformed row keeps its parts"

    async def test_the_cursor_moves_past_a_malformed_last_row_of_a_full_page(self, rig: Rig) -> None:
        await _split_file(rig, "a-ok", "rec-a")
        rig.graph.put("contentSplits", "b-bad", {"orgId": ORG, "state": SplitState.COMPLETED.value, "plan": [], "updatedAtMs": 1})
        await _split_file(rig, "c-gone", "rec-c")
        rig.graph.docs["records"].pop("rec-c")
        sweep = _sweep(rig, page_size=2)

        for _ in range(3):
            await sweep.run_once()

        assert await rig.store.get("c-gone") is None
