"""The recovery pass for split PDFs, run beside the stranded-record sweep.

It finishes what a crashed or slow parent attempt left (parts nobody queued), resumes purges a
busy part held up, and retires parts whose file is gone.
"""
from __future__ import annotations

import contextlib
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.config.constants.arangodb import CollectionNames, ProgressStatus
from app.core.org_context import for_org
from app.modules.indexing.split_owner import (
    NoLiveHolder,
    rehome_if_owner_gone,
    rehome_parts_owned_by,
)
from app.modules.indexing.split_purge import FILE_REMOVED_REASON
from app.modules.indexing.split_rollup import HEAL_AFTER_MS, Outcome
from app.modules.indexing.split_state import SplitRow, SplitState, SplitStore
from app.services.graph_db.common.record_visibility import RecordVisibility

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from app.modules.indexing.split_purge import GenerationPurger
    from app.modules.indexing.split_rollback import SplitRollback
    from app.modules.indexing.split_rollup import SplitRollup
    from app.modules.indexing.split_state import PartSpec
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value
_logger = logging.getLogger(__name__)
# Every state that holds parts a file's removal must retire. A SPLITTING row counts: a parent that crashed between
# writing its row and queueing the parts, for a file that was then deleted, would otherwise keep its parts forever.
_LIVE_STATES = [SplitState.SPLITTING, SplitState.INDEXING_PARTS, SplitState.COMPLETED, SplitState.PARTIAL, SplitState.FAILED]


@dataclass(frozen=True)
class SweepReport:
    requeued: int = 0
    purges_resumed: int = 0
    orphans_purged: int = 0
    healed: int = 0
    revived: int = 0

    @property
    def total(self) -> int:
        return self.requeued + self.purges_resumed + self.orphans_purged + self.healed + self.revived


class SplitSweep:
    def __init__(
        self,
        *,
        graph: IGraphDBProvider,
        store: SplitStore,
        purger: GenerationPurger,
        queue_parts: Callable[[SplitRow], Awaitable[None]],
        parts_missing_files: Callable[[SplitRow], Awaitable[int]],
        lease_ms: int,
        rollup: SplitRollup | None = None,
        part_block_count: Callable[[SplitRow, PartSpec], Awaitable[int | None]] | None = None,
        rollback: SplitRollback | None = None,
        page_size: int = 100,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._graph = graph
        self._store = store
        self._purger = purger
        self._queue_parts = queue_parts
        self._parts_missing_files = parts_missing_files
        self._lease_ms = lease_ms
        self._rollup = rollup
        self._part_block_count = part_block_count
        self._rollback = rollback
        self._count_cursor: str | None = None
        self._page_size = page_size
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._orphan_cursor: str | None = None
        self._drive_cursor: str | None = None
        self._heal_cursor: str | None = None
        self._finish_cursor: str | None = None
        self._purge_cursor: str | None = None
        self._revived = 0

    async def run_once(self) -> SweepReport:
        """Every step runs, whatever an earlier one did: a row or a read that fails costs its own step the rest of
        that page, never the purges, orphans and heal behind it. The first failure is raised once all steps ran."""
        cutoff = self._clock_ms() - self._lease_ms
        results: dict[str, int] = {}
        failure: Exception | None = None
        if self._rollback is not None:
            # Un-splitting every file, for an operator about to downgrade: nothing else is worth doing meanwhile.
            report = await self._rollback.run_once()
            return SweepReport(report.requeued, report.unsplit, 0, 0, 0)
        for name, step in (
            ("requeued", lambda: self._finish_splitting(cutoff)),
            ("resumed", lambda: self._resume_purges(cutoff)),
            ("orphans", self._retire_orphans),
            ("healed", self._heal),
        ):
            try:
                results[name] = await step()
            except Exception as exc:
                _logger.warning("Split-PDF sweep step %s failed", name, exc_info=True)
                failure = failure or exc
        if failure is not None:
            raise failure
        return SweepReport(results["requeued"], results["resumed"], results["orphans"], results["healed"], self._revived)

    async def _each(self, rows: Iterable[SplitRow], step: str, work: Callable[[SplitRow], Awaitable[int]]) -> int:
        """Run *work* for each row inside its organisation. A row whose work fails is logged and touched, so it leaves
        the front of the next page and the rows behind it are reached; it never ends the pass."""
        total = 0
        for row in rows:
            try:
                with for_org(row.org_id, job="split_sweep"):
                    total += await work(row)
            except NoLiveHolder as exc:
                # An expected wait: a file in the trash has its parts queued once it is restored.
                _logger.info("Split-PDF sweep (%s): %s waits: %s", step, row.v, exc)
                await self._touch_quietly(row)
            except Exception:
                _logger.warning("Split-PDF sweep (%s): %s failed; tried again later", step, row.v, exc_info=True)
                await self._touch_quietly(row)
        return total

    async def _touch_quietly(self, row: SplitRow) -> None:
        with contextlib.suppress(Exception), for_org(row.org_id, job="split_sweep"):
            await self._store.touch(row.v, row.gen)

    async def _heal(self) -> int:
        """Recount the files still indexing that no roll-up has written for a while: the same function a part's own
        terminal write calls, so a roll-up that gave up or never ran (a writer that crashed) converges here. The
        page moves on by key: a recount that changes nothing (parts still running) writes no timestamp, so without a
        cursor the same first rows would come back for ever and starve the ones behind them."""
        if self._rollup is None:
            return 0
        rows = await self._store.page(
            [SplitState.INDEXING_PARTS], updated_before_ms=self._clock_ms() - HEAL_AFTER_MS, after_key=self._heal_cursor,
            limit=self._page_size,
        )
        self._heal_cursor = rows.next_cursor(self._page_size)
        rollup = self._rollup

        async def recount(row: SplitRow) -> int:
            return await rollup.recount(row.v, gen=row.gen) is Outcome.WRITTEN

        written = await self._each(rows, "heal", recount)
        await self._drive_undriven()
        await self._backfill_block_counts()
        return written

    async def _backfill_block_counts(self) -> None:
        """Parts that completed before their block count was recorded get it from their stored record: one
        read per part, once. A part with a count is never read again; one that has no stored record is not read."""
        if self._part_block_count is None:
            return
        rows = await self._store.page(
            [SplitState.COMPLETED, SplitState.PARTIAL], after_key=self._count_cursor, limit=self._page_size,
        )
        self._count_cursor = rows.next_cursor(self._page_size)
        counts = await self._store.block_counts({row.v: row.gen for row in rows})
        part_block_count = self._part_block_count

        async def backfill(row: SplitRow) -> int:
            missing = [p for p in row.parts if counts.get(row.v, {}).get(p.k, 0) is None]
            if not missing:
                return 0
            statuses = await self._store.part_statuses(row)
            for part in missing:
                if statuses.get(part.vrid) != ProgressStatus.COMPLETED.value:
                    continue
                try:
                    count = await part_block_count(row, part)
                    if count is not None:
                        await self._store.set_block_count(part.node_key, row.gen, count)
                except Exception:
                    _logger.warning("Block count of part %s of %s not backfilled", part.k, row.v, exc_info=True)
            return 0

        await self._each(rows, "block counts", backfill)

    async def _drive_undriven(self) -> None:
        """A settled file whose parent's stages were never driven (a trigger lost, or a drive that failed and gave its
        claim back, or a claim held since a process died) is driven here when its parts have ended their own stages. The drive's claim keeps a file
        that was driven from being driven again."""
        assert self._rollup is not None
        rows = await self._store.page(
            [SplitState.COMPLETED, SplitState.PARTIAL, SplitState.FAILED],
            updated_before_ms=self._clock_ms() - HEAL_AFTER_MS, after_key=self._drive_cursor, limit=self._page_size,
        )
        self._drive_cursor = rows.next_cursor(self._page_size)
        rollup = self._rollup

        async def drive(row: SplitRow) -> int:
            if not row.redrive_token:
                await rollup.drive(row.v, gen=row.gen)
            else:
                # A claim held past the bound belongs to a process that died before its drive ended.
                await rollup.reclaim_stale(row)
            return 0

        await self._each(rows, "drive", drive)

    async def _finish_splitting(self, cutoff: int) -> int:
        rows = await self._store.page(
            [SplitState.SPLITTING], updated_before_ms=cutoff, after_key=self._finish_cursor, limit=self._page_size,
        )
        self._finish_cursor = rows.next_cursor(self._page_size)

        async def finish(row: SplitRow) -> int:
            if await self._parts_missing_files(row):
                # Only the parent's own attempt has the bytes to cut again; it is redelivered by the stranded sweep.
                await self._store.touch(row.v, row.gen)
                return 0
            # A file with no live holder (in the trash) raises IndexingError here: the row waits, see ``_each``.
            await self._queue_parts(row)
            return 1

        return await self._each(rows, "finish splitting", finish)

    async def _resume_purges(self, cutoff: int) -> int:
        rows = await self._store.page(
            [SplitState.PURGING], updated_before_ms=cutoff, after_key=self._purge_cursor, limit=self._page_size,
        )
        self._purge_cursor = rows.next_cursor(self._page_size)

        async def resume(row: SplitRow) -> int:
            result = await self._purger.purge_generation(row)
            if not result.complete:
                await self._store.touch(row.v, row.gen)
            return 1

        return await self._each(rows, "resume purge", resume)

    async def _retire_orphans(self) -> int:
        rows = await self._store.page(_LIVE_STATES, after_key=self._orphan_cursor, limit=self._page_size)
        self._orphan_cursor = rows.next_cursor(self._page_size)
        self._revived = 0

        async def retire(row: SplitRow) -> int:
            # A file in the trash keeps its parts (a restore brings them back); only a deleted one loses them.
            holders = await self._graph.get_records_by_virtual_record_id(
                row.v, raise_on_error=True, visibility=RecordVisibility.ALL,
            )
            if holders:
                self._revived += await self._revive_restored(row)
                # The parts belong to the content: while any copy of the file lives they stay, answering to its connector.
                await rehome_if_owner_gone(self._graph, self._store, row)
                return 0
            _logger.info("The file %s is gone; retiring its parts", row.v)
            await self._purger.purge_generation(row)
            return 1

        return await self._each(rows, "retire orphans", retire)

    async def rehome_owned_by(self, org_id: str, connector_id: str) -> int:
        """A connector was deleted: the parts it owned of files another copy still holds go to that copy's connector."""
        return await rehome_parts_owned_by(self._graph, self._store, org_id, connector_id)

    async def _revive_restored(self, row: SplitRow) -> int:
        """Parts stopped as "file removed" while the file sat in the trash start again once it has a live holder (a
        restore re-embeds from storage and never reaches the parent attempt)."""
        docs = await self._graph.get_nodes_by_field_in(
            RECORDS, "virtualRecordId", [p.vrid for p in row.parts],
            return_fields=["virtualRecordId", "indexingStatus", "reason"], raise_on_error=True,
        )
        stopped = [
            d for d in docs
            if d.get("indexingStatus") == ProgressStatus.AUTO_INDEX_OFF.value and d.get("reason") == FILE_REMOVED_REASON
        ]
        if not stopped or not await self._graph.get_records_by_virtual_record_id(row.v, raise_on_error=True):
            return 0
        await self._queue_parts(row)
        return len(stopped)
