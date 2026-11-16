"""The file's progress, rolled up from its parts.

Every terminal write of a part calls ``part_terminal`` once the write has committed. The roll-up reads the
``contentSplits`` row, recounts the parts of its generation, and writes the counters with a compare-and-set
on ``epoch``: a write lands only if no other roll-up wrote since this one read. So for each part the last
write can only come after a recount that began after that part's own commit, and the stored state ends equal
to the recount with no lease. The heal sweep calls the same function.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from app.config.constants.arangodb import CollectionNames, ProgressStatus
from app.modules.indexing.split_state import (
    PartInfo,
    SplitRow,
    SplitState,
    SplitStore,
    cached_part_info,
    is_part,
    part_info,
)
from app.modules.pipeline.models import stage_state_key

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

_logger = logging.getLogger(__name__)

DONE_STATUSES = frozenset({ProgressStatus.COMPLETED.value, ProgressStatus.EMPTY.value})
FAILED_STATUSES = frozenset({
    ProgressStatus.FAILED.value, ProgressStatus.PASSWORD_PROTECTED.value, ProgressStatus.FILE_TYPE_NOT_SUPPORTED.value,
})
TERMINAL_STATUSES = DONE_STATUSES | FAILED_STATUSES
# AUTO_INDEX_OFF, PAUSED, ENABLE_MULTIMODAL_MODELS and QUEUED wait for something; they never count.

# The stages of a part whose outputs the parent's own stages read, and the states that end one.
PART_STAGES = ("classify", "entities")
STAGE_TERMINAL = frozenset({ProgressStatus.COMPLETED.value, ProgressStatus.SKIPPED.value, ProgressStatus.FAILED.value})
_SETTLED_STATES = (SplitState.COMPLETED, SplitState.PARTIAL, SplitState.FAILED)
_SEARCHABLE_STATES = (SplitState.COMPLETED, SplitState.PARTIAL)
RECORDS = CollectionNames.RECORDS.value

MAX_TRIES = 8
_BACKOFF_S = (0.025, 0.2)
HEAL_AFTER_MS = 2 * 60 * 1000
# A drive claim still held after this long was taken by a process that died before it finished (the stage sweep's
# 30-minute rule for stuck jobs); the heal sweep gives it back and drives again.
DRIVE_CLAIM_STALE_MS = 30 * 60 * 1000


class Outcome(StrEnum):
    STALE = "stale"
    UNCHANGED = "unchanged"
    WRITTEN = "written"
    GAVE_UP = "gave_up"


@dataclass(frozen=True)
class Counts:
    total: int
    done: int
    failed: int

    @property
    def settled(self) -> bool:
        return self.done + self.failed == self.total


def state_of(counts: Counts, current: SplitState) -> SplitState:
    if not counts.settled:
        # The parent's own attempt moves SPLITTING on once every part is queued.
        return current if current is SplitState.SPLITTING else SplitState.INDEXING_PARTS
    if counts.failed == 0:
        return SplitState.COMPLETED
    return SplitState.FAILED if counts.done == 0 else SplitState.PARTIAL


@dataclass(frozen=True)
class _Counted:
    outcome: Outcome
    driven: bool


class Redrive(Protocol):
    async def __call__(self, row: SplitRow) -> None:
        """Run the parent's ``classify`` and ``entities`` stages again, unforced."""


class SplitRollup:
    def __init__(
        self,
        graph: Any,  # noqa: ANN401
        store: SplitStore,
        redrive: Redrive | None = None,
        *,
        on_searchable: Callable[[SplitRow], Awaitable[None]] | None = None,
        tries: int = MAX_TRIES,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._graph = graph
        self._store = store
        self._redrive = redrive
        self._on_searchable = on_searchable
        self._tries = tries
        self._sleep = sleep
        self._jitter = jitter or (lambda: random.uniform(*_BACKOFF_S))
        self._locks: dict[str, tuple[asyncio.Lock, int]] = {}

    async def on_part_terminal(self, part: PartInfo) -> Outcome:
        return await self.recount(part.root, gen=part.gen)

    async def on_part_stage_settled(self, part_vrid: str) -> bool:
        """A part's ``classify`` or ``entities`` reached a terminal state: the parent's stages run once every
        part of the live generation has both. Returns whether this call drove them."""
        root = (await self._store.roots_of([part_vrid])).get(part_vrid)
        if root is None:
            return False
        return (await self._recount(root)).driven

    async def on_part_output_replaced(self, part_vrid: str) -> bool:
        """A part's kept extraction was replaced by a more complete one: what the parent's stages were driven for is out of
        date. The drive claim is given back (the token of how the parts ended does not change with an output) and the
        stages are driven again; their run-time digest changes with the union, so they run. Returns whether they were driven."""
        root = (await self._store.roots_of([part_vrid])).get(part_vrid)
        if root is None:
            return False
        for attempt in range(self._tries):
            row = await self._store.get(root)
            if row is None or row.state not in _SETTLED_STATES:
                return False
            if not row.redrive_token and not row.redrive_at_ms:
                break
            if await self._store.roll_up(
                row, done=row.done, failed=row.failed, state=row.state, redriveToken="", redriveAtMs=0,
            ):
                break
            if attempt + 1 >= self._tries:
                return False
            await self._sleep(self._jitter())
        return await self.drive(root, gen=row.gen)

    async def recount(self, v: str, *, gen: str | None = None) -> Outcome:
        """Bring the row of *v* to the recount of its live generation, then drive the parent's stages if its parts
        are all done (``drive``). *gen* names the generation the caller's part belongs to: a part of another
        generation changes nothing."""
        return (await self._recount(v, gen=gen)).outcome

    async def _recount(self, v: str, *, gen: str | None = None) -> _Counted:
        outcome, state = await self._write_counts(v, gen=gen)
        if outcome in (Outcome.WRITTEN, Outcome.UNCHANGED) and state in _SETTLED_STATES:
            return _Counted(outcome, await self.drive(v, gen=gen))
        return _Counted(outcome, False)

    async def _write_counts(self, v: str, *, gen: str | None) -> tuple[Outcome, SplitState | None]:
        for attempt in range(self._tries):
            row = await self._store.get(v)
            if row is None or row.state is SplitState.PURGING or (gen is not None and row.gen != gen):
                return Outcome.STALE, None
            statuses = await self._store.part_statuses(row)
            counts = _count(row, statuses)
            state = state_of(counts, row.state)
            if (row.done, row.failed, row.state) == (counts.done, counts.failed, state):
                return Outcome.UNCHANGED, state
            if await self._store.roll_up(row, done=counts.done, failed=counts.failed, state=state):
                if state is not row.state and state in _SEARCHABLE_STATES:
                    await self._notify_searchable(row)
                return Outcome.WRITTEN, state
            if attempt + 1 < self._tries:
                await self._sleep(self._jitter())
        _logger.warning("Roll-up of %s gave up after %d tries; the heal sweep recounts it", v, self._tries)
        return Outcome.GAVE_UP, None

    async def _notify_searchable(self, row: SplitRow) -> None:
        """The file became searchable (every part ended, at least one indexed): what a record's own completion does for
        an unsplit file. Never raises; the write stands."""
        if self._on_searchable is None:
            return
        try:
            await self._on_searchable(row)
        except Exception as exc:
            _logger.warning("Invalidation after split file %s became searchable failed: %s", row.v, type(exc).__name__)

    async def drive(self, v: str, *, gen: str | None = None) -> bool:
        """Run the parent's ``classify`` and ``entities`` when every part of the live generation has ended and each
        part that indexed has both stages terminal. Triggered by a part's stage completion, by the recount
        that settles the file, and by the heal sweep. One claim per distinct state of the parts: the token of what
        the parts ended with is written to the row by compare-and-set on ``epoch`` *before* the drive, so concurrent
        triggers, stale readers (a retry has bumped the epoch) and repeats drive once. Never raises."""
        if self._redrive is None:
            return False
        lock, users = self._locks.get(v) or (asyncio.Lock(), 0)
        self._locks[v] = (lock, users + 1)
        try:
            async with lock:
                return await self._claim_and_drive(v, gen)
        except Exception as exc:
            _logger.warning("Driving the stages of split file %s failed: %s", v, type(exc).__name__)
            return False
        finally:
            lock, users = self._locks[v]
            if users <= 1:
                del self._locks[v]
            else:
                self._locks[v] = (lock, users - 1)

    async def _claim_and_drive(self, v: str, gen: str | None) -> bool:
        assert self._redrive is not None
        for attempt in range(self._tries):
            row = await self._store.get(v)
            if row is None or row.state not in _SETTLED_STATES or (gen is not None and row.gen != gen):
                return False
            token = await self._ready_token(row)
            if token is None or token == row.redrive_token:
                return False
            claimed = {"redriveToken": token, "redriveAtMs": self._clock_ms()}
            if await self._store.roll_up(row, done=row.done, failed=row.failed, state=row.state, **claimed):
                try:
                    await self._redrive(row)
                except Exception:
                    await self._release(v, row.gen, token)
                    raise
                await self._finish_claim(v, row.gen, token)
                return True
            if attempt + 1 < self._tries:
                await self._sleep(self._jitter())
        return False

    async def _release(self, v: str, gen: str, token: str) -> None:
        """The drive failed after its claim: give the claim back so the next trigger can drive."""
        await self._rewrite_claim(v, gen, token, redriveToken="", redriveAtMs=0)

    async def _finish_claim(self, v: str, gen: str, token: str) -> None:
        """The drive finished: the token stays (it is the once-only mark) and the claim time is cleared, so the heal
        sweep can tell a finished drive from one whose process died. Best effort: if this write is lost the claim is
        reclaimed after the bound and the drive repeats, which costs nothing (run-time digests)."""
        await self._rewrite_claim(v, gen, token, redriveAtMs=0)

    async def _rewrite_claim(self, v: str, gen: str, token: str, **fields: object) -> None:
        try:
            for _ in range(self._tries):
                row = await self._store.get(v)
                if row is None or row.gen != gen or row.redrive_token != token:
                    return
                if await self._store.roll_up(row, done=row.done, failed=row.failed, state=row.state, **fields):
                    return
        except Exception as exc:
            _logger.warning("Could not update the drive claim of %s: %s", v, type(exc).__name__)

    async def reclaim_stale(self, row: SplitRow) -> bool:
        """Give back the drive claim of *row* if it has been held longer than ``DRIVE_CLAIM_STALE_MS`` (the claiming
        process died between its claim and the end of its drive), then drive. The give-back is the same compare-and-set
        as every claim, so of several sweepers one wins; the drive that follows claims once like any other."""
        if not row.redrive_token or not row.redrive_at_ms or self._clock_ms() - row.redrive_at_ms < DRIVE_CLAIM_STALE_MS:
            return False
        if not await self._store.roll_up(
            row, done=row.done, failed=row.failed, state=row.state, redriveToken="", redriveAtMs=0,
        ):
            return False
        _logger.warning("Reclaimed a stale drive claim of split file %s", row.v)
        return await self.drive(row.v, gen=row.gen)

    async def _ready_token(self, row: SplitRow) -> str | None:
        """None while a part is still running, or has indexed and not yet ended its ``classify`` and ``entities``.
        Otherwise a digest of how every part ended (status, and for each part that indexed, each stage's revision,
        status and attempt): equal when nothing a retry or a re-run could change has changed."""
        docs = await self._graph.get_nodes_by_field_in(
            RECORDS, "virtualRecordId", [p.vrid for p in row.parts],
            return_fields=["virtualRecordId", "indexingStatus", "contentRev"], raise_on_error=True,
        )
        by_vrid = {str(d["virtualRecordId"]): d for d in docs}
        ended: list[list[object]] = [[row.gen]]
        for spec in row.parts:
            doc = by_vrid.get(spec.vrid)
            status = doc.get("indexingStatus") if doc else None
            if status not in TERMINAL_STATUSES:
                return None
            entry: list[object] = [spec.vrid, status]
            if status == ProgressStatus.COMPLETED.value:
                rev = doc.get("contentRev") if doc else None
                if not rev:
                    return None
                for stage in PART_STAGES:
                    state = await self._graph.stage_state_get(stage_state_key(spec.vrid, str(rev), stage))
                    stage_status = getattr(state.get("status"), "value", state.get("status")) if state else None
                    if stage_status not in STAGE_TERMINAL:
                        return None
                    entry += [rev, stage, stage_status, state.get("attempt") if state else None]
            ended.append(entry)
        return hashlib.sha256(json.dumps(ended, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:32]

    async def reopen(self, v: str, gen: str) -> bool:
        """A retry of failed parts: the file is indexing again. The same compare-and-set as every roll-up write."""
        for attempt in range(self._tries):
            row = await self._store.get(v)
            if row is None or row.gen != gen or row.state is SplitState.PURGING:
                return False
            if row.state in (SplitState.INDEXING_PARTS, SplitState.SPLITTING):
                return True
            # The drive claim goes with the reopening: what the parent's stages were driven for is not what they will be
            # driven for once the retried parts have run theirs, and the sweep drives a file that holds no claim.
            if await self._store.roll_up(
                row, done=row.done, failed=row.failed, state=SplitState.INDEXING_PARTS, redriveToken="", redriveAtMs=0,
            ):
                return True
            if attempt + 1 < self._tries:
                await self._sleep(self._jitter())
        return False


def _count(row: SplitRow, statuses: Mapping[str, str | None]) -> Counts:
    values = [statuses.get(p.vrid) for p in row.parts]
    return Counts(
        total=len(row.parts),
        done=sum(v in DONE_STATUSES for v in values),
        failed=sum(v in FAILED_STATUSES for v in values),
    )


_active: dict[str, SplitRollup | None] = {"rollup": None}


def use_rollup(rollup: SplitRollup | None) -> None:
    """The roll-up the terminal writers reach (set once by the process wiring; none means a plain process)."""
    _active["rollup"] = rollup


async def part_terminal(record: Mapping[str, Any] | object, status: str | ProgressStatus) -> None:
    """Called by every writer of a terminal ``indexingStatus`` after the write committed. Does nothing for a record
    that is not a part or a status that is not terminal, and never raises: the part's own write already stands,
    and the heal sweep recounts what a failure here missed."""
    value = status.value if isinstance(status, ProgressStatus) else status
    rollup = _active["rollup"]
    if rollup is None or value not in TERMINAL_STATUSES or not is_part(record):
        return
    try:
        info = cached_part_info(record) or await part_info(record)
        if info is not None:
            await rollup.on_part_terminal(info)
    except Exception as exc:
        _logger.warning("Roll-up after a part's terminal write failed: %s", type(exc).__name__)


async def part_stage_settled(job: object) -> None:
    """Called by the stage worker once a stage job of any record reached a terminal state. Does nothing unless the job
    is a ``classify`` or ``entities`` of a part, found by the part's own virtual record id (a job carries its owner's
    connector, never the hidden parts connector), and never raises: the stage's own state already stands, and the
    heal sweep drives what a failure here missed."""
    rollup = _active["rollup"]
    stage = getattr(job, "stage", None)
    if rollup is None or stage not in PART_STAGES:
        return
    try:
        await rollup.on_part_stage_settled(str(job.virtual_record_id))  # type: ignore[attr-defined]
    except Exception as exc:
        _logger.warning("Driving the parent after a part's %s failed: %s", stage, type(exc).__name__)


async def part_output_replaced(part_vrid: str | None) -> None:
    """A part's extraction was completed by the re-extraction loop and kept in place of the one that left text unread:
    the file's merge of its parts runs again. Never raises (the replacement stands; the heal sweep drives what this misses)."""
    rollup = _active["rollup"]
    if rollup is None or not part_vrid:
        return
    try:
        await rollup.on_part_output_replaced(str(part_vrid))
    except Exception as exc:
        _logger.warning("Driving the parent after a part's extraction was completed failed: %s", type(exc).__name__)
