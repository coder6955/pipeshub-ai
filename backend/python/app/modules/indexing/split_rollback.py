"""Turning every split file back into a whole one, for an operator who is about to downgrade.

An image that predates split files knows nothing of parts: it would index them as files of their own, leave their
points and rows behind and read the manifest as the file's content. So before such a downgrade, every split file is
un-split: its parts, their points, stored records and PDFs, its manifest points and its split row go, and the file is
queued for whole indexing again.

It runs inside the indexing service, as a step of the split sweep, when ``PDF_SPLIT_ROLLBACK`` is ``dry-run`` or
``apply`` (and optionally ``PDF_SPLIT_ROLLBACK_ORG``). ``dry-run`` counts and changes nothing. ``apply`` needs
``PDF_SPLIT_ENABLED=false``, or the files it queues would be split again. Every step is safe to repeat, and a pass that
stops half way resumes: a file is un-split in this order (parts purged with the row kept as ``PURGING``, then the file
queued, then the row deleted), so a crash at any point leaves a row the next pass finishes, never a file with neither.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from app.config.constants.arangodb import CollectionNames, EventTypes
from app.core.org_context import for_org
from app.modules.indexing.record_republish import record_event
from app.modules.indexing.split_state import SplitRow, SplitState
from app.services.graph_db.common.record_visibility import RecordVisibility
from app.services.messaging.config import Topic
from app.utils.time_conversion import get_epoch_timestamp_in_ms

if TYPE_CHECKING:
    from app.modules.indexing.split_purge import GenerationPurger
    from app.modules.indexing.split_state import SplitStore
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

ROLLBACK_ENV = "PDF_SPLIT_ROLLBACK"
ROLLBACK_ORG_ENV = "PDF_SPLIT_ROLLBACK_ORG"
RECORDS = CollectionNames.RECORDS.value
_logger = logging.getLogger(__name__)
_ALL_STATES = list(SplitState)


class EventSender(Protocol):
    async def send_event(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> bool: ...


class ManifestPoints(Protocol):
    async def __call__(self, org_id: str, v: str) -> None:
        """Delete the points the file's own manifest wrote (segment ``V`` of ``V``)."""


COMPLETE_LINE = "Split rollback complete: no split files remain"


@dataclass(frozen=True)
class RollbackReport:
    """One pass. ``files`` is how many rows the pass looked at (every row, for a dry run). ``remaining`` is whether
    any split row is still stored for the scope (the organisation, or all of them), asked of the store after the pass
    and never inferred from the size of a page: busy and failed files are still there."""

    files: int = 0
    unsplit: int = 0
    requeued: int = 0
    busy: int = 0
    failed: int = 0
    remaining: bool = True
    dry_run: bool = True

    @property
    def done(self) -> bool:
        """No split row is left to un-split."""
        return not self.remaining


def rollback_mode(environ: dict[str, str] | None = None) -> str | None:
    """``"dry-run"``, ``"apply"`` or None (off)."""
    raw = ((environ if environ is not None else os.environ).get(ROLLBACK_ENV) or "").strip().lower()
    return raw if raw in {"dry-run", "apply"} else None


class SplitRollback:
    def __init__(
        self, *, graph: IGraphDBProvider, store: SplitStore, purger: GenerationPurger, publisher: EventSender,
        manifest_points: ManifestPoints, apply: bool, org_id: str | None = None, page_size: int = 100,
        splitting_enabled: bool = False,
    ) -> None:
        self._graph = graph
        self._store = store
        self._purger = purger
        self._publisher = publisher
        self._manifest_points = manifest_points
        self._apply = apply
        self._org_id = org_id
        self._page_size = page_size
        self._splitting_enabled = splitting_enabled
        self._after: str | None = None

    async def run_once(self) -> RollbackReport:
        """One page of split files (a dry run reads them all). Safe to repeat until ``done``: a file finished here is
        gone from the next page; a busy or failed one is looked at again once the pass has been round every row."""
        if self._apply and self._splitting_enabled:
            _logger.error("%s=apply needs PDF_SPLIT_ENABLED=false: the files it queues would be split again", ROLLBACK_ENV)
            return RollbackReport(dry_run=False)
        files = unsplit = requeued = busy = failed = 0
        if not self._apply:
            files, malformed = await self._count_all()
            report = RollbackReport(files, remaining=files > 0 or malformed > 0, failed=malformed)
            _logger.warning(
                "Split rollback (dry-run): %d split file(s) would be un-split, %d unreadable", files, malformed,
            )
            return report
        page = await self._store.page(_ALL_STATES, after_key=self._after, limit=self._page_size, org_id=self._org_id)
        self._after = page.next_cursor(self._page_size)
        failed += len(page.malformed)
        for row in page:
            files += 1
            try:
                with for_org(row.org_id, job="split_rollback"):
                    result = await self._unsplit(row)
            except Exception:
                failed += 1
                _logger.warning("Split rollback: %s failed; it is tried again on the next round", row.v, exc_info=True)
                continue
            unsplit += result[0]
            requeued += result[1]
            busy += result[2]
        remaining = await self._any_left()
        report = RollbackReport(files, unsplit, requeued, busy, failed, remaining, dry_run=False)
        _logger.warning(
            "Split rollback (apply): %d file(s) on this page, %d un-split, %d queued for whole indexing, %d busy, %d failed, %s",
            files, unsplit, requeued, busy, failed, "split files remain" if remaining else COMPLETE_LINE,
        )
        return report

    async def _count_all(self) -> tuple[int, int]:
        """Every split row of the scope, read page by page: how many can be un-split and how many cannot be read."""
        total = malformed = 0
        after: str | None = None
        while True:
            page = await self._store.page(_ALL_STATES, after_key=after, limit=self._page_size, org_id=self._org_id)
            total += len(page)
            malformed += len(page.malformed)
            after = page.next_cursor(self._page_size)
            if after is None:
                return total, malformed

    async def _any_left(self) -> bool:
        """Whether a split row of the scope is stored, from a query that selects the scope itself and reads one row."""
        found = await self._graph.content_splits_page(
            [s.value for s in _ALL_STATES], limit=1, org_id=self._org_id,
        )
        return bool(found)

    async def _unsplit(self, row: SplitRow) -> tuple[int, int, int]:
        purged = await self._purger.purge_generation(row, delete_row=False)
        if not purged.complete:
            return 0, 0, 1  # a part is still being indexed: its lease frees, the next pass finishes
        await self._manifest_points(row.org_id, row.v)
        queued = await self._requeue(row)
        await self._store.delete(row.v, row.gen)
        return 1, queued, 0

    async def _requeue(self, row: SplitRow) -> int:
        """A whole-file index event for every live record holding the file."""
        keys = await self._graph.get_records_by_virtual_record_id(row.v, raise_on_error=True, visibility=RecordVisibility.LIVE)
        docs = await self._graph.get_nodes_by_field_in(RECORDS, "id", sorted(keys), raise_on_error=True) if keys else []
        sent = 0
        for doc in docs or []:
            key = str(doc.get("_key") or doc.get("id"))
            event_type, payload = record_event(doc, record_key=key, connector_id=str(doc.get("connectorId") or ""))
            if event_type != EventTypes.REINDEX_RECORD.value:
                event_type = EventTypes.REINDEX_RECORD.value
            if not await self._publisher.send_event(Topic.RECORD_EVENTS.value, event_type, {**payload, "forceReindex": True, "forceReindexAt": get_epoch_timestamp_in_ms()}, key=key):
                raise RuntimeError(f"the message broker did not accept the whole-file index of record {key}")
            sent += 1
        return sent
