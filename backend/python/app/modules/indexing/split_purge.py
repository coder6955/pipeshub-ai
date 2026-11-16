"""Retiring a generation of parts: for a re-split, a split turned back into a whole file, or a parent that is gone.

A part is removed only while holding its record lease, so a part that is still being indexed
finishes (or is lost) before its points, stored record and file go; whatever the lease
refuses stays in the plan row, which stays ``PURGING`` for the split sweep to finish.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from app.config.constants.arangodb import CollectionNames, ProgressStatus
from app.modules.indexing.split_state import (
    SplitRow,
    SplitState,
    SplitStore,
)

if TYPE_CHECKING:
    from app.modules.indexing.split_state import PartSpec
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value
SUPERSEDED_REASON = "superseded"
FILE_REMOVED_REASON = "file removed"

_logger = logging.getLogger(__name__)


class PartLeases(Protocol):
    async def claim(self, record_id: str) -> str | None: ...

    async def release(self, record_id: str, holder: str) -> None: ...


class PartResources(Protocol):
    """What lives outside the graph for a part: its points, its stored record and its PDF."""

    async def delete_points_and_stored_record(self, org_id: str, root_virtual_record_id: str, part_virtual_record_id: str) -> None:
        """The part's points (they carry the file's id as their own and the part's as their segment) and its stored record."""
        ...

    async def delete_pdf(self, org_id: str, storage_document_id: str) -> None: ...


@dataclass(frozen=True)
class PurgeResult:
    purged: int
    busy: int

    @property
    def complete(self) -> bool:
        return self.busy == 0


class GenerationPurger:
    def __init__(
        self,
        graph: IGraphDBProvider,
        store: SplitStore,
        leases: PartLeases,
        resources: PartResources,
    ) -> None:
        self._graph = graph
        self._store = store
        self._leases = leases
        self._resources = resources

    async def purge_generation(self, row: SplitRow, *, delete_row: bool = True) -> PurgeResult:
        """Retire every part of *row*'s generation, then (with *delete_row*) the row itself.

        The row is deleted last, and only when every part was removed, so a crash or a busy
        part leaves the plan that the next attempt needs.
        """
        moved = await self._store.move(row.v, row.gen, tuple(s for s in SplitState if s is not SplitState.PURGING), SplitState.PURGING)
        if not moved:
            # Not moved: the row is already ``PURGING`` for this generation (a resumed purge, which goes on), or it is gone or
            # holds another generation (another purger finished this one and the parent has since made a new one), whose parts
            # and row are not this purge's to touch.
            current = await self._store.get(row.v)
            if current is None or current.gen != row.gen:
                _logger.info("The purge of generation %s of %s found it already retired", row.gen, row.v)
                return PurgeResult(0, 0)
        purged = busy = 0
        for spec in row.parts:
            if await self._purge_part(row, spec):
                purged += 1
            else:
                busy += 1
        if busy == 0 and delete_row:
            await self._store.delete(row.v, row.gen)
        return PurgeResult(purged, busy)

    async def _purge_part(self, row: SplitRow, spec: PartSpec) -> bool:
        await self._fence(spec)
        # With no lease manager bound (distributed concurrency off) there is no concurrent holder to wait for: the
        # purge goes ahead unfenced, as revision retention does ("Record leases" in docs/indexing-service.md).
        bound = getattr(self._leases, "bound", True)
        holder = await self._leases.claim(spec.node_key) if bound else ""
        if holder is None:
            _logger.info("Part %s of %s is being indexed; its purge waits for the lease", spec.node_key, row.v)
            return False
        try:
            node = await self._graph.get_document(spec.node_key, RECORDS, raise_on_error=True)
            # The PDF first: its id lives on the node, which the steps after it remove.
            storage_document_id = (node or {}).get("storageDocumentId")
            if storage_document_id:
                await self._resources.delete_pdf(row.org_id, str(storage_document_id))
            if node is not None:
                await self._graph.delete_records_and_relations(spec.node_key, hard_delete=True)
            await self._store.delete_part(spec.node_key)
            await self._graph.stage_states_delete_for_virtual_records([spec.vrid])
            await self._resources.delete_points_and_stored_record(row.org_id, row.v, spec.vrid)
            return True
        finally:
            if bound:
                await self._leases.release(spec.node_key, holder)

    async def _fence(self, spec: PartSpec) -> None:
        """Stop a part that has not started: its handler reads ``AUTO_INDEX_OFF`` and does no work."""
        await self._graph.update_node_fields_if_match(
            spec.node_key,
            RECORDS,
            {"indexingStatus": ProgressStatus.AUTO_INDEX_OFF.value, "reason": SUPERSEDED_REASON},
            {"virtualRecordId": spec.vrid},
        )
