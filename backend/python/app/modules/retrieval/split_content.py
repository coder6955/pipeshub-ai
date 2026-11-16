"""What the query side needs to know about a split file, behind a small interface.

``live_generation`` answers which generation of a file's parts is live (the generation check), ``part_vrid``
maps a part number to the stored record that holds it (block-address resolution, storage grep), and
``part_label`` names a part for the model. The default implementation reads ``contentSplits`` through
``app.modules.indexing.split_state`` when it exists; everything else is treated as unsplit.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from app.modules.retrieval.hit_shape import part_offset

if TYPE_CHECKING:
    from app.modules.indexing.split_state import PartSpec, SplitRow, SplitStore
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PartLabel:
    """Where a passage's stored part lies in its file. The model is told only the pages (``page_start``, ``page_end``, and
    ``page_count`` of the file); ``k`` and ``total`` are for the code."""

    k: int
    total: int
    page_start: int
    page_end: int
    page_count: int


class SplitContentResolver(Protocol):
    async def live_generation(self, vrids: list[str]) -> dict[str, str]:
        """``{V: G}`` for each given file that is split; a file not in the result is unsplit."""

    async def part_vrid(self, v: str, k: int) -> str | None:
        """The stored record's virtual record id of part ``k`` of ``v``, or ``None``."""

    async def part_label(self, v: str, k: int) -> PartLabel | None:
        """Part ``k``'s place: its page range and the file's page count, or ``None``."""

    async def roots_of(self, vrids: list[str]) -> dict[str, str]:
        """``{V_k: V}`` for each given stored-record id that is a part of a split file (the stored files of
        parts are named by ``V_k``; every access check and every answer is about ``V``)."""

    async def block_offsets(self, parts: list[tuple[str, int]]) -> dict[tuple[str, int], int | None]:
        """``{(V, k): blocks in parts 1..k-1}`` for the display ordinal; ``None`` when any earlier
        part's count is unknown."""


class UnsplitResolver:
    """Everything is unsplit."""

    async def block_offsets(self, parts: list[tuple[str, int]]) -> dict[tuple[str, int], int | None]:
        return {}

    async def live_generation(self, vrids: list[str]) -> dict[str, str]:
        return {}

    async def part_vrid(self, v: str, k: int) -> str | None:
        return None

    async def part_label(self, v: str, k: int) -> PartLabel | None:
        return None

    async def roots_of(self, vrids: list[str]) -> dict[str, str]:
        return {}


class StoreSplitResolver:
    """Reads ``contentSplits`` rows with the indexing side's ``SplitStore``; one read per file per instance."""

    def __init__(self, store: SplitStore) -> None:
        self._store = store
        self._rows: dict[str, SplitRow | None] = {}
        self._gens: dict[str, str | None] = {}
        self._counts: dict[str, dict[int, int | None] | None] = {}

    async def _row(self, v: str) -> SplitRow | None:
        if v not in self._rows:
            self._rows[v] = await self._store.get(v)
        return self._rows[v]

    async def live_generation(self, vrids: list[str]) -> dict[str, str]:
        wanted = [v for v in dict.fromkeys(vrids) if v]
        unread = [v for v in wanted if v not in self._rows and v not in self._gens]
        if unread:
            # One batched read for every file asked about, however many are split.
            found = await self._store.get_many(unread)
            for v in unread:
                doc = found.get(v)
                self._gens[v] = str(doc["gen"]) if doc and doc.get("gen") else None
        gens = {v: (self._rows[v].gen if self._rows[v] is not None else None) if v in self._rows else self._gens[v] for v in wanted}
        return {v: gen for v, gen in gens.items() if gen}

    async def part_vrid(self, v: str, k: int) -> str | None:
        part = await self._part(v, k)
        return part.vrid if part is not None else None

    async def part_label(self, v: str, k: int) -> PartLabel | None:
        row = await self._row(v)
        part = await self._part(v, k)
        if row is None or part is None:
            return None
        return PartLabel(k=part.k, total=row.total, page_start=part.page_start, page_end=part.page_end, page_count=row.page_count)

    async def roots_of(self, vrids: list[str]) -> dict[str, str]:
        """Needs the indexing side's reverse lookup (``SplitStore.roots_of``); without it no stored record is a
        part, so a part's file is never found by name: the access check then denies it (fail closed)."""
        return dict(await self._store.roots_of(list(vrids))) if vrids else {}

    async def block_offsets(self, parts: list[tuple[str, int]]) -> dict[tuple[str, int], int | None]:
        """The counts of every file asked about are read in one batched read, once per resolver instance."""
        wanted = [v for v in dict.fromkeys(v for v, _ in parts) if v and v not in self._counts]
        if wanted:
            rows = await asyncio.gather(*(self._row(v) for v in wanted))
            live = {v: row.gen for v, row in zip(wanted, rows) if row is not None}
            found = await self._store.block_counts(live) if live else {}
            for v in wanted:
                self._counts[v] = found.get(v)
        return {(v, k): part_offset(self._counts.get(v), k) for v, k in parts}

    async def _part(self, v: str, k: int) -> PartSpec | None:
        row = await self._row(v)
        return next((p for p in row.parts if p.k == k), None) if row is not None else None


def default_resolver(graph_provider: IGraphDBProvider | None) -> SplitContentResolver:
    """The store-backed resolver when the indexing side has split state, else everything unsplit."""
    try:
        from app.modules.indexing.split_state import SplitStore
    except ImportError:
        return UnsplitResolver()
    if graph_provider is None:
        return UnsplitResolver()
    return StoreSplitResolver(SplitStore(graph_provider))
