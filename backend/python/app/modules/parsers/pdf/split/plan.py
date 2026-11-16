"""Where a PDF is cut: a pure function of the page count, per-page byte estimates and config."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from app.modules.parsers.pdf.split.config import SplitConfig


@dataclass(frozen=True)
class PartPlan:
    """Pages are 1-based and inclusive. The part owns ``page_start..page_end`` and also
    carries ``page_end+1..overlap_end``, so ``overlap_end >= page_end``."""

    index: int
    page_start: int
    page_end: int
    overlap_end: int

    @property
    def core_pages(self) -> int:
        return self.page_end - self.page_start + 1


@dataclass(frozen=True)
class SplitPlan:
    page_count: int
    parts: tuple[PartPlan, ...]
    digest: str


def should_split(page_count: int, cfg: SplitConfig) -> bool:
    return cfg.enabled and page_count > cfg.min_pages


def plan_parts(page_count: int, page_bytes: Sequence[int] | None, cfg: SplitConfig) -> SplitPlan:
    """Cut pages ``1..page_count`` into contiguous parts whose files hold at most ``cfg.part_pages`` pages and, when
    estimates are given, ``cfg.max_part_bytes``, both counting the part's overlap pages too.

    A single page larger than the cap stays alone. An overlap page that alone is larger than the
    cap is left out of the overlap (the part before it ends there), rather than shrinking every
    part that precedes it.
    """
    if page_count < 0:
        raise ValueError("page_count must not be negative")
    if page_bytes is not None and len(page_bytes) != page_count:
        raise ValueError(f"expected {page_count} page sizes, got {len(page_bytes)}")
    return build_plan(page_count, _ranges(page_count, page_bytes, cfg), cfg)


def build_plan(
    page_count: int, ranges: Sequence[tuple[int, int, int]], cfg: SplitConfig
) -> SplitPlan:
    """A plan for the given ``(page_start, page_end, overlap_end)`` ranges, which must tile the document."""
    parts = tuple(PartPlan(i, start, end, overlap_end) for i, (start, end, overlap_end) in enumerate(ranges))
    return SplitPlan(page_count, parts, _digest(page_count, parts, cfg))


def overlap_end_for(page_end: int, page_count: int, cfg: SplitConfig) -> int:
    """Where a part ending at *page_end* stops carrying the next part's pages, when sizes are unknown."""
    return min(page_end + cfg.overlap_pages, page_count)


def _ranges(
    page_count: int, page_bytes: Sequence[int] | None, cfg: SplitConfig
) -> list[tuple[int, int, int]]:
    if page_bytes is None:
        ranges = []
        for start in range(1, page_count + 1, cfg.core_pages):
            end = min(start + cfg.core_pages - 1, page_count)
            ranges.append((start, end, overlap_end_for(end, page_count, cfg)))
        return ranges
    prefix = [0]
    for size in page_bytes:
        prefix.append(prefix[-1] + size)
    ranges = []
    start = 1
    while start <= page_count:
        end = start
        while (
            end < page_count
            and end - start + 1 < cfg.core_pages
            and _span_bytes(prefix, start, _carried_end(page_bytes, prefix, start, end + 1, page_count, cfg))
            <= cfg.max_part_bytes
        ):
            end += 1
        ranges.append((start, end, _carried_end(page_bytes, prefix, start, end, page_count, cfg, fit=True)))
        start = end + 1
    return ranges


def _carried_end(
    page_bytes: Sequence[int],
    prefix: Sequence[int],
    start: int,
    end: int,
    page_count: int,
    cfg: SplitConfig,
    *,
    fit: bool = False,
) -> int:
    """Last page a part ``start..end`` carries: its overlap pages, stopping before one that alone
    exceeds the cap and, with *fit*, before one that would take the part past it."""
    last = end
    limit = min(end + cfg.overlap_pages, page_count)
    while last < limit:
        size = page_bytes[last]
        if size > cfg.max_part_bytes:
            break
        if fit and _span_bytes(prefix, start, last + 1) > cfg.max_part_bytes:
            break
        last += 1
    return last


def _span_bytes(prefix: Sequence[int], first: int, last: int) -> int:
    return prefix[last] - prefix[first - 1]


def _digest(page_count: int, parts: Sequence[PartPlan], cfg: SplitConfig) -> str:
    shape = {
        "pages": page_count,
        "ranges": [(p.page_start, p.page_end, p.overlap_end) for p in parts],
        "part_pages": cfg.part_pages,
        "max_part_bytes": cfg.max_part_bytes,
        "overlap_pages": cfg.overlap_pages,
    }
    return hashlib.sha256(json.dumps(shape, separators=(",", ":")).encode()).hexdigest()
