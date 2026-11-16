"""Drop the hits of a split file's old generation.

A re-split purges the old generation before it publishes the new one, but a search can still read points the
purge has not reached. A hit with ``splitGen`` is kept only when it equals the file's live generation; one
read per search, through the resolver's per-request cache. A hit with no ``splitGen`` (every unsplit record
and the file's own manifest points) is never touched, and a search with no such hit never reads the store.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from app.telemetry.backend import METRICS_BACKEND

if TYPE_CHECKING:
    from app.modules.retrieval.split_content import SplitContentResolver

logger = logging.getLogger(__name__)

STALE_HITS = METRICS_BACKEND.counter(
    "pipeshub_split_stale_hits_total",
    "Search hits of a split file dropped because their generation is not the file's live one, or could not be checked",
    ["reason"],
)


def _gen(hit: dict[str, Any]) -> str | None:
    metadata = hit.get("metadata") if isinstance(hit, dict) else None
    gen = (metadata or {}).get("splitGen")
    return str(gen) if gen not in (None, "") else None


def _vrid(hit: dict[str, Any]) -> str | None:
    return ((hit.get("metadata") if isinstance(hit, dict) else None) or {}).get("virtualRecordId")


async def drop_stale_generations(hits: list[dict[str, Any]], resolver: SplitContentResolver) -> list[dict[str, Any]]:
    """The hits that belong to their file's live generation. Unknown state fails closed: if the live
    generation cannot be read, every generation-tagged hit is dropped."""
    split = [h for h in hits if _gen(h)]
    if not split:
        return hits
    try:
        live = await resolver.live_generation(sorted({v for h in split if (v := _vrid(h))}))
    except Exception:
        logger.exception("Could not read the live generation of split files; dropping their hits")
        STALE_HITS.inc("unreadable", value=len(split))
        return [h for h in hits if not _gen(h)]
    kept: list[dict[str, Any]] = []
    stale = 0
    for hit in hits:
        gen = _gen(hit)
        live_gen = live.get(_vrid(hit))
        if gen is None or (live_gen is not None and str(live_gen) == gen):
            kept.append(hit)
        else:
            stale += 1
    if stale:
        STALE_HITS.inc("stale", value=stale)
    return kept
