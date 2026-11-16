"""What a part's handler asks before doing any work: is the file still there, and is this still the live generation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.modules.indexing.split_purge import FILE_REMOVED_REASON, SUPERSEDED_REASON
from app.modules.indexing.split_state import SplitState

if TYPE_CHECKING:
    from app.modules.indexing.split_state import PartInfo, SplitStore
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider


@dataclass(frozen=True)
class PartContext:
    """What the plan says about this part: whether its pages need OCR."""

    k: int
    ocr: bool | None


@dataclass(frozen=True)
class PartStop:
    """The part is not indexed: its handler marks it ``AUTO_INDEX_OFF`` with this reason."""

    reason: str


async def check_part(graph: IGraphDBProvider, store: SplitStore, part: PartInfo) -> PartContext | PartStop:
    """Reads the root's live holders first, then the plan row. Both reads raise on a store error,
    so an outage retries the part instead of reading as a removed file. *part* is the record's
    ``split_state.part_info``."""
    root = part.root
    holders = await graph.get_records_by_virtual_record_id(root, raise_on_error=True)
    if not holders:
        return PartStop(FILE_REMOVED_REASON)
    row = await store.get(root)
    if row is None or row.gen != part.gen or row.state is SplitState.PURGING:
        return PartStop(SUPERSEDED_REASON)
    spec = next((p for p in row.parts if p.k == part.k), None)
    if spec is None:
        return PartStop(SUPERSEDED_REASON)
    return PartContext(spec.k, spec.ocr)
