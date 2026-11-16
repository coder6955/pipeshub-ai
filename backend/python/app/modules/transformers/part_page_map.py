"""Rewrites a part's parsed container from the part PDF's own page numbers to the original file's.

A part is a window of the original PDF, parsed as a file of its own, so its blocks cite pages ``1..n``.
Everything downstream (citations, previews, the page oracle) speaks the original numbering, so the rewrite
runs once, first, before the container is validated, stored or embedded.
"""
from __future__ import annotations

import logging
import re
from collections import OrderedDict
from typing import TYPE_CHECKING

from app.models.blocks import Block, BlockGroup, BlockGroupChildren, BlocksContainer
from app.modules.transformers.block_container_validator import BlockContainerValidator

if TYPE_CHECKING:
    from collections.abc import Iterable

    from app.models.entities import Record
    from app.modules.indexing.split_state import PartInfo, SplitStore
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

_DIGITS = re.compile(r"\d+")
_SPACES = re.compile(r"\s+")
_LINES_CACHE_SIZE = 256
_running_lines: OrderedDict[tuple[str, str], tuple[str, ...]] = OrderedDict()


def normalise_line(text: str) -> str:
    """Case, whitespace and digits folded, so ``Page 12 of 900`` and ``page 13 of 900`` are one running line."""
    return _SPACES.sub(" ", _DIGITS.sub("", text).casefold()).strip()


def is_mapped(container: BlocksContainer) -> bool:
    """A container this map already rewrote. The map always leaves ``trailing_context`` a list (possibly empty),
    and a stored container keeps it, so an attempt resumed from stored blocks must not shift pages twice."""
    return container.trailing_context is not None


class PartPageOutOfRange(ValueError):
    """A mapped page lies outside the part's window (``pageStart`` .. ``pageEnd`` + overlap): the part was
    offset wrongly or parsed pages it does not own, and nothing downstream may index it."""


class PartPageMap:
    def __init__(self, part: PartInfo, running_lines: Iterable[str] = ()) -> None:
        self._part = part
        self._running = frozenset(n for n in map(normalise_line, running_lines) if n)

    def apply(self, container: BlocksContainer, validator: BlockContainerValidator | None = None) -> BlocksContainer:
        if is_mapped(container):
            return container
        offset = self._part.page_offset
        _shift_once(container, offset)

        blocks, groups = container.blocks, container.block_groups
        trailing_roots = _trailing_units(blocks, groups, self._part.page_end)
        block_root = {i: _root_of_block(blocks, groups, i) for i in range(len(blocks))}
        dead_groups = {i for i in range(len(groups)) if _root_of_group(groups, i) in trailing_roots}
        trailing_ids: set[int] = {i for i, root in block_root.items() if root in trailing_roots}
        dropped_ids: set[int] = set()
        for i, block in enumerate(blocks):
            if i not in trailing_ids and self._running and self._is_running_line(block):
                dropped_ids.add(i)

        kept_blocks = [i for i in range(len(blocks)) if i not in trailing_ids and i not in dropped_ids]
        block_map = {old: new for new, old in enumerate(kept_blocks)}
        group_map = _surviving_groups(groups, block_map, dead_groups)

        trailing = [blocks[i] for i in sorted(trailing_ids)]
        new_blocks = [blocks[i] for i in kept_blocks]
        new_groups = [groups[i] for i in sorted(group_map)]
        for old, new in block_map.items():
            block = blocks[old]
            block.index = new
            block.parent_index = group_map.get(block.parent_index) if block.parent_index is not None else None
            block.parent_block_index = (
                block_map.get(block.parent_block_index) if block.parent_block_index is not None else None
            )
        for old, new in group_map.items():
            group = groups[old]
            group.index = new
            group.parent_index = group_map.get(group.parent_index) if group.parent_index is not None else None
            group.children = _remap_children(group.children, block_map, group_map)
        for j, block in enumerate(trailing):
            # The index is the local address a reader uses (len(blocks) + j), not a position in ``blocks``.
            block.index = len(new_blocks) + j
            block.parent_index = None
            block.parent_block_index = None

        container.blocks = new_blocks
        container.block_groups = new_groups
        container.trailing_context = trailing
        self._assert_in_range(container)
        (validator or BlockContainerValidator(logger=logging.getLogger(__name__))).validate(container)
        return container

    def _assert_in_range(self, container: BlocksContainer) -> None:
        low, high = self._part.page_start, self._part.page_end + self._part.overlap_pages
        for item in (*container.blocks, *(container.trailing_context or ()), *container.block_groups):
            for page in _pages(item):
                if not low <= page <= high:
                    raise PartPageOutOfRange(
                        f"part {self._part.k} of {self._part.root}: page {page} on {type(item).__name__} "
                        f"{item.index} is outside {low}..{high}"
                    )

    def _is_running_line(self, block: Block) -> bool:
        text = block.data if isinstance(block.data, str) else None
        return bool(text) and normalise_line(text) in self._running


def _pages(item: Block | BlockGroup) -> list[int]:
    meta = item.citation_metadata
    if meta is None:
        return []
    return [*([meta.page_number] if meta.page_number is not None else []), *(meta.more_page_numbers or [])]


def _shift_once(container: BlocksContainer, offset: int) -> None:
    """Offset every distinct ``CitationMetadata`` once. The parsers share one metadata object between a table
    group and all its rows, so shifting per owner would apply the offset once per reference."""
    if not offset:
        return
    seen: set[int] = set()
    for item in (*container.blocks, *container.block_groups):
        meta = item.citation_metadata
        if meta is None or id(meta) in seen:
            continue
        seen.add(id(meta))
        if meta.page_number is not None:
            meta.page_number += offset
        if meta.more_page_numbers:
            meta.more_page_numbers = [p + offset for p in meta.more_page_numbers]


Root = tuple[str, int]


def _root_of_group(groups: list[BlockGroup], i: int) -> Root:
    seen = {i}
    while (parent := groups[i].parent_index) is not None and 0 <= parent < len(groups) and parent not in seen:
        seen.add(parent)
        i = parent
    return ("g", i)


def _root_of_block(blocks: list[Block], groups: list[BlockGroup], i: int) -> Root:
    seen = {i}
    while (parent := blocks[i].parent_block_index) is not None and 0 <= parent < len(blocks) and parent not in seen:
        seen.add(parent)
        i = parent
    group = blocks[i].parent_index
    if group is not None and 0 <= group < len(groups):
        return _root_of_group(groups, group)
    return ("b", i)


def _trailing_units(blocks: list[Block], groups: list[BlockGroup], page_end: int) -> set[Root]:
    """The top-level units that start after ``page_end``. A unit is a block with no parent, or a group chain
    with everything under it: it is core when its first page is inside the core, whatever pages its rows reach."""
    first: dict[Root, int] = {}
    members: dict[Root, list[int]] = {}
    for i, block in enumerate(blocks):
        root = _root_of_block(blocks, groups, i)
        members.setdefault(root, []).extend(_pages(block)[:1])
        if root == ("b", i) and (pages := _pages(block)):
            first[root] = pages[0]
    for i, group in enumerate(groups):
        root = _root_of_group(groups, i)
        members.setdefault(root, []).extend(_pages(group)[:1])
        if root == ("g", i) and (pages := _pages(group)):
            first[root] = pages[0]
    for root, pages in members.items():
        if root not in first and pages:
            first[root] = min(pages)
    return {root for root, page in first.items() if page > page_end}


def _surviving_groups(
    groups: list[BlockGroup], block_map: dict[int, int], dead: set[int] = frozenset(),
) -> dict[int, int]:
    """Old group index -> new index for the groups that keep something. A group that had children and lost all
    of them (its content sits in the trailing context) goes; a group that never had children stays."""
    alive: dict[int, bool] = {}

    def survives(i: int) -> bool:
        if i in dead:
            return False
        if i in alive:
            return alive[i]
        alive[i] = True  # a cycle must not recurse forever; the validator reports it
        children = groups[i].children
        if children is None or not (children.block_ranges or children.block_group_ranges):
            alive[i] = True
            return True
        result = any(b in block_map for r in children.block_ranges for b in range(r.start, r.end + 1)) or any(
            0 <= g < len(groups) and survives(g) for r in children.block_group_ranges for g in range(r.start, r.end + 1)
        )
        alive[i] = result
        return result

    kept = [i for i in range(len(groups)) if survives(i)]
    return {old: new for new, old in enumerate(kept)}


def _remap_children(
    children: BlockGroupChildren | None, block_map: dict[int, int], group_map: dict[int, int],
) -> BlockGroupChildren | None:
    if children is None:
        return None
    blocks = [block_map[b] for r in children.block_ranges for b in range(r.start, r.end + 1) if b in block_map]
    groups = [group_map[g] for r in children.block_group_ranges for g in range(r.start, r.end + 1) if g in group_map]
    return BlockGroupChildren.from_indices(blocks, groups)


async def part_page_map_for(
    record: Record, store: SplitStore | None,
) -> PartPageMap | None:
    """The map of *record* when it is a part, else None. Running lines come from the plan row and are cached
    per process and generation: they do not change within one."""
    from app.modules.indexing.split_state import (
        PartInfoMissing,
        cached_part_info,
        is_part,
    )

    part = cached_part_info(record)
    if part is None:
        if is_part(record):
            raise PartInfoMissing("a part reached the page map before its part document was read")
        return None
    key = (part.root, part.gen)
    lines = _running_lines.get(key)
    if lines is None:
        row = await store.get(part.root) if store is not None else None
        lines = row.running_lines if row is not None and row.gen == part.gen else ()
        _running_lines[key] = lines
        while len(_running_lines) > _LINES_CACHE_SIZE:
            _running_lines.popitem(last=False)
    return PartPageMap(part, lines)


def clear_running_lines_cache() -> None:
    _running_lines.clear()


async def map_part_container(record: Record, graph: IGraphDBProvider, logger: logging.Logger | None = None) -> None:
    """Rewrite ``record.block_containers`` in place when *record* is a part; a no-op for every other record,
    and for a container a previous attempt already mapped."""
    container = record.block_containers
    if container is None:
        return
    from app.modules.indexing.split_state import SplitStore

    page_map = await part_page_map_for(record, SplitStore(graph))
    if page_map is None:
        return
    page_map.apply(container, BlockContainerValidator(
        logger=logger, record_id=getattr(record, "id", None), virtual_record_id=record.virtual_record_id,
        record_name=getattr(record, "record_name", None),
    ))
