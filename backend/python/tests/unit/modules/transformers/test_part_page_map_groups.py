"""Fix of the PartPageMap table data loss: blocks are offset once by identity, the overlap decision is per
top-level unit, groups stay intact, and every mapped page lies inside the part's window.

The PdfPlumber parser hands one ``CitationMetadata`` object to a table group and to every one of its rows, so a
map that offsets per owner shifts it once per reference.
"""
from __future__ import annotations

import random

import pytest

from app.models.blocks import (
    Block,
    BlockGroup,
    BlockGroupChildren,
    BlocksContainer,
    BlockSubType,
    BlockType,
    CitationMetadata,
    DataFormat,
    GroupType,
    ImageMetadata,
    ListMetadata,
    TableMetadata,
)
from app.modules.indexing.split_state import PartInfo
from app.modules.transformers.block_container_validator import BlockContainerValidator
from app.modules.transformers.part_page_map import PartPageMap, PartPageOutOfRange

# core pages 11..20 (offset 10), one overlap page 21
PART = PartInfo(
    key="k", org_id="o", root="V", gen="G", k=2, page_start=11, page_end=20, page_offset=10,
    overlap_pages=1, owner_connector_id="kb", vrid="V2",
)
LAST = PartInfo(**{**PART.__dict__, "overlap_pages": 0})


def _text(blocks: list[Block], page: int, data: str = "t") -> Block:
    b = Block(index=len(blocks), type=BlockType.TEXT, format=DataFormat.TXT, data=data,
              citation_metadata=CitationMetadata(page_number=page))
    blocks.append(b)
    return b


def _table(blocks: list[Block], groups: list[BlockGroup], page: int, rows: int, row_pages: list[int] | None = None) -> BlockGroup:
    """Built as PdfPlumber ``create_blocks`` does: one CitationMetadata shared by the group and every row."""
    shared = CitationMetadata(page_number=page)
    group = BlockGroup(index=len(groups), type=GroupType.TABLE, table_metadata=TableMetadata(num_of_rows=rows),
                       citation_metadata=shared)
    first = len(blocks)
    for r in range(rows):
        meta = shared if row_pages is None else CitationMetadata(page_number=row_pages[r])
        blocks.append(Block(index=len(blocks), type=BlockType.TABLE_ROW, format=DataFormat.JSON,
                            data={"row_natural_language_text": f"row {r}"}, parent_index=group.index, citation_metadata=meta))
    group.children = BlockGroupChildren.from_indices(block_indices=list(range(first, len(blocks))))
    groups.append(group)
    return group


def _list(blocks: list[Block], groups: list[BlockGroup], page: int, items: int) -> BlockGroup:
    group = BlockGroup(index=len(groups), type=GroupType.LIST, list_metadata=ListMetadata(item_count=items),
                       citation_metadata=CitationMetadata(page_number=page))
    first = len(blocks)
    for i in range(items):
        blocks.append(Block(index=len(blocks), type=BlockType.TEXT, sub_type=BlockSubType.LIST_ITEM, format=DataFormat.TXT,
                            data=f"item {i}", parent_index=group.index, citation_metadata=CitationMetadata(page_number=page)))
    group.children = BlockGroupChildren.from_indices(block_indices=list(range(first, len(blocks))))
    groups.append(group)
    return group


def _image(blocks: list[Block], page: int) -> Block:
    b = Block(index=len(blocks), type=BlockType.IMAGE, format=DataFormat.BASE64, data={"uri": "data:image/png;base64,AA=="},
              image_metadata=ImageMetadata(), citation_metadata=CitationMetadata(page_number=page))
    blocks.append(b)
    return b


def _check(c: BlocksContainer, part: PartInfo) -> None:
    BlockContainerValidator().validate(c)
    hi = part.page_end + part.overlap_pages
    for item in (*c.blocks, *c.block_groups, *(c.trailing_context or [])):
        meta = item.citation_metadata
        pages = [meta.page_number, *(meta.more_page_numbers or [])] if meta and meta.page_number is not None else []
        assert all(part.page_start <= p <= hi for p in pages), pages


def _table_rows(c: BlocksContainer) -> list[Block]:
    return [b for b in c.blocks if b.type == BlockType.TABLE_ROW]


def test_single_page_table_is_offset_once_and_keeps_its_group_and_rows() -> None:
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    _text(blocks, 1)
    _table(blocks, groups, 3, rows=4)
    out = PartPageMap(PART).apply(BlocksContainer(blocks=blocks, block_groups=groups))
    assert [b.citation_metadata.page_number for b in _table_rows(out)] == [13] * 4
    assert out.block_groups[0].citation_metadata.page_number == 13
    assert out.trailing_context == []
    assert {b.parent_index for b in _table_rows(out)} == {0}
    assert [(r.start, r.end) for r in out.block_groups[0].children.block_ranges] == [(1, 4)]
    _check(out, PART)


def test_table_spilling_onto_the_overlap_page_stays_whole_in_core() -> None:
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    _table(blocks, groups, 10, rows=3, row_pages=[10, 10, 11])
    groups[0].citation_metadata.more_page_numbers = [11]
    out = PartPageMap(PART).apply(BlocksContainer(blocks=blocks, block_groups=groups))
    assert len(out.block_groups) == 1 and len(_table_rows(out)) == 3
    assert [b.citation_metadata.page_number for b in out.blocks] == [20, 20, 21]
    assert out.block_groups[0].citation_metadata.more_page_numbers == [21]
    assert out.trailing_context == []
    _check(out, PART)


def test_table_starting_on_the_overlap_page_goes_to_trailing_context_only() -> None:
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    _text(blocks, 2)
    _table(blocks, groups, 11, rows=3)
    out = PartPageMap(PART).apply(BlocksContainer(blocks=blocks, block_groups=groups))
    assert len(out.blocks) == 1 and out.block_groups == []
    assert [b.citation_metadata.page_number for b in out.trailing_context] == [21, 21, 21]
    assert [b.index for b in out.trailing_context] == [1, 2, 3]
    _check(out, PART)


def test_a_group_with_no_children_on_the_overlap_page_is_dropped_with_its_unit() -> None:
    """the overlap page's content is not embedded or classified by this part, a childless group included."""
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    _text(blocks, 2)
    groups.append(BlockGroup(index=0, type=GroupType.TEXT_SECTION, data="overlap group", citation_metadata=CitationMetadata(page_number=11)))
    groups.append(BlockGroup(index=1, type=GroupType.TEXT_SECTION, data="core group", citation_metadata=CitationMetadata(page_number=3)))
    out = PartPageMap(PART).apply(BlocksContainer(blocks=blocks, block_groups=groups))
    assert [g.data for g in out.block_groups] == ["core group"]
    assert [g.index for g in out.block_groups] == [0]
    _check(out, PART)


def test_list_group_and_images_are_one_unit_each() -> None:
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    _list(blocks, groups, 5, 3)
    _image(blocks, 6)
    _list(blocks, groups, 11, 2)   # overlap
    _image(blocks, 11)             # overlap
    out = PartPageMap(PART).apply(BlocksContainer(blocks=blocks, block_groups=groups))
    assert [g.type for g in out.block_groups] == [GroupType.LIST]
    assert [b.type for b in out.blocks] == [BlockType.TEXT] * 3 + [BlockType.IMAGE]
    assert out.blocks[3].citation_metadata.page_number == 16
    assert [b.citation_metadata.page_number for b in out.trailing_context] == [21, 21, 21]
    _check(out, PART)


def test_last_part_has_no_trailing_context() -> None:
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    _table(blocks, groups, 10, rows=5)
    _list(blocks, groups, 9, 2)
    out = PartPageMap(LAST).apply(BlocksContainer(blocks=blocks, block_groups=groups))
    assert out.trailing_context == [] and len(out.block_groups) == 2 and len(out.blocks) == 7
    _check(out, LAST)


def test_a_page_outside_the_window_fails_with_a_typed_error() -> None:
    blocks: list[Block] = []
    _text(blocks, 12)   # maps to 22, past the overlap window
    with pytest.raises(PartPageOutOfRange):
        PartPageMap(PART).apply(BlocksContainer(blocks=blocks))
    blocks = []
    _text(blocks, 11)   # a last part has no overlap page
    with pytest.raises(PartPageOutOfRange):
        PartPageMap(LAST).apply(BlocksContainer(blocks=blocks))


def test_live_shape_table_rows_referenced_from_groups_are_neither_multiplied_nor_lost() -> None:
    """Part 3 of the ARM manual: pages 1001..1138, no overlap, many tables with shared citation objects."""
    part = PartInfo(**{**PART.__dict__, "k": 3, "page_start": 1001, "page_end": 1138, "page_offset": 1000, "overlap_pages": 0})
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    for page in range(1, 139):
        _text(blocks, page)
        if page % 5 == 0:
            _table(blocks, groups, page, rows=8)
        if page % 7 == 0:
            _list(blocks, groups, page, 3)
    tables = sum(g.type == GroupType.TABLE for g in groups)
    out = PartPageMap(part).apply(BlocksContainer(blocks=blocks, block_groups=groups))
    assert out.trailing_context == []
    assert sum(g.type == GroupType.TABLE for g in out.block_groups) == tables > 0
    assert len(_table_rows(out)) == tables * 8
    assert max(b.citation_metadata.page_number for b in out.blocks) == 1138
    _check(out, part)


def _random_container(rng: random.Random, pages: int) -> BlocksContainer:
    blocks: list[Block] = []
    groups: list[BlockGroup] = []
    for page in range(1, pages + 1):
        for _ in range(rng.randint(0, 3)):
            kind = rng.choice(["text", "table", "list", "image"])
            if kind == "text":
                _text(blocks, page)
            elif kind == "image":
                _image(blocks, page)
            elif kind == "list":
                _list(blocks, groups, page, rng.randint(1, 4))
            else:
                rows = rng.randint(1, 6)
                spill = page < pages and rng.random() < 0.3
                if spill:
                    g = _table(blocks, groups, page, rows, row_pages=[page + (1 if r == rows - 1 else 0) for r in range(rows)])
                    g.citation_metadata.more_page_numbers = [page + 1]
                else:
                    _table(blocks, groups, page, rows)
    return BlocksContainer(blocks=blocks, block_groups=groups)


@pytest.mark.parametrize("seed", range(40))
def test_property_each_item_is_offset_once_pages_in_range_groups_kept(seed: int) -> None:
    rng = random.Random(seed)
    last = rng.random() < 0.3
    part = LAST if last else PART
    parsed_pages = 10 + (0 if last else 1)
    c = _random_container(rng, parsed_pages)
    expected_pages = {b.id: b.citation_metadata.page_number + part.page_offset for b in c.blocks}
    expected_more = {g.id: [p + part.page_offset for p in g.citation_metadata.more_page_numbers or []] for g in c.block_groups}
    unit_start = {g.id: g.citation_metadata.page_number + part.page_offset for g in c.block_groups}
    child_count = {g.id: sum(r.end - r.start + 1 for r in g.children.block_ranges) for g in c.block_groups}
    out = PartPageMap(part).apply(c)

    for b in (*out.blocks, *out.trailing_context):
        assert b.citation_metadata.page_number == expected_pages[b.id]
    assert len(out.blocks) + len(out.trailing_context) == len(expected_pages)
    for g in out.block_groups:
        assert unit_start[g.id] <= part.page_end
        assert g.citation_metadata.more_page_numbers == (expected_more[g.id] or None)
        assert g.children is not None
        assert sum(r.end - r.start + 1 for r in g.children.block_ranges) == child_count[g.id]  # kept whole
        for r in g.children.block_ranges:
            assert all(out.blocks[i].parent_index == g.index for i in range(r.start, r.end + 1))
    kept = {g.id for g in out.block_groups}
    assert kept == {gid for gid, start in unit_start.items() if start <= part.page_end}
    assert all(b.parent_index is None for b in out.trailing_context)
    if last:
        assert out.trailing_context == []
    _check(out, part)
