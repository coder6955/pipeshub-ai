"""T1 of S2b: a part's container is rewritten to the original file's pages, its overlap becomes trailing
context, and the container stays valid."""
from __future__ import annotations

import copy

import pytest

from app.models.blocks import (
    Block,
    BlockGroup,
    BlockGroupChildren,
    BlocksContainer,
    BlockType,
    CitationMetadata,
    DataFormat,
    GroupType,
    TableMetadata,
    TableRowMetadata,
)
from app.modules.indexing.split_state import (
    PartInfo,
    SplitRow,
    SplitState,
    SplitStore,
    clear_part_cache,
    remember_part,
)
from app.modules.transformers.block_container_validator import BlockContainerValidator
from app.modules.transformers.part_page_map import (
    PartPageMap,
    clear_running_lines_cache,
    is_mapped,
    map_part_container,
    normalise_line,
    part_page_map_for,
)
from tests.support.split_fakes import FakeGraph

PART = PartInfo(
    key="part-key-2", org_id="org-1", root="V", gen="G", k=2, page_start=11, page_end=20, page_offset=10,
    overlap_pages=1, owner_connector_id="kb-1", vrid="V2",
)


def text(i: int, page: int, data: str, parent_index: int | None = None) -> Block:
    return Block(
        index=i, type=BlockType.TEXT, format=DataFormat.TXT, data=data, parent_index=parent_index,
        citation_metadata=CitationMetadata(page_number=page),
    )


def row(i: int, page: int, parent: int) -> Block:
    return Block(
        index=i, type=BlockType.TABLE_ROW, format=DataFormat.JSON, data={"row_natural_language_text": f"row {i}"},
        parent_index=parent, citation_metadata=CitationMetadata(page_number=page),
        table_row_metadata=TableRowMetadata(row_number=i),
    )


def golden() -> BlocksContainer:
    """Parsed part pages 1..11 (original 11..21): a heading group with two paragraphs, a running line on every
    page, a table crossing into the overlap page, and a paragraph on the overlap page."""
    blocks = [
        text(0, 1, "Annual Report 12"),                      # running line
        text(1, 1, "Intro paragraph", parent_index=0),
        text(2, 2, "Second paragraph", parent_index=0),
        text(3, 2, "annual   report 13"),                    # running line, other case, spacing and digits
        row(4, 10, 1),
        row(5, 11, 1),                                       # table row on the overlap page
        text(6, 11, "Spill over text"),                      # overlap page
        text(7, 11, "Annual Report 21"),                     # running line on the overlap page: kept as context
    ]
    groups = [
        BlockGroup(
            index=0, type=GroupType.TEXT_SECTION, citation_metadata=CitationMetadata(page_number=1),
            children=BlockGroupChildren.from_indices(block_indices=[1, 2]),
        ),
        BlockGroup(
            index=1, type=GroupType.TABLE, table_metadata=TableMetadata(num_of_cells=2),
            citation_metadata=CitationMetadata(page_number=10, more_page_numbers=[11]),
            children=BlockGroupChildren.from_indices(block_indices=[4, 5]),
        ),
        BlockGroup(
            index=2, type=GroupType.TEXT_SECTION, parent_index=None, citation_metadata=CitationMetadata(page_number=11),
            children=BlockGroupChildren.from_indices(block_indices=[]),
        ),
    ]
    return BlocksContainer(blocks=blocks, block_groups=groups)


def mapped(running: tuple[str, ...] = ("Annual Report 1",)) -> BlocksContainer:
    return PartPageMap(PART, running).apply(golden())


def test_pages_are_offset_on_blocks_groups_and_more_pages() -> None:
    c = mapped(())
    pages = {b.data if isinstance(b.data, str) else b.data["row_natural_language_text"]: b.citation_metadata.page_number
             for b in (*c.blocks, *c.trailing_context)}
    assert pages["Intro paragraph"] == 11 and pages["Second paragraph"] == 12 and pages["row 4"] == 20
    assert c.block_groups[0].citation_metadata.page_number == 11
    table = next(g for g in c.block_groups if g.type == GroupType.TABLE)
    assert (table.citation_metadata.page_number, table.citation_metadata.more_page_numbers) == (20, [21])


def test_overlap_blocks_move_to_trailing_context_with_original_pages() -> None:
    c = mapped(())
    # the table starts on page 20, so it stays whole in core, with its row that spills onto page 21
    assert [b.citation_metadata.page_number for b in c.trailing_context] == [21, 21]
    assert {b.data for b in c.trailing_context} == {"Spill over text", "Annual Report 21"}
    assert [b.citation_metadata.page_number for b in c.blocks if b.type == BlockType.TABLE_ROW] == [20, 21]


def test_a_trailing_block_is_addressed_after_the_last_block_and_has_no_parents() -> None:
    c = mapped(())
    assert [b.index for b in c.trailing_context] == [len(c.blocks) + j for j in range(len(c.trailing_context))]
    assert all(b.parent_index is None and b.parent_block_index is None for b in c.trailing_context)


def test_running_lines_are_dropped_by_case_space_and_digit_folding_from_core_only() -> None:
    c = mapped(("ANNUAL report 99",))
    assert not any(isinstance(b.data, str) and "nnual" in b.data for b in c.blocks)
    assert any(b.data == "Annual Report 21" for b in c.trailing_context)
    assert normalise_line("  Page 12  OF   900 ") == normalise_line("page 13 of 901")


def test_renumbering_is_dense_and_the_container_validates() -> None:
    c = mapped()
    assert [b.index for b in c.blocks] == list(range(len(c.blocks)))
    assert [g.index for g in c.block_groups] == list(range(len(c.block_groups)))
    BlockContainerValidator().validate(c)
    table = next(g for g in c.block_groups if g.type == GroupType.TABLE)
    assert [(r.start, r.end) for r in table.children.block_ranges] == [(2, 3)]
    assert c.blocks[2].parent_index == table.index
    section = c.block_groups[0]
    assert [(r.start, r.end) for r in section.children.block_ranges] == [(0, 1)]


def test_a_group_that_starts_on_the_overlap_page_goes_with_its_children() -> None:
    c = golden()
    c.block_groups.append(BlockGroup(index=3, type=GroupType.TEXT_SECTION, children=BlockGroupChildren.from_indices(block_indices=[6])))
    c.blocks[6].parent_index = 3
    out = PartPageMap(PART).apply(c)
    BlockContainerValidator().validate(out)
    assert len(out.block_groups) == 2  # a unit that starts on the overlap page is gone with its only child
    assert out.trailing_context[1].parent_index is None


def test_a_core_only_part_still_gets_a_trailing_context_so_a_resume_is_not_mapped_twice() -> None:
    c = BlocksContainer(blocks=[text(0, 1, "only")])
    out = PartPageMap(PART).apply(c)
    assert out.trailing_context == [] and is_mapped(out)
    assert out.blocks[0].citation_metadata.page_number == 11


def test_mapping_twice_shifts_pages_once() -> None:
    once = mapped()
    snapshot = once.model_dump()
    twice = PartPageMap(PART, ("Annual Report 1",)).apply(copy.deepcopy(once))
    assert twice.model_dump() == snapshot


def test_a_stored_container_round_trips_as_mapped() -> None:
    stored = BlocksContainer.model_validate(mapped().model_dump(mode="json", exclude_none=True))
    assert is_mapped(stored)
    assert PartPageMap(PART).apply(stored) is stored


def test_an_unsplit_container_dumps_without_the_field() -> None:
    assert "trailing_context" not in BlocksContainer(blocks=[text(0, 1, "x")]).model_dump(exclude_none=True)


def test_a_part_without_overlap_moves_nothing() -> None:
    part = PartInfo(**{**PART.__dict__, "overlap_pages": 0})
    c = BlocksContainer(blocks=[text(0, 10, "last core page")])
    assert PartPageMap(part).apply(c).trailing_context == []


@pytest.fixture(autouse=True)
def _clean() -> None:
    clear_part_cache()
    clear_running_lines_cache()


class _Record:
    id = "part-key-2"
    connector_id = "pdfparts_org-1"
    virtual_record_id = "V2"
    record_name = "big.pdf (part 2)"

    def __init__(self, container: BlocksContainer) -> None:
        self.block_containers = container


@pytest.mark.asyncio
async def test_the_pipeline_helper_maps_a_part_with_the_running_lines_of_its_plan_row() -> None:
    graph = FakeGraph()
    row_doc = SplitRow(
        v="V", org_id="org-1", gen="G", parts=(), state=SplitState.INDEXING_PARTS, running_lines=("Annual Report",),
    )
    assert await SplitStore(graph).create(row_doc)
    remember_part(PART)
    rec = _Record(golden())
    await map_part_container(rec, graph)
    assert not any(isinstance(b.data, str) and "nnual" in b.data for b in rec.block_containers.blocks)
    assert rec.block_containers.trailing_context


@pytest.mark.asyncio
async def test_a_record_that_is_not_a_part_is_untouched() -> None:
    class Plain(_Record):
        connector_id = "kb-1"

    rec = Plain(golden())
    before = rec.block_containers.model_dump()
    await map_part_container(rec, FakeGraph())
    assert rec.block_containers.model_dump() == before


@pytest.mark.asyncio
async def test_a_part_whose_document_was_not_read_fails_closed() -> None:
    from app.modules.indexing.split_state import PartInfoMissing

    with pytest.raises(PartInfoMissing):
        await part_page_map_for(_Record(golden()), None)


def test_split_row_running_lines_round_trip_and_are_absent_when_empty() -> None:
    row_doc = SplitRow(v="V", org_id="o", gen="G", parts=(), state=SplitState.SPLITTING, running_lines=("a", "b"))
    assert SplitRow.from_doc({**row_doc.to_doc(), "_key": "V"}).running_lines == ("a", "b")
    assert "runningLines" not in SplitRow(v="V", org_id="o", gen="G", parts=(), state=SplitState.SPLITTING).to_doc()
