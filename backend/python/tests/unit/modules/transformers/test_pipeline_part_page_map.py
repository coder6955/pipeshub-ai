"""S2b rule 1 in ``IndexingPipeline.apply``: the map is the first step, before validation and indexing."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.blocks import (
    Block,
    BlocksContainer,
    BlockType,
    CitationMetadata,
    DataFormat,
)
from app.modules.indexing.split_state import PartInfo, clear_part_cache, remember_part
from app.modules.transformers.pipeline import IndexingPipeline
from app.modules.transformers.transformer import STORE_ONLY
from tests.support.split_fakes import FakeGraph
from tests.unit.modules.transformers.test_pipeline import _make_ctx, _make_record

PART = PartInfo(
    key="rec-1", org_id="org-1", root="V", gen="G", k=2, page_start=11, page_end=20, page_offset=10,
    overlap_pages=1, owner_connector_id="kb-1", vrid="vr-1",
)


def _container() -> BlocksContainer:
    def block(i: int, page: int, text: str) -> Block:
        return Block(
            index=i, type=BlockType.TEXT, format=DataFormat.TXT, data=text,
            citation_metadata=CitationMetadata(page_number=page),
        )

    return BlocksContainer(blocks=[block(0, 1, "core"), block(1, 11, "spill")])


async def _run(connector_id: str) -> BlocksContainer:
    extraction = AsyncMock()
    extraction.graph_provider = FakeGraph()
    sink = AsyncMock()
    seen: list[BlocksContainer] = []
    sink.index = AsyncMock(side_effect=lambda ctx: seen.append(ctx.record.block_containers))
    pipeline = IndexingPipeline(extraction, sink, stage_ingress=MagicMock(on_indexed=AsyncMock()))
    record = _make_record()
    record.id, record.connector_id, record.virtual_record_id = "rec-1", connector_id, "vr-1"
    record.block_containers = _container()
    ctx = _make_ctx(record)
    ctx.settings = {STORE_ONLY: True}
    await pipeline.apply(ctx)
    return seen[0]


@pytest.fixture(autouse=True)
def _clean() -> None:
    clear_part_cache()


@pytest.mark.asyncio
async def test_a_part_is_mapped_before_it_is_indexed() -> None:
    remember_part(PART)
    out = await _run("pdfparts_org-1")
    assert [b.citation_metadata.page_number for b in out.blocks] == [11]
    assert len(out.trailing_context) == 1


@pytest.mark.asyncio
async def test_an_ordinary_record_reaches_the_index_unmapped() -> None:
    out = await _run("kb-1")
    assert [b.citation_metadata.page_number for b in out.blocks] == [1, 11] and out.trailing_context is None


class TestSplitParentDispatch:
    """The manifest's indexed write hands neither stage over; the roll-up of its parts re-drives them."""

    @pytest.mark.asyncio
    async def test_both_stages_are_deferred_whatever_the_first_use_flag_says(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from app.modules.indexing.split_parent_flag import (
            begin_split_parent,
            end_split_parent,
        )
        from app.modules.transformers.pipeline import IndexingPipeline

        ingress = MagicMock()
        ingress.on_indexed = AsyncMock()
        pipeline = IndexingPipeline(MagicMock(), MagicMock(), ingress)
        ctx = MagicMock(event_type="newRecord", settings={})
        begin_split_parent("gen-1")
        try:
            await pipeline._dispatch_stages(ctx)
        finally:
            end_split_parent()
        ingress.on_indexed.assert_awaited_once_with(ctx.record, trigger="newRecord", defer_classify=True, defer_entities=True)

        ingress.on_indexed.reset_mock()
        await pipeline._dispatch_stages(ctx)
        ingress.on_indexed.assert_awaited_once_with(ctx.record, trigger="newRecord", defer_classify=False)
