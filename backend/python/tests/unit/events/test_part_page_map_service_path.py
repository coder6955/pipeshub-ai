"""S2b rule 1 on the parsing-service path: the container the service returns for a part is rewritten to the
original pages before it is indexed."""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.blocks import (
    Block,
    BlocksContainer,
    BlockType,
    CitationMetadata,
    DataFormat,
)
from app.modules.indexing.split_state import PartInfo, clear_part_cache, remember_part
from app.services.parsing.interface import ParseResult, ParserProvider
from tests.unit.events.test_orchestrator_flow import (
    _make_event_data,
    _make_event_processor,
)

PART = PartInfo(
    key="rec-1", org_id="org-1", root="V", gen="G", k=2, page_start=11, page_end=20, page_offset=10,
    overlap_pages=1, owner_connector_id="kb-1", vrid="vr-1",
)


def _page_block(i: int, page: int, text: str) -> Block:
    return Block(
        index=i, type=BlockType.TEXT, format=DataFormat.TXT, data=text,
        citation_metadata=CitationMetadata(page_number=page),
    )


def _graph(connector_id: str) -> MagicMock:
    graph = MagicMock()
    record = {
        "_key": "rec-1", "orgId": "org-1", "recordName": "big.pdf", "recordType": "FILE",
        "indexingStatus": "NOT_STARTED", "mimeType": "application/pdf", "connectorName": None, "origin": "UPLOAD",
        "externalRecordId": "ext-rec-1", "connectorId": connector_id, "createdAtTimestamp": 1, "updatedAtTimestamp": 1,
    }
    graph.get_document = AsyncMock(side_effect=lambda _key, collection, **_kw: dict(record) if collection == "records" else None)
    for name in ("batch_upsert_nodes", "batch_update_nodes", "update_node"):
        setattr(graph, name, AsyncMock(return_value=True))
    graph.get_departments = AsyncMock(return_value=[])
    graph.find_duplicate_records = AsyncMock(return_value=[])
    return graph


async def _indexed_container(connector_id: str) -> BlocksContainer:
    container = BlocksContainer(blocks=[_page_block(0, 1, "core"), _page_block(1, 11, "spill")])
    client = MagicMock(circuit_open=False)
    client.parse = AsyncMock(return_value=ParseResult(
        block_container=container, provider_used=ParserProvider.DEFAULT, metadata={},
    ))
    sink = MagicMock()
    sink.index = AsyncMock()
    ep = _make_event_processor(
        parsing_client=client, extraction_client=MagicMock(), sink_orchestrator=sink, graph_provider=_graph(connector_id),
    )
    with patch.dict(os.environ, {"USE_PARSING_SERVICE": "true"}):
        _ = [event async for event in ep.on_event(_make_event_data())]
    (ctx,), _kw = sink.index.await_args
    return ctx.record.block_containers


@pytest.fixture(autouse=True)
def _clean() -> None:
    clear_part_cache()


@pytest.mark.asyncio
async def test_a_part_returned_by_the_parsing_service_is_indexed_with_original_pages() -> None:
    remember_part(PART)
    out = await _indexed_container("pdfparts_org-1")
    assert [b.citation_metadata.page_number for b in out.blocks] == [11]
    assert [b.citation_metadata.page_number for b in out.trailing_context] == [21]


@pytest.mark.asyncio
async def test_any_other_record_is_indexed_exactly_as_parsed() -> None:
    out = await _indexed_container("connector-1")
    assert [b.citation_metadata.page_number for b in out.blocks] == [1, 11]
    assert out.trailing_context is None
