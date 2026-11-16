"""A part record with no ``contentSplitParts`` document fails closed for that record only: the
page-level recovery loops skip and count it, warn at most once per pass, and carry on with the rest of the page."""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames, OriginTypes, ProgressStatus
from app.modules.indexing.owner_connector import PartSkips, prime_readable
from app.modules.indexing.split_state import PartInfoMissing, clear_part_cache
from tests.unit.test_indexing_main import _sweep_graph
from tests.unit.test_stranded_sweep_parked_while_off import (
    FakeGraph,
    Producer,
    _parked,
    _sweep,
)


@pytest.fixture(autouse=True)
def _fresh() -> None:
    clear_part_cache()


def _queued(key: str, **fields: object) -> dict:
    return _parked(key, indexingStatus=ProgressStatus.QUEUED.value, reason=None, **fields)


def _orphan_part(key: str = "a-part") -> dict:
    return {"_key": key, "connectorId": "pdfparts_org-1", "origin": OriginTypes.UPLOAD.value, "virtualRecordId": "V_k"}


async def test_the_stranded_sweep_skips_a_part_without_its_document_and_republishes_the_rest() -> None:
    graph, producer = FakeGraph(), Producer()
    graph.records["a-part"] = _queued("a-part", connector_id="pdfparts_org-1", origin=OriginTypes.UPLOAD.value, virtualRecordId="V_k")
    graph.records["r1"] = _queued("r1")

    await _sweep(graph, producer)

    assert "r1" in producer.sent
    assert "a-part" not in producer.sent


async def test_the_inactive_connector_sweep_skips_a_part_without_its_document_and_parks_the_rest() -> None:
    graph = _sweep_graph(
        {ProgressStatus.QUEUED.value: [_orphan_part(), {"_key": "q1", "connectorId": "dead", "origin": "CONNECTOR"}]},
        active_ids=set(),
    )
    ordinary = graph.get_document.side_effect

    async def get_document(key, collection, **kwargs):  # noqa: ANN202
        return None if collection == CollectionNames.CONTENT_SPLIT_PARTS.value else ordinary(key, collection, **kwargs)

    graph.get_document = AsyncMock(side_effect=get_document)
    from app.indexing_main import _sweep_queued_records_for_inactive_connectors

    swept = await _sweep_queued_records_for_inactive_connectors(graph_provider=graph, logger=MagicMock(), page_size=100, org_id="org-1")

    assert swept == 1
    assert [c.args[0] for c in graph.update_node.await_args_list] == ["q1"]


async def test_a_skipped_part_is_counted_and_warned_once_per_pass(caplog: pytest.LogCaptureFixture) -> None:
    graph = FakeGraph()
    skips = PartSkips()
    page = [_orphan_part(), _orphan_part()]

    with caplog.at_level(logging.WARNING):
        first = await prime_readable(page, graph, skips)
        again = await prime_readable(page, graph, skips)

    assert first == again == []
    assert skips.count == 1
    assert len([r for r in caplog.records if "a-part" in r.getMessage()]) == 1


async def test_a_store_error_still_propagates_so_the_pass_is_retried() -> None:
    graph = FakeGraph()
    graph.get_document = AsyncMock(side_effect=RuntimeError("graph down"))

    with pytest.raises(RuntimeError):
        await prime_readable([_orphan_part()], graph, PartSkips())


async def test_the_strict_accessor_still_raises_for_an_unread_part() -> None:
    from app.modules.indexing.owner_connector import owner_connector_id

    with pytest.raises(PartInfoMissing):
        owner_connector_id(_orphan_part())
