"""The walk of an owner's parked parts reads only parked parts."""
from __future__ import annotations

from typing import Any

import pytest

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing.split_state import (
    PartInfo,
    cached_part_info,
    clear_part_cache,
    part_records_pages,
)
from app.utils.user_errors import CONNECTOR_OFF
from tests.support.split_fakes import FakeGraph

OFF = {"indexingStatus": ProgressStatus.AUTO_INDEX_OFF.value, "reason": CONNECTOR_OFF}


def _seed(graph: FakeGraph, key: str, owner: str, **fields: Any) -> None:  # noqa: ANN401
    graph.put("records", key, {
        "connectorId": "pdfparts_org-1", "orgId": "org-1", "virtualRecordId": f"V-{key}",
        "indexingStatus": ProgressStatus.COMPLETED.value, **fields,
    })
    info = PartInfo(key=key, org_id="org-1", root="V", gen="g", k=1, page_start=1, page_end=2, page_offset=0,
                    overlap_pages=0, owner_connector_id=owner, vrid=f"V-{key}")
    graph.put("contentSplitParts", key, info.to_doc())


@pytest.fixture(autouse=True)
def _fresh() -> None:
    clear_part_cache()


async def _walk(graph: FakeGraph, owner: str, org: str | None = "org-1", size: int = 2) -> list[str]:
    return [r["_key"] async for page in part_records_pages(graph, owner, org, dict(OFF), page_size=size) for r in page]


async def test_only_the_parked_parts_of_the_owner_are_returned_in_pages() -> None:
    graph = FakeGraph()
    for n in range(5):
        _seed(graph, f"done-{n}", "conn-a")
    for n in range(3):
        _seed(graph, f"off-a{n}", "conn-a", **OFF)
    _seed(graph, "off-b", "conn-b", **OFF)
    _seed(graph, "stopped-other-reason", "conn-a", indexingStatus="AUTO_INDEX_OFF")

    assert await _walk(graph, "conn-a") == ["off-a0", "off-a1", "off-a2"]
    assert cached_part_info({"_key": "off-a0", "connectorId": "pdfparts_org-1"}) is not None


async def test_no_part_record_is_read_one_by_one_and_completed_parts_cost_nothing() -> None:
    graph = FakeGraph()
    reads: list[str] = []
    real = graph.get_document

    async def counting(key: str, collection: str, **kw: object) -> dict | None:
        reads.append(collection)
        return await real(key, collection, **kw)

    graph.get_document = counting  # type: ignore[method-assign]
    for n in range(50):
        _seed(graph, f"done-{n}", "conn-a")
    _seed(graph, "off-1", "conn-a", **OFF)

    assert await _walk(graph, "conn-a", size=100) == ["off-1"]
    assert reads == []


async def test_the_org_comes_from_the_owner_app_when_the_caller_has_none() -> None:
    graph = FakeGraph()
    graph.put("apps", "conn-a", {"orgId": "org-1"})
    _seed(graph, "off-1", "conn-a", **OFF)

    assert await _walk(graph, "conn-a", org=None) == ["off-1"]
    assert await _walk(graph, "unknown-owner", org=None) == []


async def test_a_parked_part_without_its_document_is_left_out() -> None:
    graph = FakeGraph()
    _seed(graph, "off-1", "conn-a", **OFF)
    graph.docs["contentSplitParts"].pop("off-1")

    assert await _walk(graph, "conn-a") == []
