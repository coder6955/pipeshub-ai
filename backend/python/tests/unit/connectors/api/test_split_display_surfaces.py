"""The split roll-up reaches every surface a user reads a file's status from."""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.connectors.api.router import get_connector_stats_endpoint, get_record_by_id
from app.connectors.sources.localKB.api.knowledge_hub_models import (
    AppliedFilters,
    NodeItem,
)
from app.connectors.sources.localKB.api.models import RecordResponse
from app.connectors.sources.localKB.handlers.kb_service import KnowledgeBaseService
from app.connectors.sources.localKB.handlers.knowledge_hub_service import (
    KnowledgeHubService,
)
from app.modules.retrieval.selection_scope import NamedNode
from tests.unit.connectors.api.test_router_coverage_gaps import _mock_request
from tests.unit.modules.indexing.test_file_split_display import (
    ORG,
    Graph,
    _record,
    _split,
)


def _world() -> Graph:
    graph = Graph()
    _record(graph, "r1", "v1")
    _record(graph, "r2", "v2")
    _split(graph, "v1", "INDEXING_PARTS", done=1)
    return graph


FIELDS = {"splitState": "INDEXING_PARTS", "splitDone": 1, "splitTotal": 4, "splitFailed": 0}


def _hub(graph: Graph) -> KnowledgeHubService:
    log = logging.getLogger("t")
    graph.get_user_by_user_id = AsyncMock(return_value={"_key": "uk", "email": "u@x"})
    graph.get_user_kb_permission = AsyncMock(return_value="READER")
    return KnowledgeHubService(logger=log, graph_provider=graph)


def _row(i: str, node_type: str = "record") -> dict:
    return {"id": i, "name": i, "nodeType": node_type, "origin": "COLLECTION", "createdAt": 1, "updatedAt": 1,
            "hasChildren": False}


@pytest.mark.asyncio
async def test_knowledge_hub_rows_carry_the_roll_up_of_split_files_only() -> None:
    hub = _hub(_world())
    rows = [_row("r1"), _row("r2"), _row("kb1", "app")]
    await hub._stamp_split_display(rows)
    items = [hub._doc_to_node_item(r) for r in rows]
    assert {k: getattr(items[0], k) for k in FIELDS} == FIELDS
    assert items[1].splitState is None and items[1].splitDone is None
    assert items[2].splitState is None


@pytest.mark.asyncio
async def test_named_node_listing_carries_the_roll_up() -> None:
    hub = _hub(_world())
    nodes = [NamedNode(key="records", id="r1", doc={"connectorId": "kb-1"}),
             NamedNode(key="records", id="r2", doc={"connectorId": "kb-1"})]
    items = await hub.named_node_items("u1", ORG, nodes)
    assert [i.splitState for i in items] == ["INDEXING_PARTS", None]


def test_a_node_refuses_a_state_outside_the_contract() -> None:
    with pytest.raises(ValueError):
        NodeItem(id="x", name="x", nodeType="record", origin="COLLECTION", createdAt=1, updatedAt=1,
                 hasChildren=False, splitState="PURGING")


@pytest.mark.asyncio
async def test_the_legacy_listing_rows_carry_the_roll_up() -> None:
    graph = _world()
    graph.get_user_by_user_id = AsyncMock(return_value={"_key": "uk"})
    graph.list_kb_records = AsyncMock(return_value=([{"id": "r1"}, {"id": "r2"}], 2, {}))
    svc = KnowledgeBaseService(MagicMock(), graph, MagicMock())

    result = await svc.list_kb_records(kb_id="kb-1", user_id="u1", org_id=ORG)

    assert {k: result["records"][0][k] for k in FIELDS} == FIELDS
    assert "splitState" not in result["records"][1]
    assert RecordResponse.model_fields["splitState"].default is None


@pytest.mark.asyncio
async def test_the_legacy_children_listing_carries_the_roll_up() -> None:
    graph = _world()
    graph.get_kb_children = AsyncMock(return_value={"success": True, "folders": [], "records": [{"id": "r1"}],
                                                    "totalCount": 1, "counts": {"folders": 0, "records": 1}})
    svc = KnowledgeBaseService(MagicMock(), graph, MagicMock())
    svc._resolve_user_and_kb_access = AsyncMock(return_value=("uk", "OWNER", None))

    result = await svc.get_kb_children(kb_id="kb-1", user_id="u1")

    assert result["records"][0]["splitState"] == "INDEXING_PARTS"


@pytest.mark.asyncio
async def test_the_main_knowledge_hub_listing_serves_the_roll_up_through_get_nodes() -> None:
    """The call that serves every list page: deleting its stamp must fail this."""
    from app.connectors.sources.localKB.handlers.kh_search import SearchPage

    graph = _world()
    graph.get_knowledge_hub_access_context_v2 = AsyncMock(return_value={"gated_app_ids": []})
    graph.get_hidden_app_ids = AsyncMock(return_value=[])
    hub = _hub(graph)
    hub._v2_nodes = AsyncMock(return_value=(
        SearchPage(rows=[_row("r1"), _row("r2")], total=2, counts_by_type=None, start_index=1, end_index=2,
                   next_cursor=None, prev_cursor=None),
        None,
    ))
    hub._stamp_collection_roles = AsyncMock()
    hub._applied_filters = AsyncMock(return_value=AppliedFilters())

    response = await hub.get_nodes(user_id="u1", org_id=ORG)

    assert response.success, response.error
    assert [i.splitState for i in response.items] == ["INDEXING_PARTS", None]
    assert {k: getattr(response.items[0], k) for k in FIELDS} == FIELDS


@pytest.mark.asyncio
async def test_the_folder_children_listing_carries_the_roll_up() -> None:
    graph = _world()
    graph.validate_folder_in_kb = AsyncMock(return_value=True)
    graph.get_folder_children = AsyncMock(return_value={
        "success": True, "folders": [], "records": [{"id": "r1"}, {"id": "r2"}], "totalCount": 2,
        "counts": {"folders": 0, "records": 2}})
    svc = KnowledgeBaseService(MagicMock(), graph, MagicMock())
    svc._resolve_user_and_kb_access = AsyncMock(return_value=("uk", "OWNER", None))

    result = await svc.get_folder_children(kb_id="kb-1", folder_id="f1", user_id="u1")

    assert result["success"], result
    assert {k: result["records"][0][k] for k in FIELDS} == FIELDS
    assert "splitState" not in result["records"][1]


@pytest.mark.asyncio
async def test_record_details_carry_the_roll_up_and_none_for_other_records() -> None:
    graph = _world()
    graph.check_record_access_with_details = AsyncMock(side_effect=lambda **kw: {
        "record": {"id": kw["record_id"], "virtualRecordId": {"r1": "v1", "r2": "v2"}[kw["record_id"]]}})
    req = _mock_request(graph_provider=graph)

    split = await get_record_by_id("r1", req, graph_provider=graph)
    plain = await get_record_by_id("r2", req, graph_provider=graph)

    assert {k: split["record"][k] for k in FIELDS} == FIELDS
    assert "splitState" not in plain["record"]


@pytest.mark.asyncio
async def test_the_stats_endpoint_rolls_split_files_up(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.connectors.api.router.authorize_connector_stats", AsyncMock())
    graph = _world()
    graph.get_document = AsyncMock(return_value={"type": "KB", "orgId": ORG})
    counts = {"COMPLETED": 2, "IN_PROGRESS": 0}
    graph.get_connector_stats = AsyncMock(return_value={"success": True, "data": {
        "stats": {"total": 2, "indexingStatus": dict(counts)},
        "byRecordType": [{"recordType": "FILE", "total": 2, "indexingStatus": dict(counts)}]}})
    registry = AsyncMock()
    registry.get_connector_instance = AsyncMock(return_value={"_key": "kb-1"})
    req = _mock_request(graph_provider=graph, connector_registry=registry)

    result = await get_connector_stats_endpoint(req, connector_id="kb-1", graph_provider=graph)

    assert result["data"]["stats"]["indexingStatus"] == {"COMPLETED": 1, "IN_PROGRESS": 1}
    assert result["data"]["stats"]["splitFiles"]["indexingParts"] == 1


def test_the_spec_documents_the_roll_up_where_it_is_served() -> None:
    from app.connectors.sources.localKB.api.knowledge_hub_models import NodeItem
    from app.modules.indexing.file_split_display import SPLIT_FIELDS
    from tests.support.openapi_spec import openapi_spec

    schemas = openapi_spec()["components"]["schemas"]
    node = schemas["KnowledgeHubNode"]
    record = schemas["GetRecordByIdResponseSchema"]["properties"]["record"]["properties"]
    stats = schemas["ConnectorStats"]["properties"]["data"]["properties"]["stats"]["properties"]
    for name in SPLIT_FIELDS:
        assert name in node["properties"] and name not in node["required"], "a field this change adds is never required"
        assert node["properties"][name].get("nullable") is True and record[name].get("nullable") is True
        assert name in record
        assert name in NodeItem.model_fields
    states = {"SPLITTING", "INDEXING_PARTS", "COMPLETED", "PARTIAL", "FAILED"}
    assert set(node["properties"]["splitState"]["enum"]) == states | {None} == set(record["splitState"]["enum"])
    assert set(stats["splitFiles"]["properties"]) == {"preparing", "indexingParts", "partial", "failed"}
