"""The internal stream route serves a part of a split PDF from storage, without a connector."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from starlette.responses import Response

import app.edition_config  # noqa: F401  (binds the edition seams before the router loads)
from app.config.constants.arangodb import Connectors
from app.connectors.api import router
from app.models.entities import FileRecord, RecordType
from app.modules.indexing.split_state import clear_part_cache

PART_DOC = {"_key": "part-1", "orgId": "org-1", "connectorId": "pdfparts_org-1", "storageDocumentId": "stored-doc-9"}
# The part document (contentSplitParts), which is what makes the record a part.
PART_INFO = {
    "id": "part-1", "orgId": "org-1", "rootVirtualRecordId": "V", "gen": "g", "k": 2, "pageStart": 501, "pageEnd": 1000,
    "pageOffset": 500, "overlapPages": 1, "ownerConnectorId": "drive-1",
}


@pytest.fixture(autouse=True)
def _fresh_part_cache() -> None:
    clear_part_cache()


def _record(connector_id: str = "pdfparts_org-1") -> FileRecord:
    return FileRecord(
        id="part-1", org_id="org-1", external_record_id="part:g:2", record_name="big.pdf pages 1-2",
        origin="UPLOAD", connector_name=Connectors.KNOWLEDGE_BASE, connector_id=connector_id,
        record_type=RecordType.FILE, version=0, mime_type="application/pdf", is_file=True, extension="pdf",
    )


def _wire(
    monkeypatch: pytest.MonkeyPatch, doc: dict | None, part_info_doc: dict | None = PART_INFO,
) -> tuple[MagicMock, AsyncMock, AsyncMock]:
    graph = MagicMock()
    graph.get_record_by_id = AsyncMock(return_value=_record())
    graph.get_document = AsyncMock(
        side_effect=lambda key, collection, **_kw: part_info_doc if collection == "contentSplitParts" else doc,
    )
    monkeypatch.setattr(router, "get_org_or_503", AsyncMock(return_value={"_key": "org-1"}))
    invoke = AsyncMock(return_value=Response(b"from-connector"))
    monkeypatch.setattr(router, "_invoke_connector_stream", invoke)
    api = AsyncMock(return_value={"data": {"data": [37, 80, 68, 70]}})
    monkeypatch.setattr(router, "make_api_call", api)
    monkeypatch.setattr(router, "generate_jwt", AsyncMock(return_value="token"))
    return graph, invoke, api


def _call(graph: MagicMock) -> object:
    config = MagicMock()
    config.get_config = AsyncMock(return_value={"storage": {"endpoint": "http://storage"}})
    request = SimpleNamespace(app=SimpleNamespace(container=MagicMock(), state=SimpleNamespace(connector_registry=MagicMock())))
    return router.stream_record_internal(
        request=request, record_id="part-1", graph_provider=graph, config_service=config,
        claims={"orgId": "org-1", "userId": "u"},
    )


async def test_a_part_is_read_from_its_storage_document_not_from_a_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    graph, invoke, api = _wire(monkeypatch, PART_DOC)

    response = await _call(graph)

    assert response.body == b"%PDF"
    assert response.media_type == "application/pdf"
    assert api.await_args.kwargs["route"] == "http://storage/api/v1/document/internal/stored-doc-9/buffer"
    invoke.assert_not_awaited()
    assert [c.args[1] for c in graph.get_document.await_args_list] == ["contentSplitParts", "records"]


async def test_a_part_with_no_file_yet_is_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    graph, invoke, _api = _wire(monkeypatch, {**PART_DOC, "storageDocumentId": None})

    with pytest.raises(HTTPException) as exc:
        await _call(graph)

    assert exc.value.status_code == 404
    invoke.assert_not_awaited()


async def test_a_record_under_the_parts_connector_without_a_part_document_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    graph, invoke, api = _wire(monkeypatch, PART_DOC, part_info_doc=None)

    with pytest.raises(HTTPException) as exc:
        await _call(graph)

    assert exc.value.status_code == 404
    invoke.assert_not_awaited()
    api.assert_not_awaited()
