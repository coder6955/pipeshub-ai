"""``save_derived_pdf``: a file the service cut from another is stored at its own path, tagged,
and asks for the service size cap rather than the user upload limit."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.modules.transformers import blob_storage as bs_mod
from app.modules.transformers.blob_storage import BlobStorage, DerivedFileTags

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from contextlib import AbstractAsyncContextManager

    import aiohttp

TAGS = DerivedFileTags(connector_id="conn-1", virtual_record_id="vrid-1")
PDF = b"%PDF-1.7 part bytes"


def _storage(storage_type: str) -> BlobStorage:
    storage = BlobStorage(logger=MagicMock(), config_service=MagicMock(), graph_provider=MagicMock())
    storage._get_auth_and_config = AsyncMock(return_value=({"Authorization": "Bearer t"}, "http://node", storage_type))
    return storage


class _Response:
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status = status
        self._body = body

    async def json(self) -> dict[str, Any]:
        return self._body

    async def text(self) -> str:
        return ""

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: object) -> _Response:
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


def _borrowed(session: object) -> Callable[[], AbstractAsyncContextManager[object]]:
    @asynccontextmanager
    async def borrowed() -> AsyncIterator[object]:
        yield session

    return borrowed


def _fields(form: aiohttp.FormData) -> dict[str, str]:
    return {opts["name"]: value for opts, _, value in form._fields}


async def test_local_upload_goes_to_the_derived_route_with_path_name_and_tags() -> None:
    session = _Session([_Response(200, {"_id": "doc-9"})])
    with patch.object(bs_mod, "_borrowed_session", _borrowed(session)):
        document_id = await _storage("local").save_derived_pdf("org-1", "split/vrid-1/part-0003", PDF, TAGS)

    assert document_id == "doc-9"
    (call,) = session.calls
    assert call["url"] == "http://node/api/v1/document/internal/upload?derived=true"
    fields = _fields(call["data"])
    assert fields["documentName"] == "part-0003"
    assert fields["documentPath"] == "split/vrid-1"
    assert fields["isVersionedFile"] == "false"
    assert fields["customMetadata[0][key]"] == "connectorId"
    assert fields["customMetadata[0][value]"] == "conn-1"
    assert fields["customMetadata[1][key]"] == "virtualRecordId"
    assert fields["customMetadata[1][value]"] == "vrid-1"


async def test_the_same_path_and_bytes_send_the_same_idempotency_key_and_other_bytes_do_not() -> None:
    keys = []
    for data in (PDF, PDF, PDF + b"x"):
        session = _Session([_Response(200, {"_id": "d"})])
        with patch.object(bs_mod, "_borrowed_session", _borrowed(session)):
            await _storage("local").save_derived_pdf("org-1", "split/v/p", data, TAGS)
        keys.append(session.calls[0]["headers"]["Idempotency-Key"])

    assert keys[0] == keys[1]
    assert keys[0] != keys[2]
    assert len(keys[0]) <= 128


async def _local_key() -> str:
    session = _Session([_Response(200, {"_id": "d"})])
    with patch.object(bs_mod, "_borrowed_session", _borrowed(session)):
        await _storage("local").save_derived_pdf("org-1", "split/vrid-1/part-0003", PDF, TAGS)
    return session.calls[0]["headers"]["Idempotency-Key"]


async def test_a_refused_upload_raises_instead_of_returning_nothing() -> None:
    session = _Session([_Response(413, {})])
    with patch.object(bs_mod, "_borrowed_session", _borrowed(session)), pytest.raises(Exception, match="derived"):
        await _storage("local").save_derived_pdf("org-1", "split/v/p", PDF, TAGS)


async def test_cloud_storage_creates_a_tagged_placeholder_and_uploads_the_bytes() -> None:
    storage = _storage("s3")
    storage._create_placeholder = AsyncMock(return_value={"_id": "doc-5"})
    storage._get_signed_url = AsyncMock(return_value={"signedUrl": "https://s3/put"})
    storage._upload_raw_to_signed_url = AsyncMock()
    with patch.object(bs_mod, "_borrowed_session", _borrowed(MagicMock())):
        document_id = await storage.save_derived_pdf("org-1", "split/vrid-1/part-0003", PDF, TAGS)

    assert document_id == "doc-5"
    assert storage._create_placeholder.await_args.kwargs["idempotency_key"] == await _local_key()
    placeholder = storage._create_placeholder.await_args.args[2]
    assert placeholder["documentName"] == "part-0003"
    assert placeholder["documentPath"] == "split/vrid-1"
    assert placeholder["customMetadata"] == [
        {"key": "connectorId", "value": "conn-1"},
        {"key": "virtualRecordId", "value": "vrid-1"},
    ]
    assert storage._upload_raw_to_signed_url.await_args.args[2:] == (PDF, "application/pdf")


async def test_a_path_without_a_file_name_is_refused() -> None:
    with pytest.raises(ValueError, match="file name"):
        await _storage("local").save_derived_pdf("org-1", "/", PDF, TAGS)


@pytest.mark.parametrize("connector_id, vrid", [("", "v"), ("c", "")])
def test_tags_need_both_ids(connector_id: str, vrid: str) -> None:
    with pytest.raises(ValueError):
        DerivedFileTags(connector_id, vrid)


async def test_the_cloud_placeholder_request_sends_the_given_idempotency_key() -> None:
    storage = _storage("s3")
    session = _Session([_Response(200, {"_id": "doc-5"}), _Response(200, {"_id": "doc-6"})])

    await storage._create_placeholder(session, "http://node/placeholder", {}, {"A": "b"}, idempotency_key="k-1")
    await storage._create_placeholder(session, "http://node/placeholder", {}, {"A": "b"})

    assert session.calls[0]["headers"]["Idempotency-Key"] == "k-1"
    assert session.calls[1]["headers"]["Idempotency-Key"] != "k-1"
