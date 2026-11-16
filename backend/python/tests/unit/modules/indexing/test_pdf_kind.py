"""One answer to "is this file a PDF" for the split decision, the re-embed paths, the write scope and the stages."""
from __future__ import annotations

import pytest

from app.config import file_types
from app.modules.indexing.pdf_kind import is_pdf


@pytest.mark.parametrize(
    ("extension", "mime", "expected"),
    [
        ("pdf", None, True),
        ("pdf", "application/octet-stream", True),
        (".PDF", None, True),
        (None, "application/pdf", True),
        ("bin", "application/pdf", True),
        ("docx", "application/octet-stream", False),
        (None, None, False),
        ("", "", False),
    ],
)
def test_a_pdf_is_named_by_its_extension_or_its_mime_type(extension: str | None, mime: str | None, expected: bool) -> None:
    assert is_pdf(extension, mime) is expected


PDF_MIME_ALIASES = [
    "application/x-pdf", "application/acrobat", "application/vnd.pdf", "text/pdf", "text/x-pdf", "APPLICATION/PDF",
    "application/pdf; charset=binary",
]


@pytest.mark.parametrize("mime", PDF_MIME_ALIASES)
def test_every_mime_the_registry_reads_as_pdf_is_a_pdf_alone_and_beside_a_wrong_extension(mime: str) -> None:
    assert is_pdf(None, mime)
    assert is_pdf("bin", mime)


@pytest.mark.parametrize("extension", ["pdf", ".pdf", "PDF", ".Pdf"])
def test_the_extension_alone_is_enough_whatever_its_spelling(extension: str) -> None:
    assert is_pdf(extension, None) and is_pdf(extension, "application/octet-stream")


@pytest.mark.parametrize("extension", [None, "", "pdf", ".pdf", "docx", "txt", "html", "csv", "bin"])
@pytest.mark.parametrize(
    "mime",
    [None, "", "application/pdf", "text/pdf", "application/x-pdf", "text/plain", "text/html", "application/octet-stream",
     "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "image/png", "text/csv"],
)
def test_it_gives_the_answer_the_dispatch_gives(extension: str | None, mime: str | None) -> None:
    """The one predicate the split code uses is the registry's decision, for every pairing."""
    ext = extension.lstrip(".").lower() if extension else None
    dispatch = file_types.legacy_route(mime, ext, code_extension=ext, reads_as_code_file=False)
    assert is_pdf(extension, mime) is (dispatch is not None and dispatch.route is file_types.Route.PDF)


# ---------------------------------------------------------------- every call site reads the registry's answer

from types import SimpleNamespace  # noqa: E402
from unittest.mock import AsyncMock, MagicMock  # noqa: E402

CASES = [  # (extension, mime, is a PDF)
    (None, "application/pdf", True),
    (None, "application/x-pdf", True),
    (None, "text/pdf", True),
    (None, "APPLICATION/PDF", True),
    ("pdf", None, True),
    (".pdf", "application/octet-stream", True),
    ("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", False),
    ("png", "image/png", False),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("extension", "mime", "pdf"), CASES)
class TestEveryCallSiteAgrees:
    async def test_the_event_processor_may_split_and_finds_the_generation_of_exactly_the_pdfs(
        self, extension: str | None, mime: str | None, pdf: bool,
    ) -> None:
        from app.events.events import EventProcessor

        processor = object.__new__(EventProcessor)
        runtime = MagicMock()
        runtime.store.get = AsyncMock(return_value=object())
        processor.split_runtime = runtime

        assert processor._may_split(extension, mime, None, store_only=False) is pdf
        assert await processor._has_split_generation("v", extension, mime) is pdf

    async def test_the_write_scope_asks_for_the_files_generation_of_exactly_the_pdfs(
        self, extension: str | None, mime: str | None, pdf: bool,
    ) -> None:
        from app.modules.transformers.write_scope import WriteScope, use_live_generation

        asked: list[str] = []

        async def live(v: str) -> str | None:
            asked.append(v)
            return "gen-1"

        use_live_generation(live)
        try:
            scope = await WriteScope.of_record(SimpleNamespace(virtual_record_id="v", mime_type=mime or "", extension=extension))
        finally:
            use_live_generation(None)
        assert bool(asked) is pdf and scope.is_split is pdf

    async def test_the_stage_io_reads_a_pdf_from_the_record_or_the_job(
        self, extension: str | None, mime: str | None, pdf: bool,
    ) -> None:
        from app.modules.pipeline.stages.record_io import RecordStageIO

        io = object.__new__(RecordStageIO)
        io._documents = {"r": {"extension": extension, "mimeType": mime}}
        job = SimpleNamespace(record_ids=["r"], mime_type=None)

        assert await io._may_be_split(job) is pdf

    async def test_the_record_handler_looks_for_a_split_row_of_exactly_the_pdfs(
        self, extension: str | None, mime: str | None, pdf: bool,
    ) -> None:
        from app.services.messaging.kafka.handlers.record import RecordEventHandler

        handler = object.__new__(RecordEventHandler)
        runtime = MagicMock()
        runtime.reembed.row = AsyncMock(return_value=object())
        handler.event_processor = SimpleNamespace(split_runtime=runtime)

        assert await handler._is_split_parent("v", {"extension": extension, "mimeType": mime}) is pdf
