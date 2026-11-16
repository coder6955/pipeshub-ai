# ruff: noqa: F811
"""D-26: a PDF over the split threshold is split on a pdfium-only page count, without the whole-file inspection.

On a 17,153-page, 166 MB file the inspection (pdfminer open, then a full in-memory pdfium rewrite) was killed
for memory three times and the split never ran. Each case runs in both parsing modes.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.exceptions.indexing_exceptions import PasswordProtectedError
from tests.unit.events.test_events import _drain, _pdf_inspection
from tests.unit.events.test_events_split_parent import (  # noqa: F401  (fixture)
    PDF,
    _event,
    _setup,
    mode,
)


@pytest.mark.asyncio
class TestSplitBeforeInspection:
    async def test_a_pdf_over_the_threshold_never_reaches_the_whole_file_inspection(self, mode: str) -> None:
        ep, processor, _gp = _setup()

        with _pdf_inspection(counted=17153) as inspect:
            await _drain(ep.on_event(_event()))

        inspect.assert_not_awaited()
        request = ep.split_runtime.seam.split.await_args.args[0]
        assert (request.page_count, request.content, request.needs_ocr) == (17153, PDF, None)
        ep._dispatch_pdf_binary.assert_not_called()
        assert "This document has 600 pages." in processor.manifest_kwargs["md_binary"]

    async def test_a_forced_reindex_over_the_threshold_keeps_its_generation_nonce(self, mode: str) -> None:
        ep, _processor, _gp = _setup()
        event = _event()
        event["payload"].update({"forceReindex": True, "_delivery_id": "d-9"})

        with _pdf_inspection(counted=900) as inspect, patch("app.events.events.DELIVERY_ID_KEY", "_delivery_id"):
            await _drain(ep.on_event(event))

        inspect.assert_not_awaited()
        assert ep.split_runtime.seam.split.await_args.args[0].nonce == "0:d-9"  # request time 0: the sender stamped none

    async def test_a_password_protected_large_pdf_still_ends_password_protected(self, mode: str) -> None:
        ep, _processor, _gp = _setup()

        with _pdf_inspection(counted=None) as inspect, patch(
            "app.events.events.count_pdf_pages", AsyncMock(side_effect=PasswordProtectedError("locked")),
        ), pytest.raises(PasswordProtectedError):
            await _drain(ep.on_event(_event()))

        inspect.assert_not_awaited()
        ep.split_runtime.seam.split.assert_not_awaited()

    async def test_a_pdf_pdfium_cannot_open_falls_back_to_the_inspection(self, mode: str) -> None:
        ep, _processor, _gp = _setup()

        with _pdf_inspection(counted=None, page_count=1138) as inspect:
            await _drain(ep.on_event(_event()))

        inspect.assert_awaited_once()
        assert ep.split_runtime.seam.split.await_args.args[0].page_count == 1138

    async def test_a_short_pdf_is_inspected_as_before_and_not_split(self, mode: str) -> None:
        ep, _processor, _gp = _setup()

        with _pdf_inspection(counted=120, page_count=120) as inspect:
            await _drain(ep.on_event(_event()))

        inspect.assert_awaited_once()
        ep.split_runtime.seam.split.assert_not_awaited()
        ep.split_runtime.seam.unsplit.assert_awaited_once_with("vr-1")

    async def test_with_splitting_off_the_file_is_not_counted(self, mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
        ep, _processor, _gp = _setup()
        monkeypatch.setenv("PDF_SPLIT_ENABLED", "false")

        with _pdf_inspection(counted=5000, page_count=5000) as inspect, patch(
            "app.events.events.count_pdf_pages", AsyncMock(return_value=5000),
        ) as count:
            await _drain(ep.on_event(_event()))

        count.assert_not_awaited()
        inspect.assert_awaited_once()
        ep.split_runtime.seam.split.assert_not_awaited()


@pytest.mark.asyncio
class TestSplittingSwitchedOff:
    """The flag governs making new splits. A file already split stays split while its content is unchanged."""

    async def test_an_event_for_unchanged_content_continues_the_existing_split_without_counting_or_inspecting(
        self, mode: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from tests.unit.events.test_events_split_parent import ROW, _runtime

        monkeypatch.setenv("PDF_SPLIT_ENABLED", "false")
        ep, processor, _gp = _setup(runtime=_runtime(row=ROW))
        event = _event()
        event["payload"].update({"forceReindex": True, "_delivery_id": "d-3"})

        with _pdf_inspection(counted=5000, page_count=5000) as inspect, patch(
            "app.events.events.count_pdf_pages", AsyncMock(return_value=5000),
        ) as count, patch("app.events.events.DELIVERY_ID_KEY", "_delivery_id"), patch(
            "app.events.events.content_revision", return_value=ROW.content_rev,
        ):
            await _drain(ep.on_event(event))

        count.assert_not_awaited()
        inspect.assert_not_awaited()
        request = ep.split_runtime.seam.split.await_args.args[0]
        assert request.nonce == "", "a forced reindex never re-cuts a split the flag no longer makes"
        ep.split_runtime.seam.unsplit.assert_not_awaited()
        assert "This document has 600 pages." in processor.manifest_kwargs["md_binary"]

    async def test_a_content_change_is_indexed_whole_and_the_parts_are_retired(
        self, mode: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from tests.unit.events.test_events_split_parent import ROW, _runtime

        monkeypatch.setenv("PDF_SPLIT_ENABLED", "false")
        ep, _processor, _gp = _setup(runtime=_runtime(row=ROW))

        with _pdf_inspection(counted=5000, page_count=5000) as inspect, patch(
            "app.events.events.content_revision", return_value="another-revision",
        ):
            await _drain(ep.on_event(_event()))

        inspect.assert_awaited_once()
        ep.split_runtime.seam.split.assert_not_awaited()
        ep.split_runtime.seam.unsplit.assert_awaited_once_with("vr-1")


@pytest.mark.asyncio
class TestACountThatTimesOutIsNeverInspectedWhole:
    async def test_a_timed_out_count_is_retried_and_the_file_is_not_opened_whole(self, mode: str) -> None:
        from app.modules.parsers import parse_pool

        ep, _processor, _gp = _setup()
        timed_out = parse_pool.ParseWorkerError("the parse worker overran", "timed_out")

        with _pdf_inspection(counted=None) as inspect, patch(
            "app.events.events.count_pdf_pages", AsyncMock(side_effect=timed_out),
        ), pytest.raises(parse_pool.ParseWorkerError):
            await _drain(ep.on_event(_event()))

        inspect.assert_not_awaited()
        ep.split_runtime.seam.split.assert_not_awaited()
        ep.split_runtime.seam.unsplit.assert_not_awaited()


@pytest.mark.asyncio
class TestAMissingPageCountDoesNotUndoAnUnchangedSplit:
    async def test_the_split_of_unchanged_content_is_continued_not_retired(self, mode: str) -> None:
        from tests.unit.events.test_events_split_parent import ROW, _runtime

        ep, _processor, _gp = _setup(runtime=_runtime(row=ROW))

        with _pdf_inspection(counted=None, page_count=None) as inspect, patch(
            "app.events.events.content_revision", return_value=ROW.content_rev,
        ):
            await _drain(ep.on_event(_event()))

        inspect.assert_awaited_once()
        ep.split_runtime.seam.unsplit.assert_not_awaited()
        request = ep.split_runtime.seam.split.await_args.args[0]
        assert (request.nonce, request.may_heal) == ("", False), "continued, never cut or healed again"

    async def test_changed_content_with_no_page_count_is_still_indexed_whole(self, mode: str) -> None:
        from tests.unit.events.test_events_split_parent import ROW, _runtime

        ep, _processor, _gp = _setup(runtime=_runtime(row=ROW))

        with _pdf_inspection(counted=None, page_count=None), patch(
            "app.events.events.content_revision", return_value="another-revision",
        ):
            await _drain(ep.on_event(_event()))

        ep.split_runtime.seam.split.assert_not_awaited()
        ep.split_runtime.seam.unsplit.assert_awaited_once_with("vr-1")
