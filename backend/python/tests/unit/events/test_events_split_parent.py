"""``EventProcessor.on_event`` for a PDF that may be split, and for a part of one.

The split decision is made ahead of both parsing modes, so each case runs in the in-process and in
the parsing-service mode.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config.constants.arangodb import MimeTypes
from app.events.events import DedupDecision
from app.modules.indexing.part_gate import PartContext, PartStop
from app.modules.indexing.split_parent_flag import split_parent_active
from app.modules.indexing.split_purge import FILE_REMOVED_REASON
from app.modules.indexing.split_seam import SplitOutcome
from app.modules.indexing.split_state import (
    PartInfo,
    SplitRow,
    SplitState,
    clear_part_cache,
    part_specs,
    remember_part,
)
from tests.unit.events.test_events import (
    _drain,
    _make_event_payload,
    _make_event_processor,
    _mock_processor_gen,
    _pdf_inspection,
)

PDF = b"%PDF-1.7 big"
ROW = SplitRow(
    v="vr-1", org_id="org-1", gen="g1", parts=part_specs("g1", [(1, 500, 501), (501, 600, 600)], ocr=False),
    state=SplitState.INDEXING_PARTS, content_rev="r",
)


def _event(**kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
    return _make_event_payload(
        mime_type=MimeTypes.PDF.value, extension="pdf", event_type="newRecord", virtual_record_id="vr-1",
        buffer=PDF, **kwargs,
    )


def _runtime(*, split: bool = True, row: SplitRow | None = None) -> MagicMock:
    runtime = MagicMock()
    runtime.seam.split = AsyncMock(return_value=SplitOutcome(ROW, reused_generation=False))
    runtime.seam.unsplit = AsyncMock(return_value=False)
    runtime.store.get = AsyncMock(return_value=row)
    return runtime


def _setup(doc: dict[str, Any] | None = None, runtime: MagicMock | None = None) -> Any:  # noqa: ANN401
    ep, _logger, processor, gp = _make_event_processor()
    gp.get_document.return_value = doc or {"_key": "rec-1", "recordType": "FILE", "orgId": "org-1", "virtualRecordId": "vr-1"}
    gp.get_records_by_virtual_record_id = AsyncMock(return_value=[{"_key": "rec-1"}])
    ep.split_runtime = runtime if runtime is not None else _runtime()
    ep.stage_ingress = None
    ep.parsing_client = MagicMock(circuit_open=False)
    ep._check_duplicate_by_md5 = AsyncMock(return_value=DedupDecision())
    ep._dispatch_pdf_binary = MagicMock(side_effect=_mock_processor_gen)
    ep._orchestrate_via_services = MagicMock(side_effect=_mock_processor_gen)
    seen: list[bool] = []

    async def manifest(*_a: object, **kwargs: object):  # noqa: ANN202
        seen.append(split_parent_active())
        processor.manifest_kwargs = kwargs
        async for event in _mock_processor_gen():
            yield event

    processor.process_md_document = MagicMock(side_effect=manifest)
    processor.manifest_flag = seen
    return ep, processor, gp


@pytest.fixture(params=["in_process", "parsing_service"])
def mode(request: pytest.FixtureRequest) -> Any:  # noqa: ANN401
    service = request.param == "parsing_service"
    with patch("app.events.events.EventProcessor._use_service_pipeline", return_value=service):
        yield request.param


@pytest.mark.asyncio
class TestParentSplit:
    async def test_a_long_pdf_is_split_and_only_its_manifest_is_indexed(self, mode: str) -> None:
        ep, processor, _gp = _setup()

        with _pdf_inspection(page_count=1138):
            events = await _drain(ep.on_event(_event()))

        ep.split_runtime.seam.split.assert_awaited_once()
        request = ep.split_runtime.seam.split.await_args.args[0]
        assert (request.v, request.page_count, request.content) == ("vr-1", 1138, PDF)
        ep._dispatch_pdf_binary.assert_not_called()
        ep._orchestrate_via_services.assert_not_called()
        manifest = processor.manifest_kwargs["md_binary"]
        assert "This document has 600 pages." in manifest and "part" not in manifest.lower()
        assert [e.event.value for e in events if e.event.value in {"parsing_complete", "indexing_complete"}] == [
            "parsing_complete", "indexing_complete",
        ]

    async def test_no_stage_is_dispatched_for_the_manifest_and_the_flag_ends_with_it(self, mode: str) -> None:
        ep, processor, _gp = _setup()

        with _pdf_inspection(page_count=1138):
            await _drain(ep.on_event(_event()))

        assert processor.manifest_flag == [True]
        assert split_parent_active() is False

    async def test_a_short_pdf_is_indexed_whole_and_loses_parts_it_had(self, mode: str) -> None:
        ep, _processor, _gp = _setup()

        with _pdf_inspection(page_count=120):
            await _drain(ep.on_event(_event()))

        ep.split_runtime.seam.split.assert_not_awaited()
        ep.split_runtime.seam.unsplit.assert_awaited_once_with("vr-1")
        assert ep._dispatch_pdf_binary.called or ep._orchestrate_via_services.called

    async def test_a_pdf_with_no_page_count_is_indexed_whole(self, mode: str) -> None:
        ep, _processor, _gp = _setup()

        with _pdf_inspection(page_count=None):
            await _drain(ep.on_event(_event()))

        ep.split_runtime.seam.split.assert_not_awaited()

    async def test_other_types_are_never_inspected_for_a_split(self, mode: str) -> None:
        ep, _processor, _gp = _setup()
        ep.processor.process_txt_document = MagicMock(side_effect=_mock_processor_gen)

        with _pdf_inspection(page_count=5000) as inspect:
            await _drain(ep.on_event(_make_event_payload(mime_type=MimeTypes.PLAIN_TEXT.value, event_type="newRecord", buffer=b"hello")))

        inspect.assert_not_awaited()
        ep.split_runtime.seam.split.assert_not_awaited()

    async def test_a_forced_reindex_asks_for_a_generation_of_its_own(self, mode: str) -> None:
        ep, _processor, _gp = _setup()
        event = _event()
        event["payload"].update({"forceReindex": True, "_delivery_id": "d-7"})

        with _pdf_inspection(page_count=1138), patch("app.events.events.DELIVERY_ID_KEY", "_delivery_id"):
            await _drain(ep.on_event(event))

        assert ep.split_runtime.seam.split.await_args.args[0].nonce == "0:d-7"  # request time 0: the sender stamped none

    async def test_a_forced_reindex_carries_the_time_it_was_requested(self, mode: str) -> None:
        ep, _processor, _gp = _setup()
        event = _event()
        event["payload"].update({"forceReindex": True, "forceReindexAt": 1_700_000_000_000, "_delivery_id": "d-8"})

        with _pdf_inspection(page_count=1138), patch("app.events.events.DELIVERY_ID_KEY", "_delivery_id"):
            await _drain(ep.on_event(event))

        assert ep.split_runtime.seam.split.await_args.args[0].nonce == "1700000000000:d-8"

    async def test_without_the_split_runtime_every_pdf_is_indexed_whole(self, mode: str) -> None:
        ep, _processor, _gp = _setup()
        ep.split_runtime = None

        with _pdf_inspection(page_count=5000) as inspect:
            await _drain(ep.on_event(_event()))

        assert ep._dispatch_pdf_binary.called or ep._orchestrate_via_services.called
        inspect.assert_not_awaited()

    async def test_a_shared_vrid_with_a_generation_is_never_rebuilt_in_place(self, mode: str) -> None:
        # R4-6c: the in-place rebuild path would rewrite the manifest under the generation's parts.
        ep, processor, _gp = _setup(runtime=_runtime(row=ROW))
        ep._check_duplicate_by_md5 = AsyncMock(return_value=DedupDecision(rebuild_shared_vrid=True))

        with _pdf_inspection(page_count=1138):
            await _drain(ep.on_event(_event()))

        assert processor.manifest_kwargs["prev_virtual_record_id"] is None

    async def test_a_shared_vrid_without_a_generation_is_still_rebuilt(self, mode: str) -> None:
        ep, processor, _gp = _setup()
        ep._check_duplicate_by_md5 = AsyncMock(return_value=DedupDecision(rebuild_shared_vrid=True))

        with _pdf_inspection(page_count=1138):
            await _drain(ep.on_event(_event()))

        assert processor.manifest_kwargs["prev_virtual_record_id"] == "vr-1"


PART_DOC = {
    "_key": "rec-1", "recordType": "FILE", "orgId": "org-1", "virtualRecordId": "vr-part",
    "connectorId": "pdfparts_org-1",
}
PART_INFO = PartInfo(
    key="rec-1", org_id="org-1", root="vr-1", gen="g1", k=2, page_start=501, page_end=1000, page_offset=500,
    overlap_pages=1, owner_connector_id="conn-1",
)


@pytest.fixture(autouse=True)
def _part_documents_read() -> None:
    """The part document is read once and cached; here it is already read."""
    clear_part_cache()
    remember_part(PART_INFO)


@pytest.mark.asyncio
class TestPartPath:
    async def test_a_part_whose_file_is_gone_is_switched_off_and_does_no_work(self, mode: str) -> None:
        ep, _processor, gp = _setup(PART_DOC)

        with patch("app.events.events.check_part", AsyncMock(return_value=PartStop(FILE_REMOVED_REASON))), _pdf_inspection(
            page_count=10
        ) as inspect:
            events = await _drain(ep.on_event(_event()))

        inspect.assert_not_awaited()
        ep._dispatch_pdf_binary.assert_not_called()
        ep._orchestrate_via_services.assert_not_called()
        writes = [c.args[2] for c in gp.update_node.await_args_list]
        assert {"indexingStatus": "AUTO_INDEX_OFF", "reason": FILE_REMOVED_REASON} in writes
        assert [e.event.value for e in events] == ["parsing_complete", "indexing_complete"]

    async def test_a_part_is_never_split_again_and_reads_the_plans_ocr_hint(self, mode: str) -> None:
        ep, _processor, _gp = _setup(PART_DOC)

        with patch("app.events.events.check_part", AsyncMock(return_value=PartContext(2, True))), _pdf_inspection(
            page_count=900
        ):
            await _drain(ep.on_event(_event()))

        ep.split_runtime.seam.split.assert_not_awaited()
        if mode == "in_process":
            assert ep._dispatch_pdf_binary.call_args.kwargs["part"] == PartContext(2, True)


@pytest.mark.asyncio
class TestMd5CopyOfASplitFile:
    """T9: a byte-identical copy of a split file shares the finished file's content identity (V) and is never rebuilt in place."""

    @staticmethod
    def _twin() -> dict[str, Any]:
        return {
            "_key": "rec-twin", "orgId": "org-1", "virtualRecordId": "vr-1", "indexingStatus": "COMPLETED", "secretsRedacted": False,
            "md5Checksum": "abc",
        }

    @staticmethod
    def _copy() -> dict[str, Any]:
        return {"_key": "rec-copy", "orgId": "org-1", "virtualRecordId": "vr-copy", "md5Checksum": "abc", "recordType": "FILE"}

    async def test_a_copy_in_the_same_collection_attaches_to_v_and_nothing_is_rebuilt(self) -> None:
        from app.events.dedup import DuplicateMatch

        ep, _processor, _gp = _setup()
        ep._vrid_has_stored_content = AsyncMock(return_value=True)
        ep._attach_to_finished_twin = AsyncMock()
        ep._copy_twin_edges = AsyncMock()
        ep.update_record_fields = AsyncMock(return_value=True)

        decision = await ep._reuse_finished_duplicate(self._copy(), DuplicateMatch(self._twin(), True, True))

        assert decision.skip_indexing and not decision.rebuild_shared_vrid
        ep._attach_to_finished_twin.assert_awaited_once()
        assert ep._attach_to_finished_twin.await_args.args[0]["virtualRecordId"] == "vr-1"
        ep.split_runtime.seam.split.assert_not_awaited()

    async def test_a_copy_already_sharing_v_whose_stored_content_is_gone_asks_for_a_rebuild_that_the_event_refuses(self, mode: str) -> None:
        from app.events.dedup import DuplicateMatch

        ep, processor, _gp = _setup(runtime=_runtime(row=ROW))
        ep._vrid_has_stored_content = AsyncMock(return_value=False)
        shared = {**self._copy(), "virtualRecordId": "vr-1"}

        decision = await ep._reuse_finished_duplicate(shared, DuplicateMatch(self._twin(), True, True))
        assert decision.rebuild_shared_vrid, "the dedup path asks for the in-place rebuild"

        ep._check_duplicate_by_md5 = AsyncMock(return_value=decision)
        with _pdf_inspection(page_count=1138):
            await _drain(ep.on_event(_event()))

        assert processor.manifest_kwargs["prev_virtual_record_id"] is None, "a V with a generation is never rebuilt in place"
