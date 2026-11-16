"""WriteScope: which points a write owns, what it stamps, and the guard that keeps a split file's writes in their segment."""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from app.exceptions.indexing_exceptions import SplitScopeError
from app.modules.indexing.split_parent_flag import (
    begin_split_parent,
    end_split_parent,
    split_parent_active,
)
from app.modules.indexing.split_state import PartInfo, clear_part_cache, remember_part
from app.modules.transformers.vectorstore import deterministic_point_id
from app.modules.transformers.write_scope import WriteScope
from tests.support.split_scope_world import GEN, MANIFEST, ORG, PART1, V, part_vrid

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.asyncio


class _Record:
    def __init__(self, vrid: str, connector: str = "app", key: str = "r1") -> None:
        self.virtual_record_id, self.connector_id, self.id = vrid, connector, key


def test_the_filter_of_each_scope_names_only_what_it_owns() -> None:
    assert WriteScope.unsplit("u").must() == {"virtualRecordId": "u"}
    assert MANIFEST.must() == {"virtualRecordId": V, "segmentVirtualRecordId": V}
    assert PART1.must() == {"virtualRecordId": V, "segmentVirtualRecordId": part_vrid(1)}


def test_a_point_carries_the_files_identity_and_the_segment_but_its_id_comes_from_the_content() -> None:
    content = {"virtualRecordId": part_vrid(1), "blockId": "b0", "blockIndex": 4, "orgId": ORG}
    payload = PART1.payload(content)
    assert payload["virtualRecordId"] == V
    assert payload["segmentVirtualRecordId"] == part_vrid(1)
    assert payload["splitGen"] == GEN and isinstance(payload["splitGen"], str)
    assert payload["segmentIndex"] == 1 and payload["blockIndex"] == 4
    assert deterministic_point_id(content, "t") == str(uuid.uuid5(
        uuid.uuid5(uuid.NAMESPACE_URL, "pipeshub:vector-point"), f"{part_vrid(1)}|b0|unit|t"))


def test_an_unsplit_point_gets_no_segment_field() -> None:
    assert set(WriteScope.unsplit("u").payload({"virtualRecordId": "u"})) == {"virtualRecordId"}


def test_only_a_part_gives_up_the_summary_point_and_reconciliation() -> None:
    assert (PART1.writes_summary, PART1.reconciles) == (False, False)
    assert (MANIFEST.writes_summary, MANIFEST.reconciles) == (True, True)
    assert WriteScope.unsplit("u").writes_summary


class TestGuard:
    def test_a_write_naming_no_segment_while_the_manifest_is_indexed_raises(self) -> None:
        begin_split_parent(GEN)
        try:
            with pytest.raises(SplitScopeError):
                WriteScope.unsplit(V)
        finally:
            end_split_parent()
        assert not split_parent_active()
        assert WriteScope.unsplit(V).segment is None

    async def test_the_manifest_resolves_to_its_own_segment_from_the_flag(self) -> None:
        begin_split_parent(GEN)
        try:
            scope = await WriteScope.of_record(_Record(V))
        finally:
            end_split_parent()
        assert scope == MANIFEST

    async def test_the_classify_stage_names_the_generation_itself(self) -> None:
        assert await WriteScope.of_record(_Record(V), gen=GEN) == MANIFEST

    async def test_a_part_resolves_through_its_part_document(self) -> None:
        clear_part_cache()
        info = PartInfo("node-1", ORG, V, GEN, 1, 1, 500, 0, 1, "app", part_vrid(1))
        remember_part(info)
        scope = await WriteScope.of_record(_Record(part_vrid(1), connector="pdfparts_org", key="node-1"))
        assert scope == PART1

    async def test_a_part_whose_document_cannot_be_read_is_not_written(self) -> None:
        from app.modules.indexing.split_state import PartInfoMissing

        clear_part_cache()
        with pytest.raises(PartInfoMissing):
            await WriteScope.of_record(_Record("v", connector="pdfparts_org", key="ghost"))

    async def test_an_ordinary_record_owns_its_whole_vrid(self) -> None:
        assert await WriteScope.of_record(_Record("u")) == WriteScope("u", "u")


class TestEveryWritePathKnowsItsFile:
    """a write outside the manifest's attempt asks the file's row, so no writer of a split file lands file-wide."""

    @pytest.fixture(autouse=True)
    def _reset(self) -> Iterator[None]:
        from app.modules.transformers.write_scope import use_live_generation

        yield
        use_live_generation(None)

    async def test_a_write_of_a_split_parent_outside_its_attempt_resolves_to_the_manifest_segment(self) -> None:
        from app.modules.transformers.write_scope import use_live_generation

        async def live(v: str) -> str | None:
            return GEN if v == V else None

        use_live_generation(live)
        assert await WriteScope.of_record(_Record(V)) == MANIFEST
        assert await WriteScope.of_record(_Record("other")) == WriteScope("other", "other")

    async def test_a_part_resolves_to_its_own_segment_whatever_the_lookup_says(self) -> None:
        from app.modules.transformers.write_scope import use_live_generation

        async def live(v: str) -> str | None:
            raise AssertionError("a part never asks the file's row")

        use_live_generation(live)
        clear_part_cache()
        remember_part(PartInfo("node-1", ORG, V, GEN, 1, 1, 500, 0, 1, "app", part_vrid(1)))
        assert await WriteScope.of_record(_Record(part_vrid(1), connector="pdfparts_org", key="node-1")) == PART1

    async def test_a_delete_is_file_wide_and_a_record_of_another_type_costs_no_read(self) -> None:
        from app.modules.transformers.write_scope import use_live_generation

        asked: list[str] = []

        async def live(v: str) -> str | None:
            asked.append(v)
            return GEN

        use_live_generation(live)
        assert await WriteScope.of_record(_Record(V), write=False) == WriteScope(V, V)
        text = _Record(V)
        text.mime_type = "text/plain"  # type: ignore[attr-defined]
        assert await WriteScope.of_record(text) == WriteScope(V, V)
        assert asked == []

    @pytest.mark.parametrize(("extension", "mime"), [("pdf", "application/octet-stream"), (None, "application/pdf")])
    async def test_a_pdf_named_by_its_extension_or_its_mime_type_resolves_through_the_row(
        self, extension: str | None, mime: str
    ) -> None:
        from app.modules.transformers.write_scope import use_live_generation

        async def live(v: str) -> str | None:
            return GEN

        use_live_generation(live)
        record = _Record(V)
        record.mime_type = mime  # type: ignore[attr-defined]
        record.extension = extension  # type: ignore[attr-defined]
        assert await WriteScope.of_record(record) == WriteScope.manifest(V, GEN)

    async def test_a_row_that_cannot_be_read_stops_the_write(self) -> None:
        from app.modules.transformers.write_scope import use_live_generation

        async def live(v: str) -> str | None:
            raise ConnectionError("graph down")

        use_live_generation(live)
        with pytest.raises(ConnectionError):
            await WriteScope.of_record(_Record(V))

    async def test_the_vector_stores_summary_and_class_unit_writers_resolve_through_the_row(self, monkeypatch) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from app.modules.transformers.write_scope import use_live_generation
        from tests.support import split_scope_world as world
        from tests.support.in_memory_vector_db import InMemoryVectorDBService

        async def live(v: str) -> str | None:
            return GEN

        monkeypatch.setattr("app.modules.transformers.vectorstore.reset_membership_context", lambda _tokens: None)
        use_live_generation(live)
        service = InMemoryVectorDBService()
        await world.fresh(service, "records")
        store = world.store_over(service)
        assert await store._scope_for(V, _Record(V)) == MANIFEST, "index_class_units and index_record_summary"
        assert await store._scope_for(V, _Record(V), write=False) == WriteScope(V, V), "the file-level delete"
        store._collection_with_model = AsyncMock(return_value="records")
        store._bind_membership = AsyncMock(return_value=None)
        store._resync_membership_after_write = AsyncMock()
        store._process_document_chunks = AsyncMock()
        store._build_record_summary_document = MagicMock(return_value=MagicMock(metadata={}))
        await store.index_record_summary("rec", V, ORG, MagicMock(summary="s"), _Record(V))
        assert store._process_document_chunks.await_args.kwargs["scope"] == MANIFEST

    def test_every_caller_of_a_point_writer_is_known_here(self) -> None:
        """A new writer of points must be added to this list on purpose, with a test of its scope."""
        import re
        from pathlib import Path

        app = Path(__file__).resolve().parents[4] / "app"
        pattern = re.compile(r"\.(write_summary_point|index_record_summary|write_enrichment|index_class_units|index_documents|_embed_and_upsert_documents)\(")
        found = {
            str(p.relative_to(app)) for p in app.rglob("*.py")
            if pattern.search("\n".join(line for line in p.read_text().splitlines() if "def " not in line))
        }
        assert found == {
            "modules/pipeline/stages/classify_io.py",  # test_split_stages.py::TestParentSummaryScope
            "modules/transformers/vectorstore.py",  # apply -> of_record (test_vectorstore_split_scope.py)
            "modules/transformers/sink_orchestrator.py",  # write_summary_point -> of_record (above)
            "modules/indexing/class_units_backfill.py",  # _scope_for (above)
        }
