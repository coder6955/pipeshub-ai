"""The stages of a split file.

A part classifies and extracts for its file's sake and writes only its own stored record and stage state: no summary
point, no taxonomy edges, no graph entities. The parent's own stages read what its parts produced, with a
digest taken when they run, so a retry that completes a part re-runs them and an unchanged re-drive costs no model
call.
"""
from __future__ import annotations

import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames
from app.models.blocks import BlocksContainer, SemanticMetadata
from app.modules.indexing.split_inputs import (
    ParentInputs,
    ParentRedrive,
    classify_input,
    entities_input,
)
from app.modules.indexing.split_state import clear_part_cache
from app.modules.named_entities.domain.kinds import EntityKind
from app.modules.named_entities.domain.models import (
    ExtractionStats,
    Mention,
    NamedEntity,
    NamedEntityExtraction,
)
from app.modules.pipeline.leases import RecordLeases, RevisionSuperseded
from app.modules.pipeline.models import Priority, StageJob, stage_state_key
from app.modules.pipeline.policy import DEFAULT_POLICY
from app.modules.pipeline.stage import Deadline
from app.modules.pipeline.stages.classify import ClassifyStage
from app.modules.pipeline.stages.classify_io import GraphClassifyIO
from app.modules.pipeline.stages.entities import EntitiesStage
from app.modules.pipeline.stages.entities_io import GraphEntitiesIO
from app.modules.transformers.transformer import SPLIT_PARENT_GEN
from app.services.resource_governor.models import ParseTier
from tests.support.split_fakes import FakeGraph
from tests.unit.modules.indexing import test_split_seam as seam_tests
from tests.unit.modules.indexing.test_split_seam import Rig, V
from tests.unit.modules.pipeline.test_classify_io_writes import _Blob, _record, _sink


def _job(stage: str = "classify", *, vrid: str = V, record: str = "rec-1", mime: str | None = "application/pdf") -> StageJob:
    return StageJob.model_validate({
        "stage": stage, "stage_version": 1, "org_id": "org-1", "virtual_record_id": vrid, "rev": "rev-a",
        "record_ids": (record,), "connector_id": "conn-1", "tier": ParseTier.LIGHT, "priority": Priority.BULK,
        "trigger": "embed", "text_digest": "own-digest", "blocks_digest": "b", "text_chars": 10, "has_tables": False,
        "has_images": False, "mime_type": mime,
    })


def _doc(connector: str = "conn-1") -> dict[str, Any]:
    return {
        "_key": "rec-1", "orgId": "org-1", "contentRev": "rev-a", "recordName": "big.pdf", "recordType": "FILE",
        "externalRecordId": "ext-1", "connectorId": connector, "mimeType": "application/pdf", "connectorName": "KB",
        "origin": "UPLOAD",
    }


def _graph(connector: str = "conn-1", **doc: Any) -> MagicMock:  # noqa: ANN401
    graph = MagicMock()
    graph.get_document = AsyncMock(return_value={**_doc(connector), **doc})
    return graph


class _Inputs:
    """What ``ParentInputs`` answers for a split parent (or for a file that is not one)."""

    def __init__(self, *, classify: Any = None, entities: Any = None, gen: str | None = "gen-1") -> None:  # noqa: ANN401
        self._classify, self._entities, self._gen = classify, entities, gen
        self.asked = 0

    async def classify(self, v: str, name: str) -> Any:  # noqa: ANN401
        self.asked += 1
        return self._classify

    async def entities(self, v: str) -> Any:  # noqa: ANN401
        self.asked += 1
        return self._entities

    async def row(self, v: str) -> Any:  # noqa: ANN401
        self.asked += 1
        return MagicMock(gen=self._gen) if self._gen else None


def _classify_io(
    inputs: _Inputs, *, connector: str = "conn-1", blob: _Blob | None = None, sink: MagicMock | None = None, **doc: Any,  # noqa: ANN401
) -> GraphClassifyIO:
    return GraphClassifyIO(
        _job(), Deadline(30), policy=DEFAULT_POLICY, graph=_graph(connector, **doc),
        blob_storage=blob or _Blob({"block_containers": {"blocks": [], "block_groups": []}}),  # type: ignore[arg-type]
        sink=sink or _sink(), classifier=AsyncMock(), config_service=MagicMock(), record_from_document=_record,
        record_leases=RecordLeases(), split_inputs=inputs,  # type: ignore[arg-type]
    )


def _entities_io(inputs: _Inputs, *, connector: str = "conn-1", sink: MagicMock | None = None, **doc: Any) -> GraphEntitiesIO:  # noqa: ANN401
    return GraphEntitiesIO(
        _job("entities"), Deadline(30), policy=DEFAULT_POLICY, graph=_graph(connector, **doc),
        blob_storage=_Blob({"block_containers": {"blocks": [], "block_groups": []}}),  # type: ignore[arg-type]
        sink=sink or _sink(), config_service=MagicMock(), record_from_document=_record, record_leases=RecordLeases(),
        split_inputs=inputs,  # type: ignore[arg-type]
    )


PART_CONNECTOR = "pdfparts_org-1"


@pytest.mark.asyncio
class TestPartsStagesWriteOnlyTheirOwnRecord:
    async def test_a_part_classification_goes_into_its_stored_record_and_nowhere_else(self) -> None:
        blob, sink = _Blob({"block_containers": {"blocks": [], "block_groups": []}}), _sink()
        io = _classify_io(_Inputs(), connector=PART_CONNECTOR, blob=blob, sink=sink)

        await io.save(_job(), ["rec-1"], SemanticMetadata(summary="part summary"))

        assert blob.saves == 1 and blob.stored["semantic_metadata"]["summary"] == "part summary"
        sink.prepare_enrichment.assert_not_called()
        sink.write_enrichment.assert_not_called()
        sink.write_summary_point.assert_not_called()

    async def test_a_part_keeps_its_extraction_and_writes_no_entity_to_the_graph(self) -> None:
        sink = _sink()
        io = _entities_io(_Inputs(), connector=PART_CONNECTOR, sink=sink)

        outcome = await io.save(_job("entities"), ["rec-1"], NamedEntityExtraction(), BlocksContainer())

        assert outcome == "kept"
        sink.persist_named_entities.assert_not_called()

    @pytest.mark.parametrize(
        ("status", "windows_total", "windows_run", "queued"),
        [("PARTIAL", 3, 1, True), ("COMPLETED", 3, 3, False), ("PARTIAL", 3, 3, False)],
        ids=["left text unread", "complete", "partial with nothing left to read"],
    )
    async def test_a_part_that_left_text_unread_queues_its_own_reextraction(
        self, status: str, windows_total: int, windows_run: int, queued: bool,
    ) -> None:
        sink = _sink()
        io = _entities_io(_Inputs(), connector=PART_CONNECTOR, sink=sink)
        io._graph.schedule_named_entity_reextract = AsyncMock(return_value=True)
        extraction = NamedEntityExtraction(
            status=status, strategy="single_call", termination_reason="single_call",
            stats=ExtractionStats(windows_total=windows_total, windows_run=windows_run),
        )

        assert await io.save(_job("entities"), ["rec-1"], extraction, BlocksContainer()) == "kept"

        assert io._graph.schedule_named_entity_reextract.await_count == (1 if queued else 0)
        if queued:
            io._graph.schedule_named_entity_reextract.assert_awaited_once_with("org-1", "rec-1")

    async def test_an_ordinary_file_still_gets_every_write(self) -> None:
        sink = _sink()
        await _classify_io(_Inputs(gen=None), sink=sink).save(_job(), ["rec-1"], SemanticMetadata(summary="s"))
        await _entities_io(_Inputs(gen=None), sink=sink).save(_job("entities"), ["rec-1"], NamedEntityExtraction(), BlocksContainer())
        sink.write_enrichment.assert_awaited_once()
        sink.persist_named_entities.assert_awaited_once()


@pytest.mark.asyncio
class TestParentStagesReadTheirParts:
    async def test_the_parents_digest_and_blocks_come_from_the_parts_not_from_the_manifest(self) -> None:
        parent = MagicMock(digest="parts-digest", text="manifest\nPart 1: summary")
        io = _classify_io(_Inputs(classify=parent))

        assert await io.input_digest(_job()) == "parts-digest"
        blocks = await io.load_blocks(_job())
        assert [b.data for b in blocks.blocks] == ["manifest\nPart 1: summary"]

    async def test_an_ordinary_file_reads_its_own_digest_and_blocks(self) -> None:
        io = _classify_io(_Inputs())
        assert await io.input_digest(_job()) == "own-digest"
        assert (await io.load_blocks(_job())).blocks == []

    async def test_only_a_pdf_pays_for_the_lookup(self) -> None:
        inputs = _Inputs(classify=MagicMock(digest="x", text="t"))
        io = _classify_io(inputs, mimeType="text/plain")
        assert await io.input_digest(_job(mime="text/plain")) == "own-digest"
        assert inputs.asked == 0

    @pytest.mark.parametrize(
        ("job_mime", "doc"),
        [("application/octet-stream", {"extension": "pdf", "mimeType": "application/octet-stream"}),
         (None, {"extension": "pdf", "mimeType": None})],
        ids=["extension-only", "no-mime"],
    )
    async def test_a_split_pdf_known_by_its_extension_only_still_reads_its_parts(
        self, job_mime: str | None, doc: dict[str, Any]
    ) -> None:
        parent = MagicMock(digest="parts-digest", text="manifest", entities="e")
        classify_io = _classify_io(_Inputs(classify=parent), **doc)
        assert await classify_io.input_digest(_job(mime=job_mime)) == "parts-digest"
        entities_io = _entities_io(_Inputs(entities=MagicMock(digest="union", entities=[])), **doc)
        assert await entities_io.input_digest(_job("entities", mime=job_mime)) == "union"

    async def test_the_parent_writes_its_summary_point_as_the_files_own_segment(self) -> None:
        sink = _sink()
        io = _classify_io(_Inputs(gen="gen-7"), sink=sink)

        await io.save(_job(), ["rec-1"], SemanticMetadata(summary="the file"))

        ctx = sink.write_enrichment.await_args.args[0]
        assert ctx.settings[SPLIT_PARENT_GEN] == "gen-7"

    async def test_the_parents_entities_are_the_union_of_its_parts_with_no_model_call(self) -> None:
        union = NamedEntityExtraction(strategy="parts")
        sink = _sink()
        sink.extract_named_entities = AsyncMock()
        io = _entities_io(_Inputs(entities=MagicMock(digest="u", extraction=union)), sink=sink)

        assert await io.input_digest(_job("entities")) == "u"
        assert await io.extract(_job("entities"), BlocksContainer()) is union
        sink.extract_named_entities.assert_not_called()


class _Graph(FakeGraph):
    """The fake graph with the stage-state reads the inputs use."""

    def __init__(self) -> None:
        super().__init__()
        self.states: dict[str, dict[str, Any]] = {}

    async def get_records_by_virtual_record_id(self, vrid: str, **_kw: object) -> list[str]:
        """Record keys, as every real store answers."""
        return [k for k, d in self._coll("records").items() if d.get("virtualRecordId") == vrid]

    async def stage_state_get(self, key: str) -> dict[str, Any] | None:
        return self.states.get(key)

    async def get_nodes_by_field_in(self, collection: str, field_name: str, field_values: list[Any], **_kw: object) -> list[dict[str, Any]]:
        return [dict(d) for d in self._coll(collection).values() if d.get(field_name) in set(field_values)]


async def _split_world() -> tuple[Rig, _Graph]:
    clear_part_cache()
    seam_tests.NOW[0] = 1_000_000
    rig = Rig()
    graph = _Graph()
    graph.docs = rig.graph.docs
    rig.graph = graph
    await rig.seam.split(rig.request())
    for spec in (await rig.store.get(V)).parts:
        graph.docs["records"][spec.node_key]["contentRev"] = f"rev-{spec.k}"
    return rig, graph


def _kept(
    graph: _Graph, spec: Any, rev: str, *, summary: str | None = None, entities: list[NamedEntity] | None = None,  # noqa: ANN401
    extraction: NamedEntityExtraction | None = None,
) -> None:
    if extraction is not None:
        graph.states[stage_state_key(spec.vrid, rev, "entities")] = {"output": extraction.model_dump_json()}
    if summary is not None:
        graph.states[stage_state_key(spec.vrid, rev, "classify")] = {"output": SemanticMetadata(summary=summary).model_dump_json()}
    if entities is not None:
        graph.states[stage_state_key(spec.vrid, rev, "entities")] = {"output": NamedEntityExtraction(entities=entities).model_dump_json()}


def _entity(norm_key: str, *blocks: int) -> NamedEntity:
    return NamedEntity(
        kind=EntityKind.PERSON, display_name=norm_key.title(), norm_key=norm_key,
        mentions=[Mention(block_index=b, char_start=0, char_end=3, surface=norm_key, extractor="pattern") for b in blocks],
    )


@pytest.mark.asyncio
class TestInputsOfTheParent:
    async def test_the_classification_input_is_the_manifest_then_each_summary_in_part_order(self) -> None:
        rig, graph = await _split_world()
        row = await rig.store.get(V)
        _kept(graph, row.parts[2], "rev-3", summary="third")
        _kept(graph, row.parts[0], "rev-1", summary="first")

        found = await classify_input(graph, row, "big.pdf")

        text = found.text
        assert text.startswith("# big.pdf")
        assert text.index("Pages 1–") < text.index("Pages 1001–")
        assert "first" in text and "third" in text and "Pages 501–" not in text.split("Summaries")[1]
        assert not re.search(r"\bparts?\b", text, re.IGNORECASE), "the classifier is never told about parts"

    async def test_a_summary_that_arrives_changes_the_digest_and_an_unchanged_input_does_not(self) -> None:
        rig, graph = await _split_world()
        row = await rig.store.get(V)
        _kept(graph, row.parts[0], "rev-1", summary="first")
        before = await classify_input(graph, row, "big.pdf")
        again = await classify_input(graph, row, "big.pdf")
        _kept(graph, row.parts[1], "rev-2", summary="second")
        after = await classify_input(graph, row, "big.pdf")

        assert before.digest == again.digest
        assert after.digest != before.digest

    async def test_the_stage_fingerprint_follows_the_runtime_digest(self) -> None:
        rig, graph = await _split_world()
        row = await rig.store.get(V)
        _kept(graph, row.parts[0], "rev-1", summary="first", entities=[_entity("ada", 4)])
        inputs = ParentInputs(graph, rig.store)
        classify_io = _classify_io(inputs)  # type: ignore[arg-type]
        classify_io._graph = graph  # type: ignore[assignment]
        stage = ClassifyStage()
        classify_io.model_identity = AsyncMock(return_value="m")  # type: ignore[method-assign]
        classify_io.departments = AsyncMock(return_value=[])  # type: ignore[method-assign]
        classify_io.classify_config = AsyncMock(return_value={})  # type: ignore[method-assign]
        job = _job()

        first = (await stage.fingerprint(job, classify_io)).digest()
        graph.docs["records"][row.parts[1].node_key]["contentRev"] = "rev-2"
        _kept(graph, row.parts[1], "rev-2", summary="a part the retry completed")
        again_io = _classify_io(inputs)  # type: ignore[arg-type]
        again_io._graph = graph  # type: ignore[assignment]
        again_io.model_identity = AsyncMock(return_value="m")  # type: ignore[method-assign]
        again_io.departments = AsyncMock(return_value=[])  # type: ignore[method-assign]
        again_io.classify_config = AsyncMock(return_value={})  # type: ignore[method-assign]
        second = (await stage.fingerprint(job, again_io)).digest()
        third = (await stage.fingerprint(job, again_io)).digest()

        assert first != second, "a retry that completes a part changes the input, so the stage runs"
        assert second == third, "an unchanged re-drive has the same fingerprint, so it costs no model call"
        assert EntitiesStage.name == "entities"

    async def test_the_entities_are_merged_by_kind_and_key_with_block_addresses(self) -> None:
        rig, graph = await _split_world()
        row = await rig.store.get(V)
        _kept(graph, row.parts[0], "rev-1", entities=[_entity("ada", 4), _entity("grace", 1)])
        _kept(graph, row.parts[1], "rev-2", entities=[_entity("ada", 9)])

        found = await entities_input(graph, row)

        by_key = {e.norm_key: e for e in found.extraction.entities}
        assert sorted(by_key) == ["ada", "grace"]
        assert sorted(m.block_index for m in by_key["ada"].mentions) == [(1 << 20) + 4, (2 << 20) + 9]
        assert [m.block_index for m in by_key["grace"].mentions] == [(1 << 20) + 1]
        assert found.digest == (await entities_input(graph, row)).digest
        _kept(graph, row.parts[2], "rev-3", entities=[_entity("alan", 0)])
        assert (await entities_input(graph, row)).digest != found.digest


@pytest.mark.asyncio
class TestTheUnionReportsWhatItsPartsLeftUnread:
    async def test_one_partial_part_makes_the_union_partial_with_summed_stats_and_never_queues_the_file(self) -> None:
        from app.modules.named_entities.stage import needs_reextract

        rig, graph = await _split_world()
        row = await rig.store.get(V)
        stats = ExtractionStats(windows_total=4, windows_run=2, tokens_in=10)
        _kept(graph, row.parts[0], "rev-1", extraction=NamedEntityExtraction(
            status="PARTIAL", strategy="single_call", termination_reason="single_call", stats=stats, entities=[_entity("ada", 1)]))
        _kept(graph, row.parts[1], "rev-2", extraction=NamedEntityExtraction(
            status="COMPLETED", strategy="single_call", termination_reason="single_call",
            stats=ExtractionStats(windows_total=2, windows_run=2, tokens_in=5)))

        found = await entities_input(graph, row)

        assert found.extraction.status == "PARTIAL"
        assert (found.extraction.stats.windows_total, found.extraction.stats.windows_run, found.extraction.stats.tokens_in) == (6, 4, 15)
        assert not needs_reextract(found.extraction), "the file holds a manifest: recovery is the part's, never the file's"

    async def test_complete_parts_make_a_complete_union(self) -> None:
        rig, graph = await _split_world()
        row = await rig.store.get(V)
        _kept(graph, row.parts[0], "rev-1", entities=[_entity("ada", 1)])

        assert (await entities_input(graph, row)).extraction.status == "COMPLETED"


@pytest.mark.asyncio
class TestRedrive:
    async def test_the_parents_stages_are_driven_unforced_with_the_parents_revision(self) -> None:
        rig, graph = await _split_world()
        graph.docs["records"]["parent-1"]["contentRev"] = "rev-parent"
        calls: list[tuple[Any, ...]] = []

        class Ingress:
            async def redrive(self, vrid: str, rev: str, stages: Any, *, force: bool = True) -> list[str]:  # noqa: ANN401
                calls.append((vrid, rev, tuple(stages), force))
                return []

        await ParentRedrive(graph, lambda: Ingress())(await rig.store.get(V))  # type: ignore[arg-type]

        assert calls == [(V, "rev-parent", ("classify", "entities"), False)]
        assert CollectionNames.RECORDS.value == "records"


@pytest.mark.asyncio
class TestParentSummaryScope:
    """every writer of the split parent's summary point names the file's segment and generation."""

    async def test_save_names_the_generation(self) -> None:
        sink = _sink()
        await _classify_io(_Inputs(gen="gen-7"), sink=sink).save(_job(), ["rec-1"], SemanticMetadata(summary="the file"))
        assert sink.write_enrichment.await_args.args[0].settings.get(SPLIT_PARENT_GEN) == "gen-7"

    async def test_the_unchanged_put_back_names_the_generation_too(self) -> None:
        sink = _sink()
        await _classify_io(_Inputs(gen="gen-7"), sink=sink)._restore_summary_point(_job(), "rec-1", SemanticMetadata(summary="the file"))
        ctx = sink.write_summary_point.await_args.args[0]
        assert ctx.settings.get(SPLIT_PARENT_GEN) == "gen-7"
        from app.modules.transformers.write_scope import WriteScope

        scope = await WriteScope.of_record(ctx.record, gen=ctx.settings.get(SPLIT_PARENT_GEN))
        assert scope.payload({}) | {} == {"virtualRecordId": scope.read_vrid, "segmentVirtualRecordId": scope.read_vrid, "splitGen": "gen-7", "segmentIndex": 0}

    async def test_a_summary_built_under_one_generation_is_not_written_under_another(self) -> None:
        """A forced reindex of unchanged bytes keeps the revision and replaces the generation: the guard is the generation."""
        from app.modules.indexing.split_inputs import ClassifyInput

        built = ClassifyInput("manifest and summaries", "digest", "gen-1")
        for write in ("save", "put_back"):
            sink = _sink()
            io = _classify_io(_Inputs(classify=built, gen="gen-2"), sink=sink)  # the row moved to gen-2 since the input was built
            await io.input_digest(_job())
            with pytest.raises(RevisionSuperseded):
                if write == "save":
                    await io.save(_job(), ["rec-1"], SemanticMetadata(summary="old text"))
                else:
                    await io._restore_summary_point(_job(), "rec-1", SemanticMetadata(summary="old text"))
            sink.prepare_enrichment.assert_not_called()
            sink.write_enrichment.assert_not_called()
            sink.write_summary_point.assert_not_called()

        sink = _sink()
        io = _classify_io(_Inputs(classify=built, gen="gen-1"), sink=sink)
        await io.input_digest(_job())
        await io.save(_job(), ["rec-1"], SemanticMetadata(summary="same generation"))
        sink.write_enrichment.assert_awaited_once()

    async def test_an_ordinary_file_is_put_back_unscoped(self) -> None:
        sink = _sink()
        await _classify_io(_Inputs(gen=None), sink=sink)._restore_summary_point(_job(), "rec-1", SemanticMetadata(summary="a"))
        assert SPLIT_PARENT_GEN not in sink.write_summary_point.await_args.args[0].settings

    async def test_the_restore_of_an_unchanged_classification_goes_through_the_stored_write(self) -> None:
        sink = _sink()
        io = _classify_io(_Inputs(gen="gen-7"), sink=sink)
        await io.restore_stored_metadata(_job(), SemanticMetadata(summary="the file"))
        assert sink.write_summary_point.await_args.args[0].settings.get(SPLIT_PARENT_GEN) == "gen-7"
