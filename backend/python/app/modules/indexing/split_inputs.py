"""What the parent's own ``classify`` and ``entities`` stages read, once its parts have run.

The parent keeps only a manifest, so its classification reads the manifest plus the part summaries, and its
named entities are the union of what the parts extracted. Both digests are computed when the stage runs, from
the parts' current outputs: a retry that completes a part changes them, and an unchanged re-drive costs no
model call.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from app.config.constants.arangodb import CollectionNames
from app.exceptions.indexing_exceptions import IndexingError
from app.models.blocks import SemanticMetadata
from app.modules.named_entities.domain.models import (
    UNION_STRATEGY,
    ExtractionStats,
    NamedEntityExtraction,
)
from app.modules.named_entities.kept import read_kept_extraction
from app.modules.pipeline.models import stage_state_key
from app.modules.retrieval.hit_shape import block_address

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from app.modules.indexing.split_state import PartSpec, SplitRow, SplitStore
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

CLASSIFY_STAGE = "classify"
RECORDS = CollectionNames.RECORDS.value


@dataclass(frozen=True)
class ClassifyInput:
    text: str
    digest: str
    # The generation whose part summaries the text was built from: a summary written under another one is not this input's.
    gen: str = ""


@dataclass(frozen=True)
class EntitiesInput:
    extraction: NamedEntityExtraction
    digest: str


async def _part_revisions(graph: IGraphDBProvider, row: SplitRow) -> dict[str, str]:
    docs = await graph.get_nodes_by_field_in(
        RECORDS, "virtualRecordId", [p.vrid for p in row.parts],
        return_fields=["virtualRecordId", "contentRev"], raise_on_error=True,
    )
    return {str(d["virtualRecordId"]): str(d["contentRev"]) for d in docs if d.get("contentRev")}


async def _part_summary(graph: IGraphDBProvider, spec: PartSpec, rev: str) -> str:
    state: dict[str, Any] | None = await graph.stage_state_get(stage_state_key(spec.vrid, rev, CLASSIFY_STAGE))
    output = (state or {}).get("output")
    if not output:
        return ""
    try:
        return (SemanticMetadata.model_validate_json(output).summary or "").strip()
    except ValueError:
        return ""


async def classify_input(graph: IGraphDBProvider, row: SplitRow, record_name: str) -> ClassifyInput:
    """The manifest text, then the summary of each page range in page order. A range with no summary yet adds nothing."""
    from app.modules.indexing.split_seam import manifest_markdown

    revs = await _part_revisions(graph, row)
    lines = [manifest_markdown(row, record_name), "", "## Summaries by pages"]
    for spec in row.parts:
        rev = revs.get(spec.vrid)
        summary = await _part_summary(graph, spec, rev) if rev else ""
        if summary:
            lines.append(f"Pages {spec.page_start}–{spec.page_end}: {summary}")
    text = "\n".join(lines)
    return ClassifyInput(text, hashlib.sha256(text.encode("utf-8")).hexdigest()[:32], row.gen)


async def entities_input(graph: IGraphDBProvider, row: SplitRow) -> EntitiesInput:
    """The union of the parts' kept extractions: entities merged by kind and key, each mention addressed by the
    ``blockAddress`` of its block (a part's local index alone would collide with another part's)."""
    revs = await _part_revisions(graph, row)
    merged: dict[tuple[str, str], Any] = {}
    reference_time_ms: int | None = None
    tz = "UTC"
    partial = False
    totals: dict[str, int] = {}
    for spec in row.parts:
        rev = revs.get(spec.vrid)
        kept = await read_kept_extraction(graph, spec.vrid, rev) if rev else None
        if kept is None:
            continue
        partial = partial or kept.status == "PARTIAL"
        for name, value in kept.stats.model_dump().items():
            if isinstance(value, int) and not isinstance(value, bool):
                totals[name] = totals.get(name, 0) + value
        reference_time_ms = reference_time_ms if reference_time_ms is not None else kept.reference_time_ms
        tz = kept.tz if not merged else tz
        for entity in kept.entities:
            target = merged.setdefault((getattr(entity.kind, "value", str(entity.kind)), entity.norm_key), entity.model_copy(deep=True, update={"mentions": []}))
            target.mentions.extend(
                mention.model_copy(update={"block_index": block_address(spec.k, mention.block_index)})
                for mention in entity.mentions
            )
    entities = [merged[key] for key in sorted(merged)]
    extraction = NamedEntityExtraction(
        strategy=UNION_STRATEGY, status="PARTIAL" if partial else "COMPLETED", termination_reason="deterministic",
        entities=entities, stats=ExtractionStats(**totals), reference_time_ms=reference_time_ms, tz=tz,
    )
    canonical = json.dumps(extraction.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return EntitiesInput(extraction, hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32])


class ParentInputs:
    """The inputs of a split parent's stages, by the job's virtual record; None for a record that is not
    a split parent (the stage then reads its ordinary input)."""

    def __init__(self, graph: IGraphDBProvider, store: SplitStore | None = None) -> None:
        from app.modules.indexing.split_state import SplitStore

        self._graph = graph
        self._store = store or SplitStore(graph)

    async def row(self, v: str) -> SplitRow | None:
        return await self._store.get(v)

    async def classify(self, v: str, record_name: str) -> ClassifyInput | None:
        row = await self.row(v)
        return await classify_input(self._graph, row, record_name) if row is not None else None

    async def entities(self, v: str) -> EntitiesInput | None:
        row = await self.row(v)
        return await entities_input(self._graph, row) if row is not None else None


class Ingress(Protocol):
    async def redrive(self, virtual_record_id: str | None, rev: str | None, stages: Sequence[str], *, force: bool = True) -> list[str] | None: ...


class ParentRedrive:
    """Runs the parent's ``classify`` and ``entities`` again, unforced, once its parts are all terminal."""

    STAGES = (CLASSIFY_STAGE, "entities")

    def __init__(self, graph: IGraphDBProvider, ingress: Callable[[], Ingress]) -> None:
        self._graph = graph
        self._ingress = ingress

    async def __call__(self, row: SplitRow) -> None:
        # The holders come back as record keys on every store; the revision is on the documents.
        rev = row.content_rev
        for key in await self._graph.get_records_by_virtual_record_id(row.v, raise_on_error=True) or []:
            document = await self._graph.get_document(key, RECORDS)
            if document and document.get("contentRev"):
                rev = str(document["contentRev"])
                break
        dispatched = await self._ingress().redrive(row.v, rev, self.STAGES, force=False)
        if dispatched is None:
            # The runtime holds no state for this revision: nothing was dispatched, so the drive did not happen. Raising
            # gives the claim back; returning would keep it as if the parent's stages had run.
            raise IndexingError(
                "the stage runtime has no state for the file's revision; its stages were not driven",
                details={"virtual_record_id": row.v},
            )


__all__ = ["ClassifyInput", "EntitiesInput", "classify_input", "entities_input", "ParentInputs", "ParentRedrive"]
