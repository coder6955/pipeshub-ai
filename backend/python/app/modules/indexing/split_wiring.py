"""Builds the split seam, purger and sweep from the services the indexing process already has."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import CollectionNames
from app.modules.artifacts.reader import record_artifact_reader
from app.modules.indexing.owner_connector import owner_connector_id
from app.modules.indexing.split_inputs import ParentRedrive
from app.modules.indexing.split_purge import GenerationPurger
from app.modules.indexing.split_reembed import SplitReembed
from app.modules.indexing.split_reindex import retry_failed_parts_of
from app.modules.indexing.split_rollback import (
    ROLLBACK_ORG_ENV,
    SplitRollback,
    rollback_mode,
)
from app.modules.indexing.split_rollup import SplitRollup, use_rollup
from app.modules.indexing.split_seam import ParentSplitSeam, pool_cutter
from app.modules.indexing.split_state import SplitStore, use_graph
from app.modules.indexing.split_sweep import SplitSweep
from app.modules.parsers.pdf.split.config import SplitConfig
from app.modules.transformers.blob_storage import DerivedFileTags
from app.modules.transformers.write_scope import use_live_generation
from app.services.cache.invalidation_hooks import notify_record_indexed
from app.services.vector_db.strategy import RecordContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from app.modules.indexing.split_purge import PartLeases
    from app.modules.indexing.split_seam import EventSender
    from app.modules.indexing.split_state import PartSpec, SplitRow
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider


@dataclass(frozen=True)
class SplitRuntime:
    store: SplitStore
    purger: GenerationPurger
    seam: ParentSplitSeam
    sweep: SplitSweep
    rollup: SplitRollup
    reembed: SplitReembed


class _PartResources:
    """Points, stored record and file of a part, through the services the sink already uses."""

    def __init__(self, pipeline: Any, blob_storage: Any) -> None:  # noqa: ANN401
        self._pipeline = pipeline
        self._blob = blob_storage

    async def delete_points_and_stored_record(self, org_id: str, root_virtual_record_id: str, part_virtual_record_id: str) -> None:
        # The part's points carry the file's virtual record id, so they are found by it and the part's segment. By then
        # no record holds the part's own id, so its stored record and mapping go after them.
        await self._pipeline.delete_segment_points(org_id, root_virtual_record_id, part_virtual_record_id)
        await self._blob.delete_storage_docs_for_vrid(org_id, part_virtual_record_id)

    async def delete_pdf(self, org_id: str, storage_document_id: str) -> None:
        await self._blob.delete_document(org_id, storage_document_id)


class _StoredBlockCounts:
    """The number of core blocks in a part's stored record, for parts that completed before it was recorded."""

    def __init__(self, blob_storage: Any) -> None:  # noqa: ANN401
        self._blob = blob_storage

    async def of(self, row: SplitRow, part: PartSpec) -> int | None:
        stored = await record_artifact_reader(self._blob).read_record(part.vrid, row.org_id)
        blocks = ((stored or {}).get("block_containers") or {}).get("blocks")
        return len(blocks) if isinstance(blocks, list) else None


class _PartUploader:
    def __init__(self, blob_storage: Any) -> None:  # noqa: ANN401
        self._blob = blob_storage

    async def save(self, org_id: str, document_path: str, data: bytes, *, v: str, owner_connector_id: str) -> str:
        tags = DerivedFileTags(connector_id=owner_connector_id, virtual_record_id=v)
        return await self._blob.save_derived_pdf(org_id, document_path, data, tags)


class _WholeFile:
    def __init__(self, pipeline: Any) -> None:  # noqa: ANN401
        self._pipeline = pipeline

    async def delete_stray_points(self, parent: Mapping[str, Any], row: SplitRow) -> None:
        await self._pipeline.delete_points_outside_segments(
            RecordContext.from_graph_document(parent), row.org_id, row.v, [row.v, *(p.vrid for p in row.parts)],
        )

    async def has_stray_points(self, parent: Mapping[str, Any], row: SplitRow) -> bool:
        return await self._pipeline.has_points_outside_segments(
            RecordContext.from_graph_document(parent), row.org_id, row.v, [row.v, *(p.vrid for p in row.parts)],
        )


def notify_file_searchable(graph: IGraphDBProvider) -> Callable[[SplitRow], Awaitable[None]]:
    """What an unsplit file's completion does, for a split file whose roll-up completed: the accessible-record map of the
    file's own connector is stale. Read from the file's record, never from the row."""

    async def notify(row: SplitRow) -> None:
        docs = await graph.get_nodes_by_field_in(
            CollectionNames.RECORDS.value, "virtualRecordId", [row.v],
            return_fields=["connectorName", "connectorId", "externalRecordGroupId", "orgId"], raise_on_error=True,
        )
        for doc in docs:
            await notify_record_indexed(
                connector_name=doc.get("connectorName"), connector_id=owner_connector_id(doc),
                external_record_group_id=doc.get("externalRecordGroupId"), org_id=doc.get("orgId") or row.org_id,
            )

    return notify


def build_split_runtime(
    *,
    graph: IGraphDBProvider,
    pipeline: Any,  # noqa: ANN401
    blob_storage: Any,  # noqa: ANN401
    leases: PartLeases,
    publisher: EventSender,
    lease_seconds: float,
    ingress: Any,  # noqa: ANN401
) -> SplitRuntime:
    use_graph(graph)
    store = SplitStore(graph)
    purger = GenerationPurger(graph, store, leases, _PartResources(pipeline, blob_storage))
    rollup = SplitRollup(graph, store, ParentRedrive(graph, lambda: ingress), on_searchable=notify_file_searchable(graph))
    use_rollup(rollup)

    async def live_generation(v: str) -> str | None:
        # Whatever state the row is in, a write to the file stays inside a segment.
        row = await store.get(v)
        return row.gen if row is not None else None

    use_live_generation(live_generation)

    async def retry_failed(v: str, *, owner_connector_id: str) -> int:
        return await retry_failed_parts_of(graph, store, publisher, rollup, v, owner_connector_id=owner_connector_id)

    seam = ParentSplitSeam(
        graph=graph, store=store, purger=purger, cutter=pool_cutter, uploader=_PartUploader(blob_storage),
        publisher=publisher, whole_file=_WholeFile(pipeline), config=SplitConfig.from_env, retry_failed=retry_failed,
    )
    rollback = None
    if (mode := rollback_mode()) is not None:
        async def manifest_points(org_id: str, v: str) -> None:
            await pipeline.delete_segment_points(org_id, v, v)

        rollback = SplitRollback(
            graph=graph, store=store, purger=purger, publisher=publisher, manifest_points=manifest_points,
            apply=mode == "apply", org_id=os.environ.get(ROLLBACK_ORG_ENV) or None,
            splitting_enabled=SplitConfig.from_env().enabled,
        )
    sweep = SplitSweep(
        graph=graph, store=store, purger=purger, queue_parts=seam.queue_parts,
        parts_missing_files=seam.parts_missing_files, lease_ms=int(lease_seconds * 1000), rollup=rollup,
        part_block_count=_StoredBlockCounts(blob_storage).of, rollback=rollback,
    )
    return SplitRuntime(store, purger, seam, sweep, rollup, SplitReembed(graph, store, publisher))
