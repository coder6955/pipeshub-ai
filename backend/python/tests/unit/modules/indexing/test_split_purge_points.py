"""The purge of a retired part removes the part's points, which carry the file's virtual record id.

Review finding of the large-PDF split pull request: the purge deleted by the part's own id as the virtual record id,
which matched no point, so a retired generation's points stayed until the whole file was deleted.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.modules.indexing.run import IndexingPipeline
from app.modules.indexing.split_wiring import _PartResources
from app.services.vector_db.membership import delete_segment_points
from tests.support import split_scope_world as world
from tests.support.in_memory_vector_db import InMemoryVectorDBService


@pytest.mark.asyncio
async def test_the_wiring_deletes_by_the_files_id_and_the_part_segment_then_the_stored_record() -> None:
    pipeline, blob = MagicMock(), MagicMock()
    calls: list[str] = []
    pipeline.delete_segment_points = AsyncMock(side_effect=lambda *a: calls.append(("points", *a)))
    blob.delete_storage_docs_for_vrid = AsyncMock(side_effect=lambda *a: calls.append(("stored", *a)))

    await _PartResources(pipeline, blob).delete_points_and_stored_record("org-1", "file-vrid", "part-vrid")

    assert calls == [("points", "org-1", "file-vrid", "part-vrid"), ("stored", "org-1", "part-vrid")]


@pytest.mark.asyncio
async def test_the_pipeline_deletes_the_segment_in_every_collection_of_the_organisation() -> None:
    pipeline = IndexingPipeline.__new__(IndexingPipeline)
    pipeline.vector_db_service = MagicMock()
    pipeline.vector_db_service.filter_collection = AsyncMock(return_value="FILTER")
    pipeline.vector_db_service.delete_points = AsyncMock()
    pipeline.vector_db_service.unindexed_filter_fields = AsyncMock(return_value=[])
    pipeline.collection_locator = MagicMock()
    pipeline.collection_locator.all_collections = AsyncMock(return_value=["records", "records_b"])

    await pipeline.delete_segment_points("org-1", "file-vrid", "part-vrid")

    pipeline.collection_locator.all_collections.assert_awaited_once_with(fresh=True, strict=True)
    pipeline.vector_db_service.filter_collection.assert_awaited_once_with(
        must={"orgId": "org-1", "virtualRecordId": "file-vrid", "segmentVirtualRecordId": "part-vrid"},
    )
    assert [c.kwargs["collection_name"] for c in pipeline.vector_db_service.delete_points.await_args_list] == ["records", "records_b"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("org", "root", "segment"), [("", "r", "s"), ("o", "", "s"), ("o", "r", "")])
async def test_a_delete_missing_any_of_organisation_file_or_segment_is_refused(org: str, root: str, segment: str) -> None:
    with pytest.raises(ValueError):
        await delete_segment_points(InMemoryVectorDBService(), ["records"], org, root, segment)


@pytest.mark.asyncio
async def test_on_a_store_holding_the_points_only_the_segments_are_deleted_and_the_part_id_alone_deletes_nothing() -> None:
    service = InMemoryVectorDBService()
    await world.fresh(service, "records")

    assert await world.purge_by_the_part_id_alone(service, "records") == world.FULL, "the old purge: nothing deleted"
    assert await world.part_purge(service, "records") == {"manifest": 3, "part1": 0, "part2": 3, "other": 3}
