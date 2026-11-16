"""A delete scoped to a segment of a split file never runs where the store would widen it."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.exceptions.indexing_exceptions import VectorStoreError
from app.modules.transformers.write_scope import WriteScope
from app.services.vector_db.membership import (
    delete_segment_points,
    require_indexed_filter,
)
from app.services.vector_db.models import FieldCondition, FilterExpression
from app.services.vector_db.redis.redis_vector import RedisVectorService
from tests.support import split_scope_world as world
from tests.support.in_memory_vector_db import InMemoryVectorDBService


class _BlindToSegments(InMemoryVectorDBService):
    """A store whose index lacks the segment fields and the block id."""

    async def unindexed_filter_fields(self, collection_name: str, flt: FilterExpression) -> list[str]:
        return sorted(c.key for c in (*flt.must, *flt.must_not) if c.key in {"metadata.segmentVirtualRecordId", "metadata.blockId"})


@pytest.mark.asyncio
async def test_a_segment_delete_on_a_store_that_cannot_filter_by_segment_is_refused_and_deletes_nothing() -> None:
    service = _BlindToSegments()
    await world.fresh(service, "records")

    with pytest.raises(VectorStoreError, match="segmentVirtualRecordId"):
        await delete_segment_points(service, ["records"], world.ORG, world.V, world.PART1.content_vrid)

    assert await world.census(service, "records") == world.FULL


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["delete_embeddings", "delete_blocks_by_ids"])
async def test_a_scoped_write_path_delete_is_refused_the_same_way(method: str) -> None:
    service = _BlindToSegments()
    await world.fresh(service, "records")
    store = world.store_over(service)
    call = (
        store.delete_embeddings(world.PART1.content_vrid, "records", org_id=world.ORG, scope=world.PART1)
        if method == "delete_embeddings"
        else store.delete_blocks_by_ids({"b0"}, world.PART1.content_vrid, "records", org_id=world.ORG, scope=world.PART1)
    )

    with pytest.raises(VectorStoreError):
        await call

    assert await world.census(service, "records") == world.FULL


@pytest.mark.asyncio
async def test_an_unsplit_delete_does_not_ask_the_store_anything() -> None:
    service = InMemoryVectorDBService()
    await world.fresh(service, "records")
    asked = AsyncMock(side_effect=AssertionError("an unsplit delete is not scoped"))
    service.unindexed_filter_fields = asked  # type: ignore[method-assign]

    await world.store_over(service).delete_embeddings(world.OTHER, "records", org_id=world.ORG, scope=WriteScope.unsplit(world.OTHER))

    asked.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_store_that_filters_on_any_field_is_never_refused() -> None:
    await require_indexed_filter(InMemoryVectorDBService(), "records", FilterExpression(must=[FieldCondition(key="metadata.blockId", value="b")]))


def _redis(attributes: list[list[str]] | Exception) -> RedisVectorService:
    service = object.__new__(RedisVectorService)
    service._connected = True
    service.client = MagicMock()
    if isinstance(attributes, Exception):
        service.client.execute_command = AsyncMock(side_effect=attributes)
    else:
        service.client.execute_command = AsyncMock(return_value=["attributes", attributes])
    service._assert_connected = lambda: None  # type: ignore[method-assign]
    return service


FILTER = FilterExpression(must=[
    FieldCondition(key="metadata.orgId", value="o"), FieldCondition(key="metadata.segmentVirtualRecordId", value="s"),
    FieldCondition(key="metadata.blockId", values=["b"]),
])


@pytest.mark.asyncio
async def test_redis_names_the_fields_its_index_does_not_hold() -> None:
    service = _redis([["identifier", "metadata_orgId", "type", "TAG"], ["identifier", "metadata_segmentVirtualRecordId", "type", "TAG"]])

    assert await service.unindexed_filter_fields("records", FILTER) == ["metadata.blockId"]


@pytest.mark.asyncio
async def test_redis_that_cannot_read_its_index_says_everything_is_missing() -> None:
    service = _redis(RuntimeError("no such index"))

    assert await service.unindexed_filter_fields("records", FILTER) == sorted(c.key for c in FILTER.must)
