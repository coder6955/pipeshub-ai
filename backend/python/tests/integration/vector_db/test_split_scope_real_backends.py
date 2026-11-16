"""A split file's writes stay in their segment on every vector store (one world for every store).

Qdrant, OpenSearch and Redis here; pgvector in test_pgvector_split_scope.py (it needs its own PostgreSQL).
Requires: docker compose -f tests/integration/compose/vector-db.yml up -d
Run: pytest tests/integration/vector_db/test_split_scope_real_backends.py -m integration --timeout=300
"""
from __future__ import annotations

import pytest

from app.exceptions.indexing_exceptions import VectorStoreError
from app.services.vector_db.const.const import SEGMENT_VRID_FIELD, SPLIT_GEN_FIELD
from tests.integration.vector_db.conftest import make_collection
from tests.integration.vector_db.helpers import make_collection_config
from tests.support import split_scope_world as world

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module"), pytest.mark.timeout(300)]

SCENARIOS = list(world.EXPECT)


class _SplitScopeContract:
    # Scenarios a store refuses (a scoped delete it cannot keep inside the segment), each with its reason.
    expect_refused: frozenset = frozenset()

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.__name__ for s in SCENARIOS])
    async def test_a_write_never_reaches_another_segment(self, vector_service, scenario) -> None:
        col = make_collection(f"{vector_service.get_service_name()}_split")
        try:
            await world.fresh(vector_service, col)
            if scenario in self.expect_refused:
                with pytest.raises(VectorStoreError) as refused:
                    await scenario(vector_service, col)
                assert "does not hold" in f"{refused.value} {getattr(refused.value, 'details', {})}"
                assert await world.census(vector_service, col) == world.FULL, "a refused delete removes nothing"
            else:
                assert await scenario(vector_service, col) == world.EXPECT[scenario]
        finally:
            await vector_service.delete_collection(col)

    async def test_a_rollback_removes_the_points_of_every_segment_of_the_file_and_nothing_else(self, vector_service) -> None:
        """Un-splitting a file before a downgrade: its manifest's points and each part's points go (found by the file's
        id and the segment), another file's stay. The graph side is the in-memory fake; the points are on the real store."""
        col = make_collection(f"{vector_service.get_service_name()}_rollback")
        try:
            await world.fresh(vector_service, col)
            assert await world.rolled_back(vector_service, col) == {"manifest": 0, "part1": 0, "part2": 0, "other": 3}
        finally:
            await vector_service.delete_collection(col)

    async def test_a_whole_file_point_among_the_segments_is_found_and_a_split_file_alone_has_none(self, vector_service) -> None:
        """The roll-forward heal's existence check: false for the world's split file, true once a point without a segment
        (what a build that knows nothing of parts writes) sits beside it."""
        col = make_collection(f"{vector_service.get_service_name()}_stray")
        try:
            await world.fresh(vector_service, col)
            assert await world.stray_points(vector_service, col) == (False, True)
        finally:
            await vector_service.delete_collection(col)

    async def test_the_segment_fields_can_be_indexed_twice(self, vector_service) -> None:
        col = make_collection(f"{vector_service.get_service_name()}_idx")
        try:
            await vector_service.create_collection(col, make_collection_config())
            for _ in range(2):
                for field in (SEGMENT_VRID_FIELD, SPLIT_GEN_FIELD):
                    try:
                        await vector_service.create_index(col, field, {"type": "keyword"})
                    except Exception as e:
                        assert "exist" in str(e).lower() or "duplicate" in str(e).lower()
        finally:
            await vector_service.delete_collection(col)


class TestQdrantSplitScope(_SplitScopeContract):
    @pytest.fixture
    async def vector_service(self, qdrant_service) -> object:
        return qdrant_service


class TestOpenSearchSplitScope(_SplitScopeContract):
    @pytest.fixture
    async def vector_service(self, opensearch_service) -> object:
        return opensearch_service

    async def test_a_field_an_index_maps_as_text_is_refused(self, vector_service) -> None:
        col = make_collection("os_text_seg")
        await vector_service.client.indices.create(index=col, body={"mappings": {"properties": {"metadata": {"properties": {
            "segmentVirtualRecordId": {"type": "text"}}}}}})
        try:
            with pytest.raises(Exception):  # noqa: B017 - the mapping change itself, or the check after it, refuses
                await vector_service.create_index(col, SEGMENT_VRID_FIELD, {"type": "keyword"})
        finally:
            await vector_service.client.indices.delete(index=col)

    async def test_the_mapping_check_names_the_index_that_got_it_wrong(self, vector_service) -> None:
        col = make_collection("os_check_seg")
        await vector_service.create_collection(col, make_collection_config())
        try:
            await vector_service.create_index(col, SEGMENT_VRID_FIELD, {"type": "keyword"})
            await vector_service._require_keyword_mapping(col, SEGMENT_VRID_FIELD)
            with pytest.raises(VectorStoreError):
                await vector_service._require_keyword_mapping(col, "metadata.noSuchField")
        finally:
            await vector_service.delete_collection(col)


class TestRedisSplitScope(_SplitScopeContract):
    # Redis indexes no `blockId` field, so it cannot keep a block-id delete inside its segment (the unindexed condition
    # would be dropped and the whole segment removed). A scoped delete that names a field the index lacks is refused.
    expect_refused = frozenset({world.part_block_delete})

    @pytest.fixture
    async def vector_service(self, redis_service) -> object:
        return redis_service

    async def test_a_scoped_delete_is_refused_when_the_segment_field_is_not_indexed(self, vector_service) -> None:
        from app.services.vector_db.membership import delete_segment_points

        col = make_collection("redis_noseg")
        await vector_service.create_collection(col, make_collection_config())
        try:
            await vector_service.create_index(col, "metadata.orgId", {"type": "keyword"})
            await vector_service.create_index(col, "metadata.virtualRecordId", {"type": "keyword"})
            with pytest.raises(VectorStoreError, match="segmentVirtualRecordId"):
                await delete_segment_points(vector_service, [col], world.ORG, world.V, world.PART1.content_vrid)
        finally:
            await vector_service.delete_collection(col)
