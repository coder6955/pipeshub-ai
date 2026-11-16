"""A split write needs the segment fields indexed (Redis cannot filter an unindexed field; OpenSearch needs keyword mappings)."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.services.vector_db.const.const import (
    PAYLOAD_KEYWORD_INDEXES,
    SEGMENT_VRID_FIELD,
    SPLIT_GEN_FIELD,
)
from app.services.vector_db.strategy import RecordContext
from tests.unit.services.vector_db.test_collection_registry import (
    _make_registry,
    _make_vdb,
)


def test_the_segment_fields_are_payload_indexes() -> None:
    indexed = dict(PAYLOAD_KEYWORD_INDEXES)
    assert indexed[SEGMENT_VRID_FIELD] == indexed[SPLIT_GEN_FIELD] == {"type": "keyword"}


@pytest.mark.parametrize("failing,fence_ok,split_ok", [
    ({SEGMENT_VRID_FIELD}, True, False),
    ({SPLIT_GEN_FIELD}, True, False),
    ({"metadata.contentRev"}, False, True),
    (set(), True, True),
])
async def test_each_fence_is_ready_only_when_its_own_fields_are_indexed(failing, fence_ok, split_ok) -> None:
    vdb = _make_vdb(exists=True, dimension=8)

    async def create_index(collection_name, field_name, field_schema) -> None:
        if field_name in failing:
            raise RuntimeError("index refused")

    vdb.create_index = AsyncMock(side_effect=create_index)
    registry = _make_registry(vector_db_service=vdb)
    await registry.ensure_collection(RecordContext(org_id="org-1"), embedding_size=8)
    assert (registry.fence_ready("records"), registry.split_fence_ready("records")) == (fence_ok, split_ok)


async def test_a_later_success_makes_the_split_fence_ready_and_retries_only_the_missing_fields() -> None:
    failing = [True]
    vdb = _make_vdb(exists=True, dimension=8)

    async def create_index(collection_name, field_name, field_schema) -> None:
        if field_name == SEGMENT_VRID_FIELD and failing[0]:
            raise RuntimeError("index refused")

    vdb.create_index = AsyncMock(side_effect=create_index)
    registry = _make_registry(vector_db_service=vdb)
    ctx = RecordContext(org_id="org-1")
    await registry.ensure_collection(ctx, embedding_size=8)
    assert registry.split_fence_ready("records") is False
    failing[0] = False
    vdb.create_index.reset_mock()
    await registry.ensure_collection(ctx, embedding_size=8)
    assert registry.split_fence_ready("records") is True
    assert {c.kwargs["field_name"] for c in vdb.create_index.await_args_list} == {SEGMENT_VRID_FIELD, SPLIT_GEN_FIELD}


def test_a_collection_never_seen_is_ready() -> None:
    assert _make_registry().split_fence_ready("records") is True
