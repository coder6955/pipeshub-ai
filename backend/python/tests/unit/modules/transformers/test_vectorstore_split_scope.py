"""A split file's writes stay in their segment (T2 on the in-memory store; the real stores run the same world in
tests/integration/vector_db/test_split_scope_real_backends.py), and the summary and guard rules of the write scope."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.documents import Document

from app.exceptions.indexing_exceptions import SplitScopeError
from app.models.blocks import Block, BlocksContainer, BlockType, DataFormat
from app.modules.indexing.split_parent_flag import begin_split_parent, end_split_parent
from app.services.vector_db.models import CollectionConfig
from tests.support import split_scope_world as world
from tests.support.in_memory_vector_db import InMemoryVectorDBService

SCENARIOS = list(world.EXPECT)


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.__name__ for s in SCENARIOS])
async def test_a_write_never_reaches_another_segment(scenario) -> None:
    service = InMemoryVectorDBService()
    await world.fresh(service, "records")
    assert await scenario(service, "records") == world.EXPECT[scenario]


async def test_the_unscoped_delete_of_a_split_file_is_refused_while_the_manifest_is_indexed() -> None:
    store = world.store_over(InMemoryVectorDBService())
    begin_split_parent(world.GEN)
    try:
        with pytest.raises(SplitScopeError):
            await store.delete_embeddings(world.V, "records", org_id=world.ORG)
        with pytest.raises(SplitScopeError):
            await store.delete_blocks_by_ids({"b"}, world.V, "records", org_id=world.ORG)
    finally:
        end_split_parent()


async def test_a_part_scope_without_indexed_segment_fields_is_not_written() -> None:
    store = world.store_over(InMemoryVectorDBService())
    store.collection_registry.split_fence_ready = MagicMock(return_value=False)
    store.get_embedding_model_instance = AsyncMock(return_value=False)
    store._ensure_collection = AsyncMock(return_value="records")
    block = Block(id="b", index=0, type=BlockType.TEXT, format=DataFormat.TXT, data="a sentence long enough to embed")
    with pytest.raises(SplitScopeError):
        await store.index_documents(BlocksContainer(blocks=[block]), world.ORG, "rec", world.PART1.content_vrid, scope=world.PART1)


async def test_a_part_writes_no_summary_point_but_the_manifest_does(monkeypatch) -> None:
    monkeypatch.setattr("app.modules.transformers.vectorstore.reset_membership_context", lambda _tokens: None)
    service = InMemoryVectorDBService()
    store = world.store_over(service)
    await world.fresh(service, "records")
    store._collection_with_model = AsyncMock(return_value="records")
    store._bind_membership = AsyncMock(return_value=None)
    store._resync_membership_after_write = AsyncMock()
    store._process_document_chunks = AsyncMock()
    meta = MagicMock(summary="the summary")
    await store.index_record_summary("rec", world.PART1.content_vrid, world.ORG, meta, scope=world.PART1)
    store._process_document_chunks.assert_not_awaited()
    await store.index_record_summary("rec", world.V, world.ORG, meta, scope=world.MANIFEST)
    args, kwargs = store._process_document_chunks.await_args
    assert kwargs["scope"] == world.MANIFEST and args[0][0].metadata["isRecordSummary"]


async def test_the_points_of_a_part_are_written_with_the_files_identity() -> None:
    service = InMemoryVectorDBService()
    await service.create_collection("records", CollectionConfig(embedding_size=world.DIM))
    store = world.store_over(service)
    store._embed_documents_with_retry = AsyncMock(return_value=[[0.0] * world.DIM])
    store._compute_sparse_embeddings = AsyncMock(return_value=[None])
    doc = Document(page_content="text", metadata={"virtualRecordId": world.PART1.content_vrid, "blockId": "b0", "blockIndex": 7,
                                                  "orgId": world.ORG})
    await store._embed_and_upsert_documents([doc], "rec", "records", scope=world.PART1)
    (point,) = service.collections["records"].values()
    assert point.payload["metadata"]["virtualRecordId"] == world.V
    assert point.payload["metadata"]["segmentVirtualRecordId"] == world.PART1.content_vrid
    assert point.payload["metadata"]["splitGen"] == world.GEN and point.payload["metadata"]["blockIndex"] == 7
    assert point.id == world.deterministic_point_id(doc.metadata, "text")


@pytest.mark.asyncio
async def test_a_rollback_removes_the_points_of_every_segment_of_the_file_in_memory() -> None:
    service = InMemoryVectorDBService()
    await world.fresh(service, "records")
    assert await world.rolled_back(service, "records") == {"manifest": 0, "part1": 0, "part2": 0, "other": 3}
