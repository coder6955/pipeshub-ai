"""D-29: the paragraph number shown for a split file is a file-wide ordinal, never the block address.

``sum(blockCount of parts 1..k-1) + local index + 1``; omitted when an earlier count is unknown. The block address
(``blockIndex``) is unchanged: refs and URLs still resolve through it.
"""
from __future__ import annotations

import copy
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config.constants.arangodb import CollectionNames
from app.modules.indexing.split_state import (
    PartInfo,
    SplitRow,
    SplitState,
    SplitStore,
    clear_part_cache,
    part_specs,
    record_block_count,
)
from app.modules.retrieval.hit_shape import (
    display_number,
    hit_from_point,
    part_offset,
    public_hit,
    public_results,
)
from app.modules.retrieval.split_content import StoreSplitResolver
from app.utils.chat_helpers import get_flattened_results, get_record
from tests.support.split_fakes import FakeGraph
from tests.unit.modules.retrieval.split_fakes import (
    FRONTEND,
    V1,
    V2,
    FakeResolver,
    V,
    address,
    file_graph_record,
    part_blob,
    point_meta,
    raw_part_blob,
)

SPLIT_PARTS = CollectionNames.CONTENT_SPLIT_PARTS.value
SPLITS = CollectionNames.CONTENT_SPLITS.value


class TestOrdinalArithmetic:
    def test_the_offset_is_the_blocks_of_the_earlier_parts(self) -> None:
        counts = {1: 10, 2: 7, 3: 5}
        assert [part_offset(counts, k) for k in (1, 2, 3)] == [0, 10, 17]

    def test_the_number_is_offset_plus_local_plus_one(self) -> None:
        assert display_number(3, 4, 17) == 22
        assert display_number(1, 0, 0) == 1

    @pytest.mark.parametrize("counts", [{1: None, 2: 7}, {2: 7}, {1: -1}, {1: True}, {1: "10"}, None])
    def test_a_missing_or_bad_earlier_count_gives_no_offset_and_no_number(self, counts: object) -> None:
        offset = part_offset(counts, 3)  # type: ignore[arg-type]
        assert offset is None
        assert display_number(3, 4, offset) is None

    def test_the_count_of_the_part_itself_and_of_later_parts_is_not_needed(self) -> None:
        assert part_offset({1: 10, 2: None, 3: None}, 2) == 10

    def test_an_unsplit_block_is_numbered_as_it_always_was(self) -> None:
        assert display_number(None, 29, None) == 30
        assert display_number(0, 0, None) == 1
        assert display_number(None, None, None) is None

    def test_a_number_is_never_the_address(self) -> None:
        assert display_number(2, 1, 2) == 4 < address(2, 1)


class _World:
    """Parts V1, V2 (two blocks each) read through ``get_record``, as a search does."""

    async def build(self, resolver: FakeResolver) -> dict:
        blobs = {V1: raw_part_blob(1), V2: raw_part_blob(2)}
        reader = MagicMock()
        reader.read_record = AsyncMock(side_effect=lambda vrid, *_a, **_k: copy.deepcopy(blobs[vrid]))
        records: dict = {}
        with patch("app.utils.chat_helpers.record_artifact_reader", return_value=reader):
            for k, vrid in ((1, V1), (2, V2)):
                await get_record(
                    vrid, records, AsyncMock(), "org-1", {V: file_graph_record()}, None, FRONTEND,
                    identity_vrid=V, segment_index=k, split_resolver=resolver,
                )
        return records


def _hit(k: int, local: int) -> dict:
    return {"content": f"part {k} block {local}", "score": 0.9, "metadata": hit_from_point(point_meta(k, local))}


async def _units(records: dict, hits: list[dict]) -> list[dict]:
    return await get_flattened_results(
        hits, AsyncMock(), "org-1", False, records, {V: file_graph_record()}, from_retrieval_service=True,
    )


@pytest.mark.asyncio
class TestWhatSearchAndCitationsShow:
    async def test_a_split_hit_shows_the_file_wide_ordinal_and_keeps_its_address(self) -> None:
        records = await _World().build(FakeResolver(counts={1: 2, 2: 2}))
        units = await _units(records, [_hit(1, 1), _hit(2, 1)])
        shown = {u["content"]: public_hit(u) for u in units}
        assert shown["part 1 block 1"]["metadata"]["blockNum"] == [2]
        assert shown["part 2 block 1"]["metadata"]["blockNum"] == [4]  # 2 + 1 + 1, not 2**20 + 2
        assert shown["part 2 block 1"]["block_index"] == address(2, 1)  # the address still resolves refs
        body = public_results({"searchResults": units})["searchResults"]
        assert sorted(h["metadata"]["blockNum"][0] for h in body) == [2, 4]

    async def test_no_displayed_number_is_a_block_address(self) -> None:
        records = await _World().build(FakeResolver(counts={1: 2, 2: 2}))
        units = await _units(records, [_hit(1, 0), _hit(2, 0)])
        assert all(u["metadata"]["blockNum"][0] < 1 << 20 for u in units)

    async def test_an_unknown_earlier_count_omits_the_number_rather_than_showing_a_wrong_one(self) -> None:
        records = await _World().build(FakeResolver(counts={1: None, 2: 2}))
        units = await _units(records, [_hit(1, 1), _hit(2, 1)])
        by = {u["content"]: u["metadata"]["blockNum"] for u in units}
        assert by["part 2 block 1"] is None  # part 1's count is missing
        assert by["part 1 block 1"] == [2]  # nothing earlier is needed for part 1

    async def test_an_unreadable_count_lookup_omits_the_number_and_the_search_stands(self) -> None:
        resolver = FakeResolver()
        resolver.block_offsets = AsyncMock(side_effect=RuntimeError("store down"))  # type: ignore[method-assign]
        records = await _World().build(resolver)
        units = await _units(records, [_hit(2, 1)])
        assert len(units) == 1 and units[0]["metadata"]["blockNum"] is None

    async def test_the_counts_are_asked_for_in_one_batch_for_the_search(self) -> None:
        resolver = FakeResolver()
        records = {V1: part_blob(1), V2: part_blob(2)}
        reader = MagicMock()
        reader.read_record = AsyncMock(side_effect=lambda vrid, *_a, **_k: copy.deepcopy({V1: raw_part_blob(1), V2: raw_part_blob(2)}[vrid]))
        records.clear()
        with patch("app.utils.chat_helpers.record_artifact_reader", return_value=reader):
            await get_flattened_results(
                [_hit(1, 1), _hit(2, 1)], AsyncMock(), "org-1", False, records, {V: file_graph_record()},
                from_retrieval_service=True, split_resolver=resolver,
            )
        assert sorted(resolver.offset_calls[0]) == [(V, 1), (V, 2)]
        assert len(resolver.offset_calls[0]) == 2

    async def test_an_unsplit_hit_is_unchanged(self) -> None:
        blob = part_blob(1)
        blob.pop("identity_vrid"), blob.pop("segment_index")
        blob["virtual_record_id"] = "solo"
        hit = {"content": "t", "score": 0.9, "metadata": hit_from_point(
            {"virtualRecordId": "solo", "blockIndex": 1, "isBlockGroup": False, "orgId": "org-1"})}
        units = await get_flattened_results([hit], AsyncMock(), "org-1", False, {"solo": blob}, {}, from_retrieval_service=True)
        assert units[0]["metadata"]["blockNum"] == [2]


class _Counting(FakeGraph):
    def __init__(self) -> None:
        super().__init__()
        self.field_in_calls: list[tuple[str, str]] = []

    async def get_nodes_by_field_in(self, collection: str, field_name: str, field_values: list, *a: object, **k: object) -> list[dict]:  # type: ignore[override]
        self.field_in_calls.append((collection, field_name))
        return await super().get_nodes_by_field_in(collection, field_name, field_values, *a, **k)


def _seed(graph: FakeGraph, v: str, gen: str, counts: dict[int, int | None]) -> None:
    specs = part_specs(gen, [(1 + 10 * i, 10 * (i + 1), 10 * (i + 1)) for i in range(len(counts))], ocr=False)
    row = SplitRow(v=v, org_id="org-1", gen=gen, parts=specs, state=SplitState.COMPLETED)
    graph.put(SPLITS, v, {**row.to_doc(), "id": v})
    for spec in specs:
        info = PartInfo.of(spec, v=v, gen=gen, org_id="org-1", owner_connector_id="own")
        doc = {**info.to_doc(), "_key": info.key}
        if counts[spec.k] is not None:
            doc["blockCount"] = counts[spec.k]
        graph.put(SPLIT_PARTS, info.key, doc)


@pytest.mark.asyncio
class TestStoreResolver:
    async def test_one_read_of_the_part_documents_serves_every_file_of_the_search(self) -> None:
        graph = _Counting()
        _seed(graph, "va", "g1", {1: 5, 2: 6, 3: 7})
        _seed(graph, "vb", "g2", {1: 3, 2: None})
        resolver = StoreSplitResolver(SplitStore(graph))
        out = await resolver.block_offsets([("va", 3), ("vb", 2), ("va", 2), ("vb", 1)])
        assert out == {("va", 3): 11, ("vb", 2): 3, ("va", 2): 5, ("vb", 1): 0}
        assert graph.field_in_calls.count((SPLIT_PARTS, "rootVirtualRecordId")) == 1
        await resolver.block_offsets([("va", 2)])
        assert graph.field_in_calls.count((SPLIT_PARTS, "rootVirtualRecordId")) == 1  # cached for the search

    async def test_a_missing_count_or_an_unsplit_file_has_no_offset(self) -> None:
        graph = _Counting()
        _seed(graph, "vb", "g2", {1: None, 2: 4})
        out = await StoreSplitResolver(SplitStore(graph)).block_offsets([("vb", 2), ("solo", 2)])
        assert out == {("vb", 2): None, ("solo", 2): None}

    async def test_a_part_document_of_a_replaced_generation_is_not_counted(self) -> None:
        graph = _Counting()
        _seed(graph, "vc", "old", {1: 9, 2: 9})
        graph.docs[SPLITS]["vc"]["gen"] = "new"  # the row moved on; the old parts' counts are not the live ones
        out = await StoreSplitResolver(SplitStore(graph)).block_offsets([("vc", 2)])
        assert out == {("vc", 2): None}


@pytest.mark.asyncio
class TestRecordingTheCount:
    async def test_a_completed_part_gets_its_count_on_its_part_document(self) -> None:
        clear_part_cache()
        graph = FakeGraph()
        _seed(graph, "vd", "g4", {1: None, 2: None})
        spec = SplitRow.from_doc({**graph.docs[SPLITS]["vd"], "id": "vd"}).parts[1]
        record = {"_key": spec.node_key, "connectorId": "pdfparts_org-1", "virtualRecordId": spec.vrid}
        await record_block_count(record, 42, graph)
        assert graph.docs[SPLIT_PARTS][spec.node_key]["blockCount"] == 42

    async def test_a_record_that_is_not_a_part_is_left_alone(self) -> None:
        graph = FakeGraph()
        await record_block_count({"_key": "r", "connectorId": "kb-1"}, 42, graph)
        assert not graph.docs.get(SPLIT_PARTS)

    async def test_a_part_with_no_document_never_raises(self) -> None:
        clear_part_cache()
        graph = FakeGraph()
        await record_block_count({"_key": "nope", "connectorId": "pdfparts_org-1"}, 3, graph)  # no part document: logged

    async def test_the_sink_records_the_count_before_the_roll_up(self) -> None:
        from tests.unit.modules.transformers.test_sink_orchestrator import (
            _make_ctx,
            _make_orchestrator,
        )

        order: list[str] = []
        orch = _make_orchestrator()
        ctx = _make_ctx()
        ctx.record.block_containers.blocks = [object()] * 7
        with patch("app.modules.transformers.sink_orchestrator.record_block_count",
                   AsyncMock(side_effect=lambda *a, **k: order.append(f"count:{a[1]}"))), \
             patch("app.modules.transformers.sink_orchestrator.part_terminal",
                   AsyncMock(side_effect=lambda *a, **k: order.append("terminal"))):
            await orch._update_indexing_status(ctx)
        assert order == ["count:7", "terminal"]
