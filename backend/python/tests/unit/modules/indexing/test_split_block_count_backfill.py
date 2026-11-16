"""D-29: parts that completed before their block count was recorded get it from the stored record, once, in the heal sweep."""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames
from app.modules.indexing.split_state import (
    PartInfo,
    SplitRow,
    SplitState,
    SplitStore,
    part_specs,
)
from app.modules.indexing.split_sweep import SplitSweep
from tests.support.split_fakes import FakeGraph

SPLITS = CollectionNames.CONTENT_SPLITS.value
SPLIT_PARTS = CollectionNames.CONTENT_SPLIT_PARTS.value
RECORDS = CollectionNames.RECORDS.value
V, GEN, ORG = "v-old", "gen-old", "org-1"


def _seed(graph: FakeGraph, statuses: dict[int, str], counts: dict[int, int | None]) -> tuple[SplitRow, list[Any]]:
    specs = part_specs(GEN, [(1 + 10 * i, 10 * (i + 1), 10 * (i + 1)) for i in range(len(statuses))], ocr=False)
    row = SplitRow(v=V, org_id=ORG, gen=GEN, parts=specs, state=SplitState.COMPLETED)
    graph.put(SPLITS, V, {**row.to_doc(), "id": V})
    for spec in specs:
        info = PartInfo.of(spec, v=V, gen=GEN, org_id=ORG, owner_connector_id="own")
        doc = {**info.to_doc(), "_key": info.key}
        if counts[spec.k] is not None:
            doc["blockCount"] = counts[spec.k]
        graph.put(SPLIT_PARTS, info.key, doc)
        graph.put(RECORDS, info.key, {"virtualRecordId": spec.vrid, "indexingStatus": statuses[spec.k]})
    return row, specs


def _sweep(graph: FakeGraph, reader: AsyncMock) -> SplitSweep:
    return SplitSweep(
        graph=graph, store=SplitStore(graph), purger=MagicMock(), queue_parts=AsyncMock(), parts_missing_files=AsyncMock(),
        lease_ms=1, part_block_count=reader, rollup=None,
    )


@pytest.mark.asyncio
class TestBackfill:
    async def test_a_completed_part_without_a_count_gets_it_from_its_stored_record(self) -> None:
        graph = FakeGraph()
        _, specs = _seed(graph, {1: "COMPLETED", 2: "COMPLETED"}, {1: 5, 2: None})
        reader = AsyncMock(return_value=9)

        await _sweep(graph, reader)._backfill_block_counts()

        assert graph.docs[SPLIT_PARTS][specs[1].node_key]["blockCount"] == 9
        assert graph.docs[SPLIT_PARTS][specs[0].node_key]["blockCount"] == 5  # untouched
        assert [c.args[1].k for c in reader.await_args_list] == [2]  # the part with a count is not read

    async def test_it_is_a_one_time_read(self) -> None:
        graph = FakeGraph()
        _seed(graph, {1: "COMPLETED", 2: "COMPLETED"}, {1: None, 2: None})
        reader = AsyncMock(return_value=3)
        sweep = _sweep(graph, reader)

        await sweep._backfill_block_counts()
        await sweep._backfill_block_counts()

        assert reader.await_count == 2

    async def test_a_part_that_did_not_complete_has_no_stored_record_to_read(self) -> None:
        graph = FakeGraph()
        _, specs = _seed(graph, {1: "COMPLETED", 2: "FAILED"}, {1: None, 2: None})
        reader = AsyncMock(return_value=3)

        await _sweep(graph, reader)._backfill_block_counts()

        assert [c.args[1].k for c in reader.await_args_list] == [1]
        assert "blockCount" not in graph.docs[SPLIT_PARTS][specs[1].node_key]

    async def test_an_unreadable_record_leaves_the_count_for_the_next_pass(self) -> None:
        graph = FakeGraph()
        _, specs = _seed(graph, {1: "COMPLETED", 2: "COMPLETED"}, {1: None, 2: None})
        reader = AsyncMock(side_effect=[RuntimeError("blob down"), 4])

        await _sweep(graph, reader)._backfill_block_counts()

        assert "blockCount" not in graph.docs[SPLIT_PARTS][specs[0].node_key]
        assert graph.docs[SPLIT_PARTS][specs[1].node_key]["blockCount"] == 4

    async def test_the_heal_pass_runs_it(self) -> None:
        graph = FakeGraph()
        _, specs = _seed(graph, {1: "COMPLETED"}, {1: None})
        rollup = MagicMock(recount=AsyncMock(), drive=AsyncMock(), reclaim_stale=AsyncMock())
        sweep = SplitSweep(
            graph=graph, store=SplitStore(graph, clock_ms=lambda: 10**12), purger=MagicMock(), queue_parts=AsyncMock(),
            parts_missing_files=AsyncMock(), lease_ms=1, rollup=rollup, part_block_count=AsyncMock(return_value=6),
        )
        await sweep._heal()
        assert graph.docs[SPLIT_PARTS][specs[0].node_key]["blockCount"] == 6


@pytest.mark.asyncio
async def test_the_count_is_the_number_of_core_blocks_of_the_stored_record() -> None:
    from unittest.mock import patch

    from app.modules.indexing.split_wiring import _StoredBlockCounts

    spec = part_specs(GEN, [(1, 10, 10)], ocr=False)[0]
    row = SplitRow(v=V, org_id=ORG, gen=GEN, parts=(spec,), state=SplitState.COMPLETED)
    reader = MagicMock(read_record=AsyncMock(return_value={"block_containers": {"blocks": [{}, {}, {}], "block_groups": [{}]}}))
    with patch("app.modules.indexing.split_wiring.record_artifact_reader", return_value=reader):
        assert await _StoredBlockCounts(object()).of(row, spec) == 3
        reader.read_record = AsyncMock(return_value=None)
        assert await _StoredBlockCounts(object()).of(row, spec) is None
