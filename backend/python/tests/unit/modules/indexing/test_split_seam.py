"""The parent seam, purge and sweep of a split PDF, against in-memory stand-ins for the graph and the services.

The parent attempt, its crash recovery, the purge and the part gate. The real graph stores are covered by
``tests/integration/graph_db/test_content_splits_real_backends.py``.
"""
from __future__ import annotations

from typing import Any

import pytest

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing.owner_connector import part_connector_id
from app.modules.indexing.part_gate import PartContext, PartStop, check_part
from app.modules.indexing.split_purge import (
    FILE_REMOVED_REASON,
    SUPERSEDED_REASON,
    GenerationPurger,
)
from app.modules.indexing.split_seam import ParentSplitSeam, PartPurgeBusy, SplitRequest
from app.modules.indexing.split_state import (
    SplitRow,
    SplitState,
    SplitStore,
    clear_part_cache,
    derive_gen,
    display_for,
    part_info,
    part_node_key,
    part_vrid,
)
from app.modules.indexing.split_sweep import SplitSweep
from app.modules.parsers.pdf.split.config import SplitConfig
from app.schema.arango.documents import record_schema
from tests.support.split_fakes import (
    FakeGraph,
    FakeLeases,
    FakePublisher,
    FakeResources,
    FakeUploader,
    fake_cutter,
)

ORG, V, OWNER = "org-1", "vrid-parent", "conn-1"
RANGES = [(1, 500, 501), (501, 1000, 1001), (1001, 1138, 1138)]
NOW = [1_000_000]
# Fields D-21 keeps off the record: they live in the part document (and the parent's display is projected on read).
PART_ONLY_FIELDS = {
    "rootVirtualRecordId", "splitGen", "segmentIndex", "segmentCount", "pageStart", "pageEnd", "pageOffset",
    "overlapPages", "ownerConnectorId", "splitState", "splitDone", "splitTotal", "splitFailed",
}


def _parent() -> dict[str, Any]:
    return {
        "_key": "parent-1", "orgId": ORG, "connectorId": OWNER, "virtualRecordId": V, "recordName": "big.pdf",
        "indexingStatus": "IN_PROGRESS", "mimeType": "application/pdf",
    }


class Rig:
    def __init__(self, ranges: list[tuple[int, int, int]] = RANGES) -> None:
        self.graph = FakeGraph()
        self.graph.put("records", "parent-1", _parent())
        self.store = SplitStore(self.graph, clock_ms=lambda: NOW[0])
        self.leases = FakeLeases()
        self.resources = FakeResources()
        self.uploader = FakeUploader()
        self.publisher = FakePublisher()
        self.cutter = fake_cutter(ranges)
        self.purger = GenerationPurger(self.graph, self.store, self.leases, self.resources)
        self.seam = ParentSplitSeam(
            graph=self.graph, store=self.store, purger=self.purger, cutter=self.cutter, uploader=self.uploader,
            publisher=self.publisher, whole_file=self.resources, config=SplitConfig, retry_failed=self._retry_failed,
        )
        self.sweep = SplitSweep(
            graph=self.graph, store=self.store, purger=self.purger, queue_parts=self.seam.queue_parts,
            parts_missing_files=self.seam.parts_missing_files, lease_ms=120_000, clock_ms=lambda: NOW[0],
        )

    async def _retry_failed(self, v: str, *, owner_connector_id: str) -> int:
        from app.modules.indexing.split_reindex import retry_failed_parts_of
        from app.modules.indexing.split_rollup import SplitRollup

        rollup = SplitRollup(self.graph, self.store)
        return await retry_failed_parts_of(self.graph, self.store, self.publisher, rollup, v, owner_connector_id=owner_connector_id)

    def request(self, *, rev: str = "rev-1", nonce: str = "") -> SplitRequest:
        return SplitRequest(
            parent=self.graph.docs["records"]["parent-1"], v=V, org_id=ORG, content=b"%PDF", content_rev=rev,
            page_count=1138, needs_ocr=False, nonce=nonce,
        )

    def parts(self) -> list[dict[str, Any]]:
        """The part records (ordinary records under the hidden parts connector), in part order."""
        return sorted(
            (d for d in self.graph.docs["records"].values() if d.get("connectorId") == part_connector_id(ORG)),
            key=lambda d: int(d["externalRecordId"].rsplit(":", 1)[1]),
        )

    def part_docs(self) -> list[dict[str, Any]]:
        return sorted(self.graph.docs.get("contentSplitParts", {}).values(), key=lambda d: d["k"])


@pytest.fixture
def rig() -> Rig:
    NOW[0] = 1_000_000
    clear_part_cache()
    return Rig()


@pytest.mark.asyncio
class TestSplit:
    async def test_the_parent_attempt_makes_the_row_the_nodes_and_the_queue(self, rig: Rig) -> None:
        outcome = await rig.seam.split(rig.request())

        row = await rig.store.get(V)
        assert row is not None
        assert row.state is SplitState.INDEXING_PARTS
        assert (row.total, row.gen) == (3, outcome.row.gen)
        parts = rig.parts()
        assert len(parts) == 3
        first = parts[0]
        assert first["_key"] == part_node_key(row.gen, 1)
        assert first["virtualRecordId"] == part_vrid(row.gen, 1)
        assert first["connectorId"] == part_connector_id(ORG)
        assert first["externalRecordId"] == f"part:{row.gen}:1"
        # D-21: the record carries nothing the record schema lacks; the rest is in the part document.
        assert not set(first) & PART_ONLY_FIELDS
        assert set(first) - {"_key"} <= set(record_schema["rule"]["properties"]), "a part record carries only record-schema fields"
        docs = rig.part_docs()
        assert [d["k"] for d in docs] == [1, 2, 3]
        assert docs[0] == {
            "_key": first["_key"], "orgId": ORG, "rootVirtualRecordId": V, "gen": row.gen, "k": 1, "pageStart": 1,
            "pageEnd": 500, "pageOffset": 0, "overlapPages": 1, "ownerConnectorId": OWNER, "virtualRecordId": part_vrid(row.gen, 1),
        }
        assert (docs[2]["pageStart"], docs[2]["overlapPages"]) == (1001, 0)
        assert first["storageDocumentId"] == f"doc:parts/{V}/{row.gen}/1"
        assert first["recordName"] == "big.pdf · pages 1–500"
        assert first["indexingStatus"] == ProgressStatus.QUEUED.value

    async def test_each_part_is_queued_under_the_owner_connector(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())

        assert len(rig.publisher.sent) == 3
        for (topic, event_type, payload, key), part in zip(rig.publisher.sent, rig.parts(), strict=True):
            assert (topic, event_type, key) == ("record-events", "newRecord", part["_key"])
            assert payload["connectorId"] == OWNER
            assert payload["recordId"] == part["_key"]
            assert payload["virtualRecordId"] == part["virtualRecordId"]

    async def test_the_parent_record_is_not_written_and_its_display_is_read_from_the_row(self, rig: Rig) -> None:
        before = dict(rig.graph.docs["records"]["parent-1"])
        await rig.seam.split(rig.request())

        assert rig.graph.docs["records"]["parent-1"] == before
        assert await display_for([V, "never-split"], rig.graph) == {
            V: {"splitState": "INDEXING_PARTS", "splitDone": 0, "splitTotal": 3, "splitFailed": 0},
        }

    async def test_part_vrids_map_back_to_their_file_for_the_live_generation_only(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request(rev="rev-1"))).row
        assert await rig.store.roots_of([part_vrid(old.gen, 1), part_vrid(old.gen, 3), "nope"]) == {
            part_vrid(old.gen, 1): V, part_vrid(old.gen, 3): V,
        }
        new = (await rig.seam.split(rig.request(rev="rev-2"))).row

        assert await rig.store.roots_of([part_vrid(old.gen, 1)]) == {}
        assert await rig.store.roots_of([part_vrid(new.gen, 2)]) == {part_vrid(new.gen, 2): V}

    async def test_the_unsplit_points_of_the_file_go_once_the_row_exists_and_before_any_part_is_queued(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())

        assert rig.resources.whole_file_points_deleted == [V]

    async def test_a_redelivered_parent_continues_the_same_generation_without_cutting_again(self, rig: Rig) -> None:
        first = await rig.seam.split(rig.request())
        second = await rig.seam.split(rig.request())

        assert second.reused_generation and second.row.gen == first.row.gen
        assert len(rig.cutter.calls) == 1
        assert len(rig.publisher.sent) == 3
        assert len(rig.parts()) == 3

    @pytest.mark.parametrize(
        "failure",
        ["ensure_nodes", "batch_upsert_records", "upload_after_1", "publisher"],
    )
    async def test_a_crash_at_any_step_converges_on_the_next_attempt(self, rig: Rig, failure: str) -> None:
        # T5: the parent is redelivered after a failure at each step and ends with the same state.
        if failure == "upload_after_1":
            rig.uploader.fail_after = 1
        elif failure == "publisher":
            rig.publisher.fail = True
        else:
            rig.graph.fail_on.add(failure)
        with pytest.raises(Exception):  # noqa: B017
            await rig.seam.split(rig.request())
        rig.graph.fail_on.clear()
        rig.uploader.fail_after = None
        rig.publisher.fail = False

        await rig.seam.split(rig.request())

        row = await rig.store.get(V)
        assert row is not None and row.state is SplitState.INDEXING_PARTS
        parts = rig.parts()
        assert len(parts) == 3
        assert {p["indexingStatus"] for p in parts} == {ProgressStatus.QUEUED.value}
        assert all(p["storageDocumentId"] for p in parts)
        assert {payload["recordId"] for _, _, payload, _ in rig.publisher.sent} == {p["_key"] for p in parts}

    async def test_a_part_already_running_is_not_queued_again(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        rig.graph.docs["records"][rig.parts()[0]["_key"]]["indexingStatus"] = "IN_PROGRESS"

        await rig.seam.split(rig.request())

        assert len(rig.publisher.sent) == 3

    async def test_a_part_stopped_for_a_removed_file_starts_again_when_the_file_returns(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        stopped = rig.graph.docs["records"][rig.parts()[0]["_key"]]
        stopped.update({"indexingStatus": "AUTO_INDEX_OFF", "reason": FILE_REMOVED_REASON})

        await rig.seam.split(rig.request())

        assert stopped["indexingStatus"] == ProgressStatus.QUEUED.value
        assert len(rig.publisher.sent) == 4


@pytest.mark.asyncio
class TestRetire:
    async def test_a_changed_file_replaces_the_generation_and_purges_the_old_parts(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request(rev="rev-1"))).row
        old_keys = [p["_key"] for p in rig.parts()]

        new = (await rig.seam.split(rig.request(rev="rev-2"))).row

        assert new.gen != old.gen
        assert not any(k in rig.graph.docs["records"] for k in old_keys)
        assert sorted(rig.resources.points_deleted) == sorted(part_vrid(old.gen, k) for k in (1, 2, 3))
        assert len(rig.resources.pdfs_deleted) == 3
        assert {d["gen"] for d in rig.part_docs()} == {new.gen}
        assert len(rig.part_docs()) == 3

    async def test_a_resumed_attempt_cuts_the_missing_parts_from_the_ranges_the_row_stores(self, rig: Rig) -> None:
        # The attempt dies after part 1's file; the settings change before the next one. Today's settings would cut other
        # ranges (another digest): the generation in progress is finished with the cut it was made with.
        rig.uploader.fail_after = 1
        with pytest.raises(Exception):  # noqa: B017
            await rig.seam.split(rig.request())
        rig.uploader.fail_after = None
        row = await rig.store.get(V)
        assert row is not None and row.state is SplitState.SPLITTING
        rig.seam._cutter = fake_cutter([(1, 300, 301), (301, 700, 701), (701, 1138, 1138)], digest="digest-2")

        outcome = await rig.seam.split(rig.request())

        assert outcome.row.gen == row.gen and outcome.reused_generation
        assert rig.seam._cutter.asked[-1] == [(501, 1000, 1001), (1001, 1138, 1138)], "the stored ranges of the parts with no file"
        assert [f["storageDocumentId"] for f in rig.parts()] == [f"doc:parts/{V}/{row.gen}/{k}" for k in (1, 2, 3)]
        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS

    async def test_a_part_being_indexed_holds_the_purge_until_its_lease_is_free(self, rig: Rig) -> None:
        # T6: the lease is the fence. The part in flight is not touched, the row stays PURGING.
        old = (await rig.seam.split(rig.request(rev="rev-1"))).row
        busy_key = part_node_key(old.gen, 2)
        rig.leases.held.add(busy_key)

        with pytest.raises(PartPurgeBusy):
            await rig.seam.split(rig.request(rev="rev-2"))

        row = await rig.store.get(V)
        assert row is not None and row.state is SplitState.PURGING and row.gen == old.gen
        busy = rig.graph.docs["records"][busy_key]
        assert (busy["indexingStatus"], busy["reason"]) == ("AUTO_INDEX_OFF", SUPERSEDED_REASON)
        assert part_node_key(old.gen, 1) not in rig.graph.docs["records"]
        assert part_vrid(old.gen, 2) not in rig.resources.points_deleted

        rig.leases.held.clear()
        NOW[0] += 200_000
        report = await rig.sweep.run_once()

        assert report.purges_resumed == 1
        assert await rig.store.get(V) is None
        assert not rig.part_docs()
        assert part_vrid(old.gen, 2) in rig.resources.points_deleted

    async def test_a_file_that_is_no_longer_long_enough_loses_its_parts(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request())).row

        assert await rig.seam.unsplit(V) is True

        assert await rig.store.get(V) is None
        assert not rig.parts()
        assert len(rig.resources.points_deleted) == old.total

    async def test_a_file_that_was_never_split_has_nothing_to_retire(self, rig: Rig) -> None:
        assert await rig.seam.unsplit(V) is False
        assert rig.resources.points_deleted == []

    async def test_a_file_that_becomes_long_drops_its_whole_file_points_first(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())

        assert rig.resources.whole_file_points_deleted == [V]
        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS  # type: ignore[union-attr]


@pytest.mark.asyncio
class TestPartGate:
    async def _split(self, rig: Rig) -> SplitRow:
        return (await rig.seam.split(rig.request())).row

    async def test_a_live_part_of_a_live_file_runs_with_the_plans_hint(self, rig: Rig) -> None:
        await self._split(rig)

        gate = await check_part(rig.graph, rig.store, await part_info(rig.parts()[1], rig.graph))

        assert gate == PartContext(2, False)

    async def test_a_file_that_is_gone_stops_its_parts(self, rig: Rig) -> None:
        # T8
        await self._split(rig)
        rig.graph.docs["records"]["parent-1"]["isDeleted"] = True

        assert await check_part(rig.graph, rig.store, await part_info(rig.parts()[0], rig.graph)) == PartStop(FILE_REMOVED_REASON)

    async def test_a_part_of_a_replaced_generation_is_superseded(self, rig: Rig) -> None:
        old_part = None
        await self._split(rig)
        old_part = await part_info(rig.parts()[0], rig.graph)
        await rig.seam.split(rig.request(rev="rev-2"))

        assert await check_part(rig.graph, rig.store, old_part) == PartStop(SUPERSEDED_REASON)

    async def test_a_store_error_is_raised_not_read_as_a_removed_file(self, rig: Rig) -> None:
        await self._split(rig)

        async def boom(*_a: object, **_k: object) -> list[dict[str, Any]]:
            raise RuntimeError("graph down")

        rig.graph.get_records_by_virtual_record_id = boom  # type: ignore[method-assign]

        with pytest.raises(RuntimeError):
            await check_part(rig.graph, rig.store, await part_info(rig.parts()[0], rig.graph))


@pytest.mark.asyncio
class TestSweep:
    async def test_parts_nobody_queued_are_queued_once_the_parent_is_a_lease_old(self, rig: Rig) -> None:
        rig.publisher.fail = True
        with pytest.raises(Exception):  # noqa: B017
            await rig.seam.split(rig.request())
        rig.publisher.fail = False

        assert (await rig.sweep.run_once()).requeued == 0  # too fresh: the parent attempt may still be running
        NOW[0] += 200_000
        report = await rig.sweep.run_once()

        assert report.requeued == 1
        assert {p["indexingStatus"] for p in rig.parts()} == {ProgressStatus.QUEUED.value}
        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS  # type: ignore[union-attr]

    async def test_parts_without_a_file_are_left_for_the_parent_attempt(self, rig: Rig) -> None:
        rig.uploader.fail_after = 1
        with pytest.raises(Exception):  # noqa: B017
            await rig.seam.split(rig.request())
        NOW[0] += 200_000

        report = await rig.sweep.run_once()

        assert report.requeued == 0
        assert (await rig.store.get(V)).state is SplitState.SPLITTING  # type: ignore[union-attr]

    async def test_the_parts_of_a_deleted_file_are_retired(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        rig.graph.docs["records"].pop("parent-1")

        report = await rig.sweep.run_once()

        assert report.orphans_purged == 1
        assert await rig.store.get(V) is None
        assert not rig.parts()

    async def test_the_parts_of_a_file_in_the_trash_are_kept(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        rig.graph.docs["records"]["parent-1"]["isDeleted"] = True

        report = await rig.sweep.run_once()

        assert report.orphans_purged == 0
        assert len(rig.parts()) == 3


def test_a_generation_is_a_function_of_the_content_and_the_plan() -> None:
    base = derive_gen(V, "rev-1", "digest", "")

    assert derive_gen(V, "rev-1", "digest", "") == base
    assert len({base, derive_gen(V, "rev-2", "digest"), derive_gen(V, "rev-1", "other"), derive_gen(V, "rev-1", "digest", "n")}) == 4
    assert part_vrid(base, 1) != part_vrid(base, 2)
    assert part_node_key(base, 1) != part_vrid(base, 1)
