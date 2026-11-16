"""Fixes of S2a QA round 1 that the seam, purge and sweep must keep: record keys from the vrid lookup,
no lease manager, a restored file, and the owner on the event. Same rig as ``test_split_seam.py``."""
from __future__ import annotations

import pytest

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing.split_purge import FILE_REMOVED_REASON, GenerationPurger
from app.modules.indexing.split_seam import ParentSplitSeam, PartPurgeBusy
from app.modules.indexing.split_state import SplitState, clear_part_cache
from app.modules.pipeline.leases import RecordLeases
from tests.unit.modules.indexing.test_split_seam import NOW, Rig, V, _parent


@pytest.fixture
def rig() -> Rig:
    NOW[0] = 1_000_000
    clear_part_cache()
    return Rig()


@pytest.mark.asyncio
class TestRecordKeys:
    async def test_the_graph_fake_answers_record_keys_like_every_real_store(self, rig: Rig) -> None:
        """IGraphDBProvider.get_records_by_virtual_record_id returns ``list[str]`` keys (Neo4j ``r.id``, Arango
        ``record._key``, PostgreSQL ``records.py``). A fake that returns documents hid this."""
        got = await rig.graph.get_records_by_virtual_record_id(V)

        assert got == ["parent-1"]
        assert all(isinstance(item, str) for item in got)
        assert await rig.graph.get_records_by_virtual_record_id(V, visibility=object()) == ["parent-1"]

    async def test_a_split_queues_its_parts_when_the_store_answers_keys_only(self, rig: Rig) -> None:
        # With a fake that answered documents this passed while every real store raised AttributeError at the owner read.
        await rig.seam.split(rig.request())

        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS  # type: ignore[union-attr]
        assert {p["indexingStatus"] for p in rig.parts()} == {ProgressStatus.QUEUED.value}

    async def test_no_live_holder_means_no_parts_are_queued(self, rig: Rig) -> None:
        rig.graph.docs["records"]["parent-1"]["isDeleted"] = True

        with pytest.raises(Exception, match="no live record holds"):
            await rig.seam.split(rig.request())

        assert rig.publisher.sent == []


@pytest.mark.asyncio
class TestWithoutALeaseManager:
    @staticmethod
    def _unbound(rig: Rig) -> None:
        leases = RecordLeases()
        assert not leases.bound
        rig.purger = GenerationPurger(rig.graph, rig.store, leases, rig.resources)
        rig.seam = ParentSplitSeam(
            graph=rig.graph, store=rig.store, purger=rig.purger, cutter=rig.cutter, uploader=rig.uploader,
            publisher=rig.publisher, whole_file=rig.resources, config=rig.seam._config,
        )

    async def test_a_resplit_purges_the_old_generation_without_a_fence(self, rig: Rig) -> None:
        self._unbound(rig)
        old = (await rig.seam.split(rig.request(rev="rev-1"))).row

        try:
            new = (await rig.seam.split(rig.request(rev="rev-2"))).row
        except PartPurgeBusy as exc:  # the bug: RecordLeases.claim answers None for every record when unbound
            pytest.fail(f"a re-split never completes without a lease manager: {exc}")

        assert new.gen != old.gen
        assert not [s for s in old.parts if s.node_key in rig.graph.docs["records"]]
        assert sorted(rig.resources.points_deleted) == sorted(s.vrid for s in old.parts)

    async def test_an_unsplit_completes_and_removes_the_row(self, rig: Rig) -> None:
        self._unbound(rig)
        await rig.seam.split(rig.request())

        assert await rig.seam.unsplit(V) is True
        assert await rig.store.get(V) is None
        assert not rig.parts()

    async def test_a_bound_manager_still_holds_the_purge_for_a_part_in_flight(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request(rev="rev-1"))).row
        rig.leases.held.add(old.parts[0].node_key)

        with pytest.raises(PartPurgeBusy):
            await rig.seam.split(rig.request(rev="rev-2"))


@pytest.mark.asyncio
class TestRestore:
    async def test_the_sweep_requeues_parts_stopped_for_a_removed_file_once_the_file_is_live_again(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        sent = len(rig.publisher.sent)
        # Trashed: the gate stopped the parts while no live record held the file.
        rig.graph.docs["records"]["parent-1"]["isDeleted"] = True
        for part in rig.parts():
            rig.graph.docs["records"][part["_key"]].update(indexingStatus="AUTO_INDEX_OFF", reason=FILE_REMOVED_REASON)
        assert (await rig.sweep.run_once()).revived == 0, "still in the trash: the parts stay stopped"
        assert {p["indexingStatus"] for p in rig.parts()} == {"AUTO_INDEX_OFF"}

        rig.graph.docs["records"]["parent-1"]["isDeleted"] = False  # restored; the re-embed path never reaches the seam
        report = await rig.sweep.run_once()

        assert report.revived == 3
        assert {p["indexingStatus"] for p in rig.parts()} == {ProgressStatus.QUEUED.value}
        assert len(rig.publisher.sent) == sent + 3

    async def test_parts_stopped_for_another_reason_are_left_alone(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        for part in rig.parts():
            rig.graph.docs["records"][part["_key"]].update(indexingStatus="AUTO_INDEX_OFF", reason="connector off")

        report = await rig.sweep.run_once()

        assert report.revived == 0
        assert {p["indexingStatus"] for p in rig.parts()} == {"AUTO_INDEX_OFF"}


@pytest.mark.asyncio
class TestOwnerOnTheEvent:
    async def test_the_event_names_the_owner_the_part_document_names_with_md5_copies_across_two_connectors(self, rig: Rig) -> None:
        # A copy of the same bytes under another connector sorts first in the store's answer.
        parent = rig.graph.docs["records"].pop("parent-1")
        rig.graph.put("records", "copy-0", {**_parent(), "_key": "copy-0", "connectorId": "conn-0"})
        rig.graph.docs["records"]["parent-1"] = parent

        await rig.seam.split(rig.request())

        owners_doc = {d["ownerConnectorId"] for d in rig.part_docs()}
        owners_event = {payload["connectorId"] for _, _, payload, _ in rig.publisher.sent}
        assert owners_doc == owners_event == {"conn-1"}

    async def test_a_part_without_its_document_is_not_queued(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        victim = rig.parts()[0]
        rig.graph.docs["records"][victim["_key"]]["indexingStatus"] = ProgressStatus.NOT_STARTED.value
        rig.graph.docs["contentSplitParts"].pop(victim["_key"])

        with pytest.raises(Exception, match="no part document"):
            await rig.seam.queue_parts((await rig.store.get(V)))  # type: ignore[arg-type]

