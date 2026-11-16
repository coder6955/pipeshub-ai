"""Re-embedding a split file from its stored content: restore from the trash, ``vectorDbOnly``, the vector-store
rebuild.

All three reach the file through its parent record, whose stored content is only the manifest. The re-embed must
write the manifest as the file's own segment, never delete by the file's id (that would remove every part's points),
and queue each part that has stored content as an ordinary part event.
"""
from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import EventTypes
from app.modules.indexing.split_reembed import SplitReembed, reembed_targets
from app.modules.indexing.split_state import SplitState, clear_part_cache
from app.modules.transformers.transformer import SPLIT_PARENT_GEN
from app.services.messaging.kafka.handlers.record import RecordEventHandler
from tests.unit.modules.indexing import test_split_seam as seam_tests
from tests.unit.modules.indexing.test_split_seam import ORG, OWNER, Rig, V

BLOB = {"block_containers": {"blocks": [
    {"id": "b1", "index": 0, "type": "text", "format": "txt", "data": "stored text"},
], "block_groups": []}}


def _record(key: str, vrid: str, connector: str) -> dict[str, Any]:
    return {
        "_key": key, "orgId": ORG, "recordName": "big.pdf", "recordType": "FILE", "externalRecordId": f"ext-{key}",
        "connectorId": connector, "mimeType": "application/pdf", "connectorName": "KB", "origin": "UPLOAD",
        "virtualRecordId": vrid, "indexingStatus": "COMPLETED", "version": 0,
    }


class World:
    def __init__(self, rig: Rig, row: Any, publisher: Any) -> None:  # noqa: ANN401
        self.rig, self.row = rig, row
        self.sink = MagicMock()
        self.sink.blob_storage.get_record_from_storage = AsyncMock(return_value=BLOB)
        self.sink.index = AsyncMock()
        self.sink.reproject_named_entities = AsyncMock()
        self.pipeline = AsyncMock()
        processor = MagicMock()
        processor.indexing_pipeline = self.pipeline
        self.event_processor = MagicMock()
        self.event_processor.graph_provider = rig.graph
        self.event_processor.processor = processor
        self.event_processor.sink_orchestrator = self.sink
        self.event_processor.split_runtime = SimpleNamespace(reembed=SplitReembed(rig.graph, rig.store, publisher))
        self.handler = RecordEventHandler(
            logger=logging.getLogger("test"), config_service=AsyncMock(), event_processor=self.event_processor,
            producer=AsyncMock(),
        )

    async def reembed(self, record: dict[str, Any]) -> None:
        _ = [
            e async for e in self.handler._index_from_blob(
                record_id=record["_key"], record=record, payload={"orgId": ORG}, virtual_record_id=record["virtualRecordId"],
                event_type=EventTypes.REINDEX_RECORD.value,
            )
        ]


async def _world() -> World:
    clear_part_cache()
    seam_tests.NOW[0] = 1_000_000
    rig = Rig()
    outcome = await rig.seam.split(rig.request())
    rig.graph.docs["records"]["parent-1"].update({"virtualRecordId": V, "indexingStatus": "COMPLETED", "mimeType": "application/pdf"})
    for k, status in enumerate(("COMPLETED", "FAILED", "COMPLETED")):
        rig.graph.docs["records"][outcome.row.parts[k].node_key]["indexingStatus"] = status
    return World(rig, outcome.row, rig.publisher)


@pytest.mark.asyncio
class TestReembedOfASplitFile:
    async def test_the_manifest_is_embedded_as_the_files_own_segment_and_nothing_deletes_by_the_files_id(self) -> None:
        world = await _world()

        await world.reembed(_record("parent-1", V, OWNER))

        world.pipeline.delete_points_for_virtual_record.assert_not_awaited()
        ctx = world.sink.index.await_args.args[0]
        assert ctx.settings[SPLIT_PARENT_GEN] == world.row.gen
        world.sink.reproject_named_entities.assert_awaited_once()

    async def test_each_part_with_stored_content_is_queued_as_a_part_event_and_the_others_are_not(self) -> None:
        world = await _world()
        sent_before = len(world.rig.publisher.sent)

        await world.reembed(_record("parent-1", V, OWNER))

        sent = world.rig.publisher.sent[sent_before:]
        keys = [p.node_key for p in world.row.parts]
        assert [key for _t, _e, _p, key in sent] == [keys[0], keys[2]], "the FAILED part has nothing stored to embed"
        for _topic, event_type, payload, _key in sent:
            assert event_type == EventTypes.REINDEX_RECORD.value
            assert payload["vectorDbOnly"] is True and payload["forceReindex"] is True
            assert payload["connectorId"] == OWNER, "the envelope names the owner, as every part event does"

    @pytest.mark.parametrize(
        ("extension", "mime"),
        [("pdf", "application/octet-stream"), (None, "application/pdf"), (".PDF", "application/octet-stream")],
        ids=["extension-only", "mime-only", "dotted-uppercase-extension"],
    )
    async def test_a_split_file_is_found_by_its_row_however_the_connector_names_its_type(
        self, extension: str | None, mime: str | None
    ) -> None:
        """A file the split seam cut by its extension keeps every part's points on a re-embed, whatever its mime type."""
        world = await _world()
        sent_before = len(world.rig.publisher.sent)
        record = {**_record("parent-1", V, OWNER), "mimeType": mime, "extension": extension}

        await world.reembed(record)

        world.pipeline.delete_points_for_virtual_record.assert_not_awaited()
        assert world.sink.index.await_args.args[0].settings[SPLIT_PARENT_GEN] == world.row.gen
        keys = [p.node_key for p in world.row.parts]
        assert [key for _t, _e, _p, key in world.rig.publisher.sent[sent_before:]] == [keys[0], keys[2]]

    async def test_a_broker_that_refuses_a_part_fails_the_parents_event_so_it_is_retried(self) -> None:
        world = await _world()
        world.rig.publisher.fail = True
        with pytest.raises(Exception, match="did not accept"):
            await world.reembed(_record("parent-1", V, OWNER))

    async def test_a_part_re_embeds_only_itself_and_writes_no_entities(self) -> None:
        world = await _world()
        spec = world.row.parts[0]
        part = world.rig.graph.docs["records"][spec.node_key]

        await world.reembed({**part, "_key": spec.node_key, "orgId": ORG, "recordName": "big.pdf · pages 1-2",
                             "externalRecordId": f"part:{world.row.gen}:1", "mimeType": "application/pdf",
                             "connectorName": "KB", "origin": "UPLOAD", "recordType": "FILE", "version": 0})

        world.pipeline.delete_points_for_virtual_record.assert_not_awaited()
        world.sink.reproject_named_entities.assert_not_awaited()
        assert SPLIT_PARENT_GEN not in world.sink.index.await_args.args[0].settings

    async def test_an_ordinary_pdf_is_re_embedded_exactly_as_before(self) -> None:
        world = await _world()
        await world.rig.graph.delete_nodes([V], "contentSplits")
        sent_before = len(world.rig.publisher.sent)

        await world.reembed(_record("parent-1", V, OWNER))

        world.pipeline.delete_points_for_virtual_record.assert_awaited_once()
        assert SPLIT_PARENT_GEN not in world.sink.index.await_args.args[0].settings
        assert len(world.rig.publisher.sent) == sent_before


@pytest.mark.asyncio
class TestTargets:
    async def test_the_targets_are_the_manifest_then_the_live_parts(self) -> None:
        world = await _world()
        targets = reembed_targets(V, world.row)
        assert [t.segment for t in targets] == [V, *(p.vrid for p in world.row.parts)]
        assert targets[0].record_key is None
        assert [t.record_key for t in targets[1:]] == [p.node_key for p in world.row.parts]

    async def test_an_unsplit_file_and_a_purging_generation_have_only_the_manifest(self) -> None:
        world = await _world()
        assert [t.segment for t in reembed_targets(V, None)] == [V]
        await world.rig.store.move(V, world.row.gen, SplitState.INDEXING_PARTS, SplitState.PURGING)
        assert [t.segment for t in reembed_targets(V, await world.rig.store.get(V))] == [V]


@pytest.mark.asyncio
class TestFirstUse:
    async def test_a_search_never_asks_for_the_classification_of_a_split_parent(self) -> None:
        world = await _world()
        pdf = {"mimeType": "application/pdf"}
        assert await world.handler._is_split_parent(V, pdf) is True
        assert await world.handler._is_split_parent("another-file", pdf) is False
        assert await world.handler._is_split_parent(V, {"mimeType": "text/plain"}) is False


@pytest.mark.asyncio
async def test_the_first_use_event_of_a_split_parent_is_acknowledged_without_a_stage_run() -> None:
    """A search that returns the manifest's record asks for classification (``firstUse``); the roll-up of its
    parts owns that, so no stage is re-driven for it."""
    from tests.support.fake_connector_graph import FakeConnectorGraph
    from tests.unit.modules.indexing.test_connector_off_events import (
        REINDEX,
        _handler,
        _payload,
    )

    world = await _world()
    graph = FakeConnectorGraph()
    graph.add_connector("conn-on", active=True)
    graph.add_record("r1", "conn-on", indexingStatus="COMPLETED", extractionStatus="NOT_STARTED",
                     contentRev="rev-1", mimeType="application/pdf", virtualRecordId=V)
    handler, event_processor = _handler(graph)
    event_processor.split_runtime = world.event_processor.split_runtime
    event_processor.stage_ingress = MagicMock()
    event_processor.stage_ingress.handoff_lost = AsyncMock(return_value=False)
    event_processor.stage_ingress.redrive = AsyncMock(return_value=["job"])
    payload = _payload(connector_id="conn-on", stages=["classify"], stageRev="rev-1", firstUse=True,
                       virtualRecordId=V, mimeType="application/pdf", extension="pdf")

    events = [e.event async for e in handler.process_event(REINDEX, payload)]

    assert len(events) == 2
    event_processor.stage_ingress.redrive.assert_not_awaited()

    # The same event for a file that is not split still re-drives its stage.
    await world.rig.graph.delete_nodes([V], "contentSplits")
    _ = [e async for e in handler.process_event(REINDEX, payload)]
    event_processor.stage_ingress.redrive.assert_awaited_once()
