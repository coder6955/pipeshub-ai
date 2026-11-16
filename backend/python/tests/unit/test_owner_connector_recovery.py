"""Recovery paths reach a part through its owner connector: stale IN_PROGRESS republish, the stranded
sweep and the walk of records parked while the owner was off."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.config.constants.arangodb import CollectionNames, OriginTypes, ProgressStatus
from app.modules.indexing.split_state import clear_part_cache
from app.utils.user_errors import CONNECTOR_OFF
from tests.unit.test_indexing_main import (
    _document_lookup,
    _make_container,
    _make_graph_provider,
)
from tests.unit.test_stranded_sweep_parked_while_off import (
    OFF,
    ON,
    FakeGraph,
    Producer,
    _parked,
    _resume,
    _sweep,
)


class CapturingProducer(Producer):
    def __init__(self) -> None:
        super().__init__()
        self.payloads: dict[str, dict] = {}

    async def send_messages(self, topic, messages) -> list[bool]:
        for key, message in messages:
            self.payloads[key] = message["payload"]
        return await super().send_messages(topic, messages)

    async def send_event(self, topic, event_type, payload, key=None) -> bool:
        self.payloads[key] = payload
        return await super().send_event(topic, event_type, payload, key)


def _part_doc(key: str, owner: str = ON) -> dict:
    """The contentSplitParts document of a part: its owner and place (a part record has only ordinary fields)."""
    return {
        "orgId": "org-1", "rootVirtualRecordId": "V", "gen": "g", "k": 1, "pageStart": 1, "pageEnd": 500,
        "pageOffset": 0, "overlapPages": 0, "ownerConnectorId": owner, "virtualRecordId": f"V_{key}",
    }


def _part(key: str, **fields: object) -> dict:
    return _parked(
        key, connector_id="pdfparts_org-1", origin=OriginTypes.UPLOAD.value, virtualRecordId=f"V_{key}", **fields,
    )


@pytest.fixture(autouse=True)
def _fresh_part_cache() -> None:
    clear_part_cache()


def _graph_with_part(key: str, owner: str = ON) -> FakeGraph:
    graph = FakeGraph()
    graph.records[key] = _part(key)
    graph.parts[key] = _part_doc(key, owner)
    return graph


def _stale_part_gp(owner_active: bool) -> object:
    gp = _make_graph_provider()
    lookup = _document_lookup(gp, connector={"_key": ON, "isActive": owner_active})

    async def get_document(doc_id, collection, **kwargs):  # noqa: ANN202
        if collection == CollectionNames.CONTENT_SPLIT_PARTS.value:
            return {"id": doc_id, **_part_doc(doc_id)}
        return await lookup(doc_id, collection, **kwargs)

    gp.get_document = AsyncMock(side_effect=get_document)
    gp.get_nodes_by_filters = AsyncMock(return_value=[{
        "_key": "p1", "recordName": "a.pdf", "version": 0, "orgId": "org-1", "connectorId": "pdfparts_org-1",
        "origin": OriginTypes.UPLOAD.value, "virtualRecordId": "V_p1",
    }])
    return gp


async def test_the_sweep_republishes_a_part_parked_while_its_owner_was_off_with_the_owner_on_the_envelope() -> None:
    graph, producer = _graph_with_part("p1"), CapturingProducer()

    assert await _sweep(graph, producer) == 1

    assert producer.payloads["p1"]["connectorId"] == ON
    assert producer.payloads["p1"]["virtualRecordId"] == "V_p1"


async def test_the_sweep_leaves_a_part_whose_owner_is_still_off() -> None:
    graph, producer = _graph_with_part("p1", owner=OFF), CapturingProducer()

    assert await _sweep(graph, producer) == 0
    assert producer.sent == []


async def test_turning_the_owner_on_requeues_its_parts_too() -> None:
    graph, producer = _graph_with_part("p1"), CapturingProducer()
    graph.records["r1"] = _parked("r1")

    assert await _resume(graph, producer)._requeue_records_parked_while_off(ON) == 2
    assert producer.payloads["p1"]["connectorId"] == ON
    assert graph.records["p1"]["indexingStatus"] == ProgressStatus.QUEUED.value


async def test_stale_recovery_checks_the_owner_connector_of_a_part() -> None:
    from app.indexing_main import recover_in_progress_records

    container = _make_container()
    gp = _stale_part_gp(owner_active=False)

    await recover_in_progress_records(container, gp)

    container.kafka_consumers[0][2].send_event.assert_not_awaited()
    updates = [c.args[2] for c in gp.update_node.await_args_list if len(c.args) > 2]
    assert any(u.get("reason") == CONNECTOR_OFF for u in updates)


async def test_stale_recovery_republishes_a_part_on_its_owners_lane() -> None:
    from app.indexing_main import recover_in_progress_records

    container = _make_container()
    gp = _stale_part_gp(owner_active=True)

    await recover_in_progress_records(container, gp)

    producer = container.kafka_consumers[0][2]
    payload = producer.send_event.await_args.kwargs["payload"]
    assert payload["connectorId"] == ON
    assert payload["virtualRecordId"] == "V_p1"
