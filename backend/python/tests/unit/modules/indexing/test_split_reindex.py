"""'Reindex failed' on a connector also queues the failed parts of its split files."""
from __future__ import annotations

from typing import Any

import pytest

from app.modules.indexing.split_reindex import reindex_failed_parts
from tests.unit.modules.indexing.test_split_seam import ORG, OWNER, Rig, V


class Producer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any], str | None]] = []

    async def send_message(self, topic: str, message: dict[str, Any], key: str | None = None) -> bool:
        self.sent.append((topic, message, key))
        return True


async def _split_with_failures(rig: Rig, failed: int) -> list[str]:
    await rig.seam.split(rig.request())
    keys = [p["_key"] for p in rig.parts()]
    for key in keys[:failed]:
        rig.graph.docs["records"][key].update({"indexingStatus": "FAILED", "reason": "boom"})
    return keys


@pytest.mark.asyncio
async def test_failed_parts_of_the_connectors_files_are_queued_again_and_counted_by_file() -> None:
    rig = Rig()
    keys = await _split_with_failures(rig, 2)
    producer = Producer()

    files = await reindex_failed_parts(
        rig.graph, rig.store, producer, org_id=ORG, connector_id=OWNER, statuses=["FAILED"],
    )

    assert files == 1
    assert [key for _t, _m, key in producer.sent] == keys[:2]
    assert all(m["eventType"] == "newRecord" and m["payload"]["connectorId"] == OWNER for _t, m, _k in producer.sent)
    statuses = [rig.graph.docs["records"][k]["indexingStatus"] for k in keys]
    assert statuses == ["QUEUED", "QUEUED", "QUEUED"]
    assert rig.graph.docs["records"][keys[0]].get("reason") is None


@pytest.mark.asyncio
async def test_another_connectors_failed_parts_and_healthy_parts_are_left_alone() -> None:
    rig = Rig()
    await _split_with_failures(rig, 1)
    producer = Producer()

    other = await reindex_failed_parts(rig.graph, rig.store, producer, org_id=ORG, connector_id="conn-2", statuses=["FAILED"])
    healthy = await reindex_failed_parts(rig.graph, rig.store, producer, org_id="org-2", connector_id=OWNER, statuses=["FAILED"])

    assert (other, healthy) == (0, 0)
    assert producer.sent == []


@pytest.mark.asyncio
async def test_a_part_of_a_replaced_generation_is_not_queued() -> None:
    rig = Rig()
    keys = await _split_with_failures(rig, 1)
    rig.graph.docs["contentSplitParts"][keys[0]]["gen"] = "older-generation"
    producer = Producer()

    assert await reindex_failed_parts(rig.graph, rig.store, producer, org_id=ORG, connector_id=OWNER, statuses=["FAILED"]) == 0
    assert V


class _Access:
    def __init__(self, ids: set[str]) -> None:
        self.node_ids = ids


class _ScopedGraph:
    """The rig's graph with the permission model of ``check_access``: each user reaches a set of record keys."""

    def __init__(self, graph: Any, reach: dict[str, set[str]]) -> None:  # noqa: ANN401
        self._graph = graph
        self._reach = reach
        self.asked: list[tuple[str, list[str]]] = []

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        return getattr(self._graph, name)

    async def check_access(self, user_key: str, org_id: str, node_ids: list[str]) -> _Access:
        self.asked.append((user_key, list(node_ids)))
        return _Access({n for n in node_ids if n in self._reach.get(user_key, set())})


def _second_file(rig: Rig) -> str:
    """A second file in the same connector with a failed part of its own; returns its holder's record key."""
    rig.graph.put("records", "parent-2", {**{k: v for k, v in rig.graph.docs["records"]["parent-1"].items() if k != "_key"}, "virtualRecordId": "vrid-other", "recordName": "other.pdf"})
    return "parent-2"


@pytest.mark.asyncio
async def test_a_user_scoped_reindex_queues_the_failed_parts_of_the_files_that_user_can_access_only() -> None:
    from app.modules.indexing.split_seam import SplitRequest

    rig = Rig()
    await _split_with_failures(rig, 2)
    other = _second_file(rig)
    request = SplitRequest(
        parent=rig.graph.docs["records"][other], v="vrid-other", org_id=ORG, content=b"%PDF", content_rev="rev-x",
        page_count=1138, needs_ocr=False, nonce="",
    )
    await rig.seam.split(request)
    for part in rig.parts():
        rig.graph.docs["records"][part["_key"]].update({"indexingStatus": "FAILED", "reason": "boom"})
    scoped = _ScopedGraph(rig.graph, {"alice": {"parent-1"}, "bob": {other}, "nobody": set()})

    producer = Producer()
    files = await reindex_failed_parts(scoped, rig.store, producer, org_id=ORG, connector_id=OWNER, statuses=["FAILED"], user_key="alice")  # type: ignore[arg-type]

    sent_parts = {rig.graph.docs["contentSplitParts"][key]["rootVirtualRecordId"] for _t, _m, key in producer.sent}
    assert files == 1
    assert sent_parts == {V}, "only the file alice can access is retried"
    assert len(producer.sent) == 3
    statuses = {p["rootVirtualRecordId"]: rig.graph.docs["records"][p["_key"]]["indexingStatus"] for p in rig.part_docs()}
    assert statuses[V] == "QUEUED" and statuses["vrid-other"] == "FAILED", "the file she cannot see keeps its failed parts untouched"
    assert scoped.asked and all(user == "alice" for user, _ids in scoped.asked)

    nobody = Producer()
    assert await reindex_failed_parts(scoped, rig.store, nobody, org_id=ORG, connector_id=OWNER, statuses=["FAILED"], user_key="nobody") == 0  # type: ignore[arg-type]
    assert nobody.sent == []
    still_failed = {p["_key"] for p in rig.parts() if p["indexingStatus"] == "FAILED"}
    everyone = Producer()
    assert await reindex_failed_parts(scoped, rig.store, everyone, org_id=ORG, connector_id=OWNER, statuses=["FAILED"]) == 1  # type: ignore[arg-type]
    assert {m["payload"]["recordId"] for _t, m, _k in everyone.sent} == still_failed
