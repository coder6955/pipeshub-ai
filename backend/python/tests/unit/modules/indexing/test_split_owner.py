"""Parts belong to the content, not to the connector that first split it: the owner on a part document is a hint.

Unit tests of the owner rule (the real-store worlds on Neo4j, ArangoDB and PostgreSQL prove the same on real graphs):
with two or three copies of the file the owner moves to the earliest live copy, then to the next, and a re-home on a
connector's deletion stays inside its organisation.
"""
from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from app.modules.indexing.split_owner import (
    owner_of,
    rehome_if_owner_gone,
    rehome_parts_owned_by,
)
from app.modules.indexing.split_state import clear_part_cache
from tests.unit.modules.indexing.test_split_seam import NOW, ORG, OWNER, Rig


@pytest.fixture
def rig() -> Rig:
    NOW[0] = 1_000_000
    clear_part_cache()
    return Rig()


async def _split(rig: Rig, v: str, key: str, *, org: str = ORG, connector: str = OWNER, created: int | None = None) -> Any:  # noqa: ANN401
    rig.graph.put("records", key, {
        "orgId": org, "connectorId": connector, "virtualRecordId": v, "recordName": f"{v}.pdf",
        "indexingStatus": "IN_PROGRESS", "mimeType": "application/pdf",
        **({"createdAtTimestamp": created} if created is not None else {}),
    })
    request = dataclasses.replace(rig.request(), parent=rig.graph.docs["records"][key], v=v, org_id=org)
    return (await rig.seam.split(request)).row


def _app(rig: Rig, connector: str) -> None:
    rig.graph.put("apps", connector, {"orgId": ORG, "name": connector})


@pytest.mark.asyncio
async def test_owner_moves_to_the_earliest_live_copy_then_to_the_next(rig: Rig) -> None:
    for c in ("conn-a", "conn-b", "conn-c"):
        _app(rig, c)
    row = await _split(rig, "vx", "rec-a", connector="conn-a", created=100)
    # two copies of the same content in other connectors; C was created before B
    rig.graph.put("records", "rec-b", {"orgId": ORG, "connectorId": "conn-b", "virtualRecordId": "vx", "createdAtTimestamp": 300})
    rig.graph.put("records", "rec-c", {"orgId": ORG, "connectorId": "conn-c", "virtualRecordId": "vx", "createdAtTimestamp": 200})
    assert (await owner_of(rig.graph, rig.store, row)).connector == "conn-a"

    rig.graph.docs["records"].pop("rec-a")
    rig.graph.docs["apps"].pop("conn-a")
    assert await rehome_parts_owned_by(rig.graph, rig.store, ORG, "conn-a") == 1
    assert {d["ownerConnectorId"] for d in rig.part_docs()} == {"conn-c"}

    rig.graph.docs["records"].pop("rec-c")
    rig.graph.docs["apps"].pop("conn-c")
    # no explicit re-home call: the orphan sweep's check finds the hinted connector gone
    assert await rehome_if_owner_gone(rig.graph, rig.store, await rig.store.get("vx"))
    assert {d["ownerConnectorId"] for d in rig.part_docs()} == {"conn-b"}
    assert (await owner_of(rig.graph, rig.store, await rig.store.get("vx"))).connector == "conn-b"


@pytest.mark.asyncio
async def test_rehome_on_connector_delete_stays_in_its_organisation(rig: Rig) -> None:
    _app(rig, "conn-a")
    await _split(rig, "v-org1", "rec-1", connector="conn-a")
    await _split(rig, "v-org2", "rec-2", org="org-2", connector="conn-a")
    rig.graph.put("records", "rec-1b", {"orgId": ORG, "connectorId": "conn-b", "virtualRecordId": "v-org1"})
    rig.graph.put("records", "rec-2b", {"orgId": "org-2", "connectorId": "conn-b", "virtualRecordId": "v-org2"})
    rig.graph.docs["records"].pop("rec-1")

    await rehome_parts_owned_by(rig.graph, rig.store, ORG, "conn-a")

    owners = {d["rootVirtualRecordId"]: d["ownerConnectorId"] for d in rig.part_docs()}
    assert owners == {"v-org1": "conn-b", "v-org2": "conn-a"}
