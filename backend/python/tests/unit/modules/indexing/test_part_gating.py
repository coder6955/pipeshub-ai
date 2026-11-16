"""T7: a part is gated by the connector that owns its file (off, removed, being deleted), read from its part document."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import OriginTypes, ProgressStatus
from app.modules.indexing.connector_off_events import (
    ConnectorState,
    ReadTimeOutcome,
    gating_connector_id,
    read_time_outcome,
)
from app.modules.indexing.split_state import clear_part_cache, prime_parts

OWNER = "drive-1"
PART = {
    "_key": "part-1", "connectorId": "pdfparts_org-1", "origin": OriginTypes.UPLOAD.value, "isDeleted": False,
    "indexingStatus": ProgressStatus.QUEUED.value,
}
PART_DOC = {
    "id": "part-1", "orgId": "org-1", "rootVirtualRecordId": "V", "gen": "g", "k": 1, "pageStart": 1, "pageEnd": 500, "pageOffset": 0,
    "overlapPages": 0, "ownerConnectorId": OWNER, "virtualRecordId": "V1",
}
PAYLOAD = {"recordId": "part-1", "connectorId": OWNER}


@pytest.fixture(autouse=True)
def _primed() -> None:
    clear_part_cache()


async def _prime() -> None:
    graph = MagicMock()
    graph.get_document = AsyncMock(return_value=PART_DOC)
    await prime_parts([PART], graph)


async def test_a_part_is_gated_by_its_owner_and_never_by_the_hidden_connector() -> None:
    await _prime()

    assert gating_connector_id(PART) == OWNER


@pytest.mark.parametrize(
    ("state", "outcome"),
    [
        (ConnectorState.OFF, ReadTimeOutcome.SETTLE_CONNECTOR_OFF),
        (ConnectorState.REMOVED, ReadTimeOutcome.SETTLE),
        (ConnectorState.ACTIVE, ReadTimeOutcome.PASS),
        (ConnectorState.DELETING, ReadTimeOutcome.PASS),   # the handler holds the event for a delete (record.py reads gating_connector_id)
    ],
)
async def test_the_read_time_filter_judges_a_part_by_the_state_of_its_owner(state: ConnectorState, outcome: ReadTimeOutcome) -> None:
    await _prime()
    asked: list[str] = []

    def state_of(connector_id: str) -> ConnectorState:
        asked.append(connector_id)
        return state

    assert read_time_outcome("newRecord", PAYLOAD, PART, state_of) is outcome
    assert asked in ([OWNER], []), asked


def test_a_part_whose_owner_was_never_read_is_not_judged_at_all() -> None:
    from app.modules.indexing.split_state import PartInfoMissing

    with pytest.raises(PartInfoMissing):
        read_time_outcome("newRecord", PAYLOAD, PART, lambda _c: ConnectorState.ACTIVE)
