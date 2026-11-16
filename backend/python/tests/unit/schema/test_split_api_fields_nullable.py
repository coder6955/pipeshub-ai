"""The split roll-up fields of the API are optional: a response without them validates, reads as null and serialises as before."""
from __future__ import annotations

from app.connectors.sources.localKB.api.knowledge_hub_models import (
    NodeItem,
    NodeType,
    OriginType,
)
from app.connectors.sources.localKB.api.models import RecordResponse


def test_api_models_round_trip_without_the_split_fields_and_serialise_as_before() -> None:
    node = NodeItem(id="n", name="a.pdf", nodeType=NodeType.RECORD, origin=OriginType.COLLECTION, createdAt=1, updatedAt=2, hasChildren=False)
    assert (node.splitState, node.splitDone, node.splitTotal, node.splitFailed) == (None, None, None, None)
    sent = node.model_dump(exclude_none=True)
    assert not any(k.startswith("split") for k in sent)
    assert NodeItem.model_validate(sent) == node
    record = RecordResponse(
        id="r", externalRecordId="e", recordName="a.pdf", recordType="FILE", origin="UPLOAD", connectorName="c",
        indexingStatus="COMPLETED", createdAtTimestamp=1, updatedAtTimestamp=2, orgId="o", version=0, isDeleted=False,
        isLatestVersion=True,
    )
    assert record.splitState is None
    body = record.model_dump(exclude_none=True)
    assert not any(k.startswith("split") for k in body)
    assert RecordResponse.model_validate(body) == record
