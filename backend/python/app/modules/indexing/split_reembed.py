"""Re-embedding a split file from its stored content (restore from the trash, ``vectorDbOnly``, the vector-store rebuild).

A file's points sit under three kinds of owner: the manifest (segment ``V``) and one segment per part
(``V_k``). The three re-embed paths all reach the file through its parent record, which carries only the manifest,
so ``reembed_targets`` names what else must be re-embedded and ``SplitReembed.parts`` queues each part as an
ordinary part event (``vectorDbOnly``, the part's own slot): one part at a time, none of them ever deleting by ``V``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from app.config.constants.arangodb import CollectionNames, EventTypes, ProgressStatus
from app.exceptions.indexing_exceptions import IndexingError
from app.modules.indexing.split_parts import part_event
from app.modules.indexing.split_state import SplitRow, SplitState, SplitStore
from app.services.messaging.config import Topic
from app.utils.time_conversion import get_epoch_timestamp_in_ms

if TYPE_CHECKING:
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value


@dataclass(frozen=True)
class ReembedTarget:
    """One owner of points to re-embed. ``record_key`` is the part record; None for the manifest, which the
    parent record's own event handles."""

    segment: str
    record_key: str | None = None


def reembed_targets(v: str, row: SplitRow | None) -> list[ReembedTarget]:
    """``[V] + the parts of the live generation``. A file with no live split is just ``[V]``, and one whose
    generation is being purged has no parts to re-embed."""
    targets = [ReembedTarget(segment=v)]
    if row is None or row.state is SplitState.PURGING:
        return targets
    return [*targets, *(ReembedTarget(segment=p.vrid, record_key=p.node_key) for p in row.parts)]


class EventSender(Protocol):
    async def send_event(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> bool: ...


class SplitReembed:
    def __init__(self, graph: IGraphDBProvider, store: SplitStore, publisher: EventSender) -> None:
        self._graph = graph
        self._store = store
        self._publisher = publisher

    async def row(self, v: str) -> SplitRow | None:
        return await self._store.get(v)

    async def queue_parts(self, row: SplitRow, owner_connector_id: str) -> int:
        """Publish a ``vectorDbOnly`` reindex for each part that has stored content to embed. A part that
        failed or is still indexing has none; its own indexing writes its points when it finishes. Returns
        how many were queued; raises when the broker refuses one, so the parent's event is retried."""
        docs = await self._graph.get_nodes_by_field_in(
            RECORDS, "virtualRecordId", [p.vrid for p in row.parts], raise_on_error=True,
        )
        nodes = {str(d["virtualRecordId"]): d for d in docs or [] if d.get("virtualRecordId")}
        queued = 0
        for target in reembed_targets(row.v, row)[1:]:
            node = nodes.get(target.segment)
            if node is None or node.get("indexingStatus") != ProgressStatus.COMPLETED.value:
                continue
            _event, payload = part_event({**node, "_key": target.record_key}, owner_connector_id=owner_connector_id)
            payload = {**payload, "forceReindex": True, "forceReindexAt": get_epoch_timestamp_in_ms(), "vectorDbOnly": True}
            if not await self._publisher.send_event(
                Topic.RECORD_EVENTS.value, EventTypes.REINDEX_RECORD.value, payload, key=target.record_key,
            ):
                raise IndexingError(
                    "the message broker did not accept the re-embed of a part", details={"record_id": target.record_key},
                )
            queued += 1
        return queued
