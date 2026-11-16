"""Reindexing the failed parts of a connector's split files with the files themselves.

"Reindex failed" on a connector lists records by its own id, which a part does not carry (its connector
is the hidden parts connector), so the parts are found from their ``contentSplitParts`` documents,
which name the owner connector.
"""
from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from app.config.constants.arangodb import CollectionNames, ProgressStatus
from app.modules.indexing.split_parts import part_event
from app.modules.indexing.split_rollup import FAILED_STATUSES
from app.modules.indexing.split_state import SplitState
from app.services.messaging.config import Topic
from app.utils.time_conversion import get_epoch_timestamp_in_ms

if TYPE_CHECKING:
    from collections.abc import Mapping

    from app.modules.indexing.split_rollup import SplitRollup
    from app.modules.indexing.split_state import SplitRow, SplitStore
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value
_PAGE = 100


class Producer(Protocol):
    async def send_message(self, topic: str, message: dict[str, Any], key: str | None = None) -> bool: ...


async def _records_of(graph: IGraphDBProvider, vrids: list[str]) -> dict[str, dict[str, Any]]:
    """The part records with these virtual record ids, in one read, by virtual record id."""
    if not vrids:
        return {}
    docs = await graph.get_nodes_by_field_in(RECORDS, "virtualRecordId", sorted(set(vrids)), raise_on_error=True)
    return {str(d["virtualRecordId"]): d for d in docs or [] if d.get("virtualRecordId")}


async def _accessible_roots(
    graph: IGraphDBProvider, org_id: str, connector_id: str, user_key: str, roots: set[str],
) -> set[str]:
    """The files among *roots* that *user_key* may access: those with a live holder in *connector_id* that the permission
    model lets the user reach, which is the rule the connector reindex applies to its own records."""
    if not roots:
        return set()
    holders = await graph.get_nodes_by_field_in(RECORDS, "virtualRecordId", sorted(roots), raise_on_error=True)
    mine = [d for d in holders or [] if d.get("connectorId") == connector_id and not d.get("isDeleted")]
    ids = [str(d["_key"]) for d in mine]
    if not ids:
        return set()
    allowed = (await graph.check_access(user_key, org_id, node_ids=ids)).node_ids
    return {str(d["virtualRecordId"]) for d in mine if str(d["_key"]) in allowed}


async def failed_parts(
    graph: IGraphDBProvider, store: SplitStore, org_id: str, connector_id: str, statuses: list[str],
    user_key: str | None = None,
) -> list[dict[str, Any]]:
    """Part records of *connector_id*'s split files whose indexing status is one of *statuses*, each with
    its part document under ``"_part"``. The parts are found by the owner on their documents. With a *user_key*
    only the parts of files that user may access are returned."""
    found: list[dict[str, Any]] = []
    after = ""
    while True:
        page = await store.parts_page_by_owner(connector_id, org_id, after_key=after, limit=_PAGE)
        # The live generation of every file on the page, and the records of its parts, one read each.
        rows = await store.get_many({info.root for info in page})
        live_gen = {root: doc.get("gen") for root, doc in rows.items()}
        current = [info for info in page if live_gen.get(info.root) == info.gen]
        if user_key is not None:
            visible = await _accessible_roots(graph, org_id, connector_id, user_key, {info.root for info in current})
            current = [info for info in current if info.root in visible]
        nodes = await _records_of(graph, [info.vrid for info in current])
        for info in current:
            node = nodes.get(info.vrid)
            if node is not None and node.get("indexingStatus") in statuses:
                found.append({**node, "_key": info.key, "_part": info})
        if len(page) < _PAGE:
            return found
        after = page[-1].key


async def _claim_for_retry(graph: IGraphDBProvider, key: str, node: dict[str, Any]) -> bool:
    """FAILED -> QUEUED as one compare-and-set on the status that was read, before the event is published: a part
    that ended or moved on since (a quick deterministic failure, another retry) is left as it is."""
    return await graph.update_node_fields_if_match(
        key, RECORDS, {"indexingStatus": ProgressStatus.QUEUED.value, "reason": None},
        {"virtualRecordId": node["virtualRecordId"], "indexingStatus": node["indexingStatus"]},
    )


async def _give_back(graph: IGraphDBProvider, key: str, node: dict[str, Any]) -> None:
    """The broker refused the event: the part goes back to the status and reason it had, if it is still queued."""
    await graph.update_node_fields_if_match(
        key, RECORDS, {"indexingStatus": node["indexingStatus"], "reason": node.get("reason")},
        {"virtualRecordId": node["virtualRecordId"], "indexingStatus": ProgressStatus.QUEUED.value},
    )


async def reindex_failed_parts(
    graph: IGraphDBProvider, store: SplitStore, producer: Producer, *, org_id: str, connector_id: str, statuses: list[str],
    rollup: SplitRollup | None = None, user_key: str | None = None,
) -> int:
    """Queue the failed parts again. Returns the number of files that had one. With a *user_key* only the files that user
    may access are retried. With a *rollup*, each such file
    goes back to ``INDEXING_PARTS`` first (the roll-up's compare-and-set), so its state is never terminal while a
    part it counted as failed is running again."""
    parts = await failed_parts(graph, store, org_id, connector_id, statuses, user_key)
    files: set[str] = set()
    reopened: set[tuple[str, str]] = set()
    for part in parts:
        info = part["_part"]
        if rollup is not None and (info.root, info.gen) not in reopened:
            reopened.add((info.root, info.gen))
            await rollup.reopen(info.root, info.gen)
        if not await _claim_for_retry(graph, part["_key"], part):
            continue
        event_type, payload = part_event(part, owner_connector_id=connector_id)
        sent = await producer.send_message(
            Topic.RECORD_EVENTS.value, {"eventType": event_type, "timestamp": get_epoch_timestamp_in_ms(), "payload": payload}, key=part["_key"],
        )
        if not sent:
            await _give_back(graph, part["_key"], part)
            continue
        files.add(part["_part"].root)
    return len(files)


class EventSender(Protocol):
    async def send_event(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> bool: ...


async def retry_failed_parts_of(
    graph: IGraphDBProvider, store: SplitStore, publisher: EventSender, rollup: SplitRollup, v: str, *, owner_connector_id: str,
) -> int:
    """Republish the failed parts of one file's live generation (a ``PARTIAL`` or ``FAILED`` file's retry), first moving
    the file back to ``INDEXING_PARTS`` with the roll-up's compare-and-set. Returns how many parts were queued. A part
    the broker refuses stays failed, and the heal pass settles the file again."""
    row = await store.get(v)
    if row is None or not await rollup.reopen(v, row.gen):
        return 0
    nodes = await _records_of(graph, [p.vrid for p in row.parts])
    queued = 0
    for spec in row.parts:
        node = nodes.get(spec.vrid)
        if node is None or node.get("indexingStatus") not in FAILED_STATUSES:
            continue
        if node.get("indexingStatus") not in FAILED_STATUSES or not await _claim_for_retry(graph, spec.node_key, node):
            continue
        event_type, payload = part_event({**node, "_key": spec.node_key}, owner_connector_id=owner_connector_id)
        if not await publisher.send_event(Topic.RECORD_EVENTS.value, event_type, payload, key=spec.node_key):
            await _give_back(graph, spec.node_key, node)
            continue
        queued += 1
    return queued


class ReindexAction(StrEnum):
    CONTINUE = "continue"  # the ordinary parent attempt: cut, or finish an attempt that was cut short
    RETRY_FAILED = "retry_failed"  # republish the failed parts of the live generation, no re-cut
    NOTHING = "nothing"  # the live generation stands


FORCE_AT_KEY = "forceReindexAt"
HEALED_PREFIX = "healed:"


def forced_nonce(event_data: Mapping[str, Any], delivery_id: object) -> str:
    """The nonce of a ``forceReindex`` event, empty for any other. It is ``<request time in ms>:<delivery id>``: the time
    is the monotonic part that tells a redelivered older request from a newer one (delivery ids of the three brokers
    cannot be compared); a sender that stamps no time gets 0."""
    if not (event_data.get("forceReindex") and delivery_id):
        return ""
    at = event_data.get(FORCE_AT_KEY)
    stamp = at if isinstance(at, int) and not isinstance(at, bool) and at > 0 else 0
    return f"{stamp}:{delivery_id}"


def _stamp_of(nonce: str) -> int | None:
    head, _, rest = nonce.partition(":")
    return int(head) if rest and head.isdigit() else None


def supersedes(nonce: str, current: str) -> bool:
    """Whether an event carrying *nonce* replaces the generation made under *current*. A plain event (no nonce) and the
    request that made the generation never do; the heal always does; a forced request does unless it is older than the
    one that made the generation (a redelivery of it after a newer reindex started)."""
    if not nonce or nonce == current:
        return False
    if nonce.startswith(HEALED_PREFIX) or not current:
        return True
    new, old = _stamp_of(nonce), _stamp_of(current)
    if new is None or old is None:
        return True
    return new > old or (new == old == 0)


def single_file_reindex_action(row: SplitRow | None, *, content_rev: str, nonce: str) -> ReindexAction:
    """What an event for one split file does to the generation it already has.

    *nonce* is the delivery id of a ``forceReindex`` event and empty otherwise. The content is the same
    when ``content_rev`` is: a changed file always gets a new generation. A ``PARTIAL`` or ``FAILED`` file
    retries its failed parts, forced or not, so "Retry failed parts" never re-cuts. A ``COMPLETED`` file
    is left alone unless the event is a force reindex. A file still being split or indexed continues as a
    redelivered attempt would.
    """
    if row is None or row.state is SplitState.PURGING or row.content_rev != content_rev:
        return ReindexAction.CONTINUE
    if row.state in (SplitState.PARTIAL, SplitState.FAILED):
        return ReindexAction.RETRY_FAILED
    if row.state is SplitState.COMPLETED:
        return ReindexAction.CONTINUE if supersedes(nonce, row.nonce) else ReindexAction.NOTHING
    return ReindexAction.CONTINUE
