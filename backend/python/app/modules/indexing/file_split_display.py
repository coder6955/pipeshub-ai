"""What a user sees of a file that was cut into parts: the roll-up fields of listings, record details and stats.

A split file's own record stays an ordinary record; its progress lives in ``contentSplits`` and is
projected here at read time, never stored on a record:

* ``splitState``: ``SPLITTING`` | ``INDEXING_PARTS`` | ``COMPLETED`` | ``PARTIAL`` | ``FAILED``
* ``splitDone`` / ``splitTotal`` / ``splitFailed``: parts in a terminal status, parts in all, parts failed

Every function here reads through the graph provider's generic reads, so one implementation serves ArangoDB,
Neo4j and PostgreSQL, and none of them throws: the fields are additive, and a listing that cannot read them
is served without them (counted and logged) rather than failed.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, MutableMapping
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import CollectionNames
from app.modules.indexing.split_state import ORG_FIELD, SplitState, display_for
from app.telemetry.backend import METRICS_BACKEND

if TYPE_CHECKING:
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

logger = logging.getLogger(__name__)

SPLIT_FIELDS = ("splitState", "splitDone", "splitTotal", "splitFailed")

# The states a user is told about. A file being purged (a re-split in flight) shows as a normal file.
SHOWN_STATES = frozenset(s.value for s in SplitState if s is not SplitState.PURGING)

# Stats buckets: a split file that is not fully indexed is not Completed.
_RUNNING = {SplitState.SPLITTING.value, SplitState.INDEXING_PARTS.value}
_NEEDS_ATTENTION = {SplitState.PARTIAL.value, SplitState.FAILED.value}

DISPLAY_READ_FAILURES = METRICS_BACKEND.counter(
    "pipeshub_split_display_read_failures_total",
    "Reads of the split roll-up of a listing, record or stats response that failed and were served without it",
    ["surface"],
)


SPLIT_STORE_MISSING = METRICS_BACKEND.counter(
    "pipeshub_split_display_store_missing_total",
    "Reads of the split roll-up served without it because the split collection does not exist yet (not bootstrapped)",
    ["surface"],
)


def store_not_bootstrapped(exc: BaseException) -> bool:
    """ArangoDB answers a read of a collection no process has created yet with error 1203 (``collection or view not
    found``), which is what a query pod sees when it is upgraded ahead of the indexing pods. No split file exists then."""
    text = str(exc).lower()
    return "1203" in text and "not found" in text or "collection or view not found" in text


def _unreadable(exc: BaseException, surface: str, what: str) -> None:
    """Count and log a roll-up read that failed. A split collection that is not bootstrapped yet is "no split rows":
    debug level, its own metric, no traceback."""
    if store_not_bootstrapped(exc):
        logger.debug("The split collection does not exist yet (%s); serving without the split roll-up", surface)
        SPLIT_STORE_MISSING.inc(surface)
        return
    logger.warning("Could not read %s (%s); serving them without the split roll-up", what, surface, exc_info=exc)
    DISPLAY_READ_FAILURES.inc(surface)


def _count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _clean(display: Mapping[str, Any]) -> dict[str, Any] | None:
    state = display.get("splitState")
    done, total, failed = (_count(display.get(k)) for k in ("splitDone", "splitTotal", "splitFailed"))
    if state not in SHOWN_STATES or done is None or total is None or failed is None:
        return None
    return {"splitState": state, "splitDone": done, "splitTotal": total, "splitFailed": failed}


async def split_display_by_vrid(
    vrids: Iterable[str | None], graph: IGraphDBProvider, *, surface: str,
) -> dict[str, dict[str, Any]]:
    """``{vrid: split fields}`` for the files among *vrids* that are split. One batched read."""
    ids = sorted({v for v in vrids if isinstance(v, str) and v})
    if not ids:
        return {}
    try:
        shown = await display_for(ids, graph)
    except Exception as exc:
        _unreadable(exc, surface, f"the split roll-up of {len(ids)} files")
        return {}
    return {v: clean for v, d in shown.items() if (clean := _clean(d)) is not None}


async def split_display_by_record(
    record_ids: Iterable[str | None], graph: IGraphDBProvider, *, surface: str,
) -> dict[str, dict[str, Any]]:
    """``{record id: split fields}``: one read of the records' virtual ids, then one batched roll-up read."""
    ids = sorted({r for r in record_ids if isinstance(r, str) and r})
    if not ids:
        return {}
    try:
        docs = await graph.get_nodes_by_field_in(
            CollectionNames.RECORDS.value, "id", ids, ["id", "virtualRecordId"], raise_on_error=True,
        )
    except Exception as exc:
        _unreadable(exc, surface, f"the virtual ids of {len(ids)} records")
        return {}
    vrid_of = {d["id"]: d["virtualRecordId"] for d in docs or [] if d.get("id") and d.get("virtualRecordId")}
    by_vrid = await split_display_by_vrid(vrid_of.values(), graph, surface=surface)
    return {rid: by_vrid[v] for rid, v in vrid_of.items() if v in by_vrid}


async def stamp_rows(
    rows: Iterable[MutableMapping[str, Any]], graph: IGraphDBProvider, *, surface: str, id_key: str = "id",
) -> None:
    """Put the split fields on each row (in place) whose record is a split file. The rows of one page, one read."""
    page = [r for r in rows if isinstance(r, MutableMapping)]
    shown = await split_display_by_record((r.get(id_key) for r in page), graph, surface=surface)
    for row in page:
        fields = shown.get(row.get(id_key))
        if fields:
            row.update(fields)


async def stamp_record(
    record: MutableMapping[str, Any], graph: IGraphDBProvider, *, vrid: object = None, surface: str = "record",
) -> None:
    """Put the split fields on a record's details, in place. *vrid* defaults to the record's own ``virtualRecordId``."""
    vrid = vrid or record.get("virtualRecordId")
    shown = await split_display_by_vrid([vrid], graph, surface=surface)
    if isinstance(vrid, str) and vrid in shown:
        record.update(shown[vrid])


async def roll_up_stats(
    stats: dict[str, Any], graph: IGraphDBProvider, org_id: str, connector_id: str,
) -> None:
    """Make the stats of a connector or collection count a split file by its roll-up, in place.

    The stored ``indexingStatus`` of a split file is COMPLETED once its manifest is indexed, so the counts
    would call it Completed while its parts index. Buckets keep their existing keys:

    * ``SPLITTING`` / ``INDEXING_PARTS``: moved from ``COMPLETED`` to ``IN_PROGRESS``;
    * ``PARTIAL`` / ``FAILED``: moved from ``COMPLETED`` to ``FAILED`` (the "reindex failed" action retries
      their failed parts), and reported apart in ``stats.splitFiles`` so ``PARTIAL`` is still told from ``FAILED``.

    ``stats.splitFiles`` (additive, only when a split file is not complete) counts every such file once:
    ``{preparing, indexingParts, partial, failed}``. Only files whose stored status is COMPLETED are moved;
    a file that is itself failed or in progress is already counted in its bucket. Never raises.
    """
    try:
        await _roll_up(stats, graph, org_id, connector_id)
    except Exception as exc:
        _unreadable(exc, "stats", f"the split files of {connector_id}")


async def _roll_up(stats: dict[str, Any], graph: IGraphDBProvider, org_id: str, connector_id: str) -> None:
    # One read per open state, scoped to the organisation by the query itself (the (orgId, state) index serves it): the
    # open rows of every other organisation are never read.
    state_of: dict[str, str] = {}
    for state in sorted(_RUNNING | _NEEDS_ATTENTION):
        for r in await graph.get_nodes_by_filters(
            CollectionNames.CONTENT_SPLITS.value, {"orgId": org_id, "state": state}, raise_on_error=True,
        ) or []:
            key = r.get("id") or r.get("_key")
            if key and r.get(ORG_FIELD) == org_id:
                state_of[str(key)] = state
    if not state_of:
        return
    records = await graph.get_nodes_by_field_in(
        CollectionNames.RECORDS.value, "virtualRecordId", sorted(state_of),
        ["id", "virtualRecordId", "connectorId", "orgId", "recordType", "indexingStatus",
         "isDeleted", "isInternal", "isPlaceholder"],
        raise_on_error=True,
    )
    totals = stats.get("stats", {})
    counts = totals.get("indexingStatus")
    by_type = {t.get("recordType"): t for t in stats.get("byRecordType", [])}
    files = {"preparing": 0, "indexingParts": 0, "partial": 0, "failed": 0}
    for rec in records or []:
        if (rec.get("connectorId") != connector_id or rec.get(ORG_FIELD) != org_id or rec.get("isDeleted") is True
                or rec.get("isInternal") is True or rec.get("isPlaceholder") is True):
            continue
        state = state_of.get(rec.get("virtualRecordId"))
        if state is None:
            continue
        files["preparing" if state == SplitState.SPLITTING.value else
              "indexingParts" if state == SplitState.INDEXING_PARTS.value else
              "partial" if state == SplitState.PARTIAL.value else "failed"] += 1
        if rec.get("indexingStatus") != "COMPLETED" or not isinstance(counts, dict):
            continue
        target = "IN_PROGRESS" if state in _RUNNING else "FAILED"
        _move(counts, "COMPLETED", target)
        per_type = by_type.get(rec.get("recordType"))
        if per_type is not None and isinstance(per_type.get("indexingStatus"), dict):
            _move(per_type["indexingStatus"], "COMPLETED", target)
    if any(files.values()):
        totals["splitFiles"] = files


def _move(counts: dict[str, Any], source: str, target: str) -> None:
    if isinstance(counts.get(source), int) and counts[source] > 0:
        counts[source] -= 1
        counts[target] = counts.get(target, 0) + 1
