"""Whose connector the parts of a split file answer to.

Parts belong to the content ``V``, not to a connector: several copies of one file (a duplicate in another connector)
share ``V`` and its parts. The part documents store an owner connector, but only as a hint. At use time the owner is
read from ``V``'s live holders: the stored hint while a live record of that connector still holds ``V``, else the
earliest-created live holder (ties by record key), which every process computes the same. When the hint no longer
names a holder the parts are re-homed to the resolved owner, never purged: parts are retired only when no holder is
left, which the orphan sweep decides.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.config.constants.arangodb import CollectionNames
from app.exceptions.indexing_exceptions import IndexingError
from app.modules.indexing.owner_connector import is_part_connector_id

if TYPE_CHECKING:
    from app.modules.indexing.split_state import SplitRow, SplitStore
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value
_logger = logging.getLogger(__name__)
_PAGE = 100


class NoLiveHolder(IndexingError):
    """No live record holds the file, for example one trashed while its parts were being cut. Its parts are queued
    when it is restored, and retired when it is deleted."""


async def resolve_owner_connector(graph: IGraphDBProvider, v: str, hint: str | None) -> str | None:
    """The connector the parts of *v* answer to now, or None when no live record holds *v*."""
    keys = await graph.get_records_by_virtual_record_id(v, raise_on_error=True)
    if not keys:
        return None
    docs = await graph.get_nodes_by_field_in(
        RECORDS, "id", sorted(keys), return_fields=["id", "connectorId", "createdAtTimestamp"], raise_on_error=True,
    )
    holders = [
        (_created(d), str(d.get("_key") or d.get("id")), str(d["connectorId"]))
        for d in docs or []
        if d.get("connectorId") and not is_part_connector_id(d["connectorId"])
    ]
    if not holders:
        return None
    if hint and any(connector == hint for _created_at, _key, connector in holders):
        return hint
    return min(holders)[2]


def _created(doc: dict) -> float:
    value = doc.get("createdAtTimestamp")
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else float("inf")


@dataclass(frozen=True)
class PartsOwner:
    connector: str
    # The keys of the parts that have a part document: a part without one is never acted on.
    documented: frozenset[str]


async def owner_of(graph: IGraphDBProvider, store: SplitStore, row: SplitRow) -> PartsOwner:
    """The owner connector of *row*'s parts, re-homing the part documents when it changed. Raises ``NoLiveHolder``
    when nothing live holds the file, and ``IndexingError`` when the parts have no document naming an owner."""
    infos = await store.parts_of(row.v, row.org_id, row.gen)
    hint = next((i.owner_connector_id for i in infos if i.owner_connector_id), None)
    if hint is None:
        raise IndexingError(
            f"the parts of {row.v} have no part document naming their owner; they are not queued",
            details={"virtual_record_id": row.v},
        )
    owner = await resolve_owner_connector(graph, row.v, hint)
    if owner is None:
        raise NoLiveHolder(f"no live record holds {row.v}; its parts are not queued", details={"virtual_record_id": row.v})
    if owner != hint or any(i.owner_connector_id != owner for i in infos):
        await store.rehome_parts(row.v, row.org_id, row.gen, owner)
    return PartsOwner(owner, frozenset(i.key for i in infos))


async def rehome_parts_owned_by(graph: IGraphDBProvider, store: SplitStore, org_id: str, connector_id: str) -> int:
    """After *connector_id* was deleted: every split file whose parts it owned and that another live record still holds
    gets that record's connector as owner. A file nothing holds is left to the orphan sweep, which retires its
    parts. Returns the number of files re-homed."""
    seen: set[tuple[str, str]] = set()
    after = ""
    while True:
        page = await store.parts_page_by_owner(connector_id, org_id, after_key=after, limit=_PAGE)
        for info in page:
            seen.add((info.root, info.gen))
        if len(page) < _PAGE:
            break
        after = page[-1].key
    rehomed = 0
    for root, gen in sorted(seen):
        owner = await resolve_owner_connector(graph, root, connector_id)
        if owner is None or owner == connector_id:
            continue
        if await store.rehome_parts(root, org_id, gen, owner):
            _logger.info("The parts of %s now answer to connector %s", root, owner)
            rehomed += 1
    return rehomed


async def rehome_if_owner_gone(graph: IGraphDBProvider, store: SplitStore, row: SplitRow) -> bool:
    """The sweep's check for a file some live record still holds: when the connector the parts name no longer exists
    (its deletion cannot be missed here, whichever path removed it), they are re-homed to the resolved owner."""
    infos = await store.parts_of(row.v, row.org_id, row.gen)
    hint = next((i.owner_connector_id for i in infos if i.owner_connector_id), None)
    if hint is None or await graph.get_document(hint, CollectionNames.APPS.value, raise_on_error=True) is not None:
        return False
    owner = await resolve_owner_connector(graph, row.v, hint)
    if owner is None or owner == hint:
        return False
    return bool(await store.rehome_parts(row.v, row.org_id, row.gen, owner))
