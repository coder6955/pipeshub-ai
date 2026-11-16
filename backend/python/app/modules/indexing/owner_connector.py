"""Which connector a record answers to.

A part of a split PDF lives under its own hidden connector id (``pdfparts_<org>``) so no
connector listing, count or sync sees it, but the connector that owns the parent still decides
whether the part is indexed, which lane it travels on and what it counts towards. Code that
reads a connector off a graph node for gating, lanes, activity or recovery calls
``owner_connector_id`` instead of reading ``connectorId``.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

__all__ = [
    "PART_CONNECTOR_PREFIX",
    "is_part_connector_id",
    "is_part_record",
    "owner_connector_id",
    "PartSkips",
    "part_connector_id",
    "prime_readable",
]

PART_CONNECTOR_PREFIX = "pdfparts_"


def _field(record: Mapping[str, Any] | object | None, camel: str, snake: str) -> Any:  # noqa: ANN401
    if record is None:
        return None
    if isinstance(record, Mapping):
        value = record.get(camel)
        return value if value is not None else record.get(snake)
    value = getattr(record, camel, None)
    return value if value is not None else getattr(record, snake, None)


def owner_connector_id(record: Mapping[str, Any] | object | None) -> str | None:
    """The connector that decides whether the record is indexed: its own ``connectorId``, or for a part
    (a record under the hidden parts connector) the connector that owns the file it was cut from, read
    from its part document. The document must already have been read (``split_state.prime_parts``, or
    ``part_info``); a part whose document was not read raises ``PartInfoMissing`` rather than answer
    with the hidden connector, which no gate knows."""
    connector = _field(record, "connectorId", "connector_id")
    if not is_part_connector_id(connector):
        return str(connector) if connector else None
    from app.modules.indexing.split_state import PartInfoMissing, cached_part_info

    info = cached_part_info(record)
    if info is None:
        raise PartInfoMissing("the part document of this record was not read (call split_state.prime_parts first)")
    if not info.owner_connector_id:
        # A part document written before it named an owner: no gate may decide for it until the owner is resolved.
        raise PartInfoMissing("the part document names no owner connector yet")
    return info.owner_connector_id


def is_part_record(record: Mapping[str, Any] | object | None) -> bool:
    """A part is a record under the hidden parts connector (decided by the record's own fields)."""
    return is_part_connector_id(_field(record, "connectorId", "connector_id"))


def part_connector_id(org_id: str) -> str:
    return f"{PART_CONNECTOR_PREFIX}{org_id}"


def is_part_connector_id(connector_id: str | None) -> bool:
    return bool(connector_id) and str(connector_id).startswith(PART_CONNECTOR_PREFIX)


class PartSkips:
    """The parts one recovery pass left alone because their part document is missing (D-24: fail closed for the record, not the pass)."""

    def __init__(self) -> None:
        self.keys: set[str] = set()

    @property
    def count(self) -> int:
        return len(self.keys)


async def prime_readable(
    records: Iterable[Mapping[str, Any]], graph: Any, skips: PartSkips, logger: logging.Logger | None = None,  # noqa: ANN401
) -> list[Mapping[str, Any]]:
    """``prime_parts`` for a page-level loop: the records whose owner can be told, with a part that has no
    document skipped and counted in *skips* (one warning per record per pass). A store error still propagates."""
    from app.modules.indexing.split_state import PartInfoMissing, part_info

    usable: list[Mapping[str, Any]] = []
    for record in records:
        if is_part_record(record):
            try:
                await part_info(record, graph)
            except PartInfoMissing as exc:
                key = str(record.get("_key") or record.get("id"))
                if key not in skips.keys:
                    skips.keys.add(key)
                    (logger or logging.getLogger(__name__)).warning("Skipping part %s this pass: %s", key, exc)
                continue
        usable.append(record)
    return usable
