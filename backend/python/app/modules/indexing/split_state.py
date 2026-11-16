"""The ``contentSplits`` row of a split PDF and the identities derived from it.

One row per parent virtual record ``V`` names the live generation ``G`` of its parts, so a re-split
never leaves two generations indexing. Everything here goes through ``IGraphDBProvider``'s generic
node methods, which every graph store implements.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import CollectionNames
from app.modules.indexing.owner_connector import PART_CONNECTOR_PREFIX

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable

    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

logger = logging.getLogger(__name__)
# The field that names the organisation on the split documents (and on the records they describe), read from stored
# documents, never from a claim or a payload.
ORG_FIELD = "orgId"

SPLITS = CollectionNames.CONTENT_SPLITS.value
SPLIT_PARTS = CollectionNames.CONTENT_SPLIT_PARTS.value
RECORDS = CollectionNames.RECORDS.value


class SplitState(StrEnum):
    SPLITTING = "SPLITTING"
    INDEXING_PARTS = "INDEXING_PARTS"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    PURGING = "PURGING"


@dataclass(frozen=True)
class PartSpec:
    """One part of a generation. ``k`` is 1-based: block address 0 belongs to the manifest."""

    k: int
    page_start: int
    page_end: int
    overlap_end: int
    vrid: str
    node_key: str
    ocr: bool | None = False  # None: no hint, the part is inspected on its own

    def to_doc(self) -> dict[str, Any]:
        return {
            "k": self.k, "pageStart": self.page_start, "pageEnd": self.page_end,
            "overlapEnd": self.overlap_end, "vrid": self.vrid, "nodeKey": self.node_key, "ocr": self.ocr,
        }

    @classmethod
    def from_doc(cls, doc: Mapping[str, Any]) -> PartSpec:
        return cls(
            k=int(doc["k"]), page_start=int(doc["pageStart"]), page_end=int(doc["pageEnd"]),
            overlap_end=int(doc["overlapEnd"]), vrid=str(doc["vrid"]), node_key=str(doc["nodeKey"]),
            ocr=None if "ocr" in doc and doc["ocr"] is None else bool(doc.get("ocr", False)),
        )

    @property
    def overlap_pages(self) -> int:
        return self.overlap_end - self.page_end

    @property
    def page_offset(self) -> int:
        return self.page_start - 1


@dataclass(frozen=True)
class SplitRow:
    v: str
    org_id: str
    gen: str
    parts: tuple[PartSpec, ...]
    state: SplitState
    content_rev: str = ""
    plan_digest: str = ""
    nonce: str = ""
    done: int = 0
    failed: int = 0
    epoch: int = 0
    updated_at_ms: int = 0
    running_lines: tuple[str, ...] = ()
    # What the parent's stages were last driven for; empty until the first drive of the generation.
    redrive_token: str = ""
    # When that claim was taken; 0 once the drive it covered finished (a claim still stamped long after is a crashed drive).
    redrive_at_ms: int = 0

    @property
    def total(self) -> int:
        return len(self.parts)

    @property
    def page_count(self) -> int:
        """Pages of the file: the last part's last page (the overlap pages are another part's own)."""
        return self.parts[-1].page_end if self.parts else 0

    def display(self) -> dict[str, Any]:
        return {
            "splitState": self.state.value, "splitDone": self.done, "splitTotal": self.total,
            "splitFailed": self.failed,
        }

    def to_doc(self) -> dict[str, Any]:
        return {
            "orgId": self.org_id, "gen": self.gen, "plan": [p.to_doc() for p in self.parts],
            "total": self.total, "done": self.done, "failed": self.failed, "state": self.state.value,
            "epoch": self.epoch, "updatedAtMs": self.updated_at_ms, "contentRev": self.content_rev,
            "planDigest": self.plan_digest, "nonce": self.nonce, "redriveToken": self.redrive_token,
            "redriveAtMs": self.redrive_at_ms,
            **({"runningLines": list(self.running_lines)} if self.running_lines else {}),
        }

    @classmethod
    def from_doc(cls, doc: Mapping[str, Any]) -> SplitRow:
        plan = doc.get("plan") or []
        if isinstance(plan, str):
            plan = json.loads(plan)
        return cls(
            v=str(doc.get("id") or doc["_key"]), org_id=str(doc.get(ORG_FIELD) or ""), gen=str(doc["gen"]),
            parts=tuple(PartSpec.from_doc(p) for p in plan), state=SplitState(doc["state"]),
            content_rev=str(doc.get("contentRev") or ""), plan_digest=str(doc.get("planDigest") or ""),
            nonce=str(doc.get("nonce") or ""), done=int(doc.get("done") or 0), failed=int(doc.get("failed") or 0),
            epoch=int(doc.get("epoch") or 0), updated_at_ms=int(doc.get("updatedAtMs") or 0),
            running_lines=tuple(doc.get("runningLines") or ()),
            redrive_token=str(doc.get("redriveToken") or ""), redrive_at_ms=int(doc.get("redriveAtMs") or 0),
        )


@dataclass(frozen=True)
class PartInfo:
    """The ``contentSplitParts`` document of a part record: all that marks the record as a part of a
    split file (the record itself carries only ordinary record fields)."""

    key: str
    org_id: str
    root: str
    gen: str
    k: int
    page_start: int
    page_end: int
    page_offset: int
    overlap_pages: int
    owner_connector_id: str
    vrid: str = ""
    # Core blocks in the part's stored record; written when the part completes, so None until then.
    block_count: int | None = None

    def to_doc(self) -> dict[str, Any]:
        return {
            "id": self.key, "orgId": self.org_id, "rootVirtualRecordId": self.root, "gen": self.gen, "k": self.k,
            "pageStart": self.page_start, "pageEnd": self.page_end, "pageOffset": self.page_offset,
            "overlapPages": self.overlap_pages, "ownerConnectorId": self.owner_connector_id,
            "virtualRecordId": self.vrid,
            **({"blockCount": self.block_count} if self.block_count is not None else {}),
        }

    @classmethod
    def from_doc(cls, doc: Mapping[str, Any]) -> PartInfo:
        """A field that is absent or null (a document written before it existed) reads as "not recorded"."""
        page_start = int(doc["pageStart"])
        offset = doc.get("pageOffset")
        return cls(
            key=str(doc.get("_key") or doc["id"]), org_id=str(doc[ORG_FIELD]), root=str(doc["rootVirtualRecordId"]),
            gen=str(doc["gen"]), k=int(doc["k"]), page_start=page_start, page_end=int(doc["pageEnd"]),
            page_offset=int(offset) if offset is not None else page_start - 1,
            overlap_pages=int(doc.get("overlapPages") or 0),
            owner_connector_id=str(doc.get("ownerConnectorId") or ""), vrid=str(doc.get("virtualRecordId") or ""),
            block_count=_count_of(doc.get("blockCount")),
        )

    @classmethod
    def of(cls, spec: PartSpec, *, v: str, gen: str, org_id: str, owner_connector_id: str) -> PartInfo:
        return cls(
            key=spec.node_key, org_id=org_id, root=v, gen=gen, k=spec.k, page_start=spec.page_start,
            page_end=spec.page_end, page_offset=spec.page_offset, overlap_pages=spec.overlap_pages,
            owner_connector_id=owner_connector_id, vrid=spec.vrid,
        )


def _count_of(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


class PartInfoMissing(LookupError):
    """A record under the parts connector has no ``contentSplitParts`` document (or it was not read yet):
    nothing can say whose it is, so nothing may act on it."""


_CACHE_SIZE = 4096
# The owner connector on a part document is a hint that may be re-homed (``split_owner``), so a loader re-reads a
# document it cached longer ago than this; the synchronous accessors never expire an entry mid-use.
_CACHE_REFRESH_S = 300.0
_cache: OrderedDict[str, PartInfo] = OrderedDict()
_cached_at: dict[str, float] = {}
_default: dict[str, IGraphDBProvider | None] = {"graph": None}


def use_graph(graph: IGraphDBProvider | None) -> None:
    """The graph ``part_info`` reads from when its caller passes none (set once by the process wiring)."""
    _default["graph"] = graph


def clear_part_cache() -> None:
    _cache.clear()
    _cached_at.clear()


def _record_key(record: Mapping[str, Any] | object) -> str | None:
    if isinstance(record, Mapping):
        key = record.get("_key") or record.get("id")
    else:
        key = getattr(record, "id", None)
    return str(key) if key else None


def _record_connector(record: Mapping[str, Any] | object) -> str | None:
    if isinstance(record, Mapping):
        value = record.get("connectorId") or record.get("connector_id")
    else:
        value = getattr(record, "connector_id", None)
    return str(value) if value else None


def is_part(record: Mapping[str, Any] | object | None) -> bool:
    """A record is a part of a split file iff it sits under the hidden parts connector."""
    connector = _record_connector(record) if record is not None else None
    return bool(connector) and connector.startswith(PART_CONNECTOR_PREFIX)


def cached_part_info(record: Mapping[str, Any] | object | None) -> PartInfo | None:
    """What ``part_info`` already read for this record, without any I/O."""
    key = _record_key(record) if record is not None and is_part(record) else None
    info = _cache.get(key) if key else None
    if info is not None:
        _cache.move_to_end(key)
    return info


async def part_info(record: Mapping[str, Any] | object | None, graph: IGraphDBProvider | None = None) -> PartInfo | None:
    """The part document of *record*, or None when it is not a part. Read once and cached: a part's
    document never changes within its generation, and the key (derived from the generation) is never
    reused. A part whose document is missing raises ``PartInfoMissing`` (fail closed); a store error
    propagates so the caller retries."""
    if record is None or not is_part(record):
        return None
    cached = cached_part_info(record)
    if cached is not None:
        return cached
    source = graph or _default["graph"]
    if source is None:
        raise PartInfoMissing("no graph to read the part document from")
    key = _record_key(record)
    doc = await source.get_document(key, SPLIT_PARTS, raise_on_error=True) if key else None
    if not doc:
        raise PartInfoMissing(f"part record {key} has no contentSplitParts document")
    info = PartInfo.from_doc({**doc, "_key": key})
    remember_part(info)
    return info


def remember_part(info: PartInfo) -> None:
    _cache[info.key] = info
    _cached_at.setdefault(info.key, time.monotonic())
    _cache.move_to_end(info.key)
    while len(_cache) > _CACHE_SIZE:
        evicted, _ = _cache.popitem(last=False)
        _cached_at.pop(evicted, None)


def _forget_part(key: str) -> None:
    _cache.pop(key, None)
    _cached_at.pop(key, None)


async def record_block_count(record: Mapping[str, Any] | object | None, count: int | None, graph: IGraphDBProvider | None = None) -> None:
    """Write the block count of a part that just completed onto its ``contentSplitParts`` document. Does
    nothing for a record that is not a part; never raises (the heal sweep backfills what this missed)."""
    if count is None or record is None or not is_part(record):
        return
    try:
        source = graph or _default["graph"]
        info = cached_part_info(record) or await part_info(record, source)
        if info is not None and source is not None and await source.update_node_fields_if_match(
            info.key, SPLIT_PARTS, {"blockCount": int(count)}, {"gen": info.gen},
        ):
            remember_part(replace(info, block_count=int(count)))
    except Exception as exc:
        logger.warning("Block count of a part not recorded: %s", type(exc).__name__)


async def prime_parts(records: Iterable[Mapping[str, Any] | object], graph: IGraphDBProvider | None = None) -> None:
    """Read the part documents of every part among *records*, so the synchronous owner accessor can
    answer for them. A no-op (no I/O) for records that are not parts."""
    now = time.monotonic()
    pending = []
    for record in records:
        if not is_part(record):
            continue
        key = _record_key(record)
        if key and cached_part_info(record) is not None and now - _cached_at.get(key, now) > _CACHE_REFRESH_S:
            _forget_part(key)
        if cached_part_info(record) is None:
            pending.append(record)
    if pending:
        await asyncio.gather(*(part_info(r, graph) for r in pending))


async def part_records_pages(
    graph: IGraphDBProvider, owner: str, org_id: str | None, record_filters: Mapping[str, Any], *, page_size: int = 100,
) -> AsyncIterator[list[dict[str, Any]]]:
    """Pages of the part *records* of the files *owner* owns that match *record_filters* (equality on
    record fields, in practice ``indexingStatus`` and ``reason``). A part record names only the hidden parts
    connector, so the pages are read from the records by that connector and the filters (the index the
    other parked-record walks use: only the matching parts are read), and each page's part documents in
    one read by their indexed ``virtualRecordId``; those naming another owner are dropped. A part with no
    document is left out. Records come back primed for ``owner_connector_id``. The organisation is the
    owner connector's when the caller has none."""
    org = org_id or await _owner_org(graph, owner)
    if not org:
        return
    after = ""
    while True:
        page = await graph.get_documents_paginated(
            RECORDS, limit=page_size, filters={"connectorId": f"{PART_CONNECTOR_PREFIX}{org}", **record_filters},
            sort_field="_key", raise_on_error=True, after_key=after,
        )
        if not page:
            return
        keys = [str(r.get("_key") or r.get("id")) for r in page]
        docs = await graph.get_nodes_by_field_in(
            SPLIT_PARTS, "virtualRecordId", sorted({str(r["virtualRecordId"]) for r in page if r.get("virtualRecordId")}),
            raise_on_error=True,
        )
        owned = {}
        for doc in docs:
            info = PartInfo.from_doc({**doc, "_key": doc.get("_key") or doc.get("id")})
            if info.owner_connector_id == owner and info.org_id == org:
                owned[info.key] = info
        out = []
        for record, key in zip(page, keys, strict=True):
            if key in owned:
                remember_part(owned[key])
                out.append({**record, "_key": key})
        if out:
            yield out
        if len(page) < page_size or not keys[-1]:
            return
        after = keys[-1]


async def _owner_org(graph: IGraphDBProvider, owner: str) -> str | None:
    app = await graph.get_document(owner, CollectionNames.APPS.value, raise_on_error=True)
    org = (app or {}).get(ORG_FIELD)
    return str(org) if org else None


async def display_for(vrids: Iterable[str], graph: IGraphDBProvider | None = None) -> dict[str, dict[str, Any]]:
    """The split display of each file among *vrids* that is split, read from its ``contentSplits`` row:
    ``{vrid: {splitState, splitDone, splitTotal, splitFailed}}``. Files that are not split are absent.
    Listings and record details project this at read time; none of it is stored on a record."""
    source = graph or _default["graph"]
    if source is None:
        raise PartInfoMissing("no graph to read the split rows from")
    ids = sorted({v for v in vrids if v})
    if not ids:
        return {}
    shown: dict[str, dict[str, Any]] = {}
    for v, doc in (await SplitStore(source).get_many(ids)).items():
        try:
            shown[v] = SplitRow.from_doc(doc).display()
        except (KeyError, ValueError, TypeError):
            # One unreadable row (unknown state, broken plan) shows its file as a normal one; the page stands.
            logger.warning("Split row of %s is unreadable; showing the file without its split roll-up", v)
    return shown


def derive_gen(v: str, content_rev: str, plan_digest: str, nonce: str = "") -> str:
    """The generation of ``V``'s parts for this content and plan. A different *nonce* is how a forced
    re-split of the same bytes gets a generation of its own."""
    return str(uuid.uuid5(_namespace(v), f"{content_rev}:{plan_digest}:{nonce}"))


def part_vrid(gen: str, k: int) -> str:
    return str(uuid.uuid5(_namespace(gen), str(k)))


def part_node_key(gen: str, k: int) -> str:
    return str(uuid.uuid5(_namespace(gen), f"{k}:node"))


def part_specs(gen: str, ranges: Iterable[tuple[int, int, int]], *, ocr: bool | None) -> tuple[PartSpec, ...]:
    """Parts for ``(page_start, page_end, overlap_end)`` ranges, numbered from 1."""
    return tuple(
        PartSpec(k, start, end, overlap, part_vrid(gen, k), part_node_key(gen, k), ocr)
        for k, (start, end, overlap) in enumerate(ranges, start=1)
    )


def _namespace(name: str) -> uuid.UUID:
    try:
        return uuid.UUID(name)
    except ValueError:
        return uuid.uuid5(uuid.NAMESPACE_URL, name)


class SplitPage(list):
    """The rows of one page read, and where the read ended: ``last_key`` is the last stored key whether or not the
    row could be read, ``raw_count`` how many were stored (a page is full when it equals the limit), ``malformed``
    the keys of the rows that could not be read."""

    def __init__(self, rows: Iterable[SplitRow] = (), *, raw_count: int = 0, last_key: str | None = None, malformed: Iterable[str] = ()) -> None:
        super().__init__(rows)
        self.raw_count = raw_count
        self.last_key = last_key
        self.malformed = list(malformed)

    def next_cursor(self, limit: int) -> str | None:
        """The key to read after, or None once the last page was read."""
        return self.last_key if self.raw_count >= limit else None


_MOVE_TRIES = 4


class SplitStore:
    """Reads and compare-and-set writes of the ``contentSplits`` collection."""

    def __init__(self, graph: IGraphDBProvider, *, clock_ms: Callable[[], int] | None = None) -> None:
        self._graph = graph
        self._clock_ms = clock_ms or _now_ms

    async def get(self, v: str) -> SplitRow | None:
        doc = await self._graph.get_document(v, SPLITS, raise_on_error=True)
        return SplitRow.from_doc(doc) if doc else None

    async def get_many(self, vs: Iterable[str]) -> dict[str, dict[str, Any]]:
        """The raw ``contentSplits`` documents of *vs* by id, in ONE batched read (absent ids are not in the result).
        Raises when the read fails: the caller decides what an unreadable page shows."""
        ids = sorted({v for v in vs if v})
        if not ids:
            return {}
        docs = await self._graph.get_nodes_by_field_in(
            SPLITS, "id", ids, ["id", "gen", "state", "plan", "done", "failed"], raise_on_error=True,
        )
        return {str(d["id"]): d for d in docs or [] if d.get("id")}

    async def create(self, row: SplitRow) -> bool:
        """True when this call created the row; False when one is already there."""
        doc = {**row.to_doc(), "updatedAtMs": self._clock_ms()}
        return await self._graph.create_node_if_absent(row.v, SPLITS, doc)

    async def move(
        self, v: str, gen: str, expected: SplitState | tuple[SplitState, ...], new: SplitState, **fields: object,
    ) -> bool:
        """Set the state while the row still holds ``gen`` and one of the *expected* states. Every state change takes
        ``epoch + 1`` in the same write (compare-and-set on the epoch it read), so a roll-up that read the row before
        the move can no longer write over it."""
        wanted = {s.value for s in ((expected,) if isinstance(expected, SplitState) else expected)}
        for _ in range(_MOVE_TRIES):
            doc = await self._graph.get_document(v, SPLITS, raise_on_error=True)
            if not doc or doc.get("gen") != gen or doc.get("state") not in wanted:
                return False
            epoch = doc.get("epoch")
            applied = await self._graph.update_node_fields_if_match(
                v, SPLITS, {"state": new.value, "epoch": int(epoch or 0) + 1, "updatedAtMs": self._clock_ms(), **fields},
                {"gen": gen, "state": doc["state"], "epoch": epoch},
            )
            if applied:
                return True
        return False

    async def touch(self, v: str, gen: str) -> bool:
        return await self._graph.update_node_fields_if_match(
            v, SPLITS, {"updatedAtMs": self._clock_ms()}, {"gen": gen},
        )

    async def roll_up(self, row: SplitRow, *, done: int, failed: int, state: SplitState, **fields: object) -> bool:
        """The roll-up's one write: counters and state with ``epoch + 1``, only while the row still holds
        the generation and epoch it was read at. False means another roll-up wrote first. *fields* ride
        in the same write (the parent's drive token)."""
        return await self._graph.update_node_fields_if_match(
            row.v, SPLITS,
            {
                "done": done, "failed": failed, "state": state.value, "epoch": row.epoch + 1,
                "updatedAtMs": self._clock_ms(), **fields,
            },
            {"gen": row.gen, "epoch": row.epoch, "state": row.state.value},
        )

    async def part_statuses(self, row: SplitRow) -> dict[str, str | None]:
        """``indexingStatus`` of each part record of the row's generation, by part vrid (a part with no
        record yet is absent). Raises on a store error, so an outage is never counted as a status."""
        docs = await self._graph.get_nodes_by_field_in(
            RECORDS, "virtualRecordId", [p.vrid for p in row.parts],
            return_fields=["virtualRecordId", "indexingStatus"], raise_on_error=True,
        )
        return {str(d["virtualRecordId"]): d.get("indexingStatus") for d in docs}

    async def delete(self, v: str, gen: str) -> bool:
        """Delete the row while it is still generation *gen* and ``PURGING``: a purge that lost its row to another purger and
        a new generation never deletes the new generation's row. True when it was deleted."""
        return await self._graph.delete_node_if_match(v, SPLITS, {"gen": gen, "state": SplitState.PURGING.value})

    async def page(
        self, states: list[SplitState], *, updated_before_ms: int | None = None, after_key: str | None = None,
        limit: int = 100, org_id: str | None = None,
    ) -> SplitPage:
        """A key-ordered page of rows (of one organisation when *org_id* is given, chosen in the query). A stored row
        this build cannot read (a shape a newer build wrote, or damage) is left out of the page and named in
        ``malformed``, so one such row never ends the read for every row behind it; the page still says where it
        ended and whether it was full."""
        docs = await self._graph.content_splits_page(
            [s.value for s in states], updated_before_ms=updated_before_ms, after_key=after_key, limit=limit,
            org_id=org_id,
        )
        rows: list[SplitRow] = []
        malformed: list[str] = []
        for doc in docs:
            try:
                rows.append(SplitRow.from_doc(doc))
            except (KeyError, ValueError, TypeError, AttributeError):
                key = str(doc.get("_key") or doc.get("id") or "")
                malformed.append(key)
                logger.warning("Split row %s cannot be read by this build; it is skipped", key, exc_info=True)
        last = str(docs[-1].get("_key") or docs[-1].get("id") or "") if docs else None
        return SplitPage(rows, raw_count=len(docs), last_key=last, malformed=malformed)

    async def put_parts(self, infos: Iterable[PartInfo]) -> None:
        """Write the part documents (created when absent; an existing one is never changed)."""
        infos = list(infos)
        docs = [i.to_doc() for i in infos]
        if docs:
            await self._graph.ensure_nodes(docs, SPLIT_PARTS)
        for info in infos:
            remember_part(info)

    async def part(self, key: str) -> PartInfo | None:
        doc = await self._graph.get_document(key, SPLIT_PARTS, raise_on_error=True)
        return PartInfo.from_doc({**doc, "_key": key}) if doc else None

    async def parts_of(self, v: str, org_id: str, gen: str | None = None) -> list[PartInfo]:
        filters: dict[str, Any] = {"rootVirtualRecordId": v, "orgId": org_id}
        if gen:
            filters["gen"] = gen
        docs = await self._graph.get_nodes_by_filters(SPLIT_PARTS, filters, raise_on_error=True)
        return [PartInfo.from_doc(d) for d in docs]

    async def parts_page_by_owner(
        self, owner: str, org_id: str | None = None, *, after_key: str = "", limit: int = 100,
    ) -> list[PartInfo]:
        """A key-ordered page of the part documents of the files *owner* owns (of one organisation when given)."""
        filters: dict[str, Any] = {"ownerConnectorId": owner}
        if org_id:
            filters[ORG_FIELD] = org_id
        docs = await self._graph.get_documents_paginated(
            SPLIT_PARTS, limit=limit, filters=filters, sort_field="_key",
            raise_on_error=True, after_key=after_key,
        )
        return [PartInfo.from_doc({**d, "_key": d.get("_key") or d.get("id")}) for d in docs]

    async def roots_of(self, part_vrids: Iterable[str]) -> dict[str, str]:
        """``{V_k: V}`` for the part content vrids among *part_vrids* that belong to their file's live generation
        (a part of a replaced or purging generation is left out). One read of the part documents by their indexed
        ``virtualRecordId`` and one of the rows of the files found."""
        wanted = sorted({v for v in part_vrids if v})
        if not wanted:
            return {}
        docs = await self._graph.get_nodes_by_field_in(SPLIT_PARTS, "virtualRecordId", wanted, raise_on_error=True)
        infos = [PartInfo.from_doc({**d, "_key": d.get("_key") or d.get("id")}) for d in docs]
        rows = await self.get_many({i.root for i in infos})
        return {
            i.vrid: i.root for i in infos
            if (row := rows.get(i.root)) is not None and row.get("gen") == i.gen and row.get("state") != SplitState.PURGING.value
        }

    async def block_counts(self, live_gens: Mapping[str, str]) -> dict[str, dict[int, int | None]]:
        """``{V: {k: blockCount or None}}`` of the live generation of each file in *live_gens* (``{V: gen}``), in ONE
        read of the part documents by root. A part whose count is not written yet maps to ``None``; a file with no
        part documents is absent."""
        if not live_gens:
            return {}
        docs = await self._graph.get_nodes_by_field_in(
            SPLIT_PARTS, "rootVirtualRecordId", sorted(live_gens), ["rootVirtualRecordId", "gen", "k", "blockCount"],
            raise_on_error=True,
        )
        out: dict[str, dict[int, int | None]] = {}
        for d in docs or []:
            root = str(d.get("rootVirtualRecordId") or "")
            if root and live_gens.get(root) == str(d.get("gen")):
                out.setdefault(root, {})[int(d["k"])] = _count_of(d.get("blockCount"))
        return out

    async def set_block_count(self, key: str, gen: str, count: int) -> bool:
        """Record the part's block count (only while the document still belongs to generation *gen*)."""
        return await self._graph.update_node_fields_if_match(key, SPLIT_PARTS, {"blockCount": int(count)}, {"gen": gen})

    async def rehome_parts(self, v: str, org_id: str, gen: str, owner: str) -> int:
        """Name *owner* as the owner connector of the parts of *v*'s generation *gen* (the hint is re-checked at use
        time, see ``split_owner``). Returns how many part documents changed; each changes only while it still
        belongs to the generation."""
        changed = 0
        for info in await self.parts_of(v, org_id, gen):
            if info.owner_connector_id != owner and await self._graph.update_node_fields_if_match(
                info.key, SPLIT_PARTS, {"ownerConnectorId": owner}, {"gen": gen},
            ):
                changed += 1
            _forget_part(info.key)
        return changed

    async def delete_part(self, key: str) -> None:
        await self._graph.delete_nodes([key], SPLIT_PARTS)
        _forget_part(key)


def _now_ms() -> int:
    return int(time.time() * 1000)
