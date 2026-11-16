"""World (S5be-5): the split roll-up of listings, record details and stats on a REAL graph store.

The unit tests use a FakeGraph whose ``get_nodes_by_field_in("records", "id", ...)`` answers ``id`` as the key;
this world runs the same product functions (file_split_display) against a real provider, per store.
"""
from __future__ import annotations

import uuid
from typing import Any

from app.config.constants.arangodb import CollectionNames, Connectors, OriginTypes
from app.models.entities import FileRecord, RecordType
from app.modules.indexing import file_split_display as fsd
from app.modules.indexing.split_state import SplitRow, SplitState, part_specs

SPLITS = CollectionNames.CONTENT_SPLITS.value
RECORDS = CollectionNames.RECORDS.value
NOW = 1_700_000_000_000
STATES = ["SPLITTING", "INDEXING_PARTS", "COMPLETED", "PARTIAL", "FAILED", "PURGING"]


def _file(record_id: str, org: str, connector_id: str) -> FileRecord:
    return FileRecord(
        id=record_id, org_id=org, record_name="big.pdf", record_type=RecordType.FILE, external_record_id=f"ext-{record_id}",
        version=0, origin=OriginTypes.CONNECTOR, connector_name=Connectors.GOOGLE_DRIVE, connector_id=connector_id,
        mime_type="application/pdf", is_file=True, extension="pdf", created_at=NOW, updated_at=NOW,
        source_created_at=NOW, source_updated_at=NOW,
    )


def _row(v: str, org: str, state: str, *, done: int, failed: int, total: int = 4) -> dict[str, Any]:
    parts = part_specs(f"g-{v}", [(k, k, 0) for k in range(1, total + 1)], ocr=False)
    doc = SplitRow(v=v, org_id=org, gen=f"g-{v}", parts=parts, state=SplitState(state), done=done, failed=failed,
                   epoch=1).to_doc()
    if not doc.get("redriveToken"):  # an unset token is not stored: this world never redrives
        doc.pop("redriveToken", None)
    return doc


class Counting:
    """Counts the reads the roll-up makes, passing everything through to the real provider."""

    def __init__(self, inner: Any) -> None:  # noqa: ANN401
        self._inner = inner
        self.calls: dict[str, int] = {}

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        async def wrapped(*a: Any, **kw: Any) -> Any:  # noqa: ANN401
            self.calls[name] = self.calls.get(name, 0) + 1
            return await attr(*a, **kw)
        return wrapped


async def seed(graph: Any, org: str, other_org: str, connector: str = "conn-c") -> dict[str, Any]:  # noqa: ANN401
    """One record per split state under *connector*, one unsplit record, one split file of another connector, a
    deleted split file, and a split row of another org. Every record's stored status is COMPLETED."""
    w: dict[str, Any] = {"rec": {}, "vrid": {}}
    names = [*STATES, "plain", "other_conn", "deleted"]
    recs = []
    for name in names:
        rid, v = str(uuid.uuid4()), str(uuid.uuid4())
        w["rec"][name], w["vrid"][name] = rid, v
        recs.append(_file(rid, org, "conn-d" if name == "other_conn" else connector))
    await graph.batch_upsert_records(recs)
    for name in names:
        upd = {"virtualRecordId": w["vrid"][name], "indexingStatus": "COMPLETED"}
        if name == "deleted":
            upd["isDeleted"] = True
        await graph.update_node(w["rec"][name], RECORDS, upd)
    counts = {"SPLITTING": (0, 0), "INDEXING_PARTS": (1, 0), "COMPLETED": (4, 0), "PARTIAL": (4, 1),
              "FAILED": (4, 4), "PURGING": (4, 0)}
    for state, (done, failed) in counts.items():
        assert await graph.create_node_if_absent(w["vrid"][state], SPLITS, _row(w["vrid"][state], org, state,
                                                                                 done=done, failed=failed))
    for name, state in (("other_conn", "PARTIAL"), ("deleted", "FAILED")):
        assert await graph.create_node_if_absent(w["vrid"][name], SPLITS, _row(w["vrid"][name], org, state, done=4, failed=1))
    w["other_org_v"] = str(uuid.uuid4())
    w["other_org"] = other_org
    return w


async def check_listing(graph: Any, w: dict[str, Any]) -> dict[str, Any]:  # noqa: ANN401
    rows = [{"id": w["rec"][n]} for n in [*STATES, "plain"]]
    counting = Counting(graph)
    await fsd.stamp_rows(rows, counting, surface="qa")
    by = {n: rows[i] for i, n in enumerate([*STATES, "plain"])}
    expect = {"SPLITTING": (0, 4, 0), "INDEXING_PARTS": (1, 4, 0), "COMPLETED": (4, 4, 0), "PARTIAL": (4, 4, 1),
              "FAILED": (4, 4, 4)}
    for state, (done, total, failed) in expect.items():
        assert by[state].get("splitState") == state, (state, by[state])
        assert (by[state]["splitDone"], by[state]["splitTotal"], by[state]["splitFailed"]) == (done, total, failed)
    assert "splitState" not in by["PURGING"] and "splitState" not in by["plain"]
    return counting.calls


async def check_details(graph: Any, w: dict[str, Any]) -> None:  # noqa: ANN401
    rec = {"id": w["rec"]["PARTIAL"], "virtualRecordId": w["vrid"]["PARTIAL"]}
    await fsd.stamp_record(rec, graph)
    assert rec["splitState"] == "PARTIAL" and rec["splitFailed"] == 1
    plain = {"id": w["rec"]["plain"], "virtualRecordId": w["vrid"]["plain"]}
    await fsd.stamp_record(plain, graph)
    assert "splitState" not in plain


async def check_stats(graph: Any, org: str, connector: str = "conn-c") -> dict[str, Any]:  # noqa: ANN401
    # 7 COMPLETED records of the connector are live (6 split states + plain); the deleted one is not counted.
    stats = {"stats": {"total": 7, "indexingStatus": {"COMPLETED": 7, "IN_PROGRESS": 0, "FAILED": 0}},
             "byRecordType": [{"recordType": "FILE", "total": 7,
                               "indexingStatus": {"COMPLETED": 7, "IN_PROGRESS": 0, "FAILED": 0}}]}
    await fsd.roll_up_stats(stats, graph, org, connector)
    s = stats["stats"]
    assert s["indexingStatus"] == {"COMPLETED": 3, "IN_PROGRESS": 2, "FAILED": 2}, s
    assert stats["byRecordType"][0]["indexingStatus"] == s["indexingStatus"]
    assert s["splitFiles"] == {"preparing": 1, "indexingParts": 1, "partial": 1, "failed": 1}, s
    assert s["total"] == 7
    return stats


async def page_cost(graph: Any, org: str, connector: str, n: int = 50) -> dict[str, int]:  # noqa: ANN401
    """Reads for a page of *n* unsplit records: the cost every listing page now pays."""
    recs = [_file(str(uuid.uuid4()), org, connector) for _ in range(n)]
    await graph.batch_upsert_records(recs)
    for r in recs:
        await graph.update_node(r.id, RECORDS, {"virtualRecordId": str(uuid.uuid4())})
    counting = Counting(graph)
    await fsd.stamp_rows([{"id": r.id} for r in recs], counting, surface="qa")
    return counting.calls


def assert_one_batched_read(calls: dict[str, int]) -> None:
    """A page costs two batched reads (the records' virtual ids, the split rows) and no point read per row."""
    assert calls == {"get_nodes_by_field_in": 2}, calls
