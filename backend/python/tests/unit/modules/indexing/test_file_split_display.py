"""The split roll-up of a file, projected at read time into listings, record details and stats."""
from __future__ import annotations

from typing import Any

import pytest

from app.modules.indexing import file_split_display as fsd
from app.modules.indexing.split_state import SPLITS, SplitRow, SplitState, part_specs
from tests.support.split_fakes import FakeGraph

ORG = "org-1"


class Graph(FakeGraph):
    """FakeGraph whose generic read answers ``id`` as the stored key and projects the asked fields."""

    def __init__(self) -> None:
        super().__init__()
        self.reads: list[tuple[str, str]] = []
        self.filtered: list[tuple[str, dict[str, Any]]] = []

    async def get_nodes_by_filters(self, collection: str, filters: dict[str, Any], *a: object, **kw: object) -> list[dict[str, Any]]:
        self.filtered.append((collection, dict(filters)))
        return await super().get_nodes_by_filters(collection, filters, *a, **kw)

    async def get_nodes_by_field_in(  # noqa: ANN201
        self, collection: str, field_name: str, field_values: list[Any], return_fields: list[str] | None = None,
        transaction: object = None, **_kw: object,
    ):
        self._maybe_fail("get_nodes_by_field_in")
        self.reads.append((collection, field_name))
        out = []
        for key, d in self._coll(collection).items():
            value = key if field_name == "id" else d.get(field_name)
            if value in set(field_values):
                doc = {"id": key, **d}
                out.append({f: doc.get(f) for f in return_fields} if return_fields else doc)
        return out


def _split(graph: Graph, v: str, state: str, done: int = 0, failed: int = 0, total: int = 4) -> None:
    parts = part_specs("g1", [(k, k, 0) for k in range(1, total + 1)], ocr=False)
    row = SplitRow(v=v, org_id=ORG, gen="g1", parts=parts, state=SplitState(state), done=done, failed=failed, epoch=1)
    graph.put(SPLITS, v, row.to_doc())


def _record(graph: Graph, key: str, vrid: str, **extra: object) -> None:
    graph.put("records", key, {"virtualRecordId": vrid, "connectorId": "kb-1", "orgId": ORG, "recordType": "FILE",
                               "indexingStatus": "COMPLETED", **extra})


@pytest.mark.asyncio
async def test_a_page_of_rows_gets_the_roll_up_of_its_split_files_in_one_batch() -> None:
    graph = Graph()
    _record(graph, "r1", "v1")
    _record(graph, "r2", "v2")
    _record(graph, "r3", "v3")
    _split(graph, "v1", "INDEXING_PARTS", done=1)
    _split(graph, "v2", "PARTIAL", done=4, failed=1)
    rows = [{"id": "r1"}, {"id": "r2"}, {"id": "r3"}]

    await fsd.stamp_rows(rows, graph, surface="t")

    assert rows[0] == {"id": "r1", "splitState": "INDEXING_PARTS", "splitDone": 1, "splitTotal": 4, "splitFailed": 0}
    assert rows[1]["splitState"] == "PARTIAL" and rows[1]["splitFailed"] == 1
    assert "splitState" not in rows[2]
    assert graph.reads == [("records", "id"), (SPLITS, "id")]


@pytest.mark.asyncio
async def test_a_file_being_purged_or_unreadable_shows_no_roll_up_and_does_not_fail_the_page() -> None:
    graph = Graph()
    _record(graph, "r1", "v1")
    _split(graph, "v1", "PURGING")
    rows = [{"id": "r1"}]
    await fsd.stamp_rows(rows, graph, surface="t")
    assert rows == [{"id": "r1"}]

    graph.fail_on.add("get_nodes_by_field_in")
    await fsd.stamp_rows(rows, graph, surface="t")
    assert rows == [{"id": "r1"}]


@pytest.mark.asyncio
async def test_record_details_get_the_roll_up_by_the_stored_virtual_id() -> None:
    graph = Graph()
    _split(graph, "v1", "FAILED", failed=4)
    record: dict[str, Any] = {"id": "r1", "recordName": "big.pdf"}
    await fsd.stamp_record(record, graph, vrid="v1")
    assert (record["splitState"], record["splitFailed"], record["splitTotal"]) == ("FAILED", 4, 4)

    other: dict[str, Any] = {"id": "r2"}
    await fsd.stamp_record(other, graph, vrid="unsplit")
    assert other == {"id": "r2"}


def _stats() -> dict[str, Any]:
    zero = {"COMPLETED": 5, "IN_PROGRESS": 0, "FAILED": 0}
    return {"stats": {"total": 5, "indexingStatus": dict(zero)},
            "byRecordType": [{"recordType": "FILE", "total": 5, "indexingStatus": dict(zero)}]}


@pytest.mark.asyncio
async def test_stats_count_a_split_file_by_its_roll_up_not_as_completed() -> None:
    graph = Graph()
    for i, state in enumerate(["INDEXING_PARTS", "SPLITTING", "PARTIAL", "FAILED"], start=1):
        _record(graph, f"r{i}", f"v{i}")
        _split(graph, f"v{i}", state)
    _record(graph, "r5", "v5")
    _split(graph, "v5", "COMPLETED")  # complete: stays Completed
    _record(graph, "r6", "v6", connectorId="other")
    _split(graph, "v6", "FAILED")  # another connector
    stats = _stats()

    await fsd.roll_up_stats(stats, graph, ORG, "kb-1")

    assert stats["stats"]["indexingStatus"] == {"COMPLETED": 1, "IN_PROGRESS": 2, "FAILED": 2}
    assert stats["byRecordType"][0]["indexingStatus"] == {"COMPLETED": 1, "IN_PROGRESS": 2, "FAILED": 2}
    assert stats["stats"]["total"] == 5
    assert stats["stats"]["splitFiles"] == {"preparing": 1, "indexingParts": 1, "partial": 1, "failed": 1}


@pytest.mark.asyncio
async def test_stats_without_split_files_are_untouched_and_a_failed_read_serves_them_as_stored() -> None:
    graph = Graph()
    stats = _stats()
    await fsd.roll_up_stats(stats, graph, ORG, "kb-1")
    assert stats == _stats()

    _record(graph, "r1", "v1")
    _split(graph, "v1", "FAILED")
    graph.fail_on.add("get_nodes_by_field_in")
    await fsd.roll_up_stats(stats, graph, ORG, "kb-1")
    assert stats == _stats()


@pytest.mark.asyncio
async def test_stats_ignore_another_orgs_splits_and_deleted_records() -> None:
    graph = Graph()
    _record(graph, "r1", "v1", isDeleted=True)
    _split(graph, "v1", "FAILED")
    _record(graph, "r2", "v2")
    graph.put(SPLITS, "v2", {**graph.docs[SPLITS]["v1"], "orgId": "org-x"})
    stats = _stats()
    await fsd.roll_up_stats(stats, graph, ORG, "kb-1")
    assert stats == _stats()


@pytest.mark.asyncio
async def test_the_stats_read_of_open_splits_is_scoped_to_the_organisation_in_the_query() -> None:
    """Review finding: every organisation's open rows were read and filtered in Python, on every stats poll."""
    graph = Graph()
    for n in range(20):
        graph.put(SPLITS, f"other-{n}", {"orgId": "org-x", "state": "PARTIAL", "gen": "g", "plan": [], "total": 1, "done": 0, "failed": 1})
    _record(graph, "r1", "v1")
    _split(graph, "v1", "FAILED")
    stats = _stats()

    await fsd.roll_up_stats(stats, graph, ORG, "kb-1")

    reads = [f for c, f in graph.filtered if c == SPLITS]
    assert reads and all(f["orgId"] == ORG and "state" in f for f in reads), "no read of the collection without the organisation"
    assert {f["state"] for f in reads} == {"SPLITTING", "INDEXING_PARTS", "PARTIAL", "FAILED"}
    assert stats["stats"]["splitFiles"] == {"preparing": 0, "indexingParts": 0, "partial": 0, "failed": 1}


class CountingGraph(Graph):
    """Counts point reads: a page must never cost one ``get_document`` per row."""

    def __init__(self) -> None:
        super().__init__()
        self.point_reads = 0

    async def get_document(self, key: str, collection: str, **kw: object):  # noqa: ANN201
        self.point_reads += 1
        return await super().get_document(key, collection, **kw)


@pytest.mark.asyncio
async def test_a_page_costs_one_roll_up_read_whatever_its_size() -> None:
    graph = CountingGraph()
    for i in range(50):
        _record(graph, f"r{i}", f"v{i}")
        if i % 2:
            _split(graph, f"v{i}", "INDEXING_PARTS", done=1)
    rows = [{"id": f"r{i}"} for i in range(50)]

    await fsd.stamp_rows(rows, graph, surface="t")

    assert graph.point_reads == 0
    assert graph.reads == [("records", "id"), (SPLITS, "id")]
    assert sum("splitState" in r for r in rows) == 25


@pytest.mark.asyncio
async def test_one_unreadable_split_row_degrades_only_its_own_file() -> None:
    graph = Graph()
    for i in (1, 2, 3, 4):
        _record(graph, f"r{i}", f"v{i}")
    _split(graph, "v1", "INDEXING_PARTS", done=1)
    graph.put(SPLITS, "v2", {**graph.docs[SPLITS]["v1"], "state": "NOT_A_STATE"})
    graph.put(SPLITS, "v3", {k: v for k, v in graph.docs[SPLITS]["v1"].items() if k != "state"})
    graph.put(SPLITS, "v4", {**graph.docs[SPLITS]["v1"], "plan": "{not json"})
    _split(graph, "v5", "PARTIAL", done=4, failed=1)
    _record(graph, "r5", "v5")
    rows = [{"id": f"r{i}"} for i in (1, 2, 3, 4, 5)]

    await fsd.stamp_rows(rows, graph, surface="t")

    assert [r.get("splitState") for r in rows] == ["INDEXING_PARTS", None, None, None, "PARTIAL"]


class _MissingCollection(Graph):
    """ArangoDB before any process has created ``contentSplits``: the read fails with error 1203."""

    async def get_nodes_by_field_in(self, collection: str, *a: object, **kw: object) -> list[dict[str, Any]]:
        if collection == SPLITS:
            raise RuntimeError("404 collection or view not found: contentSplits (1203)")
        return await super().get_nodes_by_field_in(collection, *a, **kw)

    async def get_nodes_by_filters(self, collection: str, *a: object, **kw: object) -> list[dict[str, Any]]:
        if collection == SPLITS:
            raise RuntimeError("404 collection or view not found: contentSplits (1203)")
        return await super().get_nodes_by_filters(collection, *a, **kw)


@pytest.mark.asyncio
async def test_a_split_collection_that_does_not_exist_yet_is_no_split_rows_quietly(caplog: pytest.LogCaptureFixture) -> None:
    """Upgrade window: query pods run before the indexing pods have created the collection. Every listing must serve
    without a warning, a traceback or a failure count, and count the missing store in a metric of its own."""
    import logging

    graph = _MissingCollection()
    _record(graph, "r1", "v1")
    rows = [{"id": "r1"}]
    stats = _stats()
    failures, missing = [], []
    original = (fsd.DISPLAY_READ_FAILURES, fsd.SPLIT_STORE_MISSING)
    fsd.DISPLAY_READ_FAILURES = type("C", (), {"inc": staticmethod(lambda s: failures.append(s))})()
    fsd.SPLIT_STORE_MISSING = type("C", (), {"inc": staticmethod(lambda s: missing.append(s))})()
    try:
        with caplog.at_level(logging.DEBUG, logger=fsd.logger.name):
            await fsd.stamp_rows(rows, graph, surface="list")
            await fsd.stamp_record({"id": "r1"}, graph, vrid="v1")
            await fsd.roll_up_stats(stats, graph, ORG, "kb-1")
    finally:
        fsd.DISPLAY_READ_FAILURES, fsd.SPLIT_STORE_MISSING = original

    assert rows == [{"id": "r1"}] and stats == _stats()
    assert failures == [], "a collection that is not there yet is not a failure"
    assert missing == ["list", "record", "stats"]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING or r.exc_info]


@pytest.mark.asyncio
async def test_any_other_read_failure_is_still_a_warning_and_a_failure_count(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    graph = Graph()
    _record(graph, "r1", "v1")
    graph.fail_on.add("get_nodes_by_field_in")
    with caplog.at_level(logging.DEBUG, logger=fsd.logger.name):
        await fsd.stamp_rows([{"id": "r1"}], graph, surface="list")
    assert [r for r in caplog.records if r.levelno == logging.WARNING]
