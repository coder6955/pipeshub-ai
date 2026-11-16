"""The split bookkeeping of a large PDF on a real Neo4j and a real ArangoDB.

Needs the graph stack of deployment/docker-compose/docker-compose.integration.graph-db.yml (NEO4J_IT_URI,
NEO4J_IT_PASSWORD, ARANGO_IT_URL, ARANGO_IT_PASSWORD; the defaults match it), as the other ``*_real_backends`` suites do.

What it pins, per store: a ``contentSplits`` row (with its plan array) round-trips and is changed only by a
compare-and-set; a part is an ordinary record (the closed record schema refuses any split field) with its own
``contentSplitParts`` document, keyed by the record, found by root, by root and generation and by owner connector;
the sweep's page of rows reads across organisations; the indexes (on the new label/collection, none on records) exist
after ``ensure_pipeline_schema``; and the parent's display is projected from the row on read, never stored.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING

import pytest

from app.config.constants.arangodb import (
    CollectionNames,
    Connectors,
    OriginTypes,
    ProgressStatus,
)
from app.models.entities import FileRecord, Record, RecordType
from app.modules.indexing.owner_connector import owner_connector_id
from app.modules.indexing.split_parts import part_file_record
from app.modules.indexing.split_state import (
    PartInfo,
    PartSpec,
    SplitStore,
    clear_part_cache,
    display_for,
    is_part,
    prime_parts,
)
from app.services.graph_db.arango.arango_http_provider import ArangoHTTPProvider
from app.services.graph_db.neo4j.neo4j_provider import Neo4jProvider
from tests.integration.real_graph import (
    backend_unavailable,
    connect_arango,
    connect_neo4j,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.timeout(300)]

SPLITS = CollectionNames.CONTENT_SPLITS.value
SPLIT_PARTS = CollectionNames.CONTENT_SPLIT_PARTS.value
RECORDS = CollectionNames.RECORDS.value
FILES = CollectionNames.FILES.value
logger = logging.getLogger("content-splits-it")
ORG = f"splits-it-{uuid.uuid4().hex[:8]}"
OTHER_ORG = f"splits-it-other-{uuid.uuid4().hex[:8]}"
NOW = 1_700_000_000_000


def _plan(vrid: str, parts: int = 2) -> list[dict]:
    return [
        {"k": k, "pageStart": 1 + (k - 1) * 600, "pageEnd": k * 600, "overlapEnd": k * 600 + 1, "vrid": f"{vrid}-{k}",
         "nodeKey": f"{vrid}-node-{k}", "ocr": k == 2, "lines": [[1, 40], [41, 80]]}
        for k in range(1, parts + 1)
    ]


def _row(vrid: str, *, org: str = ORG, state: str = "SPLITTING", updated: int = NOW) -> dict:
    return {
        "orgId": org, "gen": f"gen-{vrid}", "contentRev": "rev1", "planDigest": "d1", "nonce": "n1",
        "plan": _plan(vrid), "total": 2, "done": 0, "failed": 0, "state": state, "epoch": 0, "updatedAtMs": updated,
    }


def _part(root: str, gen: str, k: int, *, org: str = ORG) -> dict:
    """A part record: the least the strict record schema requires, plus the ordinary fields a part has. Nothing
    part-specific: that is in ``_part_doc``."""
    return {
        "id": str(uuid.uuid4()), "orgId": org, "recordName": f"big.pdf · pages {k}-{k}", "externalRecordId": f"part:{gen}:{k}",
        "recordType": RecordType.FILE.value, "origin": OriginTypes.UPLOAD.value, "createdAtTimestamp": NOW, "updatedAtTimestamp": NOW, "version": 0,
        "connectorId": f"pdfparts_{org}", "virtualRecordId": f"{root}-{k}", "mimeType": "application/pdf", "sizeInBytes": 10,
        "indexingStatus": ProgressStatus.NOT_STARTED.value, "parsingStatus": ProgressStatus.NOT_STARTED.value,
        "extractionStatus": ProgressStatus.NOT_STARTED.value, "isDeleted": False,
    }


def _part_info(part: dict, root: str, gen: str, k: int, *, owner: str = "conn-owner") -> PartInfo:
    return PartInfo(
        key=part["id"], org_id=part["orgId"], root=root, gen=gen, k=k, page_start=k, page_end=k, page_offset=k - 1,
        overlap_pages=0, owner_connector_id=owner, vrid=part["virtualRecordId"],
    )


@pytest.fixture(params=["neo4j", "arango"])
async def graph(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[IGraphDBProvider]:
    try:
        if request.param == "neo4j":
            provider = await connect_neo4j(logger, monkeypatch)
        else:
            provider = await connect_arango(logger, "content_splits_it")
    except Exception as error:
        backend_unavailable(request.param, error)
    yield provider
    for org in (ORG, OTHER_ORG):
        await _wipe(provider, org)
    await provider.disconnect()


async def _wipe(provider: IGraphDBProvider, org: str) -> None:
    for collection in (SPLITS, SPLIT_PARTS, RECORDS, FILES):
        rows = await provider.get_nodes_by_filters(collection, {"orgId": org}, ["_key"])
        if rows:
            await provider.delete_nodes([r["_key"] for r in rows], collection)


async def test_a_split_row_round_trips_and_is_changed_only_by_compare_and_set(graph: IGraphDBProvider) -> None:
    vrid = f"v-{uuid.uuid4().hex}"
    row = _row(vrid)
    assert await graph.create_node_if_absent(vrid, SPLITS, dict(row)) is True
    assert await graph.create_node_if_absent(vrid, SPLITS, {**row, "gen": "other"}) is False

    stored = await graph.get_document(vrid, SPLITS)
    assert stored is not None and stored["gen"] == f"gen-{vrid}" and stored["state"] == "SPLITTING"
    assert stored["plan"] == row["plan"], "the plan array comes back as it went in, on a store with no list-of-map property"

    assert await graph.update_node_fields_if_match(
        vrid, SPLITS, {"state": "INDEXING_PARTS", "updatedAtMs": NOW + 1}, {"gen": f"gen-{vrid}", "state": "SPLITTING"},
    ) is True
    assert await graph.update_node_fields_if_match(
        vrid, SPLITS, {"state": "FAILED"}, {"gen": f"gen-{vrid}", "state": "SPLITTING"},
    ) is False, "a stale state loses"
    assert await graph.update_node_fields_if_match(
        vrid, SPLITS, {"state": "FAILED"}, {"gen": "a-newer-generation", "state": "INDEXING_PARTS"},
    ) is False, "a stale generation loses"
    assert (await graph.get_document(vrid, SPLITS))["state"] == "INDEXING_PARTS"

    assert await graph.delete_nodes([vrid], SPLITS)
    assert await graph.get_document(vrid, SPLITS) is None


async def state_fences(graph: IGraphDBProvider, org: str) -> None:
    """Every state move takes the next epoch, a roll-up that read before a move cannot write over it, and the final delete
    is conditional on the generation and ``PURGING`` (shared with the PostgreSQL test, which runs it in its organisation)."""
    from app.modules.indexing.split_state import SplitState

    vrid = f"v-{uuid.uuid4().hex}"
    gen = f"gen-{vrid}"
    assert await graph.create_node_if_absent(vrid, SPLITS, {**_row(vrid, org=org)}) is True
    store = SplitStore(graph)
    stale = await store.get(vrid)
    assert stale is not None and stale.epoch == 0

    assert await store.move(vrid, gen, SplitState.SPLITTING, SplitState.INDEXING_PARTS) is True
    assert (await graph.get_document(vrid, SPLITS))["epoch"] == 1, "a state move takes the next epoch"
    assert await store.move(vrid, gen, SplitState.SPLITTING, SplitState.PURGING) is False, "the state it expected is gone"
    assert await store.roll_up(stale, done=2, failed=0, state=SplitState.COMPLETED) is False, "a roll-up that read before the move loses"

    assert await store.move(vrid, gen, (SplitState.SPLITTING, SplitState.INDEXING_PARTS), SplitState.PURGING) is True
    assert (await graph.get_document(vrid, SPLITS))["epoch"] == 2
    purging = await store.get(vrid)
    assert purging is not None and purging.state is SplitState.PURGING
    # the delete is conditional on the generation and on PURGING
    assert await store.delete(vrid, "a-newer-generation") is False
    await graph.update_node_fields_if_match(vrid, SPLITS, {"state": "COMPLETED"}, {"gen": gen})
    assert await store.delete(vrid, gen) is False, "a row that is not PURGING is not this purge's to delete"
    assert await graph.get_document(vrid, SPLITS) is not None
    await graph.update_node_fields_if_match(vrid, SPLITS, {"state": "PURGING"}, {"gen": gen})
    with pytest.raises(ValueError):
        await graph.delete_node_if_match(vrid, SPLITS, {})
    assert await store.delete(vrid, gen) is True
    assert await graph.get_document(vrid, SPLITS) is None
    assert await store.delete(vrid, gen) is False


async def test_state_moves_take_the_epoch_and_the_final_delete_is_conditional(graph: IGraphDBProvider) -> None:
    await state_fences(graph, ORG)


async def test_a_state_outside_the_closed_set_is_refused(graph: IGraphDBProvider) -> None:
    with pytest.raises(Exception):  # noqa: B017  # the store's own error type differs
        await graph.create_node_if_absent("bad", SPLITS, {**_row("bad"), "state": "WHATEVER"})
    assert await graph.get_document("bad", SPLITS) is None


async def test_a_part_is_an_ordinary_record_with_a_part_document_found_by_root_generation_and_owner(
    graph: IGraphDBProvider,
) -> None:
    root, other = f"root-{uuid.uuid4().hex}", f"root-{uuid.uuid4().hex}"
    parts = [_part(root, "g1", 1), _part(root, "g1", 2), _part(root, "g0", 1), _part(other, "g9", 1)]
    owners = ["conn-owner", "conn-owner", "conn-owner", "conn-other"]
    roots = [(root, "g1", 1), (root, "g1", 2), (root, "g0", 1), (other, "g9", 1)]
    assert await graph.batch_upsert_nodes(parts, RECORDS), "the closed record schema accepts a part"
    files = [
        {"id": p["id"], "orgId": ORG, "name": p["recordName"], "isFile": True, "extension": "pdf", "mimeType": "application/pdf",
         "sizeInBytes": 10}
        for p in parts
    ]
    assert await graph.batch_upsert_nodes(files, FILES)
    store = SplitStore(graph)
    infos = [_part_info(p, r, g, k, owner=o) for p, (r, g, k), o in zip(parts, roots, owners, strict=True)]
    await store.put_parts(infos)
    await store.put_parts(infos)  # idempotent: the second write creates nothing and changes nothing

    assert sorted((i.gen, i.k) for i in await store.parts_of(root, ORG)) == [("g0", 1), ("g1", 1), ("g1", 2)]
    mine = await store.parts_of(root, ORG, "g1")
    assert sorted(i.k for i in mine) == [1, 2]
    assert all(i.owner_connector_id == "conn-owner" and i.page_offset == i.k - 1 for i in mine)
    assert await store.parts_of(root, "another-org") == []
    assert (await store.part(parts[0]["id"])) == infos[0]
    assert await store.part("no-such-part") is None
    page = await store.parts_page_by_owner("conn-owner", ORG, limit=2)
    assert len(page) == 2
    rest = await store.parts_page_by_owner("conn-owner", ORG, after_key=page[-1].key, limit=2)
    assert sorted(i.key for i in [*page, *rest]) == sorted(p["id"] for p in parts[:3])
    assert [i.key for i in await store.parts_page_by_owner("conn-other", ORG)] == [parts[3]["id"]]

    # What the indexing handler reads the record through, and the one cached reader over the real store.
    records = await graph.get_nodes_by_filters(RECORDS, {"connectorId": f"pdfparts_{ORG}", "orgId": ORG})
    assert {Record.from_arango_base_record(r).connector_id for r in records} == {f"pdfparts_{ORG}"}
    clear_part_cache()
    assert is_part(records[0])
    await prime_parts(records, graph)
    assert {owner_connector_id(r) for r in records} == {"conn-owner", "conn-other"}

    # reverse lookup: only the live generation of a file with a live row answers
    live = _row(root)
    assert await graph.create_node_if_absent(root, SPLITS, {**live, "gen": "g1", "state": "INDEXING_PARTS"})
    wanted = [p["virtualRecordId"] for p in parts] + ["not-a-part"]
    assert await store.roots_of(wanted) == {parts[0]["virtualRecordId"]: root, parts[1]["virtualRecordId"]: root}
    assert await store.roots_of([]) == {}
    await graph.update_node_fields_if_match(root, SPLITS, {"state": "PURGING"}, {"gen": "g1"})
    assert await store.roots_of(wanted) == {}

    await store.delete_part(parts[0]["id"])
    assert await store.part(parts[0]["id"]) is None


async def test_the_closed_schemas_refuse_a_split_field_on_a_record_and_a_stray_field_on_a_part_document(
    graph: IGraphDBProvider,
) -> None:
    root = f"root-{uuid.uuid4().hex}"
    for field_name, value in (("rootVirtualRecordId", root), ("splitGen", "g"), ("ownerConnectorId", "c"), ("splitState", "X")):
        with pytest.raises(Exception):  # noqa: B017  # the store's own error type differs
            await graph.batch_upsert_nodes([{**_part(root, "g1", 1), field_name: value}], RECORDS)
    part = _part_info(_part(root, "g1", 1), root, "g1", 1)
    for bad in ({"k": 0}, {"stray": "x"}, {"pageOffset": -1}):
        with pytest.raises(Exception):  # noqa: B017
            await graph.batch_upsert_nodes([{**part.to_doc(), **bad}], SPLIT_PARTS)
    assert await graph.get_document(part.key, SPLIT_PARTS) is None


async def test_a_part_document_takes_its_block_count_only_while_it_holds_the_generation(graph: IGraphDBProvider) -> None:
    """D-29: ``blockCount`` is a declared field of the part document (the schema stays closed otherwise), written
    by compare-and-set on the generation and read for a whole search in one batch."""
    root = f"root-{uuid.uuid4().hex}"
    parts = [_part(root, "g1", 1), _part(root, "g1", 2), _part(root, "g0", 1)]
    infos = [_part_info(p, root, g, k) for p, (g, k) in zip(parts, [("g1", 1), ("g1", 2), ("g0", 1)], strict=True)]
    store = SplitStore(graph)
    await store.put_parts(infos)

    assert await store.block_counts({root: "g1"}) == {root: {1: None, 2: None}}
    assert await store.set_block_count(infos[0].key, "g1", 120) is True
    assert await store.set_block_count(infos[1].key, "g1", 0) is True
    assert await store.set_block_count(infos[2].key, "g1", 7) is False, "a part of another generation is not written"
    assert await store.block_counts({root: "g1"}) == {root: {1: 120, 2: 0}}
    assert await store.block_counts({root: "g0"}) == {root: {1: None}}
    assert await store.block_counts({}) == {}
    assert (await store.part(infos[0].key)).block_count == 120  # type: ignore[union-attr]

    with pytest.raises(Exception):  # noqa: B017  # the declared field is typed: a count is a non-negative integer
        await graph.batch_upsert_nodes([{**infos[0].to_doc(), "blockCount": -1}], SPLIT_PARTS)


async def test_the_page_of_split_rows_reads_across_organisations(graph: IGraphDBProvider) -> None:
    tag = uuid.uuid4().hex[:6]
    keys = [f"{tag}-{c}" for c in "abcde"]
    rows = [
        (keys[0], _row(keys[0], state="SPLITTING", updated=100)),
        (keys[1], _row(keys[1], state="PURGING", updated=200, org=OTHER_ORG)),
        (keys[2], _row(keys[2], state="COMPLETED", updated=300)),
        (keys[3], _row(keys[3], state="SPLITTING", updated=400, org=OTHER_ORG)),
        (keys[4], _row(keys[4], state="SPLITTING", updated=500)),
    ]
    for key, row in rows:
        assert await graph.create_node_if_absent(key, SPLITS, dict(row))

    def mine(page: list[dict]) -> list[dict]:
        return [r for r in page if r["_key"] in keys]

    page = mine(await graph.content_splits_page(["SPLITTING", "PURGING"], limit=100))
    assert [r["_key"] for r in page] == [keys[0], keys[1], keys[3], keys[4]], "ordered by key, any org, only these states"
    assert {r["_key"]: r["orgId"] for r in page} == {keys[0]: ORG, keys[1]: OTHER_ORG, keys[3]: OTHER_ORG, keys[4]: ORG}
    assert all(isinstance(r["plan"], list) for r in page)

    old = mine(await graph.content_splits_page(["SPLITTING", "PURGING"], updated_before_ms=300, limit=100))
    assert [r["_key"] for r in old] == [keys[0], keys[1]]

    first = await graph.content_splits_page(["SPLITTING"], limit=1)
    assert len(first) == 1
    after = mine(await graph.content_splits_page(["SPLITTING"], after_key=keys[0], limit=100))
    assert [r["_key"] for r in after] == [keys[3], keys[4]]
    assert not [r for r in await graph.content_splits_page(["FAILED"], limit=100) if r["_key"] in keys]


async def test_ensure_pipeline_schema_creates_the_split_indexes(graph: IGraphDBProvider) -> None:
    await graph.ensure_pipeline_schema()
    if isinstance(graph, Neo4jProvider):
        rows = await graph.client.execute_query("SHOW INDEXES YIELD name, labelsOrTypes, properties RETURN name, labelsOrTypes, properties")
        have = {(r["labelsOrTypes"][0], tuple(r["properties"])) for r in rows if r["labelsOrTypes"]}
        constraints = await graph.client.execute_query("SHOW CONSTRAINTS YIELD labelsOrTypes, properties RETURN labelsOrTypes, properties")
        for label in ("ContentSplit", "ContentSplitPart"):
            assert any(c["labelsOrTypes"] == [label] and c["properties"] == ["id"] for c in constraints), label
    else:
        assert isinstance(graph, ArangoHTTPProvider)
        have = set()
        for collection, label in ((SPLITS, "ContentSplit"), (SPLIT_PARTS, "ContentSplitPart"), (RECORDS, "Record")):
            for index in await graph.http_client.get_indexes(collection):
                have.add((label, tuple(index["fields"])))
    # The stats read (one organisation's rows of a state) and the sweep (every organisation's rows of a state, by age).
    assert ("ContentSplit", ("orgId", "state")) in have
    assert ("ContentSplit", ("state", "updatedAtMs")) in have
    assert ("ContentSplitPart", ("rootVirtualRecordId",)) in have
    assert ("ContentSplitPart", ("rootVirtualRecordId", "gen")) in have
    assert ("ContentSplitPart", ("ownerConnectorId",)) in have
    assert ("ContentSplitPart", ("virtualRecordId",)) in have
    assert not [index for label, index in have if label == "Record" and any(f in ("rootVirtualRecordId", "splitGen") for f in index)], (
        "records get no index for a split: the record schema is closed"
    )
    assert await graph.records_accept_indexed_copy()


def _file_record(record_id: str, connector_id: str) -> FileRecord:
    return FileRecord(
        id=record_id, org_id=ORG, record_name="big.pdf", record_type=RecordType.FILE, external_record_id=f"ext-{record_id}",
        version=0, origin=OriginTypes.CONNECTOR, connector_name=Connectors.GOOGLE_DRIVE, connector_id=connector_id,
        mime_type="application/pdf", is_file=True, extension="pdf", created_at=NOW, updated_at=NOW,
        source_created_at=NOW, source_updated_at=NOW,
    )


async def test_a_split_part_record_written_by_the_seam_carries_its_virtual_record_id_and_no_split_field(
    graph: IGraphDBProvider,
) -> None:
    """The record the seam writes (one upsert, through the production writer) round-trips on the store."""
    spec = PartSpec(1, 1, 500, 501, "v-part-1", str(uuid.uuid4()))
    parent = {"orgId": ORG, "recordName": "big.pdf", "connectorId": "conn-owner"}
    record = part_file_record(spec, gen="g", parent=parent, storage_document_id="doc-1", size_bytes=7)
    await graph.batch_upsert_records([record])

    stored = await graph.get_document(spec.node_key, RECORDS)
    assert stored is not None
    assert (stored["virtualRecordId"], stored["connectorId"], stored["externalRecordId"]) == ("v-part-1", f"pdfparts_{ORG}", "part:g:1")
    assert stored["storageDocumentId"] == "doc-1"
    assert not set(stored) & {
        "rootVirtualRecordId", "splitGen", "segmentIndex", "segmentCount", "pageStart", "pageEnd", "pageOffset",
        "overlapPages", "ownerConnectorId", "splitState", "splitDone", "splitTotal", "splitFailed",
    }
    again = await graph.batch_upsert_records([record])  # a redelivery writes the same record
    assert again is None or again is True
    assert (await graph.get_document(spec.node_key, RECORDS))["virtualRecordId"] == "v-part-1"


async def test_the_display_of_a_split_is_read_from_its_row_and_the_parent_record_is_never_written(
    graph: IGraphDBProvider,
) -> None:
    record_id, root = str(uuid.uuid4()), f"root-{uuid.uuid4().hex}"
    await graph.batch_upsert_records([_file_record(record_id, "conn-owner")])
    before = await graph.get_document(record_id, RECORDS)
    row = {**_row(root, state="INDEXING_PARTS"), "done": 1, "failed": 1}
    assert await graph.create_node_if_absent(root, SPLITS, row)

    assert await display_for([root, "never-split"], graph) == {
        root: {"splitState": "INDEXING_PARTS", "splitDone": 1, "splitTotal": 2, "splitFailed": 1},
    }
    assert await graph.get_document(record_id, RECORDS) == before

    await asyncio.sleep(0)
