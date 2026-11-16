"""The split bookkeeping of a large PDF on PostgreSQL (Neo4j and ArangoDB: ../test_content_splits_real_backends.py).

Needs a migrated database: PG_CORE_URL (a `pipeshub_connectors_login` DSN), PG_POOLER=none.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from app.config.constants.arangodb import CollectionNames
from app.core.org_context import system
from app.modules.indexing.owner_connector import owner_connector_id
from app.modules.indexing.split_parts import part_file_record
from app.modules.indexing.split_state import (
    PartSpec,
    SplitStore,
    clear_part_cache,
    display_for,
    prime_parts,
)
from tests.integration.graph_db.test_content_splits_real_backends import (
    NOW,
    _file_record,
    _part,
    _part_info,
    _plan,
    _row,
    state_fences,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from app.models.entities import FileRecord

pytestmark = pytest.mark.integration

SPLITS = CollectionNames.CONTENT_SPLITS.value
SPLIT_PARTS = CollectionNames.CONTENT_SPLIT_PARTS.value
RECORDS = CollectionNames.RECORDS.value
FILES = CollectionNames.FILES.value


@pytest.fixture(autouse=True)
def _strict(monkeypatch: pytest.MonkeyPatch) -> None:
    # After the folder's conftest, which switches validation off: these tests are about what the schema accepts.
    monkeypatch.setenv("GRAPH_SCHEMA_ENFORCEMENT", "strict")


@pytest.fixture
async def orgs(pg_db, org_ctx) -> AsyncIterator[tuple[str, str]]:
    a, b = uuid.uuid4().hex[:24], uuid.uuid4().hex[:24]
    yield a, b
    for org in (a, b):
        with org_ctx(org):
            await pg_db.run(lambda tx, org=org: tx.execute("DELETE FROM graph.nodes WHERE org_id = $1", org), write=True)


def _in(org: str, doc: dict) -> dict:
    return {**doc, "orgId": org}


async def test_a_split_row_round_trips_and_is_changed_only_by_compare_and_set(pg_provider, orgs, org_ctx) -> None:
    org, _ = orgs
    vrid = f"v-{uuid.uuid4().hex}"
    row = _in(org, _row(vrid))
    with org_ctx(org):
        assert await pg_provider.create_node_if_absent(vrid, SPLITS, dict(row)) is True
        assert await pg_provider.create_node_if_absent(vrid, SPLITS, {**row, "gen": "other"}) is False
        stored = await pg_provider.get_document(vrid, SPLITS)
        assert stored is not None and stored["gen"] == f"gen-{vrid}" and stored["plan"] == row["plan"]
        assert await pg_provider.update_node_fields_if_match(
            vrid, SPLITS, {"state": "INDEXING_PARTS", "updatedAtMs": NOW + 1}, {"gen": f"gen-{vrid}", "state": "SPLITTING"}) is True
        assert await pg_provider.update_node_fields_if_match(
            vrid, SPLITS, {"state": "FAILED"}, {"gen": f"gen-{vrid}", "state": "SPLITTING"}) is False
        assert await pg_provider.update_node_fields_if_match(
            vrid, SPLITS, {"state": "FAILED"}, {"gen": "a-newer-generation", "state": "INDEXING_PARTS"}) is False
        assert (await pg_provider.get_document(vrid, SPLITS))["state"] == "INDEXING_PARTS"
        assert await pg_provider.delete_nodes([vrid], SPLITS)
        assert await pg_provider.get_document(vrid, SPLITS) is None
        with pytest.raises(Exception):  # noqa: B017
            await pg_provider.create_node_if_absent("bad", SPLITS, {**row, "state": "WHATEVER"})


async def test_state_moves_take_the_epoch_and_the_final_delete_is_conditional(pg_provider, orgs, org_ctx) -> None:
    org, _ = orgs
    with org_ctx(org):
        await state_fences(pg_provider, org)


async def test_a_part_is_an_ordinary_record_with_a_part_document_found_by_root_generation_and_owner(pg_provider, orgs, org_ctx) -> None:
    org, _ = orgs
    root, other = f"root-{uuid.uuid4().hex}", f"root-{uuid.uuid4().hex}"
    parts = [_in(org, p) for p in (_part(root, "g1", 1, org=org), _part(root, "g1", 2, org=org), _part(root, "g0", 1, org=org), _part(other, "g9", 1, org=org))]
    owners = ["conn-owner", "conn-owner", "conn-owner", "conn-other"]
    roots = [(root, "g1", 1), (root, "g1", 2), (root, "g0", 1), (other, "g9", 1)]
    files = [{"id": p["id"], "orgId": org, "name": p["recordName"], "isFile": True, "extension": "pdf", "mimeType": "application/pdf",
              "sizeInBytes": 10} for p in parts]
    with org_ctx(org):
        assert await pg_provider.batch_upsert_nodes(parts, RECORDS)
        assert await pg_provider.batch_upsert_nodes(files, FILES)
        store = SplitStore(pg_provider)
        infos = [_part_info(p, r, g, k, owner=o) for p, (r, g, k), o in zip(parts, roots, owners, strict=True)]
        await store.put_parts(infos)
        await store.put_parts(infos)
        assert sorted((i.gen, i.k) for i in await store.parts_of(root, org)) == [("g0", 1), ("g1", 1), ("g1", 2)]
        mine = await store.parts_of(root, org, "g1")
        assert sorted(i.k for i in mine) == [1, 2]
        assert all(i.owner_connector_id == "conn-owner" and i.page_offset == i.k - 1 for i in mine)
        assert (await store.part(parts[0]["id"])) == infos[0]
        assert await store.part("no-such-part") is None
        page = await store.parts_page_by_owner("conn-owner", org, limit=2)
        rest = await store.parts_page_by_owner("conn-owner", org, after_key=page[-1].key, limit=2)
        assert sorted(i.key for i in [*page, *rest]) == sorted(p["id"] for p in parts[:3])
        assert [i.key for i in await store.parts_page_by_owner("conn-other", org)] == [parts[3]["id"]]
        records = await pg_provider.get_nodes_by_filters(RECORDS, {"connectorId": f"pdfparts_{org}"})
        clear_part_cache()
        await prime_parts(records, pg_provider)
        assert {owner_connector_id(r) for r in records} == {"conn-owner", "conn-other"}
        assert await pg_provider.create_node_if_absent(root, SPLITS, _in(org, {**_row(root), "gen": "g1", "state": "INDEXING_PARTS"}))
        wanted = [p["virtualRecordId"] for p in parts] + ["not-a-part"]
        assert await store.roots_of(wanted) == {parts[0]["virtualRecordId"]: root, parts[1]["virtualRecordId"]: root}
        assert await store.roots_of([]) == {}
        await pg_provider.update_node_fields_if_match(root, SPLITS, {"state": "PURGING"}, {"gen": "g1"})
        assert await store.roots_of(wanted) == {}
        await store.delete_part(parts[0]["id"])
        assert await store.part(parts[0]["id"]) is None


async def test_the_closed_schemas_refuse_a_split_field_on_a_record_and_a_stray_field_on_a_part_document(pg_provider, orgs, org_ctx) -> None:
    org, _ = orgs
    root = f"root-{uuid.uuid4().hex}"
    with org_ctx(org):
        for field_name, value in (("rootVirtualRecordId", root), ("splitGen", "g"), ("ownerConnectorId", "c"), ("splitState", "X")):
            with pytest.raises(Exception):  # noqa: B017
                await pg_provider.batch_upsert_nodes([_in(org, {**_part(root, "g1", 1, org=org), field_name: value})], RECORDS)
        part = _part_info(_in(org, _part(root, "g1", 1, org=org)), root, "g1", 1)
        for bad in ({"k": 0}, {"stray": "x"}, {"pageOffset": -1}):
            with pytest.raises(Exception):  # noqa: B017
                await pg_provider.batch_upsert_nodes([{**part.to_doc(), **bad}], SPLIT_PARTS)


async def test_the_page_of_split_rows_reads_across_organisations(pg_provider, orgs, org_ctx, monkeypatch) -> None:
    a, b = orgs

    real = pg_provider._sweep_orgs

    async def these_orgs() -> list[str | None]:      # the real system listing, cut to this test's orgs: the shared database holds other tests' rows
        return [o for o in await real() if o is None or o in (a, b)]

    monkeypatch.setattr(pg_provider, "_sweep_orgs", these_orgs)
    for org in (a, b):
        with org_ctx(org):
            await pg_provider.batch_upsert_nodes([{"_key": org, "name": "o", "isActive": True, "accountType": "enterprise"}], "organizations")
    tag = uuid.uuid4().hex[:6]
    keys = [f"{tag}-{c}" for c in "abcde"]
    plan = [
        (keys[0], a, "SPLITTING", 100), (keys[1], b, "PURGING", 200), (keys[2], a, "COMPLETED", 300),
        (keys[3], b, "SPLITTING", 400), (keys[4], a, "SPLITTING", 500),
    ]
    for key, org, state, updated in plan:
        with org_ctx(org):
            assert await pg_provider.create_node_if_absent(key, SPLITS, _in(org, _row(key, state=state, updated=updated)))

    def mine(page: list[dict]) -> list[dict]:
        return [r for r in page if r["_key"] in keys]

    with system("test"):
        page = mine(await pg_provider.content_splits_page(["SPLITTING", "PURGING"], limit=1000))
        assert [r["_key"] for r in page] == [keys[0], keys[1], keys[3], keys[4]]
        assert {r["_key"]: r["orgId"] for r in page} == {keys[0]: a, keys[1]: b, keys[3]: b, keys[4]: a}
        assert all(isinstance(r["plan"], list) for r in page)
        old = mine(await pg_provider.content_splits_page(["SPLITTING", "PURGING"], updated_before_ms=300, limit=1000))
        assert [r["_key"] for r in old] == [keys[0], keys[1]]
        after = mine(await pg_provider.content_splits_page(["SPLITTING"], after_key=keys[0], limit=1000))
        assert [r["_key"] for r in after] == [keys[3], keys[4]]
        assert len(await pg_provider.content_splits_page(["SPLITTING"], limit=1)) == 1
    with org_ctx(a):
        own = mine(await pg_provider.content_splits_page(["SPLITTING", "PURGING"], limit=1000))
        assert {r["orgId"] for r in own} == {a}, "inside one org the page is that org's rows"


async def test_the_split_indexes_exist_on_the_graph(pg_db, orgs, org_ctx) -> None:
    org, _ = orgs
    wanted = ["nodes_content_split_state", "nodes_content_split_part_root", "nodes_content_split_part_root_gen", "nodes_content_split_part_owner", "nodes_content_split_part_vrid"]
    with org_ctx(org):
        names = await pg_db.run(lambda tx: tx.fetch(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'graph' AND tablename = 'nodes' AND indexname = ANY($1::text[])", wanted))
        on_records = await pg_db.run(lambda tx: tx.fetch(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'graph' AND tablename = 'nodes' AND indexdef ILIKE '%rootVirtualRecordId%' AND indexdef NOT ILIKE '%contentSplitParts%'"))
    assert sorted(r["indexname"] for r in names) == sorted(wanted)
    assert on_records == [], "no index for a split on the records: the record schema is closed"


async def test_a_split_part_record_written_by_the_seam_carries_its_virtual_record_id_and_no_split_field(pg_provider, orgs, org_ctx) -> None:
    org, _ = orgs
    spec = PartSpec(1, 1, 500, 501, "v-part-1", str(uuid.uuid4()))
    record = part_file_record(spec, gen="g", parent={"orgId": org, "recordName": "big.pdf", "connectorId": "conn-owner"}, storage_document_id="doc-1", size_bytes=7)
    with org_ctx(org):
        await pg_provider.batch_upsert_records([record])
        await pg_provider.batch_upsert_records([record])
        stored = await pg_provider.get_document(spec.node_key, RECORDS)
    assert (stored["virtualRecordId"], stored["connectorId"], stored["externalRecordId"]) == ("v-part-1", f"pdfparts_{org}", "part:g:1")
    assert not set(stored) & {"rootVirtualRecordId", "splitGen", "segmentIndex", "pageStart", "ownerConnectorId", "splitState", "splitDone"}


async def test_the_display_of_a_split_is_read_from_its_row_and_the_parent_record_is_never_written(pg_provider, orgs, org_ctx) -> None:
    org, _ = orgs
    record_id, root = str(uuid.uuid4()), f"root-{uuid.uuid4().hex}"

    def record() -> FileRecord:
        r = _file_record(record_id, "conn-owner")
        r.org_id = org
        return r

    with org_ctx(org):
        await pg_provider.batch_upsert_records([record()])
        before = await pg_provider.get_document(record_id, RECORDS)
        assert await pg_provider.create_node_if_absent(root, SPLITS, _in(org, {**_row(root, state="INDEXING_PARTS"), "done": 1, "failed": 1}))
        assert await display_for([root, "never-split"], pg_provider) == {
            root: {"splitState": "INDEXING_PARTS", "splitDone": 1, "splitTotal": 2, "splitFailed": 1},
        }
        assert await pg_provider.get_document(record_id, RECORDS) == before


def test_the_sample_plan_is_an_array_of_parts() -> None:
    assert isinstance(_plan("v"), list) and all(isinstance(p, dict) for p in _plan("v"))
