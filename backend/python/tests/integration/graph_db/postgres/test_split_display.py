"""S5be-5: the split roll-up of listings, record details and stats on PostgreSQL (its conftest provides pg_db,
pg_provider and org_ctx). Same world as the Neo4j and ArangoDB test."""
from __future__ import annotations

import uuid

import pytest

from app.modules.indexing.split_state import clear_part_cache
from tests.integration.graph_db.split_display_world import (
    RECORDS,
    SPLITS,
    _file,
    _row,
    assert_one_batched_read,
    check_details,
    check_listing,
    check_stats,
    page_cost,
    seed,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _org_row(graph, org) -> None:
    await graph.batch_upsert_nodes([{"_key": org, "name": "o", "isActive": True, "accountType": "enterprise"}], "organizations")


async def test_roll_up_on_postgres(pg_db, pg_provider, org_ctx) -> None:
    graph = pg_provider
    org, other = uuid.uuid4().hex[:24], uuid.uuid4().hex[:24]
    clear_part_cache()
    try:
        with org_ctx(other):
            await _org_row(graph, other)
            rid, v = str(uuid.uuid4()), str(uuid.uuid4())
            await graph.batch_upsert_records([_file(rid, other, "conn-c")])
            await graph.update_node(rid, RECORDS, {"virtualRecordId": v, "indexingStatus": "COMPLETED"})
            assert await graph.create_node_if_absent(v, SPLITS, _row(v, other, "PARTIAL", done=4, failed=1))
        with org_ctx(org):
            await _org_row(graph, org)
            w = await seed(graph, org, other)
            assert_one_batched_read(await check_listing(graph, w))
            await check_details(graph, w)
            await check_stats(graph, org)
            assert_one_batched_read(await page_cost(graph, org, "conn-c", 50))
    finally:
        for o in (org, other):
            with org_ctx(o):
                await pg_db.run(lambda tx, o=o: tx.execute("DELETE FROM graph.nodes WHERE org_id = $1", o), write=True)
