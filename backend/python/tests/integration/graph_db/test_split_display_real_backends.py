"""S5be-5: the split roll-up of listings, record details and stats on a REAL Neo4j and ArangoDB, through the
generic reads file_split_display uses. PostgreSQL: postgres/test_split_display.py (same world)."""
from __future__ import annotations

import logging
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
from tests.integration.graph_db.test_content_splits_real_backends import _wipe
from tests.integration.real_graph import (
    backend_unavailable,
    connect_arango,
    connect_neo4j,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.timeout(300)]
logger = logging.getLogger("s5-split-display-it")


@pytest.fixture(params=["neo4j", "arango"])
async def env(request, monkeypatch):  # noqa: ANN201
    org, other = f"s5-it-{uuid.uuid4().hex[:8]}", f"s5-it-o-{uuid.uuid4().hex[:8]}"
    try:
        provider = await (connect_neo4j(logger, monkeypatch) if request.param == "neo4j"
                          else connect_arango(logger, "content_splits_it"))
    except Exception as error:
        backend_unavailable(request.param, error)
    clear_part_cache()
    yield provider, org, other, request.param
    for o in (org, other):
        await _wipe(provider, o)
    await provider.disconnect()


async def test_roll_up_on_a_real_store(env) -> None:
    graph, org, other, name = env
    w = await seed(graph, org, other)
    # Another org's split file under a connector with the same id: never counted in this org's stats.
    rid, v = str(uuid.uuid4()), str(uuid.uuid4())
    await graph.batch_upsert_records([_file(rid, other, "conn-c")])
    await graph.update_node(rid, RECORDS, {"virtualRecordId": v, "indexingStatus": "COMPLETED"})
    assert await graph.create_node_if_absent(v, SPLITS, _row(v, other, "PARTIAL", done=4, failed=1))

    assert_one_batched_read(await check_listing(graph, w))
    await check_details(graph, w)
    await check_stats(graph, org)
    assert_one_batched_read(await page_cost(graph, org, "conn-c", 50))
