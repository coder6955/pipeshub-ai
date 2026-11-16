"""The file roll-up under 35 concurrent part ends, a retry and the heal pass, on a real Neo4j and a real ArangoDB.

PostgreSQL: postgres/test_split_rollup.py. Same world, same assertions: the stored row equals the recount.
"""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

import pytest

from app.modules.indexing.split_state import SplitState
from tests.integration.graph_db.split_rollup_world import (
    PARTS,
    check,
    drive_waits_for_the_parts_stages,
    race,
    retried_part_drives_the_parent_once,
    stale_drive_claim_is_reclaimed,
    stale_snapshot,
    token_round_trip,
)
from tests.integration.real_graph import (
    backend_unavailable,
    connect_arango,
    connect_neo4j,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.timeout(300)]
logger = logging.getLogger("split-rollup-it")
ORG = f"rollup-it-{uuid.uuid4().hex[:8]}"


@pytest.fixture(params=["neo4j", "arango"])
async def graph(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[IGraphDBProvider]:
    try:
        provider = await connect_neo4j(logger, monkeypatch) if request.param == "neo4j" else await connect_arango(logger, "split_rollup_it")
    except Exception as error:
        backend_unavailable(request.param, error)
    yield provider
    for collection in ("contentSplits", "contentSplitParts", "records"):
        rows = await provider.get_nodes_by_filters(collection, {"orgId": ORG}, ["_key"])
        if rows:
            await provider.delete_nodes([r["_key"] for r in rows], collection)
    await provider.disconnect()


@pytest.mark.parametrize("round_", range(3))
async def test_the_stored_state_equals_the_recount_after_35_concurrent_ends_a_retry_and_a_heal(graph: IGraphDBProvider, round_: int) -> None:
    check(await race(graph, ORG))


async def test_a_roll_up_that_counted_early_cannot_overwrite_a_later_one(graph: IGraphDBProvider) -> None:
    truth, stored = await stale_snapshot(graph, ORG)
    assert truth == (PARTS, 0)
    assert stored == (PARTS, 0, SplitState.COMPLETED)


async def test_the_parents_stages_wait_for_every_parts_own_stages_and_run_once(graph: IGraphDBProvider) -> None:
    assert await drive_waits_for_the_parts_stages(graph, ORG) == [0, 0, 1, 1]


async def test_the_drive_token_is_stored_and_read_back(graph: IGraphDBProvider) -> None:
    assert await token_round_trip(graph, ORG) == ("", "abc123", "")


async def test_a_stale_drive_claim_is_reclaimed_and_driven_once(graph: IGraphDBProvider) -> None:
    assert await stale_drive_claim_is_reclaimed(graph, ORG) == [0, 1, 1, 1]


async def test_a_retried_part_drives_the_parent_again_exactly_once(graph: IGraphDBProvider) -> None:
    assert await retried_part_drives_the_parent_once(graph, ORG) == [1, 1, 1, 2, 2, 2]
