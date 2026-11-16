"""The file roll-up under 35 concurrent part ends, a retry and the heal pass, on PostgreSQL.

Needs a migrated database: PG_CORE_URL (a `pipeshub_connectors_login` DSN), PG_POOLER=none. Neo4j and ArangoDB:
../test_split_rollup_real_backends.py.
"""
from __future__ import annotations

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

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _strict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_SCHEMA_ENFORCEMENT", "strict")


@pytest.fixture
async def org(pg_db, org_ctx) -> AsyncIterator[str]:
    org = uuid.uuid4().hex[:24]
    yield org
    with org_ctx(org):
        await pg_db.run(lambda tx: tx.execute("DELETE FROM graph.nodes WHERE org_id = $1", org), write=True)


@pytest.mark.parametrize("round_", range(3))
async def test_the_stored_state_equals_the_recount_after_35_concurrent_ends_a_retry_and_a_heal(pg_provider, org, org_ctx, round_: int) -> None:
    with org_ctx(org):
        check(await race(pg_provider, org))


async def test_a_roll_up_that_counted_early_cannot_overwrite_a_later_one(pg_provider, org, org_ctx) -> None:
    with org_ctx(org):
        truth, stored = await stale_snapshot(pg_provider, org)
    assert truth == (PARTS, 0)
    assert stored == (PARTS, 0, SplitState.COMPLETED)


async def test_the_parents_stages_wait_for_every_parts_own_stages_and_run_once(pg_provider, org, org_ctx) -> None:
    with org_ctx(org):
        assert await drive_waits_for_the_parts_stages(pg_provider, org) == [0, 0, 1, 1]


async def test_the_drive_token_is_stored_and_read_back(pg_provider, org, org_ctx) -> None:
    with org_ctx(org):
        assert await token_round_trip(pg_provider, org) == ("", "abc123", "")


async def test_a_stale_drive_claim_is_reclaimed_and_driven_once(pg_provider, org, org_ctx) -> None:
    with org_ctx(org):
        assert await stale_drive_claim_is_reclaimed(pg_provider, org) == [0, 1, 1, 1]


async def test_a_retried_part_drives_the_parent_again_exactly_once(pg_provider, org, org_ctx) -> None:
    with org_ctx(org):
        assert await retried_part_drives_the_parent_once(pg_provider, org) == [1, 1, 1, 2, 2, 2]
