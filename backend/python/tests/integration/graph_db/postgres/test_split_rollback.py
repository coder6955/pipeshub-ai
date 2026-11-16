"""Un-splitting every file before a downgrade, on PostgreSQL (Neo4j and ArangoDB:
../test_split_rollback_real_backends.py). Needs a migrated database: PG_CORE_URL, PG_POOLER=none."""
from __future__ import annotations

import uuid

import pytest

from app.core.org_context import system
from tests.integration.graph_db.split_rollback_world import (
    run_poisoned_pages,
    run_rollback,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.timeout(300)]


@pytest.fixture(autouse=True)
def _strict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_SCHEMA_ENFORCEMENT", "strict")


async def test_every_split_file_is_unsplit_and_queued_whole(pg_provider, pg_db, org_ctx) -> None:
    org = uuid.uuid4().hex[:24]
    try:
        with org_ctx(org):
            await pg_provider.batch_upsert_nodes([{"_key": org, "name": "o", "isActive": True, "accountType": "enterprise"}], "organizations")
        await run_rollback(pg_provider, org, org_scope=org_ctx, sweep_scope=lambda: system("split-rollback-it"))
    finally:
        with org_ctx(org):
            await pg_db.run(lambda tx: tx.execute("DELETE FROM graph.nodes WHERE org_id = $1", org), write=True)


async def test_one_unreadable_row_in_the_middle_of_a_page_stops_neither_the_rollback_nor_the_sweep(pg_provider, pg_db, org_ctx) -> None:
    org = uuid.uuid4().hex[:24]
    try:
        with org_ctx(org):
            await pg_provider.batch_upsert_nodes([{"_key": org, "name": "o", "isActive": True, "accountType": "enterprise"}], "organizations")
        await run_poisoned_pages(pg_provider, org, org_scope=org_ctx, sweep_scope=lambda: system("split-rollback-it"))
    finally:
        with org_ctx(org):
            await pg_db.run(lambda tx: tx.execute("DELETE FROM graph.nodes WHERE org_id = $1", org), write=True)
