"""Parts belong to the content, not to the connector that first split it, on PostgreSQL (Neo4j and ArangoDB:
../test_split_owner_real_backends.py). Needs a migrated database: PG_CORE_URL, PG_POOLER=none."""
from __future__ import annotations

import uuid

import pytest

from app.core.org_context import system
from tests.integration.graph_db.split_owner_world import run_rehome

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.timeout(300)]


@pytest.fixture(autouse=True)
def _strict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_SCHEMA_ENFORCEMENT", "strict")


async def test_the_parts_follow_the_surviving_copy_when_the_first_connector_is_deleted(pg_provider, pg_db, org_ctx) -> None:
    org = uuid.uuid4().hex[:24]
    try:
        with org_ctx(org):
            await pg_provider.batch_upsert_nodes([{"_key": org, "name": "o", "isActive": True, "accountType": "enterprise"}], "organizations")
        await run_rehome(pg_provider, org, org_scope=org_ctx, sweep_scope=lambda: system("split-owner-it"))
    finally:
        with org_ctx(org):
            await pg_db.run(lambda tx: tx.execute("DELETE FROM graph.nodes WHERE org_id = $1", org), write=True)
