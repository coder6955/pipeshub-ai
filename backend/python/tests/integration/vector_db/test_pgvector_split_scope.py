"""A split file's writes stay in their segment on pgvector (the same world as the other stores).

The segment fields are payload keys here (``point_text.payload``), not promoted columns: every segment filter also names the
file's ``virtualRecordId`` (a promoted, indexed column), so a part-scoped delete reads one file's rows and never scans the org.

Run: KERNEL_TEST_PG_BINS=<pg bin> KERNEL_TEST_PGBOUNCER=<pgbouncer> pytest tests/integration/vector_db/test_pgvector_split_scope.py -m integration
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from app.core.org_context import for_org
from app.services.vector_db.pgvector.config import PgVectorConfig
from app.services.vector_db.pgvector.pgvector import PgVectorService
from tests.integration.db.conftest import cluster, env  # noqa: F401  (fixtures)
from tests.support import split_scope_world as world

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.integration, pytest.mark.timeout(600)]

SCENARIOS = list(world.EXPECT)


@pytest.fixture
async def store(env) -> AsyncIterator[PgVectorService]:  # noqa: F811
    port = env.start_bouncer()
    svc = await PgVectorService.create(PgVectorConfig(dsn=env.dsn("indexing", port), jobs_dsn=env.dsn("jobs", port), pool_max=4, placement="none"))
    yield svc
    await svc.disconnect()


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.__name__ for s in SCENARIOS])
async def test_a_write_never_reaches_another_segment(store, scenario) -> None:
    with for_org(world.ORG, job="split-scope-test"):
        await world.seed(store, "records")
        assert await world.census(store, "records", expected=12) == world.FULL
        assert await scenario(store, "records") == world.EXPECT[scenario]


async def test_a_whole_file_point_among_the_segments_is_found_and_a_split_file_alone_has_none(store) -> None:
    with for_org(world.ORG, job="split-scope-test"):
        await world.seed(store, "records")
        assert await world.stray_points(store, "records") == (False, True)
