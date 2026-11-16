"""Un-splitting every file before a downgrade, on a real Neo4j and a real ArangoDB
(``split_rollback_world.run_rollback``). PostgreSQL: postgres/test_split_rollback.py."""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

import pytest

from tests.integration.graph_db.split_rollback_world import (
    run_poisoned_pages,
    run_rollback,
)
from tests.integration.graph_db.test_content_splits_real_backends import _wipe
from tests.integration.real_graph import (
    backend_unavailable,
    connect_arango,
    connect_neo4j,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.timeout(300)]
logger = logging.getLogger("split-rollback-it")


@pytest.fixture(params=["neo4j", "arango"])
async def graph(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[IGraphDBProvider]:
    try:
        provider = await (connect_neo4j(logger, monkeypatch) if request.param == "neo4j" else connect_arango(logger, "split_rollback_it"))
    except Exception as error:
        backend_unavailable(request.param, error)
    yield provider
    await provider.disconnect()


async def test_every_split_file_is_unsplit_and_queued_whole(graph: IGraphDBProvider) -> None:
    org = f"rollback-it-{uuid.uuid4().hex[:8]}"
    try:
        await run_rollback(graph, org)
    finally:
        await _wipe(graph, org)


async def test_one_unreadable_row_in_the_middle_of_a_page_stops_neither_the_rollback_nor_the_sweep(graph: IGraphDBProvider) -> None:
    org = f"rollback-it-{uuid.uuid4().hex[:8]}"
    try:
        await run_poisoned_pages(graph, org)
    finally:
        await _wipe(graph, org)
