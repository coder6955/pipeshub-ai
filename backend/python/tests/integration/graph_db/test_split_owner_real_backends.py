"""Parts belong to the content, not to the connector that first split it, on a real Neo4j and a real ArangoDB
(``split_owner_world.run_rehome``). PostgreSQL: postgres/test_split_owner.py."""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

import pytest

from tests.integration.graph_db.split_owner_world import run_rehome
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
logger = logging.getLogger("split-owner-it")


@pytest.fixture(params=["neo4j", "arango"])
async def graph(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[IGraphDBProvider]:
    try:
        provider = await (connect_neo4j(logger, monkeypatch) if request.param == "neo4j" else connect_arango(logger, "split_owner_it"))
    except Exception as error:
        backend_unavailable(request.param, error)
    yield provider
    await provider.disconnect()


async def test_the_parts_follow_the_surviving_copy_when_the_first_connector_is_deleted(graph: IGraphDBProvider) -> None:
    org = f"owner-it-{uuid.uuid4().hex[:8]}"
    try:
        await run_rehome(graph, org)
    finally:
        await _wipe(graph, org)
