"""The whole life of a split file on a real Neo4j and a real ArangoDB: ``split_seam_world.run_lifecycle``.
Runs in backend-matrix on both graph jobs."""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

import pytest

from tests.integration.graph_db.split_seam_world import run_lifecycle
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
logger = logging.getLogger("split-seam-it")


@pytest.fixture(params=["neo4j", "arango"])
async def graph(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[IGraphDBProvider]:
    try:
        provider = await (connect_neo4j(logger, monkeypatch) if request.param == "neo4j" else connect_arango(logger, "split_seam_it"))
    except Exception as error:
        backend_unavailable(request.param, error)
    yield provider
    await provider.disconnect()


async def test_the_split_lifecycle_on_a_real_store(graph: IGraphDBProvider) -> None:
    org = f"seam-it-{uuid.uuid4().hex[:8]}"
    try:
        await run_lifecycle(graph, org)
    finally:
        await _wipe(graph, org)
