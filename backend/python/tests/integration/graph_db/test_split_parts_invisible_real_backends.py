"""The parts of a split PDF are invisible to every listing, count, lookup and sweep, on a real Neo4j and ArangoDB.

Scenario and surfaces: ``split_parts_world.py``. Runs in backend-matrix on both graph jobs.
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

import pytest

from tests.integration.graph_db.split_parts_world import World, problems
from tests.integration.real_graph import (
    backend_unavailable,
    connect_arango,
    connect_neo4j,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.timeout(300)]
logger = logging.getLogger("split-parts-invisible-it")


@pytest.fixture(params=["neo4j", "arango"])
async def worlds(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[tuple[World, World]]:
    async with contextlib.AsyncExitStack() as cleanup:
        try:
            graph = await (connect_neo4j(logger, monkeypatch) if request.param == "neo4j" else connect_arango(logger, "split_parts_invisible_it"))
        except Exception as exc:
            backend_unavailable(request.param, exc)
        cleanup.push_async_callback(graph.disconnect)
        split, control = World(graph, uuid.uuid4().hex[:10], True), World(graph, uuid.uuid4().hex[:10], False)
        for world in (split, control):
            cleanup.push_async_callback(world.remove)
            await world.seed()
        yield split, control


async def test_the_parent_appears_once_and_no_part_appears_anywhere(worlds: tuple[World, World]) -> None:
    split, control = worlds
    seen = await split.observe()

    found = problems(seen, await control.observe(), split.part_ids)
    assert not found, "\n".join(found)
    assert seen["access"]["kbx-file"] == "kbx-file" and not any(seen["part_access"].values())
    assert seen["kh_search"]["ids"].count("kbx-file") == 1 and seen["kh_search"]["ids"].count("drv-file") == 1
    assert seen["kh_tree"]["kbx_flat"]["ids"].count("kbx-file") == 1
    assert seen["by_weburl"]["https://kb.example/kbx-file"] == "kbx-file"


async def test_a_full_sync_unlinks_the_same_with_and_without_parts(worlds: tuple[World, World]) -> None:
    split, control = worlds

    assert await split.unlink_count() == await control.unlink_count()


@pytest.mark.parametrize("how", ["edge", "connector"])
async def test_the_scenario_catches_a_part_wired_into_its_parents_tree_or_given_its_connector(worlds: tuple[World, World], how: str) -> None:
    split, control = worlds
    await split.leak(how)

    seen = await split.observe()
    found = problems(seen, await control.observe(), split.part_ids)

    assert found, found
    assert found, f"a part given a {how} leak went unnoticed"
    logger.info("leak %s caught by: %s", how, found)
