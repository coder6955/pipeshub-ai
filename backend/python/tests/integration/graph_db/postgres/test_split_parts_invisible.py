"""The parts of a split PDF are invisible to every listing, count, lookup and sweep, on PostgreSQL.

The scenario and the surfaces are the ones of ``../test_split_parts_invisible_real_backends.py`` (``../split_parts_world.py``).
Needs a migrated database: PG_CORE_URL, PG_POOLER=none.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from tests.integration.graph_db.postgres.test_kg_read import flip
from tests.integration.graph_db.split_parts_world import World, problems

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.integration


@pytest.fixture
async def worlds(pg_provider, pg_db, org_ctx) -> AsyncIterator[tuple[World, World]]:
    split = World(pg_provider, uuid.uuid4().hex[:10], True, org_scope=org_ctx, org_id=uuid.uuid4().hex[:24])
    control = World(pg_provider, uuid.uuid4().hex[:10], False, org_scope=org_ctx, org_id=uuid.uuid4().hex[:24])
    for world in (split, control):
        await world.seed()
        await flip(pg_db, world.org)      # the connectors are served from kg, as on a running install
    yield split, control
    for world in (split, control):
        with org_ctx(world.org):
            await pg_provider._db.run(lambda tx, o=world.org: tx.execute("DELETE FROM graph.nodes WHERE org_id = $1", o), write=True)


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

    found = problems(await split.observe(), await control.observe(), split.part_ids)

    assert found, f"a part given a {how} leak went unnoticed"
