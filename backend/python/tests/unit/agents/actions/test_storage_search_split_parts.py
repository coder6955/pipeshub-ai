"""T7: grep over stored files finds a part's text and reports the file, never the part."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.agents.actions.storage_search.storage_search import StoragePatternMatch
from tests.unit.modules.retrieval.split_fakes import RECORD_ID, V1, V2, FakeResolver, V


def _tool(resolver: FakeResolver, accessible: dict[str, str]) -> tuple[StoragePatternMatch, AsyncMock]:
    graph = MagicMock()
    graph.filter_accessible_virtual_record_ids = AsyncMock(
        side_effect=lambda vrids, *a, **k: {v: accessible[v] for v in vrids if v in accessible},
    )
    state = {"user_id": "u", "org_id": "o", "graph_provider": graph, "split_resolver": resolver}
    tool = StoragePatternMatch(state)
    tool._scope_connector_ids = lambda: None  # type: ignore[method-assign]
    return tool, graph.filter_accessible_virtual_record_ids


@pytest.mark.asyncio
async def test_a_stored_part_is_checked_as_its_file_and_answers_with_the_files_record() -> None:
    tool, check = _tool(FakeResolver(), {V: RECORD_ID})
    access = await tool._check_accessible_vrids([V1, V2])
    assert access == {V1: RECORD_ID, V2: RECORD_ID}
    assert check.await_args.args[0] == [V]  # the part ids are never asked about
    assert tool._file_of == {V1: V, V2: V}


@pytest.mark.asyncio
async def test_a_part_of_a_file_the_user_cannot_read_is_denied() -> None:
    tool, _ = _tool(FakeResolver(), {})
    assert await tool._check_accessible_vrids([V1]) == {}


@pytest.mark.asyncio
async def test_an_unresolvable_part_is_denied_not_guessed() -> None:
    class Blind(FakeResolver):
        async def roots_of(self, vrids: list[str]) -> dict[str, str]:
            return {}

    tool, _ = _tool(Blind(), {V: RECORD_ID})
    assert await tool._check_accessible_vrids([V1]) == {}


@pytest.mark.asyncio
async def test_a_stored_record_that_is_not_a_part_is_checked_as_itself() -> None:
    tool, check = _tool(FakeResolver(), {"plain": "rec-plain"})
    assert await tool._check_accessible_vrids(["plain"]) == {"plain": "rec-plain"}
    assert check.await_args.args[0] == ["plain"]


@pytest.mark.asyncio
async def test_when_the_split_lookup_fails_only_the_parts_are_denied() -> None:
    class Down(FakeResolver):
        async def roots_of(self, vrids: list[str]) -> dict[str, str]:
            raise RuntimeError("split state down")

    tool, _ = _tool(Down(), {V: RECORD_ID, "plain": "rec-plain"})
    assert await tool._check_accessible_vrids([V1, "plain"]) == {"plain": "rec-plain"}
