"""T2: a hit of a file's old generation is dropped before anything reads its content."""
from __future__ import annotations

import pytest

from app.modules.retrieval.generation_check import STALE_HITS, drop_stale_generations
from tests.unit.modules.retrieval.split_fakes import GEN, FakeResolver, point_meta


def _hit(k: int | None, local: int, gen: str | None = GEN) -> dict:
    return {"metadata": point_meta(k, local, gen=gen), "content": f"{k}-{local}"}


@pytest.mark.asyncio
async def test_a_hit_of_an_old_generation_is_dropped_and_the_live_ones_kept() -> None:
    hits = [_hit(1, 0), _hit(1, 1, gen="gen-old"), _hit(2, 0), _hit(2, 5, gen="gen-older")]
    kept = await drop_stale_generations(hits, FakeResolver())
    assert [h["content"] for h in kept] == ["1-0", "2-0"]


@pytest.mark.asyncio
async def test_a_file_that_is_no_longer_split_has_no_live_generation() -> None:
    kept = await drop_stale_generations([_hit(1, 0)], FakeResolver(live={}))
    assert kept == []


@pytest.mark.asyncio
async def test_unsplit_hits_never_touch_the_store() -> None:
    resolver = FakeResolver()
    hits = [_hit(None, 0), _hit(None, 1)]
    assert await drop_stale_generations(hits, resolver) == hits
    assert resolver.live_calls == []


@pytest.mark.asyncio
async def test_one_read_for_all_the_files_of_a_search() -> None:
    resolver = FakeResolver()
    await drop_stale_generations([_hit(1, 0), _hit(2, 0), _hit(2, 1)], resolver)
    assert len(resolver.live_calls) == 1


@pytest.mark.asyncio
async def test_unknown_state_fails_closed_for_split_hits_only() -> None:
    kept = await drop_stale_generations([_hit(1, 0), _hit(None, 0)], FakeResolver(fail=True))
    assert [h["content"] for h in kept] == ["None-0"]


@pytest.mark.asyncio
async def test_dropped_hits_are_counted() -> None:
    counts: list[tuple] = []
    original = STALE_HITS.inc
    STALE_HITS.inc = lambda *labels, value=1.0: counts.append((labels, value))  # type: ignore[method-assign]
    try:
        await drop_stale_generations([_hit(1, 0, gen="old"), _hit(2, 0, gen="old")], FakeResolver())
    finally:
        STALE_HITS.inc = original  # type: ignore[method-assign]
    assert counts == [(("stale",), 2)]


@pytest.mark.asyncio
async def test_a_generation_written_as_a_number_is_compared_as_text() -> None:
    hit = {"metadata": point_meta(1, 0, gen=123), "content": "n"}
    assert await drop_stale_generations([hit], FakeResolver(live={"v-file": "123"})) == [hit]
    assert await drop_stale_generations([hit], FakeResolver(live={"v-file": "124"})) == []
