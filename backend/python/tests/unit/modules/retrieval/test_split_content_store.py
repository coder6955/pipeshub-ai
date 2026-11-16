"""``StoreSplitResolver`` against the real ``SplitStore`` (read only) over a fake graph provider."""
from __future__ import annotations

import pytest

from app.modules.indexing.split_state import (
    SPLIT_PARTS,
    SPLITS,
    PartInfo,
    SplitRow,
    SplitState,
    SplitStore,
    derive_gen,
    part_specs,
)
from app.modules.retrieval.split_content import PartLabel, StoreSplitResolver
from tests.support.split_fakes import FakeGraph

V = "11111111-1111-1111-1111-111111111111"


def _world(*, parts_gen: str | None = None) -> tuple[FakeGraph, str, tuple]:
    graph = FakeGraph()
    gen = derive_gen(V, "rev", "plan")
    specs = part_specs(gen, [(1, 40, 41), (41, 80, 80)], ocr=False)
    graph.put(SPLITS, V, SplitRow(v=V, org_id="o1", gen=gen, parts=specs, state=SplitState.COMPLETED).to_doc())
    for spec in specs:
        info = PartInfo.of(spec, v=V, gen=parts_gen or gen, org_id="o1", owner_connector_id="kb-1")
        graph.put(SPLIT_PARTS, info.key, info.to_doc())
    return graph, gen, specs


@pytest.mark.asyncio
async def test_the_resolver_answers_from_the_real_store() -> None:
    graph, gen, specs = _world()
    resolver = StoreSplitResolver(SplitStore(graph))

    assert await resolver.live_generation([V, "plain"]) == {V: gen}
    assert await resolver.part_vrid(V, 2) == specs[1].vrid
    assert await resolver.part_vrid(V, 3) is None
    assert await resolver.part_label(V, 2) == PartLabel(k=2, total=2, page_start=41, page_end=80, page_count=80)
    assert await resolver.roots_of([specs[0].vrid, specs[1].vrid, "plain"]) == {specs[0].vrid: V, specs[1].vrid: V}


@pytest.mark.asyncio
async def test_a_part_of_a_replaced_generation_has_no_root() -> None:
    graph, _gen, specs = _world(parts_gen="gen-old")
    assert await StoreSplitResolver(SplitStore(graph)).roots_of([specs[0].vrid]) == {}


@pytest.mark.asyncio
async def test_a_file_that_is_not_split_has_no_generation_and_no_label() -> None:
    resolver = StoreSplitResolver(SplitStore(FakeGraph()))
    assert await resolver.live_generation([V]) == {}
    assert await resolver.part_label(V, 1) is None
    assert await resolver.roots_of(["x"]) == {}
