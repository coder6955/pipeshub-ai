# ruff: noqa: F811
"""A purge deletes only what is still its own generation's.

Concurrent callers retire the same generation (the seam, the sweep's orphan and resume steps, the rollback): when one has
finished and the parent has made the next generation, a late purge of the old one must leave the new row and its parts.
"""
from __future__ import annotations

from typing import Any

import pytest

from app.modules.indexing.split_state import (
    SplitRow,
    SplitState,
    derive_gen,
    part_specs,
)
from tests.support.split_fakes import FakeResources
from tests.unit.modules.indexing.test_split_seam import RANGES, Rig, V, rig  # noqa: F401  (fixture)


def _next_generation(rig: Rig) -> SplitRow:
    gen = derive_gen(V, "rev-2", "digest-2", "")
    return SplitRow(
        v=V, org_id="org-1", gen=gen, parts=part_specs(gen, RANGES, ocr=False), state=SplitState.SPLITTING, content_rev="rev-2",
        plan_digest="digest-2",
    )


@pytest.mark.asyncio
class TestAPurgeThatLostItsRowStops:
    async def test_a_purge_of_a_generation_another_purger_retired_touches_nothing_of_the_next_one(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request())).row
        new = _next_generation(rig)
        rig.graph.docs["contentSplits"][V] = {"_key": V, **new.to_doc()}

        result = await rig.purger.purge_generation(old)

        assert (result.purged, result.busy) == (0, 0)
        held = await rig.store.get(V)
        assert held is not None and held.gen == new.gen and held.state is SplitState.SPLITTING
        assert rig.resources.points_deleted == [], "the parts of the next generation are not this purge's to touch"

    async def test_the_final_delete_leaves_a_row_that_became_another_generation_while_the_parts_went(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request())).row
        new = _next_generation(rig)

        class SwapsTheRow(FakeResources):
            async def delete_points_and_stored_record(self, org_id: str, root: str, part: str) -> None:
                await super().delete_points_and_stored_record(org_id, root, part)
                # another purger finishes the old generation and the parent makes the next one, between this purge's steps
                rig.graph.docs["contentSplits"][V] = {"_key": V, **new.to_doc()}

        rig.purger._resources = SwapsTheRow()

        result = await rig.purger.purge_generation(old)

        assert result.complete
        held = await rig.store.get(V)
        assert held is not None and held.gen == new.gen, "the conditional delete matched neither generation nor PURGING"

    async def test_a_resumed_purge_of_the_same_generation_still_goes_on_and_deletes_its_row(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request())).row
        rig.leases.held.add(old.parts[0].node_key)
        first = await rig.purger.purge_generation(old)
        assert not first.complete
        rig.leases.held.clear()

        second = await rig.purger.purge_generation(old)

        assert second.complete and await rig.store.get(V) is None

    async def test_the_store_deletes_only_a_purging_row_of_the_generation_it_names(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request())).row
        assert await rig.store.delete(V, old.gen) is False, "not PURGING"
        assert await rig.store.move(V, old.gen, SplitState.INDEXING_PARTS, SplitState.PURGING)
        assert await rig.store.delete(V, "another-generation") is False
        assert await rig.store.delete(V, old.gen) is True
        assert await rig.store.get(V) is None
        assert await rig.store.delete(V, old.gen) is False
        _: Any = None
