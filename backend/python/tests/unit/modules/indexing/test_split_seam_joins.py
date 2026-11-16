# ruff: noqa: F811
"""A second attempt for a file whose generation another attempt has made or is indexing never removes its parts.

Two records holding the same file, a redelivery and the stale-recovery republish can all reach the parent seam for the same
file. The cleanup of the file's whole-file points runs only once the row exists and deletes only points outside the
generation's own segments, so the attempt that joins a generation cannot wipe what the first has indexed.
"""
from __future__ import annotations

from typing import Any

import pytest

from app.modules.indexing.split_seam import ParentSplitSeam
from app.modules.indexing.split_state import SplitRow, SplitStore
from app.modules.indexing.split_wiring import _WholeFile
from app.modules.parsers.pdf.split.config import SplitConfig
from app.modules.transformers.write_scope import WriteScope
from tests.support.in_memory_vector_db import InMemoryVectorDBService
from tests.unit.modules.indexing.test_split_seam import NOW, ORG, Rig, V, rig  # noqa: F401  (fixture)
from tests.unit.modules.indexing.test_split_stray_points import (
    COLLECTION,
    _pipeline,
    _points,
    _split_file,
)


async def _count(service: InMemoryVectorDBService, **must: object) -> int:
    return await service.count_points(COLLECTION, await service.filter_collection(must={"orgId": ORG, "virtualRecordId": V, **must}))


class _LateReader(SplitStore):
    """A store whose first read of the file's row finds nothing: the attempt read before another one created the row."""

    def __init__(self, graph: Any, **kw: Any) -> None:  # noqa: ANN401
        super().__init__(graph, **kw)
        self._blind = True

    async def get(self, v: str) -> SplitRow | None:
        if self._blind:
            self._blind = False
            return None
        return await super().get(v)


@pytest.mark.asyncio
class TestAnAttemptThatJoinsAGenerationRemovesNothingItOwns:
    async def test_a_late_attempt_whose_create_loses_leaves_the_parts_the_winner_indexed(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        row, scopes = await _split_file(rig, service)  # the winner: row, parts queued, parts' points written
        indexed = await _count(service)
        assert indexed == 2 * len(scopes)
        late = ParentSplitSeam(
            graph=rig.graph, store=_LateReader(rig.graph, clock_ms=lambda: NOW[0]), purger=rig.purger, cutter=rig.cutter,
            uploader=rig.uploader, publisher=rig.publisher, whole_file=_WholeFile(_pipeline(service)), config=SplitConfig,
        )

        outcome = await late.split(rig.request())

        assert outcome.row.gen == row.gen
        assert await _count(service) == indexed, "the parts the first attempt indexed are still there"

    async def test_the_attempt_that_makes_a_generation_clears_the_whole_file_points_and_a_repeat_adds_no_deletion(self, rig: Rig) -> None:
        service = InMemoryVectorDBService()
        await service.create_collection(COLLECTION)
        await service.upsert_points(COLLECTION, _points(WriteScope(V, V), "older", vrid=V, n=3))
        rig.seam._whole_file = _WholeFile(_pipeline(service))

        await rig.seam.split(rig.request())
        assert await _count(service) == 0, "the file's whole-file points go once the row exists"
        row = await rig.store.get(V)
        scopes = [WriteScope.manifest(row.v, row.gen)] + [WriteScope.part(_info(row, spec)) for spec in row.parts]
        for scope in scopes:
            await service.upsert_points(COLLECTION, _points(scope, scope.segment or "m"))

        for _ in range(3):
            await rig.seam.split(rig.request())

        assert await _count(service) == 2 * len(scopes), "a repeated attempt leaves the indexed parts alone"


def _info(row: Any, spec: Any) -> Any:  # noqa: ANN401
    from app.modules.indexing.split_state import PartInfo

    return PartInfo.of(spec, v=row.v, gen=row.gen, org_id=ORG, owner_connector_id="conn-1")
