# ruff: noqa: F811
"""Un-splitting every file before a downgrade: dry-run by default, idempotent and resumable."""
from __future__ import annotations

import dataclasses

import pytest

from app.modules.indexing.split_rollback import SplitRollback, rollback_mode
from app.modules.indexing.split_state import SplitState, part_node_key
from tests.unit.modules.indexing.test_split_seam import (  # noqa: F401  (fixture)
    ORG,
    Rig,
    V,
    rig,
)


def _rollback(rig: Rig, *, apply: bool = True, enabled: bool = False, org_id: str | None = None, manifest: list | None = None,
              fail_manifest: bool = False) -> SplitRollback:
    calls = manifest if manifest is not None else []

    async def manifest_points(org: str, v: str) -> None:
        if fail_manifest:
            raise RuntimeError("vector store down")
        calls.append((org, v))

    return SplitRollback(
        graph=rig.graph, store=rig.store, purger=rig.purger, publisher=rig.publisher, manifest_points=manifest_points,
        apply=apply, org_id=org_id, splitting_enabled=enabled,
    )


def test_the_mode_is_off_unless_asked_for() -> None:
    assert rollback_mode({}) is None
    assert rollback_mode({"PDF_SPLIT_ROLLBACK": "yes"}) is None
    assert rollback_mode({"PDF_SPLIT_ROLLBACK": "Dry-Run"}) == "dry-run"
    assert rollback_mode({"PDF_SPLIT_ROLLBACK": "apply"}) == "apply"


@pytest.mark.asyncio
class TestRollback:
    async def test_a_dry_run_counts_the_files_and_changes_nothing(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        sent = len(rig.publisher.sent)

        report = await _rollback(rig, apply=False).run_once()

        assert (report.files, report.unsplit, report.dry_run) == (1, 0, True)
        assert (await rig.store.get(V)).state is SplitState.INDEXING_PARTS  # type: ignore[union-attr]
        assert len(rig.parts()) == 3 and rig.resources.points_deleted == [] and len(rig.publisher.sent) == sent

    async def test_apply_refuses_while_splitting_is_still_enabled(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())

        report = await _rollback(rig, enabled=True).run_once()

        assert (report.files, report.unsplit) == (0, 0)
        assert await rig.store.get(V) is not None and len(rig.parts()) == 3

    async def test_apply_removes_parts_points_files_and_row_and_queues_the_whole_file(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request())).row
        manifest: list = []
        sent = len(rig.publisher.sent)

        report = await _rollback(rig, manifest=manifest).run_once()

        assert (report.files, report.unsplit, report.requeued, report.busy) == (1, 1, 1, 0)
        assert await rig.store.get(V) is None and not rig.part_docs() and not rig.parts()
        assert sorted(rig.resources.points_deleted) == sorted(s.vrid for s in old.parts)
        assert len(rig.resources.pdfs_deleted) == 3
        assert manifest == [(ORG, V)], "the manifest's points go too"
        _topic, event_type, payload, key = rig.publisher.sent[sent:][0]
        assert (event_type, key, payload["forceReindex"], payload["recordId"]) == ("reindexRecord", "parent-1", True, "parent-1")
        assert isinstance(payload["forceReindexAt"], int) and payload["forceReindexAt"] > 0, "the request time orders redeliveries"

    async def test_a_second_run_finds_nothing_to_do(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())
        rollback = _rollback(rig)
        await rollback.run_once()
        sent = len(rig.publisher.sent)

        report = await rollback.run_once()

        assert report.done and len(rig.publisher.sent) == sent

    async def test_a_part_still_being_indexed_leaves_the_row_for_the_next_pass(self, rig: Rig) -> None:
        old = (await rig.seam.split(rig.request())).row
        rig.leases.held.add(part_node_key(old.gen, 2))
        sent = len(rig.publisher.sent)

        first = await _rollback(rig).run_once()

        assert (first.unsplit, first.busy) == (0, 1)
        assert (await rig.store.get(V)).state is SplitState.PURGING  # type: ignore[union-attr]
        assert len(rig.publisher.sent) == sent, "the file is not queued while a part is running"

        rig.leases.held.clear()
        second = await _rollback(rig).run_once()

        assert (second.unsplit, second.requeued) == (1, 1) and await rig.store.get(V) is None

    async def test_a_pass_that_stops_after_the_purge_is_finished_by_the_next_one(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())

        failed = await _rollback(rig, fail_manifest=True).run_once()
        assert (failed.failed, failed.unsplit, failed.done) == (1, 0, False)
        assert (await rig.store.get(V)).state is SplitState.PURGING  # type: ignore[union-attr]

        report = await _rollback(rig).run_once()

        assert (report.unsplit, report.requeued) == (1, 1) and await rig.store.get(V) is None

    async def test_only_the_named_organisation_is_touched(self, rig: Rig) -> None:
        await rig.seam.split(rig.request())

        report = await _rollback(rig, org_id="another-org").run_once()

        assert report.files == 0 and await rig.store.get(V) is not None
        assert report.done, "no row of that organisation is left, whatever the other organisations hold"


async def _split(rig: Rig, v: str, key: str, *, org: str = ORG) -> None:
    rig.graph.put("records", key, {
        "orgId": org, "connectorId": "conn-1", "virtualRecordId": v, "recordName": f"{v}.pdf",
        "indexingStatus": "IN_PROGRESS", "mimeType": "application/pdf",
    })
    await rig.seam.split(dataclasses.replace(rig.request(), parent=rig.graph.docs["records"][key], v=v, org_id=org))


@pytest.mark.asyncio
class TestCompletionIsAnExistenceQuery:
    """"Done" is "no split row is left for the scope", asked of the store, never the size of a page."""

    async def test_an_organisations_files_behind_a_page_of_another_organisations_are_reached(self, rig: Rig) -> None:
        for n in range(110):
            await _split(rig, f"a{n:03d}", f"rec-a{n}", org="org-other")
        for n in range(3):
            await _split(rig, f"z{n:03d}", f"rec-z{n}", org="org-target")
        rollback = _rollback(rig, org_id="org-target")

        report = await rollback.run_once()

        assert (report.files, report.unsplit, report.done) == (3, 3, True)
        assert [d for d in rig.graph.docs["contentSplits"].values() if d["orgId"] == "org-target"] == []
        assert len(rig.graph.docs["contentSplits"]) == 110, "the other organisation is not touched"

    async def test_a_busy_file_on_an_early_page_keeps_the_rollback_from_being_done(self, rig: Rig) -> None:
        for n in range(200):
            await _split(rig, f"v{n:03d}", f"rec-{n}")
        busy = await rig.store.get("v005")
        rig.leases.held.add(busy.parts[0].node_key)  # type: ignore[union-attr]
        rollback = _rollback(rig)

        reports = [await rollback.run_once() for _ in range(6)]

        assert not any(r.done for r in reports)
        assert sum(r.busy for r in reports) >= 1
        assert list(rig.graph.docs["contentSplits"]) == ["v005"]
        rig.leases.held.clear()
        last = [await rollback.run_once() for _ in range(2)][-1]
        assert last.done and not rig.graph.docs["contentSplits"]

    async def test_a_dry_run_reports_the_true_total_not_a_page(self, rig: Rig) -> None:
        for n in range(230):
            await _split(rig, f"v{n:03d}", f"rec-{n}")
        rollback = _rollback(rig, apply=False)

        first, second = await rollback.run_once(), await rollback.run_once()

        assert first.files == second.files == 230
        assert not first.done and not second.done
        assert len(rig.graph.docs["contentSplits"]) == 230

    async def test_a_dry_run_over_a_scope_with_no_split_file_is_done(self, rig: Rig) -> None:
        assert (await _rollback(rig, apply=False).run_once()).done

    async def test_the_complete_line_is_logged_only_when_nothing_remains(self, rig: Rig, caplog: pytest.LogCaptureFixture) -> None:
        from app.modules.indexing.split_rollback import COMPLETE_LINE

        await _split(rig, "v1", "rec-1")
        rig.leases.held.add((await rig.store.get("v1")).parts[0].node_key)  # type: ignore[union-attr]
        rollback = _rollback(rig)
        with caplog.at_level("WARNING"):
            await rollback.run_once()
            assert COMPLETE_LINE not in caplog.text
            rig.leases.held.clear()
            await rollback.run_once()
        assert COMPLETE_LINE in caplog.text and COMPLETE_LINE == "Split rollback complete: no split files remain"


@pytest.mark.asyncio
class TestOneBadRowDoesNotStopTheRest:
    async def test_a_refused_event_costs_its_own_file_only(self, rig: Rig) -> None:
        for n in range(5):
            await _split(rig, f"v{n}", f"rec-{n}")
        real = rig.publisher.send_event

        async def refuse_first(topic: str, event_type: str, payload: dict, key: str | None = None) -> bool:
            return False if key == "rec-0" else await real(topic, event_type, payload, key)

        rig.publisher.send_event = refuse_first  # type: ignore[method-assign]

        report = await _rollback(rig).run_once()

        assert (report.unsplit, report.failed, report.done) == (4, 1, False)
        assert sorted(rig.graph.docs["contentSplits"]) == ["v0"]

    async def test_a_row_this_build_cannot_read_is_reported_and_keeps_the_rollback_open(self, rig: Rig) -> None:
        for n in range(3):
            await _split(rig, f"v{n}", f"rec-{n}")
        rig.graph.put("contentSplits", "v1-bad", {"orgId": ORG, "state": SplitState.COMPLETED.value, "plan": [], "updatedAtMs": 1})

        report = await _rollback(rig).run_once()

        assert (report.unsplit, report.failed, report.done) == (3, 1, False)
        assert list(rig.graph.docs["contentSplits"]) == ["v1-bad"]

    async def test_a_part_mid_index_is_never_requeued_until_it_is_free(self, rig: Rig) -> None:
        await _split(rig, "v1", "rec-1")
        row = await rig.store.get("v1")
        rig.leases.held.add(row.parts[1].node_key)  # type: ignore[union-attr]
        sent = len(rig.publisher.sent)
        rollback = _rollback(rig)

        for _ in range(3):
            r = await rollback.run_once()
            assert (r.unsplit, r.busy, r.done) == (0, 1, False)
        assert len(rig.publisher.sent) == sent
        assert (await rig.store.get("v1")).state is SplitState.PURGING  # type: ignore[union-attr]
        rig.leases.held.clear()
        r = await rollback.run_once()
        assert (r.unsplit, r.requeued, r.done) == (1, 1, True) and await rig.store.get("v1") is None
