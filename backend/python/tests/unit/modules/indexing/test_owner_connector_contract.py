"""A part of a split PDF answers to its parent's connector, not to its own hidden ``connectorId``.

Any code that reads a connector off a graph node to gate, route, count or recover a record goes
through ``owner_connector_id``. The scan below fails when a new raw read appears in the indexing
paths without an entry (and a reason) in ``ALLOWED``, and when an entry stops matching anything.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import OriginTypes, ProgressStatus
from app.modules.indexing.connector_off_events import gating_connector_id
from app.modules.indexing.owner_connector import (
    PART_CONNECTOR_PREFIX,
    is_part_connector_id,
    is_part_record,
    owner_connector_id,
    part_connector_id,
)
from app.modules.indexing.record_republish import republish_eligible
from app.modules.indexing.split_state import (
    PartInfoMissing,
    clear_part_cache,
    prime_parts,
)

APP = Path(__file__).resolve().parents[4] / "app"
SCANNED = [
    APP / "modules" / "indexing",
    APP / "services" / "messaging",
    APP / "modules" / "pipeline",
    APP / "events",
    APP / "indexing_main.py",
]
RAW_READ = re.compile(r"""get\(["']connectorId["']|\[["']connectorId["']\]|\.connector_id\b""")

# (file suffix, regex the line must match, why the raw read is right)
ALLOWED: list[tuple[str, str, str]] = [
    ("modules/indexing/split_state.py", r"record\.get\(\"connectorId\"\)",
     "deciding whether a record is a part (its connector starts with the hidden prefix) is what reads the raw field"),
    ("modules/indexing/file_split_display.py", r"rec\.get\(\"connectorId\"\)",
     "stats: the records matched are split parents (looked up by the key of a contentSplits row; a part has its own vrid), counted in the connector they sit in"),
    ("modules/indexing/split_reindex.py", r"d\.get\(\"connectorId\"\) == connector_id",
     "a user-scoped retry keeps the files whose live holders sit in the connector being reindexed: the holders of a file are never parts"),
    ("modules/indexing/split_seam.py", r"(parent|record)\.get\(\"connectorId\"\)",
     "the record being split is the parent, never a part: its own connector is the owner a part records"),
    ("modules/indexing/split_owner.py", r"d\.get\(\"connectorId\"\)|d\[\"connectorId\"\]",
     "resolving the owner of a file's parts reads the connector off the file's live holders, which are never parts (the hidden prefix is excluded)"),
    ("modules/indexing/split_rollback.py", r"doc\.get\(\"connectorId\"\)",
     "the records queued for whole indexing are the file's live holders, never parts (parts are retired first)"),
    ("modules/indexing/run.py", r"ctx\.connector_id", "a connector-scoped run names the connector it was started for, not a record"),
    ("modules/indexing/entity_index_rebuild.py", r"intent\[", "an intent row names the connector it rebuilds"),
    ("modules/indexing/connector_off_events.py", r"payload", "envelope: a part's event already carries its owner"),
    ("services/messaging/message_log.py", r"payload", "envelope"),
    ("services/messaging/connector_off.py", r"payload", "envelope"),
    ("services/messaging/lanes/assignment.py", r"request\.connector_id", "a lane request is built from the envelope's key"),
    ("services/messaging/kafka/handlers/entity.py", r"payload|\(value or \{\}\)", "envelope of app events"),
    ("services/messaging/kafka/utils/utils.py", r"payload", "envelope"),
    ("services/messaging/kafka/handlers/record.py", r"payload", "envelope"),
    ("services/messaging/kafka/handlers/record.py", r"record\.get\(\"connectorId\"\)|record\[\"connectorId\"\]",
     "rate-limit deferral: guarded by origin == CONNECTOR, which a part never has (parts are read from storage)"),
    ("modules/pipeline/worker.py", r"job\.connector_id", "a job's connector comes from the RecordView, built with owner_connector_id"),
    ("modules/pipeline/coordinator.py", r"job\.connector_id", "as above"),
    ("modules/pipeline/models.py", r"(self|job|view)\.connector_id", "as above"),
    ("modules/pipeline/stages/record_io.py", r"job\.connector_id", "as above"),
    ("modules/indexing/record_title_consumer.py", r"get\(\"connectorId\"\)",
     "titles are for records of live connectors; the hidden part connector is not one, so a part is skipped"),
    ("modules/indexing/class_units_backfill.py", r"row\.get\(\"connectorId\"\)",
     "class-unit projection of an indexed row; a part's units belong to the root record"),
    ("events/events.py", r"(record_doc|doc)\.get\(\"connectorId\"\)",
     "storage tags and code-graph projection: a part is a PDF in its own hidden connector scope"),
    ("events/processor.py", r"record\.connector_id", "code-graph projection of a code file"),
]


def _hits() -> list[tuple[str, int, str]]:
    found = []
    for root in SCANNED:
        for path in sorted([root] if root.is_file() else root.rglob("*.py")):
            rel = path.relative_to(APP).as_posix()
            for number, line in enumerate(path.read_text().splitlines(), 1):
                if RAW_READ.search(line):
                    found.append((rel, number, line.strip()))
    return found


def test_every_raw_connector_read_is_allowed_with_a_reason() -> None:
    unexplained = [
        f"{rel}:{number}: {line}"
        for rel, number, line in _hits()
        if not any(rel.endswith(suffix) and re.search(pattern, line) for suffix, pattern, _ in ALLOWED)
    ]
    assert not unexplained, "read the owner with owner_connector_id, or allow-list with a reason:\n" + "\n".join(unexplained)


def test_every_allow_list_entry_still_matches_a_line() -> None:
    hits = _hits()
    stale = [
        (suffix, pattern) for suffix, pattern, _ in ALLOWED
        if not any(rel.endswith(suffix) and re.search(pattern, line) for rel, _, line in hits)
    ]
    assert not stale


# ---- the accessor ---------------------------------------------------------------------------------

PART = {"_key": "part-1", "connectorId": "pdfparts_org-1", "origin": OriginTypes.UPLOAD.value}
PART_DOC = {
    "id": "part-1", "orgId": "org-1", "rootVirtualRecordId": "V", "gen": "g", "k": 1, "pageStart": 1, "pageEnd": 500,
    "pageOffset": 0, "overlapPages": 1, "ownerConnectorId": "drive-1",
}


@pytest.fixture(autouse=True)
def _fresh_part_cache() -> None:
    clear_part_cache()


def _graph(doc: dict | None = PART_DOC) -> MagicMock:
    graph = MagicMock()
    graph.get_document = AsyncMock(return_value=doc)
    return graph


async def test_a_part_answers_to_the_owner_in_its_part_document_and_a_plain_record_to_its_connector() -> None:
    graph = _graph()
    await prime_parts([PART], graph)

    assert owner_connector_id(PART) == "drive-1"
    assert owner_connector_id({"connectorId": "drive-1"}) == "drive-1"
    # nothing is read for a record that is not a part, and a part is read once (cached)
    await prime_parts([{"connectorId": "drive-1", "_key": "x"}, PART], graph)
    graph.get_document.assert_awaited_once_with("part-1", "contentSplitParts", raise_on_error=True)


def test_a_part_whose_document_was_not_read_is_never_answered_for() -> None:
    with pytest.raises(PartInfoMissing):
        owner_connector_id(PART)
    with pytest.raises(PartInfoMissing):
        gating_connector_id(PART)


async def test_a_part_without_a_part_document_fails_closed() -> None:
    with pytest.raises(PartInfoMissing):
        await prime_parts([PART], _graph(None))
    with pytest.raises(PartInfoMissing):
        owner_connector_id(PART)


def test_the_accessor_reads_models_and_tolerates_nothing() -> None:
    class Plain:
        connector_id = "kb-1"

    assert owner_connector_id(Plain()) == "kb-1"
    assert owner_connector_id(None) is None
    assert owner_connector_id({}) is None
    assert owner_connector_id({"connectorId": ""}) is None


async def test_a_model_part_is_read_by_its_id_and_connector() -> None:
    class Model:
        id = "part-1"
        connector_id = "pdfparts_org-1"

    await prime_parts([Model()], _graph())

    assert owner_connector_id(Model()) == "drive-1"


def test_a_part_is_a_record_under_the_hidden_parts_connector() -> None:
    assert is_part_record(PART)
    assert not is_part_record({"connectorId": "c", "rootVirtualRecordId": "V", "segmentIndex": 1})
    assert not is_part_record({"segmentIndex": 0})
    assert not is_part_record(None)


def test_the_hidden_connector_id_names_the_org() -> None:
    assert part_connector_id("org-1") == PART_CONNECTOR_PREFIX + "org-1" == "pdfparts_org-1"
    assert is_part_connector_id("pdfparts_org-1")
    assert not is_part_connector_id("drive-1")
    assert not is_part_connector_id(None)


# ---- the converted sites ---------------------------------------------------------------------------

async def test_a_part_is_gated_by_its_owner_connector() -> None:
    await prime_parts([PART], _graph())

    assert gating_connector_id(PART) == "drive-1"
    assert gating_connector_id({"connectorId": "kb-1", "origin": OriginTypes.UPLOAD.value}) is None
    assert gating_connector_id({"connectorId": "drive-1", "origin": OriginTypes.CONNECTOR.value}) == "drive-1"


@pytest.mark.parametrize("status", [ProgressStatus.QUEUED.value, ProgressStatus.NOT_STARTED.value])
def test_a_waiting_part_is_eligible_for_the_stranded_sweep(status: str) -> None:
    assert republish_eligible({**PART, "isDeleted": False}, status)[0] is True
    assert republish_eligible({"connectorId": "kb-1", "origin": OriginTypes.UPLOAD.value}, status)[0] is False


async def test_lane_upkeep_never_frees_or_reads_the_hidden_connector() -> None:
    from app.modules.indexing.lane_upkeep import _reconcile_with_graph

    entry = MagicMock(is_live=True)
    assignments = MagicMock()
    assignments.release = AsyncMock(return_value=1)
    graph = MagicMock()
    graph.get_document = AsyncMock(return_value=None)

    released, corrected = await _reconcile_with_graph(
        assignments, graph, {"pdfparts_org-1": entry}, {}, logging.getLogger("t"),  # type: ignore[arg-type]
    )

    assert (released, corrected) == (0, 0)
    assignments.release.assert_not_awaited()
    graph.get_document.assert_not_awaited()


def test_a_stage_job_for_a_part_is_gated_by_the_owner() -> None:
    from app.modules.pipeline.ingress import owner_connector_id as used

    assert used is owner_connector_id
