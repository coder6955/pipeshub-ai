"""A part of a split file never takes part in MD5 dedup: it is not a duplicate of any record and has none."""
from __future__ import annotations

from app.config.constants.arangodb import ProgressStatus
from app.events.dedup import select_duplicate, will_promote_queued_copies


def _doc(key: str, connector: str, status: str = ProgressStatus.COMPLETED.value) -> dict:
    return {"_key": key, "connectorId": connector, "indexingStatus": status, "virtualRecordId": f"V-{key}"}


def _same(_record: object) -> str:
    return "records"


def test_a_part_among_the_duplicates_is_never_selected() -> None:
    part = _doc("part-1", "pdfparts_org-1")

    assert select_duplicate([part], "records", _same) is None
    chosen = select_duplicate([part, _doc("file", "drive-1")], "records", _same)
    assert chosen is not None and chosen.record["_key"] == "file"


def test_a_part_that_is_still_queued_does_not_promote_copies() -> None:
    assert will_promote_queued_copies(_doc("part-1", "pdfparts_org-1", ProgressStatus.QUEUED.value), None) is False
    assert will_promote_queued_copies(_doc("file", "drive-1", ProgressStatus.QUEUED.value), None) is True


def test_a_part_being_processed_finds_no_duplicate_among_ordinary_files() -> None:
    part = _doc("part-1", "pdfparts_org-1", ProgressStatus.IN_PROGRESS.value)

    assert select_duplicate([_doc("file", "drive-1")], "records", _same, current=part) is None
    assert select_duplicate([_doc("file", "drive-1")], "records", _same) is not None
