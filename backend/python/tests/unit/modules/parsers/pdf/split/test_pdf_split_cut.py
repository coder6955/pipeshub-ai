"""T6, T7, T10 and the estimator: cutting real PDFs."""
from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pypdfium2
import pytest

from app.exceptions.indexing_exceptions import PasswordProtectedError
from app.modules.parsers.pdf.split import cut as cut_module
from app.modules.parsers.pdf.split import job as job_module
from app.modules.parsers.pdf.split.config import SplitConfig
from app.modules.parsers.pdf.split.cut import (
    CutPart,
    OversizeReason,
    PdfSplitError,
    cut_parts,
    estimate_page_bytes,
    replan_oversize,
)
from app.modules.parsers.pdf.split.job import split_pdf
from app.modules.parsers.pdf.split.plan import plan_parts
from tests.support import pdf_samples

if TYPE_CHECKING:
    from pathlib import Path

MIB = 1 << 20


def _page_text(path: str, index: int) -> str:
    document = pypdfium2.PdfDocument(path)
    try:
        page = document[index]
        text = page.get_textpage().get_text_bounded()
        page.close()
        return text
    finally:
        document.close()


def _page_count(path: str) -> int:
    document = pypdfium2.PdfDocument(path)
    try:
        return len(document)
    finally:
        document.close()


@pytest.fixture
def source_1200(tmp_path: Path) -> str:
    path = tmp_path / "source.pdf"
    path.write_bytes(pdf_samples.numbered_pdf(1200))
    return str(path)


def test_t6_each_part_holds_its_pages_and_the_overlap_and_starts_at_the_right_page(
    source_1200: str, tmp_path: Path
) -> None:
    cfg = SplitConfig()
    plan = plan_parts(1200, None, cfg)

    parts = cut_parts(source_1200, plan, str(tmp_path / "out"), max_part_bytes=cfg.max_part_bytes)

    assert len(parts) == 3
    assert len(os.listdir(tmp_path / "out")) == 3
    for part in parts:
        assert _page_count(part.path) == part.overlap_end - part.page_start + 1
        assert pdf_samples.page_marker(part.page_start) in _page_text(part.path, 0)
        assert pdf_samples.page_marker(part.overlap_end) in _page_text(part.path, part.overlap_end - part.page_start)
    assert [(p.page_start, p.page_end, p.overlap_end) for p in parts] == [
        (1, 499, 500), (500, 998, 999), (999, 1200, 1200),
    ]
    assert not any(p.oversize for p in parts)


@pytest.mark.parametrize("overlap", [0, 1])
def test_t7_heavy_pages_the_estimate_missed_are_cut_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overlap: int
) -> None:
    monkeypatch.setattr(job_module, "estimate_page_bytes", lambda _path: [1000] * 12)
    source = tmp_path / "heavy.pdf"
    source.write_bytes(pdf_samples.heavy_pages_pdf(12, {3, 4, 5, 6, 7, 8}))
    cfg = SplitConfig(part_pages=12, max_part_bytes=450_000, overlap_pages=overlap)

    result = split_pdf(str(source), str(tmp_path / "out"), cfg)

    assert len(result.parts) > 1
    assert sum(p.page_end - p.page_start + 1 for p in result.parts) == 12
    for part in result.parts:
        assert part.size_bytes <= cfg.max_part_bytes or part.oversize_reason is OversizeReason.SINGLE_PAGE
        assert os.path.getsize(part.path) == part.size_bytes
    assert sorted(os.listdir(tmp_path / "out")) == sorted(os.path.basename(p.path) for p in result.parts)
    assert [p.index for p in result.parts] == list(range(len(result.parts)))


@pytest.mark.parametrize("overlap", [0, 1])
def test_a_heavy_page_after_a_part_does_not_break_that_part_into_many(tmp_path: Path, overlap: int) -> None:
    source = tmp_path / "heavy.pdf"
    source.write_bytes(pdf_samples.heavy_pages_pdf(12, {7}))
    cfg = SplitConfig(part_pages=12, max_part_bytes=200_000, overlap_pages=overlap)

    result = split_pdf(str(source), str(tmp_path / "out"), cfg)

    assert [(p.page_start, p.page_end, p.overlap_end) for p in result.parts] == [(1, 6, 6), (7, 7, 7), (8, 12, 12)]
    assert [p.oversize_reason for p in result.parts] == [None, OversizeReason.SINGLE_PAGE, None]


@pytest.mark.parametrize("overlap", [0, 1])
def test_a_scanned_file_is_cut_once_to_parts_that_fit_with_their_overlap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overlap: int
) -> None:
    source = tmp_path / "scan.pdf"
    source.write_bytes(pdf_samples.heavy_pages_pdf(120, set(range(1, 121)), image_side=100))
    cfg = SplitConfig(part_pages=500, max_part_bytes=200_000, overlap_pages=overlap)
    rounds = []
    real = job_module.cut_parts
    monkeypatch.setattr(job_module, "cut_parts", lambda *a, **k: rounds.append(1) or real(*a, **k))

    result = split_pdf(str(source), str(tmp_path / "out"), cfg)

    assert len(rounds) == 1
    assert all(p.size_bytes <= cfg.max_part_bytes for p in result.parts)
    assert len(result.parts) <= 120 // 4 + 1


def _ranges(plan: object) -> list[tuple[int, int, int]]:
    return [(p.page_start, p.page_end, p.overlap_end) for p in plan.parts]


def test_t7_replan_drops_the_carried_pages_first_and_then_halves_the_core() -> None:
    cfg = SplitConfig(part_pages=5, overlap_pages=1)  # a part owns four pages and carries one
    plan = plan_parts(8, None, cfg)
    over = OversizeReason.SPLITTABLE

    first = replan_oversize(plan, [CutPart(0, "a", 9, 1, 4, 5, over), CutPart(1, "b", 9, 5, 8, 8)], cfg)
    assert _ranges(first) == [(1, 4, 4), (5, 8, 8)]

    second = replan_oversize(first, [CutPart(0, "a", 9, 1, 4, 4, over), CutPart(1, "b", 9, 5, 8, 8)], cfg)
    assert _ranges(second) == [(1, 2, 3), (3, 4, 5), (5, 8, 8)]


def test_t7_replan_leaves_a_single_page_alone() -> None:
    cfg = SplitConfig(part_pages=4, overlap_pages=0)
    part = CutPart(0, "a", 9, 1, 1, 1, OversizeReason.SINGLE_PAGE)

    assert _ranges(replan_oversize(plan_parts(1, None, cfg), [part], cfg)) == [(1, 1, 1)]


def test_t7_a_cut_part_over_the_cap_is_reported_not_kept_silently(tmp_path: Path) -> None:
    source = tmp_path / "heavy.pdf"
    source.write_bytes(pdf_samples.heavy_pages_pdf(4, {1, 2, 3, 4}))
    plan = plan_parts(4, None, SplitConfig(part_pages=4, overlap_pages=0))

    parts = cut_parts(str(source), plan, str(tmp_path / "out"), max_part_bytes=100_000)

    assert [p.oversize_reason for p in parts] == [OversizeReason.SPLITTABLE]


def test_estimates_sum_to_the_file_and_put_the_weight_on_the_heavy_pages(tmp_path: Path) -> None:
    source = tmp_path / "heavy.pdf"
    source.write_bytes(pdf_samples.heavy_pages_pdf(10, {5}))

    estimate = estimate_page_bytes(str(source))

    assert len(estimate) == 10
    assert abs(sum(estimate) - os.path.getsize(source)) <= 10
    assert estimate[4] > 5 * estimate[0]


def test_estimates_sample_a_long_file_instead_of_reading_every_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cut_module, "ESTIMATE_SAMPLE_PAGES", 5)
    source = tmp_path / "long.pdf"
    source.write_bytes(pdf_samples.numbered_pdf(40))
    read: list[int] = []
    real = cut_module._image_bytes

    def counting(document: object, index: int) -> int:
        read.append(index)
        return real(document, index)

    monkeypatch.setattr(cut_module, "_image_bytes", counting)

    assert len(estimate_page_bytes(str(source))) == 40
    assert len(read) == 5
    assert 0 in read
    assert 39 in read


def test_t10_a_file_pdfium_cannot_read_raises_a_typed_error_and_writes_nothing(tmp_path: Path) -> None:
    source = tmp_path / "bad.pdf"
    source.write_bytes(b"%PDF-1.7\nthis is not a pdf")
    out = tmp_path / "out"

    with pytest.raises(PdfSplitError):
        split_pdf(str(source), str(out), SplitConfig())

    assert not out.exists() or os.listdir(out) == []


def test_t10_a_password_protected_file_raises_password_protected(tmp_path: Path) -> None:
    source = tmp_path / "locked.pdf"
    source.write_bytes(pdf_samples.password_protected_pdf())
    out = tmp_path / "out"

    with pytest.raises(PasswordProtectedError):
        split_pdf(str(source), str(out), SplitConfig())

    assert not out.exists() or os.listdir(out) == []


def test_t10_a_failure_part_way_removes_the_parts_already_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.pdf"
    source.write_bytes(pdf_samples.numbered_pdf(30))
    cfg = SplitConfig(part_pages=10, overlap_pages=0)
    real = cut_module._save_pages
    calls = 0

    def failing(document: object, first: int, last: int, path: str) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise PdfSplitError("disk full")
        real(document, first, last, path)

    monkeypatch.setattr(cut_module, "_save_pages", failing)
    out = tmp_path / "out"

    with pytest.raises(PdfSplitError, match="disk full"):
        split_pdf(str(source), str(out), cfg)

    assert calls == 3
    assert os.listdir(out) == []


def test_t10_a_broken_file_after_a_locked_one_is_not_called_locked(tmp_path: Path) -> None:
    locked = tmp_path / "locked.pdf"
    locked.write_bytes(pdf_samples.password_protected_pdf())
    no_pages = tmp_path / "empty.pdf"
    no_pages.write_bytes(_pdf_without_pages())

    with pytest.raises(PasswordProtectedError):
        split_pdf(str(locked), str(tmp_path / "out1"), SplitConfig())
    with pytest.raises(PdfSplitError) as raised:
        split_pdf(str(no_pages), str(tmp_path / "out2"), SplitConfig())

    assert not isinstance(raised.value, PasswordProtectedError)


def test_t10_a_failure_in_the_re_cut_removes_the_parts_kept_from_the_first_cut(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(job_module, "estimate_page_bytes", lambda _path: [1000] * 12)
    source = tmp_path / "heavy.pdf"
    source.write_bytes(pdf_samples.heavy_pages_pdf(12, {1, 2, 3, 4, 5, 6}))
    cfg = SplitConfig(part_pages=6, max_part_bytes=450_000, overlap_pages=0)
    real = job_module.cut_parts
    rounds = []

    def failing_second_round(*args: object, **kwargs: object) -> list[CutPart]:
        rounds.append(1)
        if len(rounds) == 2:
            raise PdfSplitError("disk full")
        return real(*args, **kwargs)

    monkeypatch.setattr(job_module, "cut_parts", failing_second_round)
    out = tmp_path / "out"

    with pytest.raises(PdfSplitError, match="disk full"):
        split_pdf(str(source), str(out), cfg)

    assert len(rounds) == 2
    assert os.listdir(out) == []


def _pdf_without_pages() -> bytes:
    import io

    from reportlab.pdfgen import canvas

    buffer = io.BytesIO()
    canvas.Canvas(buffer).save()
    return buffer.getvalue()
