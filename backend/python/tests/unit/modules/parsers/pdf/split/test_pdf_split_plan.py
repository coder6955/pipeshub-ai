"""T1-T5: when a PDF is split, where it is cut, and how the settings are read."""
from __future__ import annotations

import logging
import random

import pytest

from app.modules.parsers.pdf.split.config import SplitConfig
from app.modules.parsers.pdf.split.plan import plan_parts, should_split

MIB = 1 << 20
ENV_NAMES = (
    "PDF_SPLIT_ENABLED", "PDF_SPLIT_MIN_PAGES", "PDF_SPLIT_PART_PAGES",
    "PDF_SPLIT_MAX_PART_BYTES", "PDF_SPLIT_OVERLAP_PAGES", "VLM_OCR_MAX_PAGES",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_t1_split_starts_above_the_minimum_page_count() -> None:
    cfg = SplitConfig()
    assert [should_split(n, cfg) for n in (1, 500, 501)] == [False, False, True]


def test_t1_a_disabled_split_never_splits() -> None:
    assert not should_split(50_000, SplitConfig(enabled=False))


def test_t2_1138_pages_make_three_part_files_of_at_most_500_pages_overlap_included() -> None:
    plan = plan_parts(1138, None, SplitConfig())
    assert [(p.page_start, p.page_end, p.overlap_end) for p in plan.parts] == [
        (1, 499, 500), (500, 998, 999), (999, 1138, 1138),
    ]
    assert [p.index for p in plan.parts] == [0, 1, 2]


def test_t3_plans_cover_every_page_once_and_respect_the_caps() -> None:
    rng = random.Random(20261115)
    # Bounded: cost grows with pages / part_pages, so tiny parts only get small documents.
    for _ in range(60):
        part_pages = rng.randint(1, 800)
        pages = rng.randint(1, min(50_000, max(2_000, part_pages * 40)))
        cfg = SplitConfig(
            part_pages=part_pages,
            max_part_bytes=rng.randint(1, 40) * MIB,
            overlap_pages=rng.randint(0, 3),
        )
        profile = rng.choice(("flat", "spiky", "heavy"))
        sizes = [_page_size(rng, profile) for _ in range(pages)] if rng.random() < 0.8 else None

        plan = plan_parts(pages, sizes, cfg)

        assert plan == plan_parts(pages, sizes, cfg)
        assert plan.parts[0].page_start == 1
        assert plan.parts[-1].page_end == pages
        for before, after in zip(plan.parts, plan.parts[1:], strict=False):
            assert after.page_start == before.page_end + 1
        for part in plan.parts:
            assert part.page_start <= part.page_end <= part.overlap_end <= pages
            assert part.core_pages <= cfg.part_pages
            assert part.overlap_end - part.page_end <= cfg.overlap_pages
            if sizes is None:
                assert part.overlap_end == min(part.page_end + cfg.overlap_pages, pages)
            elif part.core_pages > 1:
                assert sum(sizes[part.page_start - 1 : part.overlap_end]) <= cfg.max_part_bytes


def test_t3_the_digest_follows_the_ranges_and_the_settings_that_shaped_them() -> None:
    base = plan_parts(1138, None, SplitConfig())
    assert plan_parts(1138, None, SplitConfig()).digest == base.digest
    assert plan_parts(1139, None, SplitConfig()).digest != base.digest
    assert plan_parts(1138, None, SplitConfig(overlap_pages=2)).digest != base.digest
    assert plan_parts(1138, None, SplitConfig(max_part_bytes=MIB)).digest != base.digest


def test_t4_a_page_larger_than_the_byte_cap_gets_a_part_to_itself() -> None:
    cfg = SplitConfig(max_part_bytes=10 * MIB)
    sizes = [MIB] * 10
    sizes[4] = 30 * MIB

    plan = plan_parts(10, sizes, cfg)

    assert [(p.page_start, p.page_end) for p in plan.parts] == [(1, 4), (5, 5), (6, 10)]


def test_plan_rejects_estimates_that_do_not_match_the_page_count() -> None:
    with pytest.raises(ValueError, match="page sizes"):
        plan_parts(10, [1] * 9, SplitConfig())


def test_t5_defaults_follow_the_ocr_page_cap_and_the_heavy_byte_limit() -> None:
    cfg = SplitConfig.from_env()
    assert cfg == SplitConfig(
        enabled=True, min_pages=500, part_pages=500, max_part_bytes=25 * MIB, overlap_pages=1
    )


def test_t5_part_pages_follow_a_lower_ocr_page_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VLM_OCR_MAX_PAGES", "120")
    assert SplitConfig.from_env().part_pages == 120


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PDF_SPLIT_MIN_PAGES", "lots"),
        ("PDF_SPLIT_MIN_PAGES", "0"),
        ("PDF_SPLIT_PART_PAGES", "-5"),
        ("PDF_SPLIT_MAX_PART_BYTES", "0"),
        ("PDF_SPLIT_OVERLAP_PAGES", "-1"),
        ("PDF_SPLIT_ENABLED", "maybe"),
    ],
)
def test_t5_an_invalid_value_falls_back_to_the_default_with_one_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    with caplog.at_level(logging.WARNING):
        cfg = SplitConfig.from_env()

    assert cfg == SplitConfig()
    assert len([r for r in caplog.records if name in r.getMessage()]) == 1


def test_t5_valid_values_are_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PDF_SPLIT_ENABLED", "off")
    monkeypatch.setenv("PDF_SPLIT_MIN_PAGES", "50")
    monkeypatch.setenv("PDF_SPLIT_OVERLAP_PAGES", "0")
    cfg = SplitConfig.from_env()
    assert (cfg.enabled, cfg.min_pages, cfg.overlap_pages) == (False, 50, 0)


def _page_size(rng: random.Random, profile: str) -> int:
    if profile == "flat":
        return rng.randint(1_000, 60_000)
    if profile == "spiky":
        return rng.choice((2_000, 2_000, 2_000, 9 * MIB))
    return rng.randint(0, 30 * MIB)


@pytest.mark.parametrize("overlap", [0, 1])
def test_the_overlap_pages_count_toward_the_byte_cap(overlap: int) -> None:
    cfg = SplitConfig(part_pages=500 + overlap, overlap_pages=overlap)  # a part owns 500 pages whatever the overlap
    sizes = [10_000] * 1200
    sizes[500] = 30 * MIB

    plan = plan_parts(1200, sizes, cfg)

    assert [(p.page_start, p.page_end, p.overlap_end) for p in plan.parts] == [
        (1, 500, 500),
        (501, 501, 501),
        (502, 1001, 1001 + overlap),
        (1002, 1200, 1200),
    ]


def test_an_overlap_page_that_fits_is_carried_and_one_that_would_overflow_ends_the_core_earlier() -> None:
    cfg = SplitConfig(max_part_bytes=10 * MIB, part_pages=100, overlap_pages=1)
    sizes = [MIB] * 30

    plan = plan_parts(30, sizes, cfg)

    assert [(p.page_start, p.page_end, p.overlap_end) for p in plan.parts] == [
        (1, 9, 10),
        (10, 18, 19),
        (19, 27, 28),
        (28, 30, 30),
    ]


def test_a_single_page_over_the_cap_carries_no_overlap() -> None:
    cfg = SplitConfig(max_part_bytes=10 * MIB, overlap_pages=2)
    sizes = [MIB, 30 * MIB, MIB, MIB]

    plan = plan_parts(4, sizes, cfg)

    assert [(p.page_start, p.page_end, p.overlap_end) for p in plan.parts] == [(1, 1, 1), (2, 2, 2), (3, 4, 4)]


# ---------------------------------------------------------------- a part FILE never exceeds the page cap

OCR_PAGE_CAP = 500  # VLM_OCR_MAX_PAGES: the VLM OCR strategy refuses a file of one page more


@pytest.mark.parametrize("pages", [501, 1000, 1001, 1138, 17_153])
@pytest.mark.parametrize("overlap", [0, 1, 2])
def test_every_part_file_including_its_overlap_fits_the_ocr_page_cap(pages: int, overlap: int) -> None:
    cfg = SplitConfig(overlap_pages=overlap)
    assert cfg.part_pages == OCR_PAGE_CAP

    plan = plan_parts(pages, None, cfg)

    assert [p.overlap_end - p.page_start + 1 for p in plan.parts if p.overlap_end - p.page_start + 1 > OCR_PAGE_CAP] == []
    assert plan.parts[0].page_start == 1 and plan.parts[-1].page_end == pages
    assert plan == plan_parts(pages, None, cfg), "the plan, and so its digest, is a pure function"


def test_a_part_owns_at_least_one_page_whatever_the_overlap() -> None:
    cfg = SplitConfig(part_pages=1, overlap_pages=3)
    assert cfg.core_pages == 1
    assert [(p.page_start, p.page_end) for p in plan_parts(3, None, cfg).parts] == [(1, 1), (2, 2), (3, 3)]


def test_every_part_of_a_1001_page_file_is_within_the_limit_the_vlm_strategy_enforces() -> None:
    """The VLM strategy refuses a file with more pages than ``vlm_ocr_max_pages()`` (``vlm_ocr_strategy.py``); the old plan
    made 501-page files of every part but the last."""
    from app.modules.parsers.pdf.ocr_limits import vlm_ocr_max_pages

    cfg = SplitConfig()
    plan = plan_parts(1001, None, cfg)
    assert max(p.overlap_end - p.page_start + 1 for p in plan.parts) <= vlm_ocr_max_pages()
