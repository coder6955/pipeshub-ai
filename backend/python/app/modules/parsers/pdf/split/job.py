"""The parse-pool entry that splits a PDF: estimate, plan, cut, and re-cut what came out too big.

It runs in a worker process and returns only paths and sizes, never part bytes.
"""
from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.modules.parsers.pdf.split.cut import (
    CutPart,
    PartKey,
    cut_parts,
    estimate_page_bytes,
    replan_oversize,
)
from app.modules.parsers.pdf.split.plan import build_plan, plan_parts

if TYPE_CHECKING:
    from collections.abc import Sequence

    from app.modules.parsers.pdf.split.config import SplitConfig
    from app.modules.parsers.pdf.split.plan import SplitPlan


@dataclass(frozen=True)
class SplitResult:
    page_count: int
    digest: str
    parts: tuple[CutPart, ...]


def split_pdf(
    pdf_path: str, out_dir: str, cfg: SplitConfig, ranges: Sequence[tuple[int, int, int]] | None = None,
) -> SplitResult:
    """Split the PDF at *pdf_path* into files in *out_dir*, which the caller owns.

    With *ranges* (``(page_start, page_end, overlap_end)``, as a stored plan holds them) exactly those parts are cut and
    nothing is planned or re-cut: a generation already in progress is finished with the cut it was made with, whatever the
    settings are now.

    Raises ``PasswordProtectedError`` for an encrypted file and ``PdfSplitError`` for one pdfium
    cannot read; no part file of this call is left behind in either case.
    """
    page_bytes = estimate_page_bytes(pdf_path)
    if ranges is not None:
        plan = build_plan(len(page_bytes), ranges, cfg)
        parts = cut_parts(pdf_path, plan, out_dir, max_part_bytes=cfg.max_part_bytes, done={})
        return SplitResult(plan.page_count, plan.digest, tuple(parts))
    plan = plan_parts(len(page_bytes), page_bytes, cfg)
    done: dict[PartKey, CutPart] = {}
    try:
        while True:
            parts = cut_parts(pdf_path, plan, out_dir, max_part_bytes=cfg.max_part_bytes, done=done)
            smaller = replan_oversize(plan, parts, cfg)
            if _ranges(smaller) == _ranges(plan):
                return SplitResult(plan.page_count, plan.digest, tuple(parts))
            kept = {(p.page_start, p.page_end, p.overlap_end) for p in smaller.parts}
            done = {part.key: part for part in parts if part.key in kept}
            for part in parts:
                if part.key not in kept:
                    _remove(part.path)
            plan = smaller
    except BaseException:
        for part in done.values():
            _remove(part.path)
        raise


def _ranges(plan: SplitPlan) -> list[tuple[int, int, int]]:
    return [(p.page_start, p.page_end, p.overlap_end) for p in plan.parts]


def _remove(path: str) -> None:
    with contextlib.suppress(OSError):
        os.remove(path)
