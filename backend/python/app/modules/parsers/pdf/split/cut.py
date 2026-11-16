"""Writing the parts of a split PDF, one at a time, with pypdfium2.

Runs in a parse worker (see ``job``): keep imports to the PDF libraries and light modules.
"""
from __future__ import annotations

import contextlib
import logging
import os
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

import pypdfium2
import pypdfium2.raw as pdfium_raw

from app.exceptions.indexing_exceptions import (
    DocumentProcessingError,
    PasswordProtectedError,
)
from app.modules.parsers.encryption import (
    PASSWORD_PROTECTED_MESSAGE,
    open_pdfium_document,
    pdfium_password_error,
)
from app.modules.parsers.pdf.split.plan import (
    PartPlan,
    SplitPlan,
    build_plan,
    overlap_end_for,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from app.modules.parsers.pdf.split.config import SplitConfig

_logger = logging.getLogger(__name__)

# Reading the raw stream size of every image of a page costs about 1 ms per page on a
# scanned file; a sample keeps the estimate flat for a 17,000 page one. A heavy page
# the sample misses is caught by the byte check on the part that holds it.
ESTIMATE_SAMPLE_PAGES = 2000

# (page_start, page_end, overlap_end): the pages a part file holds.
PartKey = tuple[int, int, int]


class PdfSplitError(DocumentProcessingError):
    """The PDF could not be opened or cut."""


class OversizeReason(StrEnum):
    """Why a cut part is over the byte cap."""

    SINGLE_PAGE = "single_page"  # one page and nothing carried with it: it cannot be made smaller
    SPLITTABLE = "splittable"  # a smaller core or a shorter overlap would fit


@dataclass(frozen=True)
class CutPart:
    index: int
    path: str
    size_bytes: int
    page_start: int
    page_end: int
    overlap_end: int
    oversize_reason: OversizeReason | None = None

    @property
    def oversize(self) -> bool:
        return self.oversize_reason is not None

    @property
    def key(self) -> PartKey:
        return (self.page_start, self.page_end, self.overlap_end)


def estimate_page_bytes(pdf_path: str) -> list[int]:
    """Estimated bytes of each page, summing to the file size.

    Raw image stream sizes of a sample of pages (read without decoding), interpolated
    between the samples; whatever the images do not account for is spread evenly.

    Known limit: a resource shared by many pages (one large background image drawn on every
    page) is counted on each page that draws it, then scaled down to the file size, so the
    estimate is low for such a file and its parts come out larger than planned. The byte check
    on each cut part and the re-cut are the backstop.
    """
    document = open_document(pdf_path)
    try:
        count = len(document)
        if count == 0:
            return []
        indices = _sample_indices(count, ESTIMATE_SAMPLE_PAGES)
        samples = [_image_bytes(document, i) for i in indices]
    finally:
        document.close()
    return _scale_to_file(_interpolate(count, indices, samples), os.path.getsize(pdf_path))


def cut_parts(
    pdf_path: str,
    plan: SplitPlan,
    out_dir: str,
    *,
    max_part_bytes: int,
    done: Mapping[PartKey, CutPart] | None = None,
) -> list[CutPart]:
    """Write each part of *plan* to its own file in *out_dir*, closing each before the next.

    A part already in *done* (same page range) is reused rather than cut again. A part over
    *max_part_bytes* is returned with ``oversize=True``. When anything fails, the files this
    call wrote are removed before the error is raised.
    """
    done = done or {}
    source = open_document(pdf_path)
    written: list[str] = []
    try:
        if len(source) != plan.page_count:
            raise PdfSplitError(f"The PDF has {len(source)} pages, the plan {plan.page_count}")
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        parts: list[CutPart] = []
        for part in plan.parts:
            kept = done.get((part.page_start, part.page_end, part.overlap_end))
            if kept is not None:
                parts.append(replace(kept, index=part.index))
                continue
            path = str(Path(out_dir) / f"part-{part.page_start:06d}-{part.page_end:06d}.pdf")
            written.append(path)
            _save_pages(source, part.page_start, part.overlap_end, path)
            size = os.path.getsize(path)
            parts.append(
                CutPart(part.index, path, size, part.page_start, part.page_end, part.overlap_end,
                        oversize_reason=_oversize_reason(part, size, max_part_bytes))
            )
        return parts
    except BaseException:
        for path in written:
            with contextlib.suppress(OSError):
                os.remove(path)
        raise
    finally:
        source.close()


def replan_oversize(plan: SplitPlan, parts: Sequence[CutPart], cfg: SplitConfig) -> SplitPlan:
    """*plan* with each oversize part made smaller: first without the pages it carries from the next
    part (the estimate may have missed a heavy one), and if it is still over, with its core halved.
    A single page with nothing carried cannot be reduced, so a plan in which nothing can change comes
    back with the same ranges."""
    oversize = {p.key for p in parts if p.oversize}
    ranges: list[tuple[int, int, int]] = []
    for part in plan.parts:
        start, end, carried = part.page_start, part.page_end, part.overlap_end
        if (start, end, carried) not in oversize:
            ranges.append((start, end, carried))
        elif carried > end:
            ranges.append((start, end, end))
        elif end > start:
            middle = (start + end) // 2
            ranges.append((start, middle, overlap_end_for(middle, plan.page_count, cfg)))
            ranges.append((middle + 1, end, overlap_end_for(end, plan.page_count, cfg)))
        else:
            ranges.append((start, end, carried))
    return build_plan(plan.page_count, ranges, cfg)


def _oversize_reason(part: PartPlan, size: int, max_part_bytes: int) -> OversizeReason | None:
    if size <= max_part_bytes:
        return None
    single = part.page_start == part.page_end and part.overlap_end == part.page_end
    return OversizeReason.SINGLE_PAGE if single else OversizeReason.SPLITTABLE


def open_document(pdf_path: str) -> pypdfium2.PdfDocument:
    try:
        return open_pdfium_document(pdf_path)
    except pypdfium2.PdfiumError as exc:
        if pdfium_password_error(exc):
            raise PasswordProtectedError(PASSWORD_PROTECTED_MESSAGE, details={"format": "pdf"}) from exc
        raise PdfSplitError(f"pdfium could not open the PDF: {exc}") from exc


def _save_pages(source: pypdfium2.PdfDocument, first: int, last: int, path: str) -> None:
    part = pypdfium2.PdfDocument.new()
    try:
        part.import_pages(source, list(range(first - 1, last)))
        part.save(path)
    except pypdfium2.PdfiumError as exc:
        raise PdfSplitError(f"pdfium could not write pages {first}-{last}: {exc}") from exc
    finally:
        part.close()


def _image_bytes(document: pypdfium2.PdfDocument, index: int) -> int:
    try:
        page = document[index]
    except pypdfium2.PdfiumError:
        _logger.warning("Could not read page %d to estimate its size", index + 1)
        return 0
    try:
        return sum(
            max(0, pdfium_raw.FPDFImageObj_GetImageDataRaw(obj.raw, None, 0))
            for obj in page.get_objects(filter=[pdfium_raw.FPDF_PAGEOBJ_IMAGE])
        )
    finally:
        page.close()


def _sample_indices(count: int, limit: int) -> list[int]:
    """Every page up to *limit*; past it, *limit* pages evenly spread, first and last included."""
    if count <= limit:
        return list(range(count))
    return sorted({round(i * (count - 1) / (limit - 1)) for i in range(limit)})


def _interpolate(count: int, indices: Sequence[int], values: Sequence[int]) -> list[float]:
    """Each page's value, linear between the sampled pages."""
    out = [0.0] * count
    for (left, right), (low, high) in zip(
        zip(indices, indices[1:], strict=False), zip(values, values[1:], strict=False), strict=True
    ):
        for page in range(left, right):
            out[page] = low + (high - low) * (page - left) / (right - left)
    out[indices[-1]] = float(values[-1])
    return out


def _scale_to_file(image_bytes: Sequence[float], file_size: int) -> list[int]:
    """Images are never more than the file: scale them down if they are, and spread the
    rest of the file (text, fonts, structure) evenly over the pages."""
    count = len(image_bytes)
    total = sum(image_bytes)
    if total >= file_size:
        factor = file_size / total if total else 0.0
        return [int(b * factor) for b in image_bytes]
    spread = (file_size - total) / count
    return [int(b + spread) for b in image_bytes]
