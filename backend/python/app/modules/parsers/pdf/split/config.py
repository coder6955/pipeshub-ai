"""How a large PDF is split, read from the environment.

Imports only light modules: the split runs in a parse worker.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from app.modules.parsers.pdf.ocr_limits import vlm_ocr_max_pages
from app.services.resource_governor.tiers import XL_HEAVY_BYTES

_logger = logging.getLogger(__name__)

ENABLED_ENV = "PDF_SPLIT_ENABLED"
MIN_PAGES_ENV = "PDF_SPLIT_MIN_PAGES"
PART_PAGES_ENV = "PDF_SPLIT_PART_PAGES"
MAX_PART_BYTES_ENV = "PDF_SPLIT_MAX_PART_BYTES"
OVERLAP_PAGES_ENV = "PDF_SPLIT_OVERLAP_PAGES"

DEFAULT_MIN_PAGES = 500
DEFAULT_PART_PAGES = 500
DEFAULT_OVERLAP_PAGES = 1

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


@dataclass(frozen=True)
class SplitConfig:
    """A PDF with more than ``min_pages`` pages is cut into part files of at most
    ``part_pages`` pages (the pages a part carries from the next one included, so a part
    owns ``part_pages - overlap_pages`` of its own, at least one) and ``max_part_bytes``
    bytes; each part also carries the first ``overlap_pages`` pages of the next one."""

    enabled: bool = True
    min_pages: int = DEFAULT_MIN_PAGES
    part_pages: int = DEFAULT_PART_PAGES
    max_part_bytes: int = XL_HEAVY_BYTES
    overlap_pages: int = DEFAULT_OVERLAP_PAGES

    @property
    def core_pages(self) -> int:
        """Pages a part owns: the file's page cap less the overlap it carries, so a part file never exceeds ``part_pages``
        (the VLM OCR page cap refuses a file of one page more)."""
        return max(1, self.part_pages - self.overlap_pages)

    @classmethod
    def from_env(cls) -> SplitConfig:
        return cls(
            enabled=_flag(ENABLED_ENV, default=True),
            min_pages=_int(MIN_PAGES_ENV, DEFAULT_MIN_PAGES, minimum=1),
            part_pages=_int(PART_PAGES_ENV, min(DEFAULT_PART_PAGES, vlm_ocr_max_pages()), minimum=1),
            max_part_bytes=_int(MAX_PART_BYTES_ENV, XL_HEAVY_BYTES, minimum=1),
            overlap_pages=_int(OVERLAP_PAGES_ENV, DEFAULT_OVERLAP_PAGES, minimum=0),
        )


def _warn(name: str, raw: str, default: object) -> None:
    _logger.warning("Ignoring invalid %s=%r; using %r", name, raw, default)


def _int(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        value = minimum - 1
    if value < minimum:
        _warn(name, raw, default)
        return default
    return value


def _flag(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    word = raw.strip().lower()
    if word in _TRUE:
        return True
    if word in _FALSE:
        return False
    _warn(name, raw, default)
    return default
