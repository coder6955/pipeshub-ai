"""T8: the split job through the parse pool."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from app.modules.parsers import parse_pool
from app.modules.parsers.pdf.split.config import SplitConfig
from app.modules.parsers.pdf.split.job import SplitResult, split_pdf
from app.services.resource_governor.kinds import ParseKind
from tests.support import pdf_samples

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(autouse=True)
def _one_worker(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv(parse_pool.WORKERS_ENV, "1")
    monkeypatch.delenv("PARSE_LANES", raising=False)
    governor = MagicMock()
    governor.ceilings = SimpleNamespace(heavy=2)
    parse_pool.set_resource_governor(governor)
    try:
        yield
    finally:
        parse_pool.shutdown_parse_pool()
        parse_pool.set_resource_governor(None)


def test_t8_the_job_returns_paths_and_sizes_from_a_worker_process(tmp_path: Path) -> None:
    source = tmp_path / "source.pdf"
    source.write_bytes(pdf_samples.numbered_pdf(25))
    out = tmp_path / "out"
    cfg = SplitConfig(part_pages=11, overlap_pages=1)  # a part owns ten pages and carries one

    async def run() -> tuple[object, object]:
        worker = await parse_pool.submit(os.getpid, label="pid", kind=ParseKind.PDF_SPLIT)
        done = await parse_pool.submit(
            split_pdf, str(source), str(out), cfg, label="split", size=source.stat().st_size,
            kind=ParseKind.PDF_SPLIT,
        )
        return worker, done

    worker_pid, result = asyncio.run(run())

    assert worker_pid != os.getpid()

    assert isinstance(result, SplitResult)
    assert result.page_count == 25
    assert [(p.page_start, p.page_end, p.overlap_end) for p in result.parts] == [
        (1, 10, 11), (11, 20, 21), (21, 25, 25),
    ]
    for part in result.parts:
        assert os.path.getsize(part.path) == part.size_bytes
        assert isinstance(part.path, str)
    assert len(result.digest) == 64


def test_t8_a_worker_imports_of_the_split_job_stay_light() -> None:
    probe = (
        "import sys; import app.modules.parsers.pdf.split.job; import app.modules.parsers.pdf.split.config; "
        "print([m for m in ('langchain_core', 'transformers', 'torch', 'docling') if m in sys.modules])"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True, text=True, check=True, timeout=25, cwd=parse_pool._PYTHON_ROOT,
    )
    assert result.stdout.strip() == "[]"


def test_a_cut_by_stored_ranges_makes_exactly_those_parts_whatever_the_settings_are_now(tmp_path: Path) -> None:
    source = tmp_path / "source.pdf"
    source.write_bytes(pdf_samples.numbered_pdf(25))
    cfg = SplitConfig(part_pages=4, overlap_pages=1)  # today's settings would cut ten-page parts into threes

    result = split_pdf(str(source), str(tmp_path / "out"), cfg, [(11, 20, 21)])

    assert result.page_count == 25
    assert [(p.page_start, p.page_end, p.overlap_end) for p in result.parts] == [(11, 20, 21)]
    assert os.path.getsize(result.parts[0].path) == result.parts[0].size_bytes
