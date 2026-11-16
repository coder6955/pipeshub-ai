"""D-26: the page count a split decision rests on comes from a pdfium-only open, in a parse worker."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.exceptions.indexing_exceptions import PasswordProtectedError
from app.modules.indexing.split_seam import count_pdf_pages
from app.modules.parsers import parse_pool
from app.modules.parsers.pdf.split.count import count_pages
from app.modules.parsers.pdf.split.cut import PdfSplitError
from tests.support import pdf_samples


@pytest.fixture
def _one_worker(monkeypatch: pytest.MonkeyPatch):  # noqa: ANN202
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


def test_the_count_is_pdfiums() -> None:
    assert count_pages(pdf_samples.numbered_pdf(7)) == 7


def test_a_locked_file_is_password_protected() -> None:
    with pytest.raises(PasswordProtectedError):
        count_pages(pdf_samples.password_protected_pdf())


def test_a_file_pdfium_cannot_open_is_a_split_error() -> None:
    with pytest.raises(PdfSplitError):
        count_pages(b"not a pdf at all")


def test_the_worker_answers_the_count_and_none_for_an_unopenable_file(_one_worker: None) -> None:
    async def run() -> tuple[int | None, int | None]:
        return (
            await count_pdf_pages(pdf_samples.numbered_pdf(5), "five"),
            await count_pdf_pages(b"not a pdf at all", "broken"),
        )

    assert asyncio.run(run()) == (5, None)


def test_a_locked_file_raises_password_protected_through_the_worker(_one_worker: None) -> None:
    with pytest.raises(PasswordProtectedError):
        asyncio.run(count_pdf_pages(pdf_samples.password_protected_pdf(), "locked"))


def test_the_worker_imports_of_the_count_stay_light() -> None:
    import subprocess
    import sys

    probe = (
        "import sys; import app.modules.parsers.pdf.split.count; "
        "bad=[m for m in ('pdfminer','pdfplumber','docling','torch','fitz') if m in sys.modules]; "
        "print(bad); sys.exit(1 if bad else 0)"
    )
    done = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert done.returncode == 0, done.stdout + done.stderr


def test_the_count_is_charged_as_an_inspection_not_as_a_cut(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every PDF is counted, so a count charged as a cut (6.5 times the file's size, 0.8 GiB for 100 MB) would
    tax every upload. It is measured at milliseconds and a few MiB above the bytes held, like an inspection."""
    from unittest.mock import AsyncMock

    from app.services.resource_governor.kinds import ParseKind

    submit = AsyncMock(return_value=3)
    monkeypatch.setattr(parse_pool, "submit", submit)

    assert asyncio.run(count_pdf_pages(b"%PDF", "x")) == 3

    assert submit.await_args.kwargs["kind"] is ParseKind.INSPECT


def test_a_count_that_times_out_is_raised_and_never_read_as_an_unopenable_file(monkeypatch: pytest.MonkeyPatch) -> None:
    """``None`` sends the file to the whole-file inspection, which is what runs a pathological file out of memory."""
    from unittest.mock import AsyncMock

    monkeypatch.setattr(parse_pool, "submit", AsyncMock(side_effect=parse_pool.ParseWorkerError("overran", "timed_out")))

    with pytest.raises(parse_pool.ParseWorkerError):
        asyncio.run(count_pdf_pages(b"%PDF", "x"))
