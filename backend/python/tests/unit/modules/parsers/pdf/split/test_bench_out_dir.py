"""The split benchmark removes only the directory it made, never one the caller named with ``--out``."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType

BENCH = Path(__file__).resolve().parents[5] / "perf" / "bench_pdf_split.py"


def _bench() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bench_pdf_split_under_test", BENCH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("fails", [False, True], ids=["after a run", "after a failed run"])
def test_a_directory_named_with_out_is_left_alone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fails: bool) -> None:
    bench = _bench()
    mine = tmp_path / "mine"
    mine.mkdir()
    (mine / "keep.txt").write_text("the caller's file")

    async def run(pdf: str, out: str) -> dict:
        if fails:
            raise RuntimeError("boom")
        return {}

    monkeypatch.setattr(bench, "run", run)
    monkeypatch.setattr(sys, "argv", ["bench", str(tmp_path / "x.pdf"), "--out", str(mine)])
    if fails:
        with pytest.raises(RuntimeError):
            bench.main()
    else:
        bench.main()

    assert (mine / "keep.txt").read_text() == "the caller's file"


def test_a_directory_the_script_made_is_removed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bench = _bench()
    made: list[str] = []

    async def run(pdf: str, out: str) -> dict:
        made.append(out)
        return {}

    monkeypatch.setattr(bench, "run", run)
    monkeypatch.setattr(sys, "argv", ["bench", str(tmp_path / "x.pdf")])
    bench.main()

    assert made and not Path(made[0]).exists()
