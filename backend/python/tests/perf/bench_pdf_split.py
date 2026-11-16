"""Wall time, worker peak memory and part sizes of splitting one large PDF through the parse pool.

Run from backend/python (the app image has everything it needs):

    python tests/perf/bench_pdf_split.py /path/to/large.pdf [--out DIR] [--keep]

The split runs in a pool worker, as in production. The report is one JSON object.
Not collected by pytest (no ``test_`` prefix).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.modules.parsers import parse_pool
from app.modules.parsers.pdf.split.config import SplitConfig
from app.modules.parsers.pdf.split.job import split_pdf
from app.services.resource_governor.kinds import ParseKind


def peak_rss_mib(pid: int) -> float:
    """The process's high-water resident set, from the kernel (Linux)."""
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) / 1024
    raise RuntimeError("no VmHWM in /proc status")


async def run(pdf: Path, out_dir: str) -> dict:
    cfg = SplitConfig.from_env()
    worker_pid = await parse_pool.submit(os.getpid, label="pid", kind=ParseKind.PDF_SPLIT)
    started = time.monotonic()
    result = await parse_pool.submit(
        split_pdf, str(pdf), out_dir, cfg,
        label="pdf split bench", size=pdf.stat().st_size, kind=ParseKind.PDF_SPLIT,
    )
    wall = time.monotonic() - started
    parts = result.parts
    return {
        "file_bytes": pdf.stat().st_size,
        "pages": result.page_count,
        "parts": len(parts),
        "wall_seconds": round(wall, 2),
        "worker_peak_rss_mib": round(peak_rss_mib(worker_pid)),
        "max_part_mib": round(max(p.size_bytes for p in parts) / 2**20, 2),
        "sum_parts_mib": round(sum(p.size_bytes for p in parts) / 2**20, 1),
        "oversize_parts": sum(p.oversize for p in parts),
        "config": cfg.__dict__,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--out", help="directory for the parts (default: a temporary one)")
    parser.add_argument("--keep", action="store_true", help="keep the parts")
    args = parser.parse_args()

    os.environ.setdefault(parse_pool.WORKERS_ENV, "1")
    governor = MagicMock()
    governor.ceilings = SimpleNamespace(heavy=2)
    parse_pool.set_resource_governor(governor)
    # Only a directory this script made is removed: one passed with --out is the caller's, whatever happens.
    created = args.out is None
    out = args.out or tempfile.mkdtemp(prefix="pdf-split-bench-")
    try:
        print(json.dumps(asyncio.run(run(args.pdf, out)), indent=2))
    finally:
        parse_pool.shutdown_parse_pool()
        if created and not args.keep:
            shutil.rmtree(out, ignore_errors=True)


if __name__ == "__main__":
    main()
