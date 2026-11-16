"""The parent attempt of a split PDF: cut it into parts, write their nodes and files, queue them.

``ParentSplitSeam.split`` is the whole of steps 2 to 5 of the parent path. Every step is safe to
run again, so a parent attempt that is redelivered after a crash at any point finishes the work:
nodes are created only when absent, a file is uploaded only for a node that has none, and a part
is queued only while it is ``NOT_STARTED``.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from app.config.constants.arangodb import CollectionNames, ProgressStatus
from app.exceptions.indexing_exceptions import IndexingError
from app.modules.indexing.owner_connector import part_connector_id
from app.modules.indexing.split_owner import owner_of
from app.modules.indexing.split_parts import part_event, part_file_record
from app.modules.indexing.split_purge import FILE_REMOVED_REASON
from app.modules.indexing.split_reindex import (
    HEALED_PREFIX,
    ReindexAction,
    single_file_reindex_action,
    supersedes,
)
from app.modules.indexing.split_state import (
    PartInfo,
    SplitRow,
    SplitState,
    SplitStore,
    derive_gen,
    part_specs,
)
from app.services.messaging.config import Topic

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from app.modules.indexing.split_purge import GenerationPurger
    from app.modules.indexing.split_state import PartSpec
    from app.modules.parsers.pdf.split.config import SplitConfig
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

RECORDS = CollectionNames.RECORDS.value
PART_PDF_ROOT = "parts"

_logger = logging.getLogger(__name__)


class PartPurgeBusy(IndexingError):
    """An older generation still has a part being indexed; the parent is tried again shortly."""


@dataclass(frozen=True)
class CutFile:
    page_start: int
    page_end: int
    overlap_end: int
    path: str
    size_bytes: int
    # S1's reason a part is over the byte cap; a SINGLE_PAGE one stays a valid part and costs what the governor charges it.
    oversize_reason: str | None = None


@dataclass(frozen=True)
class CutOutcome:
    page_count: int
    digest: str
    files: tuple[CutFile, ...]
    cleanup: Callable[[], None]


class PdfCutter(Protocol):
    async def __call__(
        self, content: bytes, label: str, cfg: SplitConfig, ranges: Sequence[tuple[int, int, int]] | None = None,
    ) -> CutOutcome:
        """Cut by the settings, or (with *ranges*) exactly those ``(page_start, page_end, overlap_end)`` parts."""
        ...


class PartUploader(Protocol):
    async def save(self, org_id: str, document_path: str, data: bytes, *, v: str, owner_connector_id: str) -> str:
        """Store one part's PDF and return its storage document id."""
        ...


class EventSender(Protocol):
    async def send_event(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> bool: ...


class FailedPartsRetry(Protocol):
    async def __call__(self, v: str, *, owner_connector_id: str) -> int:
        """Republish the failed parts of *v*'s live generation; returns how many were queued."""
        ...


class WholeFileCleaner(Protocol):
    async def delete_stray_points(self, parent: Mapping[str, Any], row: SplitRow) -> None:
        """Drop the file's points that belong to no segment of *row*'s generation (the unsplit file's points). The
        points of the generation's own segments are never touched, so an attempt that joins a generation another
        attempt is indexing cannot remove its parts."""
        ...

    async def has_stray_points(self, parent: Mapping[str, Any], row: SplitRow) -> bool:
        """Whether the file's points include any that belong to no segment of its live generation (one existence
        check, not a count). A build that knows nothing of parts writes such points when it indexes the file whole;
        every write of this build to a split file carries a segment, so nothing else does."""
        ...


@dataclass(frozen=True)
class SplitRequest:
    parent: Mapping[str, Any]
    v: str
    org_id: str
    content: bytes
    content_rev: str
    page_count: int
    needs_ocr: bool | None  # None: the plan has no hint, each part is inspected on its own
    nonce: str = ""
    # False when splitting is switched off: the file keeps the generation it has and is never cut again.
    may_heal: bool = True


@dataclass(frozen=True)
class SplitOutcome:
    row: SplitRow
    reused_generation: bool


class ParentSplitSeam:
    def __init__(
        self,
        *,
        graph: IGraphDBProvider,
        store: SplitStore,
        purger: GenerationPurger,
        cutter: PdfCutter,
        uploader: PartUploader,
        publisher: EventSender,
        whole_file: WholeFileCleaner,
        config: Callable[[], SplitConfig],
        retry_failed: FailedPartsRetry | None = None,
    ) -> None:
        self._graph = graph
        self._store = store
        self._purger = purger
        self._cutter = cutter
        self._uploader = uploader
        self._publisher = publisher
        self._whole_file = whole_file
        self._config = config
        self._retry_failed = retry_failed

    async def unsplit(self, v: str) -> bool:
        """Retire the parts of a file that is no longer split. True when a generation existed."""
        row = await self._store.get(v)
        if row is None:
            return False
        await self._retire(row)
        return True

    async def split(self, req: SplitRequest) -> SplitOutcome:
        current = await self._store.get(req.v)
        action = single_file_reindex_action(current, content_rev=req.content_rev, nonce=req.nonce)
        if current is not None and action is ReindexAction.NOTHING and req.may_heal and await self._whole_file.has_stray_points(req.parent, current):
            # A build that knows nothing of parts indexed the file whole after its parts completed (a rollback, then a
            # roll-forward): points under the file's id that belong to no segment of the generation are the only
            # evidence, so the generation no longer describes what is stored and the file is split again. A restore,
            # a vector rebuild and a copy of the file leave every point in a segment and never get here.
            _logger.warning("The split of %s was overwritten by a whole-file index; splitting it again", req.v)
            req = replace(req, nonce=f"{HEALED_PREFIX}{current.gen}")
            action = ReindexAction.CONTINUE
        if current is not None and action is not ReindexAction.CONTINUE:
            if action is ReindexAction.RETRY_FAILED:
                await self._retry_failed_parts(current)
            return SplitOutcome(current, reused_generation=True)
        row, reused = await self._current_generation(req)
        cut: CutOutcome | None = None
        try:
            if row is None:
                cut = await self._cut(req)
                row = await self._create_row(req, cut)
            await self._ensure_nodes(row, req.parent)
            if row.state is SplitState.SPLITTING:
                # Whatever the file was indexed as before, the manifest and the parts are all that stay under V. Only what
                # lies outside this generation's segments goes, and only once the row exists, so a second attempt that
                # joins the generation deletes nothing the first has indexed; and before any part is queued.
                await self._whole_file.delete_stray_points(req.parent, row)
            missing = await self._parts_without_file(row)
            if missing:
                # A part that has no file is cut from the page range the row stores, never from the settings of today: the
                # generation's plan is the row's, and a setting changed since (or another build's planner) cannot make this
                # attempt disagree with it.
                cut = cut or await self._cut(req, [(s.page_start, s.page_end, s.overlap_end) for s in missing])
                self._check_plan(row, cut)
                await self._upload(row, missing, cut)
        finally:
            if cut is not None:
                cut.cleanup()
        await self.queue_parts(row)
        return SplitOutcome(row, reused)

    async def _retry_failed_parts(self, row: SplitRow) -> None:
        if self._retry_failed is None:
            raise IndexingError("no retry of failed parts is wired", details={"virtual_record_id": row.v})
        owner = await owner_of(self._graph, self._store, row)
        await self._retry_failed(row.v, owner_connector_id=owner.connector)

    async def queue_parts(self, row: SplitRow) -> None:
        """Queue every part that has its file and has not been queued, then mark the row indexing."""
        await self._publish_unqueued(row)
        await self._enter_indexing(row)

    async def parts_missing_files(self, row: SplitRow) -> int:
        return len(await self._parts_without_file(row))

    async def _current_generation(self, req: SplitRequest) -> tuple[SplitRow | None, bool]:
        """The row this parent attempt continues, or None after retiring one it must replace."""
        row = await self._store.get(req.v)
        if row is None:
            return None, False
        # An event with no nonce (a stale-recovery republish, a connector update of the same content) continues what is
        # running, forced or not; only a newer forced reindex replaces a running generation, never a redelivery of an older one.
        same_attempt = not supersedes(req.nonce, row.nonce)
        if row.state is not SplitState.PURGING and row.content_rev == req.content_rev and same_attempt:
            return row, True
        await self._retire(row)
        return None, False

    async def _retire(self, row: SplitRow) -> None:
        result = await self._purger.purge_generation(row)
        if not result.complete:
            raise PartPurgeBusy(
                f"{result.busy} part(s) of an older generation of {row.v} are still being indexed",
                details={"virtual_record_id": row.v},
            )

    async def _cut(self, req: SplitRequest, ranges: Sequence[tuple[int, int, int]] | None = None) -> CutOutcome:
        return await self._cutter(req.content, str(req.parent.get("recordName") or req.v), self._config(), ranges)

    async def _create_row(self, req: SplitRequest, cut: CutOutcome) -> SplitRow:
        gen = derive_gen(req.v, req.content_rev, cut.digest, req.nonce)
        ranges = [(f.page_start, f.page_end, f.overlap_end) for f in cut.files]
        row = SplitRow(
            v=req.v, org_id=req.org_id, gen=gen, parts=part_specs(gen, ranges, ocr=req.needs_ocr),
            state=SplitState.SPLITTING, content_rev=req.content_rev, plan_digest=cut.digest, nonce=req.nonce,
        )
        if await self._store.create(row):
            return row
        current = await self._store.get(req.v)
        if current is None or current.gen != gen:
            raise PartPurgeBusy(f"another attempt is splitting {req.v}", details={"virtual_record_id": req.v})
        return current

    async def _ensure_nodes(self, row: SplitRow, parent: Mapping[str, Any]) -> None:
        # The part documents first: a record under the parts connector without one is never acted on, a document
        # without its record is harmless, and a redelivery finds both written or writes what is missing.
        owner = str(parent.get("connectorId") or "")
        await self._store.put_parts(
            PartInfo.of(spec, v=row.v, gen=row.gen, org_id=row.org_id, owner_connector_id=owner) for spec in row.parts
        )
        present = await self._nodes(row)
        records = [
            part_file_record(spec, gen=row.gen, parent=parent, storage_document_id=None, size_bytes=0)
            for spec in row.parts if spec.vrid not in present
        ]
        if records:
            await self._graph.batch_upsert_records(records)

    async def _nodes(self, row: SplitRow) -> dict[str, dict[str, Any]]:
        """The part records of *row* that exist, by part virtual record id, in one read."""
        docs = await self._graph.get_nodes_by_field_in(
            RECORDS, "virtualRecordId", [spec.vrid for spec in row.parts], raise_on_error=True,
        )
        return {str(d["virtualRecordId"]): d for d in docs or [] if d.get("virtualRecordId")}

    async def _parts_without_file(self, row: SplitRow) -> list[PartSpec]:
        nodes = await self._nodes(row)
        return [spec for spec in row.parts if not (nodes.get(spec.vrid) or {}).get("storageDocumentId")]

    @staticmethod
    def _check_plan(row: SplitRow, cut: CutOutcome) -> None:
        """Every file cut is one of the row's parts, as the row stores it: an invariant of a cut made from those ranges. A file
        that is not (the file's pages changed under the same content revision) is never uploaded as a part of this generation."""
        stored = {(s.page_start, s.page_end, s.overlap_end) for s in row.parts}
        cut_ranges = {(f.page_start, f.page_end, f.overlap_end) for f in cut.files}
        if not cut_ranges <= stored or cut.page_count != row.parts[-1].page_end:
            raise PartPurgeBusy(
                f"the cut of {row.v} does not match its stored plan", details={"virtual_record_id": row.v},
            )

    async def _upload(self, row: SplitRow, specs: Sequence[PartSpec], cut: CutOutcome) -> None:
        by_range = {(f.page_start, f.page_end, f.overlap_end): f for f in cut.files}
        for spec in specs:
            await self._upload_part(row, spec, by_range[(spec.page_start, spec.page_end, spec.overlap_end)])

    async def _upload_part(self, row: SplitRow, spec: PartSpec, file: CutFile) -> None:
        if file.oversize_reason:
            _logger.warning("Part %d of %s is over the byte cap (%s); indexed as it is", spec.k, row.v, file.oversize_reason)
        data = await _read(file.path)
        document_id = await self._uploader.save(
            row.org_id, f"{PART_PDF_ROOT}/{row.v}/{row.gen}/{spec.k}", data,
            v=row.v, owner_connector_id=part_connector_id(row.org_id),
        )
        await self._graph.update_node_fields_if_match(
            spec.node_key, RECORDS, {"storageDocumentId": document_id, "sizeInBytes": file.size_bytes},
            {"virtualRecordId": spec.vrid},
        )

    async def _publish_unqueued(self, row: SplitRow) -> None:
        # Resolved from the file's live holders, so a copy of the file in another connector keeps its parts moving
        # after the first copy's connector is gone.
        owner = await owner_of(self._graph, self._store, row)
        queued: list[str] = []
        nodes = await self._nodes(row)
        for spec in row.parts:
            node = nodes.get(spec.vrid)
            if node is None or not node.get("storageDocumentId"):
                continue
            if spec.node_key not in owner.documented:
                raise IndexingError(
                    f"part {spec.node_key} has no part document naming its owner; it is not queued", details={"record_id": spec.node_key},
                )
            if await self._revive_if_parent_returned(spec, node):
                node = {**node, "indexingStatus": ProgressStatus.NOT_STARTED.value}
            if node.get("indexingStatus") != ProgressStatus.NOT_STARTED.value:
                continue
            # The owner the event names is the one the part documents now name, which every gate reads, so the two agree.
            event_type, payload = part_event({**node, "_key": spec.node_key}, owner_connector_id=owner.connector)
            if not await self._publisher.send_event(Topic.RECORD_EVENTS.value, event_type, payload, key=spec.node_key):
                raise IndexingError("the message broker did not accept a part's event", details={"record_id": spec.node_key})
            queued.append(spec.node_key)
        if queued:
            # After the publish, never before: a part marked QUEUED for an event that failed to publish waits for nobody.
            try:
                await self._graph.compare_and_set_indexing_status(
                    queued, ProgressStatus.NOT_STARTED.value, ProgressStatus.QUEUED.value,
                )
            except Exception:
                # Not fatal (the events are out), but every redelivery publishes these parts again until it succeeds.
                _logger.warning(
                    "Could not mark %d part(s) of %s QUEUED after publishing them; a redelivery publishes them again",
                    len(queued), row.v, exc_info=True,
                )

    async def _revive_if_parent_returned(self, spec: PartSpec, node: Mapping[str, Any]) -> bool:
        """A part stopped because its file was removed starts again once the file is back (a restore)."""
        if node.get("indexingStatus") != ProgressStatus.AUTO_INDEX_OFF.value or node.get("reason") != FILE_REMOVED_REASON:
            return False
        return await self._graph.update_node_fields_if_match(
            spec.node_key, RECORDS, {"indexingStatus": ProgressStatus.NOT_STARTED.value, "reason": None},
            {"indexingStatus": ProgressStatus.AUTO_INDEX_OFF.value, "reason": FILE_REMOVED_REASON},
        )

    async def _enter_indexing(self, row: SplitRow) -> None:
        await self._store.move(row.v, row.gen, SplitState.SPLITTING, SplitState.INDEXING_PARTS)


async def _read(path: str) -> bytes:
    return await asyncio.to_thread(Path(path).read_bytes)


async def pool_cutter(
    content: bytes, label: str, cfg: SplitConfig, ranges: Sequence[tuple[int, int, int]] | None = None,
) -> CutOutcome:
    """Cut *content* in a parse worker, from a file of its own so the worker never holds the bytes twice."""
    from app.modules.parsers import parse_pool
    from app.modules.parsers.pdf.split.job import split_pdf
    from app.services.resource_governor.kinds import ParseKind

    work = tempfile.mkdtemp(prefix="pdf-split-")
    source = str(Path(work) / "source.pdf")
    out_dir = str(Path(work) / "parts")

    def cleanup() -> None:
        shutil.rmtree(work, ignore_errors=True)

    try:
        await _write(source, content)
        result = await parse_pool.submit(
            split_pdf, source, out_dir, cfg, tuple(ranges) if ranges is not None else None, label=label, size=len(content), kind=ParseKind.PDF_SPLIT,
            timeout=_split_timeout_seconds(),
        )
    except BaseException:
        cleanup()
        raise
    files = tuple(CutFile(
        p.page_start, p.page_end, p.overlap_end, p.path, p.size_bytes,
        p.oversize_reason.value if p.oversize_reason else None,
    ) for p in result.parts)
    return CutOutcome(result.page_count, result.digest, files, cleanup)


async def _write(path: str, content: bytes) -> None:
    await asyncio.to_thread(Path(path).write_bytes, content)


def _split_timeout_seconds() -> float:
    raw = (os.environ.get("PDF_SPLIT_TIMEOUT_SECONDS") or "").strip()
    try:
        return float(raw) if raw and float(raw) > 0 else 1800.0
    except ValueError:
        return 1800.0


def manifest_markdown(row: SplitRow, record_name: str, outline: Sequence[str] = ()) -> str:
    """The text the parent keeps under V: its name, how many pages it has, then its outline. How the file was cut for
    indexing is not in it: the model is told pages, never the units of work."""
    lines = [f"# {record_name}", "", f"This document has {row.page_count} pages."]
    if outline:
        lines += ["", "## Outline", *outline]
    return "\n".join(lines)


async def count_pdf_pages(content: bytes, label: str = "") -> int | None:
    """Pages of *content* from a pdfium-only open in a parse worker, ahead of any whole-file inspection.

    ``None`` only when pdfium cannot open the file, so the caller inspects it as it always did. Raises
    ``PasswordProtectedError`` for a locked file. A count that timed out, a worker that died or a stopped caller is raised
    as it is, so the record is retried under the normal attempt budget and never inspected whole: a file whose count
    overran is the one the whole-file inspection would run out of memory on.
    """
    from app.exceptions.indexing_exceptions import PasswordProtectedError
    from app.modules.parsers import parse_pool
    from app.modules.parsers.pdf.split.count import count_pages
    from app.services.resource_governor.kinds import ParseKind

    try:
        return await parse_pool.submit(
            count_pages, content, label=label, size=len(content), kind=ParseKind.INSPECT, timeout=_COUNT_TIMEOUT_SECONDS,
        )
    except PasswordProtectedError:
        raise
    except (parse_pool.ParseWorkerError, parse_pool.ParseAbandonedError, MemoryError, ImportError):
        raise
    except Exception:
        _logger.info("pdfium could not count the pages of %s; it is inspected whole", label, exc_info=True)
        return None


_COUNT_TIMEOUT_SECONDS = 120.0
# Every PDF is counted before it is parsed, so the count is charged as a PDF inspection (a flat lane charge and the
# light PDF permit, ``ParseKind.INSPECT``), not as a cut (``PDF_SPLIT``, 6.5 times the file's size). Measured on the 0.7 MB,
# 5.7 MB and 166.6 MB fixtures: 4 ms, 1 ms and 445 ms, and 46 MiB of memory above the bytes the caller already holds.
