"""Which points of a virtual record one indexing write owns.

An unsplit file owns every point of its ``virtualRecordId``. A split file's points all carry the file's
``virtualRecordId`` ``V`` (so access, membership and file-level deletes stay as they are) and a
``segmentVirtualRecordId``: ``V`` for the manifest, ``V_k`` for part k. A write deletes and resumes only
inside its segment, so one part never touches another's points.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from app.exceptions.indexing_exceptions import SplitScopeError
from app.modules.indexing.pdf_kind import is_pdf
from app.modules.indexing.split_parent_flag import split_parent_gen
from app.modules.indexing.split_state import cached_part_info, is_part, part_info

if TYPE_CHECKING:
    from app.modules.indexing.split_state import PartInfo

class LiveGeneration(Protocol):
    async def __call__(self, v: str) -> str | None:
        """The live generation of split file *v*, or None when it is not split. Raises when it cannot be read."""


_lookup: dict[str, LiveGeneration | None] = {"live_gen": None}


def use_live_generation(lookup: LiveGeneration | None) -> None:
    """How a write outside the manifest's attempt learns that its file is split (set once by the process wiring)."""
    _lookup["live_gen"] = lookup


SEGMENT_KEY = "segmentVirtualRecordId"
GEN_KEY = "splitGen"
SEGMENT_INDEX_KEY = "segmentIndex"


@dataclass(frozen=True)
class WriteScope:
    content_vrid: str  # whose stored blocks the write indexes; point ids derive from it
    read_vrid: str  # the file's V: payload, access and membership
    segment: str | None = None
    gen: str | None = None
    segment_index: int | None = None

    @classmethod
    def unsplit(cls, vrid: str) -> WriteScope:
        if split_parent_gen():
            raise SplitScopeError(
                "a write to a split file named no segment", details={"virtual_record_id": vrid},
            )
        return cls(vrid, vrid)

    @classmethod
    def manifest(cls, v: str, gen: str) -> WriteScope:
        return cls(v, v, segment=v, gen=gen, segment_index=0)

    @classmethod
    def part(cls, info: PartInfo) -> WriteScope:
        return cls(info.vrid, info.root, segment=info.vrid, gen=info.gen, segment_index=info.k)

    @classmethod
    async def of_record(
        cls, record: object, gen: str | None = None, vrid: str | None = None, *, write: bool = True,
    ) -> WriteScope:
        """The scope of *record*'s writes: its part document says it is a part, the manifest being indexed
        names its generation (or the caller does: the classify stage runs after the manifest's context is gone),
        and a *write* that names none asks the file's row, so no write path of a split file can land unscoped
        (a restore, a backfill, a stage of the parent). Anything else owns its whole vrid. A part whose document,
        or a file whose row, cannot be read raises. A delete passes ``write=False``: a file-level purge owns
        every segment."""
        vrid = vrid or record.virtual_record_id  # type: ignore[attr-defined]
        if is_part(record):
            info = cached_part_info(record) or await part_info(record)
            return cls.part(info)
        gen = gen or split_parent_gen()
        lookup = _lookup["live_gen"]
        # Only a PDF is ever split, so a record of another type costs no read.
        mime = getattr(record, "mime_type", None)
        if not gen and write and lookup is not None and (not isinstance(mime, str) or is_pdf(getattr(record, "extension", None), mime)):
            gen = await lookup(vrid)
        return cls.manifest(vrid, gen) if gen else cls(vrid, vrid)

    @property
    def is_split(self) -> bool:
        return self.segment is not None

    @property
    def is_part(self) -> bool:
        return self.is_split and self.segment != self.read_vrid

    @property
    def writes_summary(self) -> bool:
        """A part is classified into its stored record only; the file's summary point is the parent's."""
        return not self.is_part

    @property
    def reconciles(self) -> bool:
        """A part is never updated in place: a new revision is a new generation."""
        return not self.is_part

    def must(self) -> dict[str, Any]:
        """The filter (beyond org and block ids) that selects the points this write owns."""
        if not self.is_split:
            return {"virtualRecordId": self.read_vrid}
        return {"virtualRecordId": self.read_vrid, SEGMENT_KEY: self.segment}

    def payload(self, metadata: dict[str, Any]) -> dict[str, Any]:
        out = {**metadata, "virtualRecordId": self.read_vrid}
        if self.is_split:
            out[SEGMENT_KEY] = self.segment
            out[GEN_KEY] = str(self.gen)
            out[SEGMENT_INDEX_KEY] = self.segment_index
        return out
