"""Fakes for the query side of a split file: a resolver and the parts' stored records.

The split file ``V`` has parts ``V_1`` (pages 1-40) and ``V_2`` (pages 41-80). Both parts have a block with local
index 0 and 1, so every address collision the design is about is present.
"""
from __future__ import annotations

from app.models.blocks import BlockType
from app.modules.retrieval.hit_shape import SEGMENT_SHIFT
from app.modules.retrieval.split_content import PartLabel

V = "v-file"
V1 = "v-part-1"
V2 = "v-part-2"
GEN = "gen-live"
RECORD_ID = "rec-file"
FRONTEND = "https://app.example"
PARTS = {1: (V1, 1, 40), 2: (V2, 41, 80)}


class FakeResolver:
    """``live_generation`` / ``part_vrid`` / ``part_label`` / ``roots_of`` over an in-memory plan."""

    def __init__(self, live: dict[str, str] | None = None, *, fail: bool = False, counts: dict[int, int | None] | None = None) -> None:
        self.live = {V: GEN} if live is None else live
        self.fail = fail
        # blockCount of each part (each part of part_blob has 2 blocks)
        self.counts: dict[int, int | None] = {1: 2, 2: 2} if counts is None else counts
        self.offset_calls: list[list[tuple[str, int]]] = []
        self.live_calls: list[list[str]] = []

    async def live_generation(self, vrids: list[str]) -> dict[str, str]:
        self.live_calls.append(list(vrids))
        if self.fail:
            raise RuntimeError("store down")
        return {v: self.live[v] for v in vrids if v in self.live}

    async def part_vrid(self, v: str, k: int) -> str | None:
        return PARTS[k][0] if v == V and k in PARTS else None

    async def part_label(self, v: str, k: int) -> PartLabel | None:
        if v != V or k not in PARTS:
            return None
        return PartLabel(k=k, total=len(PARTS), page_start=PARTS[k][1], page_end=PARTS[k][2],
                         page_count=max(p[2] for p in PARTS.values()))

    async def roots_of(self, vrids: list[str]) -> dict[str, str]:
        return {p[0]: V for p in PARTS.values() if p[0] in vrids}

    async def block_offsets(self, parts: list[tuple[str, int]]) -> dict[tuple[str, int], int | None]:
        from app.modules.retrieval.hit_shape import part_offset

        self.offset_calls.append(list(parts))
        return {(v, k): part_offset(self.counts, k) if v == V else None for v, k in parts}


def point_meta(k: int | None, local: int, *, gen: str | None = GEN, **extra: object) -> dict:
    """The metadata of a point as S2b writes it: ``k`` None is an unsplit record's point."""
    meta: dict = {"virtualRecordId": V, "blockIndex": local, "isBlockGroup": False, "orgId": "org-1", **extra}
    if k is not None:
        meta.update(segmentVirtualRecordId=PARTS[k][0], segmentIndex=k, splitGen=gen)
    return meta


def part_blob(k: int) -> dict:
    """The stored record of part ``k``: what the blob store returns for ``V_k`` (graph fields come from the file)."""
    blocks = [
        {"id": f"b{k}-{i}", "index": i, "type": BlockType.TEXT.value, "data": f"part {k} block {i}",
         "citation_metadata": {"page_number": PARTS[k][1] + i}, "parent_index": None}
        for i in range(2)
    ]
    return {
        "virtual_record_id": PARTS[k][0], "id": RECORD_ID, "org_id": "org-1", "record_name": "big.pdf",
        "record_type": "FILE", "version": 1, "origin": "UPLOAD", "connector_name": "KB", "connector_id": "c1",
        "mime_type": "application/pdf", "weburl": "https://files.example/big.pdf", "semantic_metadata": {},
        "context_metadata": "Record: big.pdf", "frontend_url": FRONTEND,
        "identity_vrid": V, "segment_index": k,
        "block_containers": {"blocks": blocks, "block_groups": []},
    }


def address(k: int, local: int) -> int:
    return (k << SEGMENT_SHIFT) + local


PART_NODE = {1: "part-node-key-1", 2: "part-node-key-2"}
PARTS_CONNECTOR = "pdfparts_org-1"


def raw_part_blob(k: int) -> dict:
    """The stored record of part ``k`` exactly as the blob store holds it: the part's own id, connector and
    name, which a search must replace with the file's."""
    blob = part_blob(k)
    for key in ("identity_vrid", "segment_index", "frontend_url"):
        blob.pop(key)
    blob.update(id=PART_NODE[k], connector_id=PARTS_CONNECTOR, record_name=f"big.pdf (part {k})",
                weburl=f"https://internal.example/{PART_NODE[k]}")
    return blob


def file_graph_record() -> dict:
    """The graph record of the file ``V``: what every user-visible field is taken from."""
    return {"_key": RECORD_ID, "id": RECORD_ID, "recordType": "FILE", "recordName": "big.pdf", "connectorName": "KB",
            "connectorId": "kb-1", "origin": "UPLOAD", "webUrl": "https://files.example/big.pdf",
            "mimeType": "application/pdf", "version": 1, "virtualRecordId": V}
