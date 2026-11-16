"""The one shape of a search hit, and the one place a hit stops being internal.

A passage of a split file lives in a *part* (``segmentVirtualRecordId`` = ``V_k``), but the file is the
identity every user, access rule and citation is about (``virtualRecordId`` = ``V``). Inside the query
service a hit therefore carries both:

* ``virtualRecordId``  the file, unchanged, for access, membership and the record maps;
* ``contentVrid``      which stored record holds the passage: the only key a loader reads content by;
* ``blockIndex``       the passage's index *inside that stored record*, for loaders;
* ``blockAddress``     ``k * 2**20 + blockIndex`` for a part: the index the user and the model see, unique
  across the parts of one file. Unsplit hits: the same as ``blockIndex``.

``public_hit`` / ``public_block_index`` are the only serialisation boundary: they drop what is internal and
write the address where the local index was. Nothing else in the query side may branch on "is it split".
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

SEGMENT_SHIFT = 20
LOCAL_MASK = (1 << SEGMENT_SHIFT) - 1

# Point-payload fields of a part that never leave the process.
INTERNAL_PAYLOAD_KEYS = ("segmentVirtualRecordId", "segmentIndex", "splitGen", "contentVrid", "blockAddress")
# Keys a flattened hit (or one of its children) carries only inside the process.
INTERNAL_HIT_KEYS = ("identity_vrid", "segment_index")


MAX_SEGMENT_INDEX = (1 << 31 >> SEGMENT_SHIFT) - 1  # 2047: the address of its last block still fits a signed 32-bit index


class BlockAddressError(ValueError):
    """A part's block cannot be given an address: it would collide with another part's or overflow."""


def block_address(segment_index: int | None, local_index: int | None) -> int | None:
    """The user-visible index of block ``local_index`` of part ``segment_index`` (``None``/0: not a part)."""
    if local_index is None:
        return None
    try:
        local = int(local_index)
        k = int(segment_index) if segment_index else 0
    except (TypeError, ValueError):
        # A payload this build did not write (a non-numeric index) is one malformed point, not a failed search.
        raise BlockAddressError(f"no block address for block {local_index!r} of part {segment_index!r}") from None
    if not k:
        return local
    if not 0 < k <= MAX_SEGMENT_INDEX or not 0 <= local <= LOCAL_MASK:
        raise BlockAddressError(f"no block address for block {local} of part {k}")
    return (k << SEGMENT_SHIFT) + local


def part_offset(counts: Mapping[int, int | None] | None, segment_index: int) -> int | None:
    """Blocks in the parts before part ``segment_index`` (``sum(blockCount of parts 1..k-1)``), or ``None`` when
    any of them has no count: a wrong number is worse than none."""
    if counts is None:
        return None
    total = 0
    for earlier in range(1, int(segment_index)):
        count = counts.get(earlier)
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            return None
        total += count
    return total


def display_number(segment_index: int | None, local_index: int | None, offset: int | None) -> int | None:
    """The paragraph number a user sees: the file-wide ordinal, never the block address.

    Unsplit: ``local + 1``, as it always was. A part's block: ``offset + local + 1``; ``None`` (omit the
    number) when the offset is not known.
    """
    if local_index is None:
        return None
    if not segment_index:
        return int(local_index) + 1
    return None if offset is None else offset + int(local_index) + 1


def decode_address(address: int) -> tuple[int, int]:
    """``(k, local)`` of an address; ``k`` is 0 for an address that names the file's own block."""
    return int(address) >> SEGMENT_SHIFT, int(address) & LOCAL_MASK


def hit_from_point(metadata: dict[str, Any]) -> dict[str, Any]:
    """Stamp ``contentVrid`` and ``blockAddress`` on a point's metadata, in place, and return it.

    ``virtualRecordId`` is left alone. A point with no segment fields (every unsplit record) gets
    ``contentVrid = virtualRecordId`` and ``blockAddress = blockIndex``.
    """
    vrid = metadata.get("virtualRecordId")
    metadata["contentVrid"] = metadata.get("segmentVirtualRecordId") or vrid
    local = metadata.get("blockIndex")
    if local is None:
        local = metadata.get("blockGroupIndex")
    metadata["blockAddress"] = block_address(metadata.get("segmentIndex"), local)
    return metadata


def content_vrid(hit: Mapping[str, Any]) -> str | None:
    """The stored record holding a hit's content: the only way a loader picks which record to read.

    Takes a point's metadata (``contentVrid``), a hit from the vector search (``{"metadata": ..}``) or a
    flattened passage, whose ``virtual_record_id`` is already the stored record it was read from.
    """
    metadata = hit.get("metadata") if isinstance(hit.get("metadata"), Mapping) else {}
    return (
        hit.get("contentVrid") or metadata.get("contentVrid")
        or hit.get("virtual_record_id") or hit.get("virtualRecordId") or metadata.get("virtualRecordId")
    )


def identity_vrid(hit: Mapping[str, Any]) -> str | None:
    """The file a hit is about (the key of ``virtual_to_record_map``), whatever stored record holds its text."""
    metadata = hit.get("metadata") if isinstance(hit.get("metadata"), Mapping) else {}
    return metadata.get("virtualRecordId") or hit.get("identity_vrid") or hit.get("virtual_record_id")


def segment_index_of(hit: Mapping[str, Any]) -> int | None:
    metadata = hit.get("metadata") if isinstance(hit.get("metadata"), Mapping) else {}
    found = hit.get("segment_index")
    if found is None:
        found = hit.get("segmentIndex")
    if found is None:
        found = metadata.get("segmentIndex")
    return int(found) if found else None


def public_block_index(hit: Mapping[str, Any]) -> int | None:
    """The block index a client sees for a flattened hit: its address."""
    return block_address(segment_index_of(hit), hit.get("block_index"))


def carry_part(source: Mapping[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    """Give a passage made from the same stored record as ``source`` (a neighbour) the same part marks."""
    for key in INTERNAL_HIT_KEYS:
        if source.get(key) is not None:
            target[key] = source[key]
    return target


def _public_children(content: object) -> object:
    # A group's content is ``(summary, children)``; JSON turns it into a list.
    if isinstance(content, (tuple, list)) and len(content) == 2 and isinstance(content[1], list):
        return type(content)((content[0], [public_hit(child) if isinstance(child, dict) else child for child in content[1]]))
    return content


def public_hit(hit: Mapping[str, Any]) -> dict[str, Any]:
    """A copy of a flattened hit safe to send: addresses for local indices, no part identity."""
    out = dict(hit)
    if isinstance(out.get("metadata"), Mapping) and out["metadata"].get("segmentIndex") and out["metadata"].get("blockAddress") is not None:
        # A hit that was not flattened: its metadata still holds the part-local index.
        out["metadata"] = {**out["metadata"], "blockIndex": out["metadata"]["blockAddress"]}
    if "block_index" in out and not isinstance(out["block_index"], bool):
        out["block_index"] = public_block_index(hit)
    metadata = out.get("metadata")
    if isinstance(metadata, Mapping):
        out["metadata"] = {k: v for k, v in metadata.items() if k not in INTERNAL_PAYLOAD_KEYS}
    if "content" in out:
        out["content"] = _public_children(out["content"])
    if out.get("identity_vrid"):
        out["virtual_record_id"] = out["identity_vrid"]
    for key in INTERNAL_HIT_KEYS:
        out.pop(key, None)
    return out


def public_hits(hits: Iterable[Any]) -> list[Any]:
    return [public_hit(h) if isinstance(h, dict) else h for h in hits]


_FILE_KEYED = frozenset({"records", "virtual_to_record_map"})


def _is_hit(value: object) -> bool:
    return isinstance(value, dict) and (isinstance(value.get("metadata"), Mapping) or any(k in value for k in INTERNAL_HIT_KEYS))


def _public(value: Any) -> Any:  # noqa: ANN401
    if isinstance(value, list):
        return [public_hit(v) if _is_hit(v) else _public(v) for v in value]
    if isinstance(value, dict):
        return {k: (v if k in _FILE_KEYED else _public(v)) for k, v in value.items()}
    return value


def public_results(results: Mapping[str, Any]) -> dict[str, Any]:
    """A search response safe to send: every passage in it, wherever the response holds one (``searchResults``,
    ``outsideDates.searchResults``, any collection a later path adds), goes through ``public_hit``: the one boundary
    where part identity and part-local block indices are removed. ``records`` and ``virtual_to_record_map`` describe
    files and are keyed by the file, so they are sent as they are."""
    return _public(dict(results))
