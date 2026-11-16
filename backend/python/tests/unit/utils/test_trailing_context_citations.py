"""S2b addressing rule: a local block index at or past ``len(blocks)`` names ``trailing_context``, so a citation
of an overlap block opens its own original page; unsplit records resolve as they always did."""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.utils.citations import (
    normalize_citations_and_chunks,
    normalize_citations_and_chunks_for_agent,
)
from app.utils.trailing_blocks import addressable_blocks

if TYPE_CHECKING:
    from collections.abc import Callable

RECORD_ID = "rec-1"


def _block(i: int, text: str, page: int) -> dict:
    return {
        "id": f"b{i}", "index": i, "type": "text", "format": "txt", "data": text,
        "citation_metadata": {"page_number": page},
    }


def _record(*, trailing: bool = True) -> dict:
    containers: dict = {"blocks": [_block(0, "core text", 20), _block(1, "more core", 20)], "block_groups": []}
    if trailing:
        containers["trailing_context"] = [_block(2, "spill over text", 21)]
    return {
        "id": RECORD_ID, "record_name": "big.pdf", "virtual_record_id": "V", "org_id": "org", "origin": "UPLOAD",
        "mime_type": "application/pdf", "block_containers": containers, "weburl": "", "version": 0,
        "connector_id": "kb-1",
    }


def _url(index: int) -> str:
    return f"https://app/record/{RECORD_ID}/preview#blockIndex={index}"


def _cite(fn: Callable[..., tuple], index: int, record: dict) -> tuple:
    answer = f"claim [x]({_url(index)})."
    return fn(answer, [], [record], {}, {"V": record})


def test_the_addressable_blocks_are_blocks_then_trailing_context() -> None:
    rec = _record()
    assert [b["id"] for b in addressable_blocks(rec["block_containers"])] == ["b0", "b1", "b2"]


def test_an_unsplit_record_resolves_to_the_same_list() -> None:
    rec = _record(trailing=False)
    assert addressable_blocks(rec["block_containers"]) is rec["block_containers"]["blocks"]
    assert addressable_blocks(None) == []


def test_chat_citation_of_a_trailing_block_opens_its_original_page() -> None:
    _, chunks = _cite(normalize_citations_and_chunks, 2, _record())
    assert len(chunks) == 1
    assert chunks[0]["content"] == "spill over text"
    assert chunks[0]["metadata"]["pageNum"] == [21]


def test_agent_citation_of_a_trailing_block_opens_its_original_page() -> None:
    _, chunks = normalize_citations_and_chunks_for_agent(f"claim [x]({_url(2)}).", [], None, [_record()])
    assert [c["content"] for c in chunks] == ["spill over text"]
    assert chunks[0]["metadata"]["pageNum"] == [21]


def test_an_index_past_the_trailing_context_is_still_dropped() -> None:
    _, chunks = _cite(normalize_citations_and_chunks, 3, _record())
    assert chunks == []


def test_without_trailing_context_the_same_index_is_dropped_as_before() -> None:
    _, chunks = _cite(normalize_citations_and_chunks, 2, _record(trailing=False))
    assert chunks == []


def test_a_core_block_cites_as_before() -> None:
    _, chunks = _cite(normalize_citations_and_chunks, 1, _record())
    assert chunks[0]["content"] == "more core" and chunks[0]["metadata"]["pageNum"] == [20]
