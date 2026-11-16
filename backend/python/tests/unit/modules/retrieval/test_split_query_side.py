"""T3, T4, T5, T6, T8 for the query side of a split file, against fakes of the stores.

The file ``V`` has two parts whose blocks both start at local index 0, so every assertion here would hold for the
wrong part if an index or an id were taken from the wrong place.
"""
from __future__ import annotations

import copy
import json
import re
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.api.routes.search import _retrieve_chunks
from app.modules.retrieval.context.targeted import cap_passages_per_record
from app.modules.retrieval.hit_shape import (
    BlockAddressError,
    hit_from_point,
    public_hit,
    public_results,
)
from app.utils.chat_helpers import (
    CitationRefMapper,
    _describe_part,
    build_message_content_array,
    get_flattened_results,
    get_record,
)
from app.utils.citations import (
    normalize_citations_and_chunks,
    normalize_citations_and_chunks_for_agent,
)
from tests.unit.modules.retrieval.split_fakes import (
    FRONTEND,
    RECORD_ID,
    V1,
    V2,
    FakeResolver,
    V,
    address,
    file_graph_record,
    part_blob,
    point_meta,
    raw_part_blob,
)


def _hit(k: int, local: int, score: float = 0.9) -> dict:
    return {"content": f"part {k} block {local}", "score": score, "metadata": hit_from_point(point_meta(k, local))}


def _content_map() -> dict:
    return {V1: part_blob(1), V2: part_blob(2)}


async def _flatten(hits: list[dict]) -> tuple[list[dict], dict]:
    records = _content_map()
    units = await get_flattened_results(
        hits, AsyncMock(), "org-1", False, records, {V: {"_key": RECORD_ID, "id": RECORD_ID}},
        from_retrieval_service=True,
    )
    return units, records


@pytest.mark.asyncio
class TestTwoPartsInOneAnswer:
    async def test_each_passage_is_read_from_its_own_part_and_carries_the_files_identity(self) -> None:
        units, _ = await _flatten([_hit(1, 1), _hit(2, 1)])
        by_text = {u["content"]: u for u in units}
        assert set(by_text) == {"part 1 block 1", "part 2 block 1"}
        for k in (1, 2):
            unit = by_text[f"part {k} block 1"]
            assert unit["virtual_record_id"] == (V1, V2)[k - 1]  # the content key
            assert unit["identity_vrid"] == V and unit["segment_index"] == k
            assert unit["block_index"] == 1  # local, for loaders
            assert unit["metadata"]["virtualRecordId"] == V and unit["metadata"]["recordId"] == RECORD_ID

    async def test_every_reference_resolves_to_its_own_parts_block(self) -> None:
        units, records = await _flatten([_hit(1, 1), _hit(2, 1)])
        mapper = CitationRefMapper()
        build_message_content_array(units, records, ref_mapper=mapper)
        urls = {u["content"]: u["block_web_url"] for u in units}
        assert urls["part 1 block 1"] == f"{FRONTEND}/record/{RECORD_ID}/preview#blockIndex={address(1, 1)}"
        assert urls["part 2 block 1"] == f"{FRONTEND}/record/{RECORD_ID}/preview#blockIndex={address(2, 1)}"
        refs = {u["content"]: u["citation_ref"] for u in units}
        answer = f"One [x]({refs['part 1 block 1']}). Two [y]({refs['part 2 block 1']})."
        _, citations = normalize_citations_and_chunks_for_agent(
            answer, units, records, [], ref_to_url=dict(mapper.ref_to_url),
        )
        assert [c["content"] for c in citations] == ["part 1 block 1", "part 2 block 1"]
        assert all(c["metadata"]["virtualRecordId"] == V for c in citations)

    async def test_a_url_that_is_only_an_address_finds_the_part_it_names(self) -> None:
        _, records = await _flatten([_hit(1, 1), _hit(2, 1)])
        for k in (1, 2):
            url = f"{FRONTEND}/record/{RECORD_ID}/preview#blockIndex={address(k, 0)}"
            _, citations = normalize_citations_and_chunks_for_agent(f"[a]({url})", [], records, [])
            assert [c["content"] for c in citations] == [f"part {k} block 0"]

    async def test_retrieve_chunks_have_unique_addresses_and_the_files_id(self) -> None:
        units, _ = await _flatten([_hit(1, 1), _hit(2, 1)])
        chunks = _retrieve_chunks({"searchResults": units, "records": [{"_key": RECORD_ID, "recordName": "big.pdf"}]}, None)
        assert sorted(c["citation"]["blockIndex"] for c in chunks) == [address(1, 1), address(2, 1)]
        assert len({c["id"] for c in chunks}) == 2
        assert all(c["id"].startswith(f"{V}:") for c in chunks)
        assert {c["record"]["recordId"] for c in chunks} == {RECORD_ID}

    async def test_a_second_hit_on_the_same_block_of_a_part_is_one_passage(self) -> None:
        units, _ = await _flatten([_hit(1, 1), _hit(1, 1, 0.5), _hit(2, 1)])
        assert len(units) == 2


@pytest.mark.asyncio
class TestNothingInternalLeaves:
    """T4: the response scanner over every serialised form the query side produces."""

    FORBIDDEN = (V1, V2, "pdfparts_", "segmentVirtualRecordId", "segmentIndex", "splitGen", "contentVrid", "blockAddress",
                 "identity_vrid", "segment_index")

    def _scan(self, payload: object) -> None:
        text = json.dumps(payload, default=str)
        for needle in self.FORBIDDEN:
            assert needle not in text, needle

    async def test_search_response_and_retrieve_response_and_the_prompt(self) -> None:
        units, records = await _flatten([_hit(1, 1), _hit(2, 0)])
        results = {"searchResults": units, "records": [{"_key": RECORD_ID}], "virtual_to_record_map": {V: {"_key": RECORD_ID}}}
        self._scan(public_results(results))
        self._scan({"chunks": _retrieve_chunks(results, None)})
        mapper = CitationRefMapper()
        content, _ = build_message_content_array(units, records, ref_mapper=mapper)
        self._scan(content)
        self._scan(mapper.ref_to_url)

    async def test_stream_frames_name_the_file_and_the_addresses(self) -> None:
        from app.agents.agent_loop.retrieval_ledger import RetrievalContextLedger

        units, records = await _flatten([_hit(1, 1), _hit(2, 1)])
        delta = RetrievalContextLedger().take_delta({"final_results": units, "virtual_record_id_to_result": records})
        payloads = [r.model_dump() if hasattr(r, "model_dump") else dict(r) for r in delta.records]
        self._scan(payloads)
        assert len(payloads) == 1 and payloads[0]["virtualRecordId"] == V
        assert sorted(payloads[0]["blockIndices"]) == [address(1, 1), address(2, 1)]


class TestOneFileOneShare:
    """T5: caps and diversity key on the file."""

    def _unit(self, k: int, local: int) -> dict:
        return {
            "content": f"p{k}-{local}", "block_type": "text", "block_index": local, "score": 1.0 - 0.01 * local,
            "virtual_record_id": (V1, V2)[k - 1], "identity_vrid": V, "segment_index": k,
            "metadata": {"virtualRecordId": V, "recordId": RECORD_ID},
        }

    def test_the_passage_cap_counts_the_parts_of_a_file_together(self) -> None:
        ranked = [self._unit(k, i) for k in (1, 2) for i in range(3)]
        capped = cap_passages_per_record(ranked, 2)
        assert len(capped.units) == 2
        assert sum(capped.omitted.values()) == 4

    def test_a_file_that_is_not_split_is_capped_the_same(self) -> None:
        ranked = [{**self._unit(1, i), "virtual_record_id": "solo", "metadata": {"virtualRecordId": "solo"}} for i in range(5)]
        assert len(cap_passages_per_record(ranked, 2).units) == 2

    def test_record_cut_and_diversity_key_on_the_file(self) -> None:
        from app.modules.retrieval.record_cut import hit_record_id, point_record_id

        meta = hit_from_point(point_meta(2, 3))
        assert hit_record_id({"metadata": meta}) == V
        assert point_record_id(MagicMock(payload={"metadata": meta})) == V


@pytest.mark.asyncio
class TestWhatTheModelIsTold:
    """T6."""

    async def test_a_split_hit_says_its_pages_and_the_files_page_count(self) -> None:
        record = part_blob(2)
        await _describe_part(record, V, 2, FakeResolver())
        header = record["context_metadata"]
        assert header.splitlines()[0] == "Pages 41\u201380 of 80 of big.pdf"
        assert "fetch_record" not in header
        assert record["page_range"] == {"pageStart": 41, "pageEnd": 80, "pageTotal": 80}

    async def test_the_header_reaches_the_prompt(self) -> None:
        units, records = await _flatten([_hit(2, 0)])
        await _describe_part(records[V2], V, 2, FakeResolver())
        content, _ = build_message_content_array(units, records, ref_mapper=CitationRefMapper())
        assert "Pages 41\\u201380 of 80 of big.pdf" in json.dumps(content)

    async def test_a_record_that_is_not_a_part_gets_no_header(self) -> None:
        record = part_blob(1)
        before = record["context_metadata"]
        await _describe_part(record, V, None, FakeResolver())
        await _describe_part(record, V, 1, None)
        assert record["context_metadata"] == before

    async def test_pages_are_the_original_pages(self) -> None:
        """T8 of the rules: S2b writes original page numbers; nothing here remaps them."""
        units, _ = await _flatten([_hit(2, 1)])
        assert units[0]["metadata"]["pageNum"] == [42]


@pytest.mark.asyncio
class TestUnsplitIsUnchanged:
    """The golden check that nothing is added to a record that is not split."""

    async def test_no_part_marks_and_the_same_url(self) -> None:
        blob = part_blob(1)
        blob.pop("identity_vrid"), blob.pop("segment_index")
        blob["virtual_record_id"] = "solo"
        hit = {"content": "t", "score": 0.9, "metadata": hit_from_point(
            {"virtualRecordId": "solo", "blockIndex": 1, "isBlockGroup": False, "orgId": "org-1"})}
        units = await get_flattened_results([hit], AsyncMock(), "org-1", False, {"solo": blob}, {}, from_retrieval_service=True)
        assert len(units) == 1
        assert "identity_vrid" not in units[0] and "segment_index" not in units[0]
        assert public_hit(units[0]) == units[0]
        build_message_content_array(units, {"solo": blob}, ref_mapper=CitationRefMapper())
        assert units[0]["block_web_url"] == f"{FRONTEND}/record/{RECORD_ID}/preview#blockIndex=1"
        assert re.fullmatch(r".*#blockIndex=1", units[0]["block_web_url"])


class TestTrailingContextSeam:
    """The neighbour just past a part's last block reads ``trailing_context`` when the record has it."""

    def _record(self, trailing: object) -> dict:
        record = part_blob(1)
        if trailing is not None:
            record["block_containers"]["trailing_context"] = trailing
        return record

    def _unit(self, index: int) -> dict:
        return {"content": "x", "block_type": "text", "block_index": index, "virtual_record_id": V1,
                "identity_vrid": V, "segment_index": 1, "metadata": {"virtualRecordId": V}}

    def test_the_block_after_the_last_one_comes_from_trailing_context(self) -> None:
        from app.modules.retrieval.context.neighbours import (
            expand_neighbours,
        )

        tail = {"id": "t0", "index": 0, "type": "text", "data": "next part opens", "citation_metadata": None}
        out = expand_neighbours([self._unit(1)], {V1: self._record([tail])})
        added = [u for u in out if u["content"] == "next part opens"]
        assert len(added) == 1 and added[0]["block_index"] == 2 and added[0]["trailing_context"] is True
        assert added[0]["segment_index"] == 1

    @pytest.mark.parametrize("trailing", [None, [], "bad"])
    def test_without_trailing_context_nothing_is_added(self, trailing: object) -> None:
        from app.modules.retrieval.context.neighbours import (
            expand_neighbours,
        )

        out = expand_neighbours([self._unit(1)], {V1: self._record(trailing)})
        assert [u["block_index"] for u in out] == [1, 0]  # only the block before it: nothing past the last


@pytest.mark.asyncio
class TestRealisticPartRecord:
    """T4 over a part stored under its own record id, connector and name, through ``get_record`` and every
    serialised form, including the citation payload and the path that flattens raw hits."""

    LEAKS = ("pdfparts_", "part-node-key", "(part 1)", "(part 2)", "internal.example", V1, V2)

    async def _world(self) -> tuple[dict, dict]:
        from unittest.mock import patch

        blobs = {V1: raw_part_blob(1), V2: raw_part_blob(2)}
        reader = MagicMock()
        reader.read_record = AsyncMock(side_effect=lambda vrid, *_a, **_k: copy.deepcopy(blobs[vrid]))
        records: dict = {}
        with patch("app.utils.chat_helpers.record_artifact_reader", return_value=reader):
            for k, vrid in ((1, V1), (2, V2)):
                await get_record(
                    vrid, records, AsyncMock(), "org-1", {V: file_graph_record()}, None, FRONTEND,
                    identity_vrid=V, segment_index=k, split_resolver=FakeResolver(),
                )
        return records, blobs

    def _scan(self, payload: object) -> None:
        text = json.dumps(payload, default=str)
        for needle in self.LEAKS:
            assert needle not in text, needle

    async def test_get_record_names_the_file_not_the_part(self) -> None:
        records, _ = await self._world()
        for _k, vrid in ((1, V1), (2, V2)):
            rec = records[vrid]
            assert (rec["id"], rec["record_name"], rec["connector_id"]) == (RECORD_ID, "big.pdf", "kb-1")
            assert rec["context_metadata"].splitlines()[0].startswith("Pages ")

    async def test_nothing_of_the_part_reaches_any_output(self) -> None:
        records, _ = await self._world()
        hits = [_hit(1, 1), _hit(2, 0)]
        units = await get_flattened_results(
            hits, AsyncMock(), "org-1", False, records, {V: file_graph_record()}, from_retrieval_service=True,
        )
        results = {"searchResults": units, "records": [file_graph_record()], "virtual_to_record_map": {V: file_graph_record()}}
        mapper = CitationRefMapper()
        content, _ = build_message_content_array(units, records, ref_mapper=mapper)
        answer = " ".join(f"c [x]({u['citation_ref']})." for u in units)
        _, agent = normalize_citations_and_chunks_for_agent(answer, units, records, [], ref_to_url=dict(mapper.ref_to_url))
        _, chat = normalize_citations_and_chunks(answer, units, [], dict(mapper.ref_to_url), records)
        assert len(agent) == len(chat) == 2
        for payload in (public_results(results), _retrieve_chunks(results, None), content, mapper.ref_to_url, agent, chat):
            self._scan(payload)

    async def test_the_path_that_flattens_raw_hits_gives_the_same(self) -> None:
        records, _ = await self._world()
        raw = [{"content": "x", "score": 0.9, "metadata": hit_from_point(point_meta(2, 1, recordId=RECORD_ID))}]
        units = await get_flattened_results(raw, AsyncMock(), "org-1", False, records, {V: file_graph_record()})
        assert [u["block_index"] for u in units] == [1] and units[0]["segment_index"] == 2
        self._scan(public_results({"searchResults": units}))


class TestHeaderIsOneLine:
    def test_a_file_name_cannot_add_lines(self) -> None:
        from app.modules.retrieval.split_content import PartLabel
        from app.utils.chat_helpers import page_range_header

        header = page_range_header("a\nb\r\x00c\u2028d.pdf", PartLabel(k=1, total=2, page_start=1, page_end=3, page_count=80))
        assert header == "Pages 1\u20133 of 80 of a b c d.pdf"


class TestAddressGuards:
    def test_a_local_index_that_would_land_in_the_next_part_is_refused(self) -> None:
        from app.modules.retrieval.hit_shape import (
            BlockAddressError,
            block_address,
        )

        assert block_address(1, 2**20 - 1) == 2**21 - 1
        with pytest.raises(BlockAddressError):
            block_address(1, 2**20)
        with pytest.raises(BlockAddressError):
            block_address(1, -1)

    def test_a_part_number_past_what_a_32_bit_index_holds_is_refused(self) -> None:
        from app.modules.retrieval.hit_shape import (
            BlockAddressError,
            block_address,
        )

        assert block_address(2047, 2**20 - 1) == 2**31 - 1
        with pytest.raises(BlockAddressError):
            block_address(2048, 0)

    def test_an_unsplit_index_is_never_refused(self) -> None:
        from app.modules.retrieval.hit_shape import block_address

        assert block_address(None, 2**22) == 2**22 and block_address(0, 2**22) == 2**22


@pytest.mark.asyncio
class TestUnsplitRecordWithMoreThan2Pow20Blocks:
    """A record that is not split is cited by its own index, however large."""

    async def test_a_url_with_a_large_index_still_resolves(self) -> None:
        big = (1 << 20) + 5
        filler = {"id": "f", "index": 0, "type": "text", "data": "filler", "citation_metadata": None, "parent_index": None}
        far = {"id": "big", "index": big, "type": "text", "data": "far block", "citation_metadata": None, "parent_index": None}
        blob = part_blob(1)
        blob.pop("identity_vrid"), blob.pop("segment_index")
        blob["virtual_record_id"] = "solo"
        blob["block_containers"] = {"blocks": [filler] * big + [far], "block_groups": []}
        url = f"{FRONTEND}/record/{RECORD_ID}/preview#blockIndex={big}"
        for normalise in (
            lambda: normalize_citations_and_chunks_for_agent(f"[a]({url})", [], {"solo": blob}, []),
            lambda: normalize_citations_and_chunks(f"[a]({url})", [], [], None, {"solo": blob}),
        ):
            _, citations = normalise()
            assert [c["content"] for c in citations] == ["far block"]

    async def test_a_split_file_with_a_part_in_hand_is_decoded(self) -> None:
        from app.utils.citations import split_block_address

        assert split_block_address(address(2, 3), RECORD_ID, {V2: part_blob(2)}) == (2, 3)
        assert split_block_address(address(2, 3), RECORD_ID, {"solo": {"id": RECORD_ID}}) == (0, address(2, 3))
        assert split_block_address(7, RECORD_ID, {V2: part_blob(2)}) == (0, 7)


@pytest.mark.parametrize(
    "metadata",
    [
        {"virtualRecordId": "v", "segmentVirtualRecordId": "v-1", "segmentIndex": "x", "blockIndex": 3},
        {"virtualRecordId": "v", "segmentVirtualRecordId": "v-1", "segmentIndex": 1, "blockIndex": "x"},
        {"virtualRecordId": "v", "blockIndex": "x"},
        {"virtualRecordId": "v", "segmentIndex": [1], "blockIndex": 2, "segmentVirtualRecordId": "v-1"},
    ],
    ids=["segment-not-numeric", "block-not-numeric", "unsplit-block-not-numeric", "segment-wrong-type"],
)
def test_a_point_with_a_non_numeric_address_is_one_malformed_point_the_search_can_drop(metadata: dict) -> None:
    """Review finding: a plain ValueError from ``int()`` escaped the search's ``except BlockAddressError``."""
    with pytest.raises(BlockAddressError):
        hit_from_point(metadata)


@pytest.mark.asyncio
async def test_the_live_generation_of_many_files_is_one_store_read() -> None:
    from app.modules.retrieval.split_content import StoreSplitResolver

    calls: list[list[str]] = []

    class Store:
        async def get_many(self, vs):  # noqa: ANN202
            calls.append(list(vs))
            return {v: {"id": v, "gen": f"g-{v}"} for v in vs if v != "unsplit"}

        async def get(self, v):  # noqa: ANN202
            raise AssertionError("one read per file")

    resolver = StoreSplitResolver(Store())  # type: ignore[arg-type]
    files = ["a", "b", "c", "d", "unsplit"]

    assert await resolver.live_generation(files) == {"a": "g-a", "b": "g-b", "c": "g-c", "d": "g-d"}
    assert await resolver.live_generation(files) == {"a": "g-a", "b": "g-b", "c": "g-c", "d": "g-d"}
    assert len(calls) == 1, "five files, one read; asked again, none"


@pytest.mark.asyncio
class TestTheModelIsNeverToldAboutParts:
    """Parts are how the file is indexed: the model sees pages and the file's page count, in the hit's header and in the
    text of the manifest the model can retrieve, whatever else the passage says."""

    async def test_no_model_facing_text_of_a_split_file_says_part(self) -> None:
        from app.modules.indexing.split_seam import manifest_markdown
        from app.modules.indexing.split_state import SplitRow, SplitState, part_specs

        records = _content_map()
        for k, record in ((1, records[V1]), (2, records[V2])):
            for block in record["block_containers"]["blocks"]:
                block["data"] = f"passage {k} of the report"  # the fixture's own text says "part"
        hits = [{"content": f"passage {k} of the report", "score": 0.9, "metadata": hit_from_point(point_meta(k, 1))} for k in (1, 2)]
        units = await get_flattened_results(
            hits, AsyncMock(), "org-1", False, records, {V: {"_key": RECORD_ID, "id": RECORD_ID}}, from_retrieval_service=True,
        )
        for k, vrid in ((1, V1), (2, V2)):
            await _describe_part(records[vrid], V, k, FakeResolver())
        content, _ = build_message_content_array(units, records, ref_mapper=CitationRefMapper())
        row = SplitRow(
            v=V, org_id="org-1", gen="g", state=SplitState.COMPLETED, parts=part_specs("g", [(1, 40, 41), (41, 80, 80)], ocr=False),
        )
        manifest = manifest_markdown(row, "big.pdf", outline=["- Introduction", "- Results"])

        context = json.dumps(content, ensure_ascii=False)
        assert "Pages 41–80 of 80 of big.pdf" in context
        assert manifest.splitlines()[:3] == ["# big.pdf", "", "This document has 80 pages."]
        for name, text in (("the model context", context), ("the manifest", manifest)):
            assert not re.search(r"\bparts?\b", text, re.IGNORECASE), f"{name} says part: {text[:400]}"
