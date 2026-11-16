"""T1: the one hit shape. T4 (unit part): what leaves the process. T8 (unit part): unsplit hits are unchanged."""
from __future__ import annotations

import json

from app.modules.retrieval.hit_shape import (
    SEGMENT_SHIFT,
    block_address,
    carry_part,
    content_vrid,
    decode_address,
    hit_from_point,
    identity_vrid,
    public_block_index,
    public_hit,
    public_results,
)
from tests.unit.modules.retrieval.split_fakes import V1, V2, V, address, point_meta


class TestHitFromPoint:
    def test_a_part_point_keeps_the_file_as_identity_and_names_its_stored_record(self) -> None:
        meta = hit_from_point(point_meta(2, 7))
        assert meta["virtualRecordId"] == V
        assert meta["contentVrid"] == V2
        assert meta["blockIndex"] == 7  # local: loaders index the part's blocks with it
        assert meta["blockAddress"] == 2 * 2**20 + 7

    def test_the_manifest_point_is_the_files_own_record_at_segment_zero(self) -> None:
        meta = hit_from_point(point_meta(None, 3, segmentVirtualRecordId=V, segmentIndex=0, splitGen="g"))
        assert meta["contentVrid"] == V
        assert meta["blockAddress"] == 3

    def test_an_unsplit_point_is_unchanged_apart_from_the_two_derived_fields(self) -> None:
        before = point_meta(None, 5)
        meta = hit_from_point(dict(before))
        assert {k: v for k, v in meta.items() if k not in ("contentVrid", "blockAddress")} == before
        assert meta["contentVrid"] == V
        assert meta["blockAddress"] == 5

    def test_a_block_group_point_is_addressed_by_its_group_index(self) -> None:
        meta = hit_from_point({"virtualRecordId": V, "blockGroupIndex": 4, "segmentVirtualRecordId": V1, "segmentIndex": 1})
        assert meta["blockAddress"] == address(1, 4)

    def test_addresses_of_two_parts_never_collide_and_round_trip(self) -> None:
        a, b = block_address(1, 0), block_address(2, 0)
        assert a != b
        assert decode_address(a) == (1, 0)
        assert decode_address(block_address(2047, 2**20 - 1)) == (2047, 2**20 - 1)
        assert block_address(2047, 2**20 - 1) < 2**31  # fits a 32-bit signed client index

    def test_content_vrid_reads_a_point_a_hit_and_a_passage(self) -> None:
        meta = hit_from_point(point_meta(1, 0))
        assert content_vrid(meta) == V1
        assert content_vrid({"metadata": meta}) == V1
        assert content_vrid({"virtual_record_id": V1, "metadata": {"virtualRecordId": V}}) == V1
        assert content_vrid({"metadata": {"virtualRecordId": V}}) == V  # a hit of a test double with no stamp
        assert identity_vrid({"virtual_record_id": V1, "metadata": {"virtualRecordId": V}}) == V
        assert identity_vrid({"identity_vrid": V, "virtual_record_id": V1}) == V


class TestPublicHit:
    def _passage(self, k: int, local: int) -> dict:
        return {
            "content": f"text {k}", "score": 0.5, "block_index": local, "virtual_record_id": f"v-part-{k}",
            "identity_vrid": V, "segment_index": k,
            "metadata": {"virtualRecordId": V, "recordId": "rec", "segmentVirtualRecordId": f"v-part-{k}",
                         "splitGen": "g", "contentVrid": f"v-part-{k}", "blockAddress": 1},
        }

    def test_the_boundary_writes_the_address_and_drops_the_part(self) -> None:
        out = public_hit(self._passage(2, 1))
        assert out["block_index"] == address(2, 1)
        assert out["virtual_record_id"] == V
        text = json.dumps(out)
        for leaked in (V1, V2, "v-part", "segmentVirtualRecordId", "splitGen", "contentVrid", "identity_vrid", "segment_index"):
            assert leaked not in text

    def test_the_children_of_a_group_are_addressed_too(self) -> None:
        group = self._passage(2, 0)
        group["content"] = ("summary", [{**self._passage(2, 3), "content": "row"}])
        out = public_hit(group)
        assert out["content"][1][0]["block_index"] == address(2, 3)
        assert "identity_vrid" not in out["content"][1][0]

    def test_the_input_is_not_changed(self) -> None:
        passage = self._passage(1, 2)
        public_hit(passage)
        assert passage["block_index"] == 2 and passage["segment_index"] == 1

    def test_public_results_maps_only_the_passages(self) -> None:
        results = {"searchResults": [self._passage(1, 0)], "records": [{"_key": "rec"}], "virtual_to_record_map": {V: {"_key": "rec"}}}
        out = public_results(results)
        assert out["records"] == results["records"] and out["virtual_to_record_map"] == results["virtual_to_record_map"]
        assert out["searchResults"][0]["block_index"] == address(1, 0)

    def test_an_unsplit_passage_goes_out_as_it_came(self) -> None:
        passage = {"content": "t", "score": 1.0, "block_index": 9, "virtual_record_id": V,
                   "metadata": {"virtualRecordId": V, "recordId": "rec", "blockType": "text"}}
        assert public_hit(passage) == passage
        assert public_block_index(passage) == 9
        assert public_hit({"block_index": None, "virtual_record_id": V}) == {"block_index": None, "virtual_record_id": V}

    def test_carry_part_copies_only_what_is_set(self) -> None:
        assert carry_part({"identity_vrid": V, "segment_index": 2}, {"a": 1}) == {"a": 1, "identity_vrid": V, "segment_index": 2}
        assert carry_part({}, {"a": 1}) == {"a": 1}
        assert SEGMENT_SHIFT == 20


def test_a_raw_part_hit_goes_out_with_its_address() -> None:
    hit = {"content": "t", "metadata": hit_from_point(point_meta(2, 4, recordId="rec"))}
    out = public_hit(hit)
    assert out["metadata"]["blockIndex"] == address(2, 4)
    assert "blockAddress" not in out["metadata"] and "segmentIndex" not in out["metadata"]
    unsplit = {"content": "t", "metadata": hit_from_point(point_meta(None, 4))}
    assert public_hit(unsplit)["metadata"]["blockIndex"] == 4


class TestEveryResponseCollectionOfPassagesLeavesThroughThePublicBoundary:
    """A response path that bypasses the one serialisation boundary leaks part identity and part-local block indices.
    The boundary walks the whole response, so a collection added later is covered by construction."""

    @staticmethod
    def _leaks(value: object) -> list[str]:
        found: list[str] = []

        def walk(node: object, path: str) -> None:
            if isinstance(node, dict):
                for key, item in node.items():
                    if key in ("segmentVirtualRecordId", "segmentIndex", "splitGen", "contentVrid", "blockAddress", "identity_vrid", "segment_index"):
                        found.append(f"{path}.{key}")
                    walk(item, f"{path}.{key}")
            elif isinstance(node, list):
                for i, item in enumerate(node):
                    walk(item, f"{path}[{i}]")

        walk(value, "$")
        return found

    def _hit(self) -> dict:
        hit = hit_from_point(point_meta(2, 7))
        return {"content": "text", "block_index": 7, "metadata": hit, "identity_vrid": V, "segment_index": 2}

    def test_the_passages_outside_the_dates_are_sanitised_like_the_ones_inside(self) -> None:
        response = {
            "searchResults": [self._hit()],
            "outsideDates": {"searchResults": [self._hit()], "records": [{"id": "r"}]},
        }

        assert self._leaks(response), "the control: the raw response does carry part identity"
        assert self._leaks(public_results(response)) == []

    def test_a_collection_no_one_has_added_yet_is_sanitised_too(self) -> None:
        response = {"searchResults": [], "someFutureGroup": {"nested": [{"hits": [self._hit()]}]}, "other": [self._hit()]}

        assert self._leaks(public_results(response)) == []

    def test_the_file_keyed_maps_are_sent_as_they_are(self) -> None:
        records = {"r1": {"virtualRecordId": V, "metadata": {"segmentIndex": 2}}}
        sent = public_results({"records": records, "virtual_to_record_map": {V: "r1"}})
        assert sent["records"] is records and sent["virtual_to_record_map"] == {V: "r1"}
