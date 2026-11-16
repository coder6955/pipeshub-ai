"""Every field the split-PDF change adds is nullable, and absent means null.

Data written by an earlier release (a document, a record, a response body, a vector point) has none of the new
fields and must keep validating and reading. ``merge_base_schema_fields.json`` pins the ``required`` list and the
property names of every Arango schema as the release before this change defined them: a schema may not require
anything more than it did, and a property it gained must accept null.
"""
from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from app.config.constants.arangodb import CollectionNames
from app.modules.indexing.split_state import PartInfo, SplitRow, SplitState
from app.schema.arango import documents
from app.schema.node_schema_registry import NODE_SCHEMA_REGISTRY, adapt_schema

BASE = json.loads((Path(__file__).parent / "merge_base_schema_fields.json").read_text())
# The collections this change adds: what they have always required, from their first design, and nothing more.
NEW_COLLECTIONS = {
    "content_split_schema": {"orgId", "gen", "plan", "total", "done", "failed", "state", "epoch", "updatedAtMs"},
    "content_split_part_schema": {"orgId", "rootVirtualRecordId", "gen", "k", "pageStart", "pageEnd"},
}
SCHEMAS = {
    name: value["rule"] for name, value in vars(documents).items()
    if name.endswith("_schema") and isinstance(value, dict) and "rule" in value
}


def _allows_null(prop: dict) -> bool:
    kind = prop.get("type")
    return kind == "null" or (isinstance(kind, list) and "null" in kind) or "null" in (prop.get("enum") or [])


@pytest.mark.parametrize("name", sorted(BASE))
def test_an_existing_schema_requires_nothing_it_did_not_require_before(name: str) -> None:
    assert set(SCHEMAS[name].get("required", [])) <= set(BASE[name]["required"])


@pytest.mark.parametrize("name", sorted(BASE))
def test_a_property_an_existing_schema_gained_accepts_null_and_is_not_required(name: str) -> None:
    rule = SCHEMAS[name]
    gained = set(rule.get("properties", {})) - set(BASE[name]["properties"])
    for prop in gained:
        assert prop not in rule.get("required", []), f"{name}.{prop} is required"
        assert _allows_null(rule["properties"][prop]), f"{name}.{prop} does not allow null"


def test_no_schema_appeared_that_this_test_does_not_know() -> None:
    assert set(SCHEMAS) - set(BASE) == set(NEW_COLLECTIONS)


@pytest.mark.parametrize("name", sorted(NEW_COLLECTIONS))
def test_a_new_collection_requires_only_its_identity_and_everything_else_accepts_null(name: str) -> None:
    rule = SCHEMAS[name]
    assert set(rule["required"]) == NEW_COLLECTIONS[name]
    for prop, spec in rule["properties"].items():
        if prop not in rule["required"]:
            assert _allows_null(spec), f"{name}.{prop} does not allow null"


def test_a_row_with_only_the_core_fields_validates_and_reads() -> None:
    doc = {"orgId": "o", "gen": "g", "plan": [], "total": 0, "done": 0, "failed": 0, "state": "COMPLETED", "epoch": 0, "updatedAtMs": 1}
    jsonschema.validate(doc, adapt_schema({"rule": SCHEMAS["content_split_schema"]}))
    row = SplitRow.from_doc({**doc, "_key": "v"})
    assert (row.content_rev, row.plan_digest, row.nonce, row.redrive_token, row.redrive_at_ms, row.running_lines) == ("", "", "", "", 0, ())
    assert row.state is SplitState.COMPLETED


def test_a_row_whose_new_fields_are_null_validates_and_reads_the_same() -> None:
    doc = {"orgId": "o", "gen": "g", "plan": [], "total": 0, "done": 0, "failed": 0, "state": "COMPLETED", "epoch": 0, "updatedAtMs": 1}
    nulls = {**doc, **dict.fromkeys(("contentRev", "planDigest", "nonce", "redriveToken", "redriveAtMs", "runningLines"))}
    jsonschema.validate(nulls, adapt_schema({"rule": SCHEMAS["content_split_schema"]}))
    assert SplitRow.from_doc({**nulls, "_key": "v"}) == SplitRow.from_doc({**doc, "_key": "v"})


def test_a_part_document_with_only_the_core_fields_validates_and_reads() -> None:
    doc = {"orgId": "o", "rootVirtualRecordId": "v", "gen": "g", "k": 2, "pageStart": 11, "pageEnd": 20}
    jsonschema.validate(doc, adapt_schema({"rule": SCHEMAS["content_split_part_schema"]}))
    info = PartInfo.from_doc({**doc, "_key": "p"})
    assert (info.page_offset, info.overlap_pages, info.owner_connector_id, info.vrid, info.block_count) == (10, 0, "", "", None)


def test_a_part_document_whose_new_fields_are_null_validates_and_reads_the_same() -> None:
    doc = {"orgId": "o", "rootVirtualRecordId": "v", "gen": "g", "k": 2, "pageStart": 11, "pageEnd": 20}
    nulls = {**doc, **dict.fromkeys(("pageOffset", "overlapPages", "ownerConnectorId", "virtualRecordId", "blockCount"))}
    jsonschema.validate(nulls, adapt_schema({"rule": SCHEMAS["content_split_part_schema"]}))
    assert PartInfo.from_doc({**nulls, "_key": "p"}) == PartInfo.from_doc({**doc, "_key": "p"})


def test_a_part_that_names_no_owner_is_not_decided_for_by_any_gate() -> None:
    from app.modules.indexing import split_state
    from app.modules.indexing.owner_connector import (
        owner_connector_id,
        part_connector_id,
    )
    from app.modules.indexing.split_state import PartInfoMissing, clear_part_cache

    clear_part_cache()
    record = {"_key": "p", "connectorId": part_connector_id("o")}
    split_state._cache["p"] = (PartInfo.from_doc({"_key": "p", "orgId": "o", "rootVirtualRecordId": "v", "gen": "g", "k": 1, "pageStart": 1, "pageEnd": 2}))
    with pytest.raises(PartInfoMissing):
        owner_connector_id(record)


def test_a_record_without_any_split_field_validates_against_the_record_schema() -> None:
    record = {
        "recordName": "a.pdf", "externalRecordId": "e", "recordType": "FILE", "origin": "UPLOAD", "createdAtTimestamp": 1,
        "connectorId": "c",
    }
    jsonschema.validate(record, adapt_schema({"rule": SCHEMAS["record_schema"]}))
    assert NODE_SCHEMA_REGISTRY[CollectionNames.RECORDS.value] is not None
    assert not {p for p in SCHEMAS["record_schema"]["properties"] if p.startswith("split")}, "the record schema stays closed (no split field)"
