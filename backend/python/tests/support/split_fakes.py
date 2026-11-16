"""In-memory stand-ins for what the split seam, purger and sweep talk to.

The graph fake implements only the generic node methods those modules use, with the semantics the
real stores are tested for (``tests/integration/graph_db/test_content_splits_real_backends.py``).
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from app.modules.indexing.split_seam import CutFile, CutOutcome


class FakeGraph:
    def __init__(self) -> None:
        self.docs: dict[str, dict[str, dict[str, Any]]] = {}
        self.stage_state_deletes: list[str] = []
        self.fail_on: set[str] = set()
        # Stage states by key, as ``stage_state_get`` answers them (the roll-up reads each part's classify and entities).
        self.states: dict[str, dict[str, Any]] = {}

    async def stage_state_get(self, key: str) -> dict[str, Any] | None:
        return copy.deepcopy(self.states.get(key))

    def _coll(self, name: str) -> dict[str, dict[str, Any]]:
        return self.docs.setdefault(name, {})

    def _maybe_fail(self, op: str) -> None:
        if op in self.fail_on:
            raise RuntimeError(f"injected failure in {op}")

    def put(self, collection: str, key: str, doc: dict[str, Any]) -> None:
        self._coll(collection)[key] = {"_key": key, **copy.deepcopy(doc)}

    async def get_document(self, key: str, collection: str, **_kw: object) -> dict[str, Any] | None:
        doc = self._coll(collection).get(key)
        return copy.deepcopy(doc) if doc else None

    async def create_node_if_absent(self, key: str, collection: str, node: dict[str, Any], transaction: object = None) -> bool:
        self._maybe_fail("create_node_if_absent")
        if key in self._coll(collection):
            return False
        self.put(collection, key, node)
        return True

    async def ensure_nodes(self, nodes: list[dict[str, Any]], collection: str, transaction: object = None) -> None:
        self._maybe_fail("ensure_nodes")
        for node in nodes:
            key = node["id"]
            if key not in self._coll(collection):
                self.put(collection, key, {k: v for k, v in node.items() if k != "id"})

    async def get_documents_paginated(
        self, collection: str, skip: int = 0, limit: int = 50, filters: dict[str, Any] | None = None,
        sort_field: str | None = None, transaction: object = None, *, raise_on_error: bool = False,
        after_key: str | None = None, return_fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            (key, d) for key, d in sorted(self._coll(collection).items())
            if all(d.get(f) == v for f, v in (filters or {}).items()) and (not after_key or key > after_key)
        ]
        return [copy.deepcopy(d) for _key, d in rows[skip:skip + limit]]

    async def update_node_fields_if_match(
        self, key: str, collection: str, updates: dict[str, Any], expected: dict[str, Any],
        transaction: object = None, **_kw: object,
    ) -> bool:
        doc = self._coll(collection).get(key)
        if doc is None or any(doc.get(f) != v for f, v in expected.items()):
            return False
        for field_name, value in updates.items():
            if value is None:
                doc.pop(field_name, None)
            else:
                doc[field_name] = value
        return True

    async def delete_node_if_match(self, key: str, collection: str, expected: dict[str, Any], transaction: object = None) -> bool:
        doc = self._coll(collection).get(key)
        if doc is None or any(doc.get(f) != v for f, v in expected.items()):
            return False
        del self._coll(collection)[key]
        return True

    async def update_node(self, key: str, collection: str, updates: dict[str, Any], transaction: object = None) -> bool:
        doc = self._coll(collection).get(key)
        if doc is None:
            return False
        doc.update(updates)
        return True

    async def batch_update_nodes(self, updates: list[dict[str, Any]], collection: str, transaction: object = None) -> bool:
        for item in updates:
            doc = self._coll(collection).get(item["_key"])
            if doc is not None:
                doc.update({k: v for k, v in item.items() if k != "_key"})
        return True

    async def batch_upsert_records(self, records: list[Any], transaction: object = None, **_kw: object) -> None:
        self._maybe_fail("batch_upsert_records")
        for record in records:
            base = record.to_arango_base_record()
            key = base.pop("_key")
            existing = self._coll("records").get(key, {})
            self.put("records", key, {**existing, **base})

    async def get_nodes_by_filters(
        self, collection: str, filters: dict[str, Any], return_fields: list[str] | None = None,
        transaction: object = None, **_kw: object,
    ) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(d) for d in self._coll(collection).values()
            if all(d.get(f) == v for f, v in filters.items())
        ]

    async def get_nodes_by_field_in(
        self, collection: str, field_name: str, field_values: list[Any], return_fields: object = None,
        transaction: object = None, **_kw: object,
    ) -> list[dict[str, Any]]:
        # Like the real stores, ``id`` is the document key.
        wanted = set(field_values)
        if field_name == "id":
            return [
                {"id": d["_key"], **copy.deepcopy(d)} for d in self._coll(collection).values()
                if d.get("id", d["_key"]) in wanted
            ]
        return [copy.deepcopy(d) for d in self._coll(collection).values() if d.get(field_name) in wanted]

    async def delete_nodes(self, keys: list[str], collection: str, transaction: object = None) -> bool:
        for key in keys:
            self._coll(collection).pop(key, None)
        return True

    async def delete_records_and_relations(self, record_key: str, *, hard_delete: bool = False, transaction: object = None) -> bool:
        self._maybe_fail("delete_records_and_relations")
        return self._coll("records").pop(record_key, None) is not None

    async def stage_states_delete_for_virtual_records(self, virtual_record_ids: list[str]) -> int:
        self.stage_state_deletes += virtual_record_ids
        return 0

    async def get_records_by_virtual_record_id(self, vrid: str, *, visibility: object = None, **_kw: object) -> list[str]:
        """Record KEYS, as ``IGraphDBProvider.get_records_by_virtual_record_id`` answers on every store."""
        everything = visibility is not None
        return [
            key for key, d in self._coll("records").items()
            if d.get("virtualRecordId") == vrid
            and (everything or not d.get("isDeleted"))
        ]

    async def compare_and_set_indexing_status(self, ids: list[str], expected: str, new: str, transaction: object = None) -> list[str]:
        moved = []
        for key in ids:
            doc = self._coll("records").get(key)
            if doc is not None and doc.get("indexingStatus") == expected:
                doc["indexingStatus"] = new
                moved.append(key)
        return moved

    async def content_splits_page(
        self, states: list[str], *, updated_before_ms: int | None = None, after_key: str | None = None, limit: int = 100,
        org_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            d for key, d in sorted(self._coll("contentSplits").items())
            if d["state"] in states
            and (updated_before_ms is None or d.get("updatedAtMs", 0) < updated_before_ms)
            and (after_key is None or key > after_key)
            and (org_id is None or d.get("orgId") == org_id)
        ]
        return [{"id": d["_key"], **copy.deepcopy(d)} for d in rows[:limit]]


@dataclass
class FakeLeases:
    held: set[str] = field(default_factory=set)
    claims: list[str] = field(default_factory=list)

    async def claim(self, record_id: str) -> str | None:
        if record_id in self.held:
            return None
        self.held.add(record_id)
        self.claims.append(record_id)
        return f"holder:{record_id}"

    async def release(self, record_id: str, holder: str) -> None:
        self.held.discard(record_id)


@dataclass
class FakeResources:
    points_deleted: list[str] = field(default_factory=list)
    roots_of_points_deleted: list[str] = field(default_factory=list)
    pdfs_deleted: list[str] = field(default_factory=list)
    whole_file_points_deleted: list[str] = field(default_factory=list)
    stray_points: bool = False

    async def delete_points_and_stored_record(self, org_id: str, root_virtual_record_id: str, part_virtual_record_id: str) -> None:
        self.points_deleted.append(part_virtual_record_id)
        self.roots_of_points_deleted.append(root_virtual_record_id)

    async def delete_pdf(self, org_id: str, storage_document_id: str) -> None:
        self.pdfs_deleted.append(storage_document_id)

    async def delete_stray_points(self, parent: Any, row: Any) -> None:  # noqa: ANN401
        self.whole_file_points_deleted.append(row.v)

    async def has_stray_points(self, parent: Any, row: Any) -> bool:  # noqa: ANN401
        return self.stray_points


@dataclass
class FakeUploader:
    saved: list[str] = field(default_factory=list)
    fail_after: int | None = None

    async def save(self, org_id: str, document_path: str, data: bytes, *, v: str, owner_connector_id: str) -> str:
        if self.fail_after is not None and len(self.saved) >= self.fail_after:
            raise RuntimeError("injected upload failure")
        self.saved.append(document_path)
        return f"doc:{document_path}"


@dataclass
class FakePublisher:
    sent: list[tuple[str, str, dict[str, Any], str | None]] = field(default_factory=list)
    fail: bool = False

    async def send_event(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> bool:
        if self.fail:
            return False
        self.sent.append((topic, event_type, payload, key))
        return True


def fake_cutter(ranges: list[tuple[int, int, int]], *, digest: str = "digest-1") -> Any:  # noqa: ANN401
    """A cutter that reports *ranges* and leaves tiny files for the uploader to read."""
    import tempfile
    from pathlib import Path

    cuts: list[str] = []
    asked: list[Any] = []

    async def cut(content: bytes, label: str, cfg: object, wanted: Any = None) -> CutOutcome:  # noqa: ANN401
        work = Path(tempfile.mkdtemp(prefix="split-fake-"))
        files = []
        asked.append(None if wanted is None else [tuple(r) for r in wanted])
        for start, end, overlap in (ranges if wanted is None else wanted):
            path = work / f"{start}-{end}.pdf"
            path.write_bytes(b"%PDF-1.4 fake")
            files.append(CutFile(start, end, overlap, str(path), path.stat().st_size))
        cuts.append(label)

        def cleanup() -> None:
            import shutil

            shutil.rmtree(work, ignore_errors=True)

        return CutOutcome(sum(1 for _ in files) and ranges[-1][1], digest, tuple(files), cleanup)

    cut.calls = cuts  # type: ignore[attr-defined]
    cut.asked = asked  # type: ignore[attr-defined]
    return cut
