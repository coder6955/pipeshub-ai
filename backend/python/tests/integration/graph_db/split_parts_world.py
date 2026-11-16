"""A seeded org for the "parts of a split PDF are invisible" scenarios, shared by the Neo4j/ArangoDB
suite (``test_split_parts_invisible_real_backends.py``) and the PostgreSQL one (``postgres/test_split_parts_invisible.py``).

One org holds a user, a knowledge base (a folder and a file in it) and a Drive connector (a record group and a file),
and each of the two files is split into three parts by the product seam (``ParentSplitSeam``, with a fake cutter and
uploader): part records under the hidden ``pdfparts_<org>`` connector with no edges, their ``contentSplitParts`` documents
and the file's ``contentSplits`` row, exactly as production writes them. A control world has the same
structure without the parts. A surface hides the parts when it answers the same for both (ids and names taken
without the per-run suffix) and no part id or part name shows in what it answers.

The graph is seeded through the provider's generic node and edge writers with the helpers of
``tests/integration/graph_permissions`` (the shape the access suites already use), so it loads the same on every store.
"""

from __future__ import annotations

import json
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import CollectionNames, ProgressStatus
from app.connectors.sources.localKB.handlers.kh_search import search_page
from app.modules.indexing.split_purge import GenerationPurger
from app.modules.indexing.split_seam import ParentSplitSeam, SplitRequest
from app.modules.indexing.split_state import SplitStore, clear_part_cache
from app.modules.parsers.pdf.split.config import SplitConfig
from tests.integration.graph_permissions.fixture_graph import (
    DRIVE,
    GROUP_G,
    KB,
    ORG,
    TS,
    USER_U,
    _principals,
    app,
    bt,
    nr,
    perm,
    rec,
    rg,
    user_app,
)
from tests.integration.graph_permissions.loaders import (
    EDGE_TARGETS,
    NODE_TARGETS,
    arango_shape,
)
from tests.support.split_fakes import (
    FakeLeases,
    FakePublisher,
    FakeResources,
    FakeUploader,
    fake_cutter,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

SPLITS = CollectionNames.CONTENT_SPLITS.value
SPLIT_PARTS = CollectionNames.CONTENT_SPLIT_PARTS.value
RECORDS = CollectionNames.RECORDS.value
USER_ID = "uid-u"
STATUSES = [s.value for s in ProgressStatus if s.value in ("NOT_STARTED", "QUEUED", "IN_PROGRESS", "COMPLETED", "FAILED", "AUTO_INDEX_OFF")]

# (parent record, its connector, the virtual record id of the whole file)
FILES = (("kbx-file", "kbx", "V-kb"), ("drv-file", "drv", "V-drv"))
PART_NAME_MARK = "· pages"


def _graph(with_parts: bool) -> tuple[list[dict], list[dict]]:
    nodes, edges = _principals()
    for node in nodes:
        if node["id"] == USER_U:
            node["props"].update(userId=USER_ID, isActive=True)
    nodes += [
        app("kbx", "My KB", connector=KB, app_group="Local Storage", scope="personal", hideConnector=True),
        rec("kbx-folder", "Folder", record_type="FILE", origin="UPLOAD", connector_id="kbx", mimeType="application/vnd.folder"),
        rec("kbx-file", "big.pdf", origin="UPLOAD", connector_id="kbx", mimeType="application/pdf", virtualRecordId="V-kb",
            indexingStatus="COMPLETED", extractionStatus="COMPLETED", parsingStatus="COMPLETED", webUrl="https://kb.example/kbx-file"),
        app("drv", "Drive", connector=DRIVE, app_group="Google Workspace", permissionModel="APP_LEVEL"),
        rg("drv-dir", "dir", group_type=DRIVE, connector=DRIVE, connectorId="drv", permissionModel="RECORD_GROUP_LEVEL"),
        rec("drv-file", "big-drive.pdf", connector_id="drv", mimeType="application/pdf", virtualRecordId="V-drv",
            indexingStatus="COMPLETED", extractionStatus="COMPLETED", parsingStatus="COMPLETED", webUrl="https://drive.example/drv-file"),
    ]
    edges += [
        perm(USER_U, "kbx", role="WRITER"), nr("kbx-folder", "kbx-file"), bt("kbx-folder", "kbx", "KB"), bt("kbx-file", "kbx", "KB"),
        user_app(USER_U, "drv"), nr("drv", "drv-dir"), bt("drv-dir", "drv"), perm(GROUP_G, "drv-dir", grant_type="GROUP"),
        nr("drv-dir", "drv-file"), bt("drv-file", "drv-dir"),
    ]
    for node in nodes:
        if node["kind"] == "Record":
            node["props"].update(version=0, updatedAtTimestamp=TS)
    return nodes, edges


@dataclass
class World:
    graph: IGraphDBProvider
    run: str
    with_parts: bool
    org_scope: Callable[[str], AbstractContextManager] = field(default=lambda _org: nullcontext())
    org_id: str | None = None      # a store that fixes the shape of an org id (PostgreSQL: 24 hex digits) names it

    @property
    def org(self) -> str:
        return self.org_id or f"{ORG}-{self.run}"

    @property
    def user_id(self) -> str:
        return f"{USER_ID}-{self.run}"

    def sid(self, node_id: str) -> str:
        return f"{node_id}-{self.run}"

    def plain(self, text: str) -> str:
        return text.replace(self.org, "org").replace(f"-{self.run}", "")

    part_ids: list[str] = field(default_factory=list)       # the keys of the part records the seam wrote (after ``seed``)

    def _props(self, node: dict) -> dict:
        props = dict(node["props"])
        for key in ("id", "recordGroupId"):
            if props.get(key):
                props[key] = self.sid(props[key])
        if props.get("userId"):
            props["userId"] = self.user_id
        if props.get("connectorId"):
            props["connectorId"] = f"pdfparts_{self.org}" if props["connectorId"] == "pdfparts" else self.sid(props["connectorId"])
        for key in ("virtualRecordId",):
            if props.get(key):
                props[key] = self.sid(props[key])
        if "orgId" in props:
            props["orgId"] = self.org
        return props

    async def seed(self) -> None:
        nodes, edges = _graph(self.with_parts)
        nodes, edges = arango_shape(nodes, edges)
        kind_of = {n["id"]: n["kind"] for n in nodes}
        with self.org_scope(self.org):
            for kind, collection in NODE_TARGETS.items():
                rows = [self._props(n) for n in nodes if n["kind"] == kind]
                if rows:
                    assert await self.graph.batch_upsert_nodes(rows, collection=collection)
            for edge_type, collection in EDGE_TARGETS.items():
                rows = [
                    {"from_id": self.sid(e["from"]), "from_collection": NODE_TARGETS[kind_of[e["from"]]],
                     "to_id": self.sid(e["to"]), "to_collection": NODE_TARGETS[kind_of[e["to"]]], **e["props"]}
                    for e in edges if e["type"] == edge_type
                ]
                if rows:
                    assert await self.graph.batch_create_edges(rows, collection=collection)
            await self._seed_file_documents([n for n in nodes if n["kind"] == "Record"])
            if self.with_parts:
                await self._seed_splits()

    async def _seed_file_documents(self, records: list[dict]) -> None:
        """What every stored record has: its type document (files) and the isOfType edge to it."""
        files = [
            {"id": self.sid(n["id"]), "orgId": self.org, "name": n["props"]["recordName"], "isFile": True, "extension": "pdf",
             "mimeType": "application/pdf"}
            for n in records
        ]
        assert await self.graph.batch_upsert_nodes(files, "files")
        assert await self.graph.batch_create_edges(
            [{"from_id": f["id"], "from_collection": "records", "to_id": f["id"], "to_collection": "files",
              "createdAtTimestamp": TS} for f in files],
            collection="isOfType",
        )

    async def _seed_splits(self) -> None:
        """Split each file through the product seam (``ParentSplitSeam``): the part records, their ``contentSplitParts``
        documents and the file's ``contentSplits`` row are what production writes, not what a fixture imagines."""
        clear_part_cache()
        store, resources = SplitStore(self.graph), FakeResources()
        seam = ParentSplitSeam(
            graph=self.graph, store=store, purger=GenerationPurger(self.graph, store, FakeLeases(), resources),
            cutter=fake_cutter([(1, 600, 601), (601, 1200, 1201), (1201, 1800, 1800)]), uploader=FakeUploader(),
            publisher=FakePublisher(), whole_file=resources, config=SplitConfig,
        )
        for parent, _owner, vrid in FILES:
            key = self.sid(parent)
            doc = await self.graph.get_document(key, RECORDS)
            assert doc is not None, parent
            outcome = await seam.split(SplitRequest(
                parent={**doc, "_key": key}, v=self.sid(vrid), org_id=self.org, content=b"%PDF", content_rev="rev1",
                page_count=1800, needs_ocr=False,
            ))
            assert outcome.row.state.value in ("SPLITTING", "INDEXING_PARTS")
            self.part_ids += [spec.node_key for spec in outcome.row.parts]

    async def leak(self, how: str) -> None:
        """The two mistakes the scenarios must catch: a part wired into its parent's tree, and a part under its parent's connector."""
        with self.org_scope(self.org):
            if how == "edge":
                edges = [{"from_id": self.part_ids[0], "from_collection": "records", "to_id": self.sid("kbx"), "to_collection": "apps",
                          "createdAtTimestamp": TS, "entityType": "KB"},
                         {"from_id": self.part_ids[3], "from_collection": "records", "to_id": self.sid("drv-dir"), "to_collection": "recordGroups",
                          "createdAtTimestamp": TS}]
                assert await self.graph.batch_create_edges(edges, collection="belongsTo")
            elif how == "connector":
                for part, owner in ((self.part_ids[0], "kbx"), (self.part_ids[3], "drv")):
                    assert await self.graph.update_node_fields_if_match(
                        part, RECORDS, {"connectorId": self.sid(owner)}, {"orgId": self.org})
            else:
                raise ValueError(how)

    async def remove(self) -> None:
        nodes, _ = _graph(with_parts=True)
        ids = [self.sid(n["id"]) for n in nodes]
        ids += [self.sid(v) for _p, _o, v in FILES]
        ids += self.part_ids
        with self.org_scope(self.org):
            for collection in (*NODE_TARGETS.values(), "files", SPLITS, SPLIT_PARTS):
                for key in ids:
                    if await self.graph.get_document(key, collection):
                        await self.graph.delete_nodes_and_edges([key], collection)

    # ---- what each surface answers -------------------------------------------------------------------------

    async def observe(self) -> dict[str, Any]:
        """Every surface of the scenario, as JSON-able data with the per-run suffix removed."""
        graph, org = self.graph, self.org
        out: dict[str, Any] = {}
        with self.org_scope(org):
            user_key = self.sid(USER_U)
            access = await graph.get_knowledge_hub_access_v3(user_key=user_key, org_id=org)

            async def page(app_id: str, **kw: Any) -> dict[str, Any]:  # noqa: ANN401
                if "start_id" in kw:
                    kw["start_id"] = self.sid(kw["start_id"])
                result = await graph.get_knowledge_hub_connector_page_v3(
                    app_id=self.sid(app_id), org_id=org, grantee_ids=access["grantee_ids"], gated_app_ids=access["gated_app_ids"],
                    grants_by_connector=access["by_connector"], limit=50, sort_field="name", sort_dir="ASC", include_total=True, **kw)
                return {"ids": sorted(r["id"] for r in result["rows"]), "total": result.get("total"), "counts": result.get("counts")}

            out["kh_tree"] = {
                "kbx_flat": await page("kbx", flatten=True), "drv_flat": await page("drv", flatten=True),
                "kbx_root": await page("kbx", flatten=False),
                "kbx_folder": await page("kbx", flatten=False, start_id="kbx-folder", start_type="record"),
                "drv_dir": await page("drv", flatten=False, start_id="drv-dir", start_type="recordGroup"),
            }
            found = await search_page(
                graph, user_key=user_key, user_id=self.user_id, org_id=org, limit=50, filters={"search_query": "pdf"}, access=access)
            out["kh_search"] = {"ids": sorted(r["id"] for r in found.rows), "total": found.total, "counts": found.counts_by_type}

            kb = self.sid("kbx")
            listed, total, _filters = await graph.list_kb_records(
                kb, self.user_id, org, 0, 50, None, None, None, None, None, None, None, "name", "asc")
            out["kb_records"] = {"ids": sorted(r["id"] for r in listed), "total": total}
            root = await graph.get_kb_children(kb, 0, 50)
            folder = await graph.get_folder_children(kb, self.sid("kbx-folder"), 0, 50)
            out["kb_children"] = {"root": _children(root), "folder": _children(folder)}

            out["stats"] = {c: await graph.get_connector_stats(org, self.sid(c)) for c in ("kbx", "drv")}

            by_status: dict[str, Any] = {}
            for connector in ("kbx", "drv"):
                for status in [*STATUSES, None]:
                    rows = await graph.get_records_by_status(org, self.sid(connector), [status] if status else None, limit=100)
                    by_status[f"{connector}/{status}"] = sorted(r.id for r in rows)
            out["by_status"] = by_status
            out["by_weburl"] = {
                url: getattr(await graph.get_record_by_weburl(url, org), "id", None)
                for url in ("https://kb.example/kbx-file", "https://drive.example/drv-file", "https://kb.example/none")
            }
            async def can_open(record_id: str, *, exact: bool = False) -> Any:  # noqa: ANN401
                details = await graph.check_record_access_with_details(self.user_id, org, record_id if exact else self.sid(record_id))
                return None if details is None else (details.get("record") or {}).get("id")

            out["access"] = {r: await can_open(r) for r in ("kbx-file", "kbx-folder", "drv-file")}
            out["part_access"] = {r: await can_open(r, exact=True) for r in self.part_ids}
        return json.loads(self.plain(json.dumps(out, default=str, sort_keys=True)))

    async def unlink_count(self) -> tuple[int, bool]:
        """What a finished full sync that saw nothing would unlink for the Drive connector."""
        with self.org_scope(self.org):
            return await self.graph.unlink_nodes_missed_by_full_sync(self.sid("drv"), {RECORDS: [], "recordGroups": [], "groups": [], "roles": []})


def _children(result: dict[str, Any]) -> dict[str, Any]:
    names = lambda key: sorted(str(x.get("id")) for x in (result.get(key) or []))  # noqa: E731
    return {"folders": names("folders"), "records": names("records"), "total": result.get("totalCount")}


def problems(with_parts: dict[str, Any], control: dict[str, Any], part_ids: list[str]) -> list[str]:
    """Surfaces on which the world with parts differs from the control, or shows a part."""
    found = [
        f"{surface} differs from the control: {with_parts[surface]!r} != {control.get(surface)!r}"[:600] for surface in sorted(with_parts)
        if surface != "part_access" and with_parts[surface] != control.get(surface)
    ]
    found += [f"a user can open {part}" for part, opened in with_parts.get("part_access", {}).items() if opened is not None]
    text = json.dumps({k: v for k, v in with_parts.items() if k != "part_access"})
    found += [f"{part} shows" for part in part_ids if part in text]
    if PART_NAME_MARK in text:
        found.append("a part name shows")
    return found
