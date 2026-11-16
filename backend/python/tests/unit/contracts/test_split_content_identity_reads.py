"""T9: a part's content is reached through the file's identity, and only through ``content_vrid()``.

Every read of a hit's ``virtualRecordId`` under the query side means "the file" (access, record maps keyed by the
file, dedup of a record). Picking which stored record to *read content from* must go through
``hit_shape.content_vrid``. The allow-list below names each file that may read ``virtualRecordId`` directly, with
how many reads and why; adding a read, or a new file, fails this test until the author decides which kind it is.
"""
from __future__ import annotations

import re
from pathlib import Path

APP = Path(__file__).resolve().parents[3] / "app"
SCOPES = (
    "modules/retrieval", "utils", "agents", "modules/qna", "modules/agents",
    "api/routes/search.py", "api/routes/records.py", "api/routes/chatbot.py",
)
READ = re.compile(r"""get\(\s*["']virtualRecordId["']|\[\s*["']virtualRecordId["']\s*\]""")

BLOCK_INDEX_READ = re.compile(r"""get\(\s*["']blockIndex["']|\[\s*["']blockIndex["']\s*\]""")
CONTENT_MAP_READ = re.compile(r"virtual_record_id_to_result\s*\[")
ALIAS = re.compile(r"""^\s*\w+\s*=\s*["']virtualRecordId["']""", re.MULTILINE)

IDENTITY = "identity: access, membership or the record map keyed by the file"
ATTACHMENT = "an attachment's own record, never a hit"
GRAPH_ROW = "a graph row or index row, not a search hit"
LEGACY = "points of the old shape, which predate splitting"

# file -> (reads, why)
ALLOWED: dict[str, tuple[int, str]] = {
    "api/routes/chatbot.py": (2, GRAPH_ROW),
    "api/routes/records.py": (1, GRAPH_ROW + " (the record's file, to ask whether it is split)"),
    "modules/agents/qna/router.py": (1, ATTACHMENT),
    "agents/actions/code_graph/history.py": (1, GRAPH_ROW),
    "agents/actions/code_graph/read.py": (1, GRAPH_ROW),
    "agents/actions/code_graph/search.py": (3, GRAPH_ROW),
    "agents/actions/knowledge_graph/ops/co_mentions.py": (1, GRAPH_ROW),
    "agents/actions/knowledge_graph/ops/entity_records.py": (1, GRAPH_ROW),
    "agents/actions/knowledge_graph/ops/outside_dates.py": (1, "dedup key of a shown passage: the content key is read first"),
    "agents/actions/knowledge_graph/ops/timeline.py": (1, "dedup key of a passage: the content key is read first"),
    "agents/agent_loop/hooks/attachment_resolver.py": (5, ATTACHMENT),
    "agents/agent_loop/retrieval_ledger.py": (1, GRAPH_ROW),
    "agents/chat_modes/attachments.py": (1, ATTACHMENT),
    "api/routes/search.py": (1, "the /retrieve chunk id of a passage already through public_hit"),
    "modules/retrieval/entity_filters.py": (1, GRAPH_ROW),
    "modules/retrieval/generation_check.py": (1, IDENTITY + " (the live generation is the file's)"),
    "modules/retrieval/hit_shape.py": (4, "the shape itself"),
    "modules/retrieval/provenance.py": (1, GRAPH_ROW),
    "modules/retrieval/record_cut.py": (2, IDENTITY + " (one file, one share)"),
    "modules/retrieval/result_merging.py": (2, "dedup of fused hits by file and block"),
    "modules/retrieval/retrieval_service.py": (16, IDENTITY + "; lexical and access checks"),
    "modules/retrieval/selection_scope.py": (1, GRAPH_ROW),
    "modules/retrieval/title_hits.py": (1, GRAPH_ROW),
    "utils/attachment_utils.py": (6, ATTACHMENT),
    "utils/chat_helpers.py": (5, IDENTITY + "; the old-shape loop (" + LEGACY + ")"),
    "utils/citations.py": (1, "fallback after the passage's own content key"),
    "utils/fetch_full_record.py": (3, "a graph record, not a hit"),
    "utils/storage_move.py": (1, "a request body, not a hit"),
}
# Files that load a stored record for a hit must pick it with content_vrid().
LOADERS = (
    "utils/chat_helpers.py",
    "utils/record_block_selection.py",
    "modules/retrieval/retrieval_service.py",
)


def _files() -> list[Path]:
    out: list[Path] = []
    for scope in SCOPES:
        path = APP / scope
        out += [path] if path.is_file() else sorted(path.rglob("*.py"))
    return out


def test_every_direct_read_of_virtual_record_id_is_accounted_for() -> None:
    found = {}
    for path in _files():
        count = len(READ.findall(path.read_text()))
        if count:
            found[str(path.relative_to(APP))] = count
    unexpected = {f: n for f, n in found.items() if f not in ALLOWED}
    changed = {f: (n, ALLOWED[f][0]) for f, n in found.items() if f in ALLOWED and ALLOWED[f][0] != n}
    assert not unexpected, f"new direct reads of virtualRecordId; use content_vrid() to load content: {unexpected}"
    assert not changed, f"read count changed (found, allowed): {changed}"
    assert set(ALLOWED) <= set(found), f"allow-list entries with no read left: {sorted(set(ALLOWED) - set(found))}"


def test_loaders_pick_the_stored_record_with_the_accessor() -> None:
    missing = [f for f in LOADERS if "content_vrid(" not in (APP / f).read_text()]
    assert not missing, f"these load content for a hit but do not call content_vrid(): {missing}"


def test_emitters_use_the_one_boundary() -> None:
    wants = {
        "api/routes/search.py": ("public_results(", "public_hit("),
        "agents/agent_loop/retrieval_ledger.py": ("block_address(",),
    }
    for file, needles in wants.items():
        text = (APP / file).read_text()
        for needle in needles:
            assert needle in text, f"{file} must serialise through {needle}"


def test_no_other_module_computes_a_block_address() -> None:
    shift = re.compile(r"<<\s*(SEGMENT_SHIFT|20)\b|>>\s*(SEGMENT_SHIFT|20)\b|\*\s*2\s*\*\*\s*20|\*\s*1048576")
    offenders = [
        str(p.relative_to(APP)) for p in _files()
        if p.name != "hit_shape.py" and shift.search(p.read_text())
    ]
    assert not offenders, f"block addresses are computed only in hit_shape.py: {offenders}"


# A point's ``blockIndex`` is local to the stored record named by ``content_vrid``; it may be read only to index
# that record's blocks (the loaders) and never sent. file -> (reads, why)
BLOCK_INDEX_ALLOWED: dict[str, tuple[int, str]] = {
    "agents/actions/code_graph/semantic.py": (2, "a code-graph point, not a split file"),
    "modules/retrieval/hit_shape.py": (1, "the shape itself"),
    "utils/chat_helpers.py": (2, "the loader that indexes the record content_vrid() chose"),
    "utils/record_block_selection.py": (1, "indexes the record content_vrid() chose"),
}
# Content maps are keyed by the stored record; subscripting one is only right with a content key.
CONTENT_MAP_ALLOWED: dict[str, tuple[int, str]] = {
    "utils/chat_helpers.py": (12, "keys come from content_vrid() or the loop's content key"),
    "utils/citations.py": (1, "the passage's own content key"),
    "utils/fetch_slack_thread.py": (1, "a Slack record, never a part"),
    "utils/pattern_match.py": (1, "a grep record keyed by the file (the graph record is the content)"),
}


def _counts(pattern: re.Pattern[str]) -> dict[str, int]:
    found = {}
    for path in _files():
        count = len(pattern.findall(path.read_text()))
        if count:
            found[str(path.relative_to(APP))] = count
    return found


def _check(found: dict[str, int], allowed: dict[str, tuple[int, str]], what: str) -> None:
    expected = {f: n for f, (n, _why) in allowed.items()}
    assert found == expected, f"{what}: found {found}, allowed {expected}; decide whether the new read is local or a content key"


def test_a_points_local_block_index_is_read_only_by_the_loaders() -> None:
    _check(_counts(BLOCK_INDEX_READ), BLOCK_INDEX_ALLOWED, "blockIndex reads")


def test_content_maps_are_subscripted_only_where_listed() -> None:
    _check(_counts(CONTENT_MAP_READ), CONTENT_MAP_ALLOWED, "virtual_record_id_to_result[ reads")


def test_virtual_record_id_is_not_aliased_to_hide_a_read() -> None:
    assert _counts(ALIAS) == {}, "a constant holding 'virtualRecordId' would hide reads from this contract"
