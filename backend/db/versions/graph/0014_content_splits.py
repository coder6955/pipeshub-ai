"""graph: the indexes of split files: the split rows by state and age, and a split file's parts (node kind contentSplitParts, keyed by the part record) by their root, by root and generation, and by owner connector"""
from alembic import op
from db.lib.ddl import ensure_partitioned_index

revision = "graph_0014"
down_revision = "graph_0013"
branch_labels = None
depends_on = None
older_code_compatible = "adds indexes only; older code reads the same rows through the indexes it used"

ROOT = "(doc->>'rootVirtualRecordId')"
# (name, columns, predicate). Partial on the node kind: only a split row has a state, only a part document has a root and an owner.
INDEXES = [
    ("nodes_content_split_state", "org_id, (doc->>'state'), (doc->'updatedAtMs')", "coll = 'contentSplits'"),
    ("nodes_content_split_part_root", f"org_id, {ROOT}", "coll = 'contentSplitParts'"),
    ("nodes_content_split_part_root_gen", f"org_id, {ROOT}, (doc->>'gen')", "coll = 'contentSplitParts'"),
    ("nodes_content_split_part_owner", "org_id, (doc->>'ownerConnectorId')", "coll = 'contentSplitParts'"),
    ("nodes_content_split_part_vrid", "org_id, (doc->>'virtualRecordId')", "coll = 'contentSplitParts'"),
]

def upgrade():
    # Built inside the upgrade's one transaction, so the writers of a leaf wait for its build (as graph_0006 and graph_0011 do).
    # shortcut: a build reads a whole leaf, with writers blocked: run this upgrade on a graph that holds many rows in a maintenance window. No release holds a graph built before these revisions.
    for name, cols, where in INDEXES:
        ensure_partitioned_index(op.get_bind(), "graph.nodes", name, cols, where=where, blocking=True)


def downgrade():
    for name, *_ in INDEXES:
        op.execute(f"DROP INDEX IF EXISTS graph.{name}")
