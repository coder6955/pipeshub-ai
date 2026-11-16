"""Search points of a split file dropped because their block address cannot be built.

A part point whose local block index or part number is out of range is malformed;
it is dropped from the search and counted here, and the search keeps its other hits.
Nothing about the query or the record is recorded.
"""

from app.telemetry.backend import METRICS_BACKEND

BAD_BLOCK_ADDRESS_POINTS = METRICS_BACKEND.counter(
    "pipeshub_split_bad_block_address_total",
    "Search points of a split file dropped because their block has no address (malformed part point)",
    [],
)
