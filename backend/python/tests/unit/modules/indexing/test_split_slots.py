"""What a part of a split PDF costs the resource governor.

A part is an ordinary PDF record: one heavy index slot and one heavy parse permit, never the two a file over
the extra-large size takes, and memory reserved for the part's own size, not the parent's.
"""
from __future__ import annotations

import pytest

from app.modules.indexing.split_parts import part_event
from app.modules.parsers.pdf.split.config import SplitConfig
from app.services.resource_governor.models import ParseTier, Pool
from app.services.resource_governor.tiers import (
    XL_HEAVY_BYTES,
    classify,
    gate_pool,
    index_pool,
    parse_cost,
)
from tests.unit.modules.indexing.test_split_seam import RANGES, Rig


@pytest.mark.asyncio
async def test_a_part_event_takes_one_heavy_index_slot_and_one_heavy_parse_permit() -> None:
    rig = Rig(RANGES)
    await rig.seam.split(rig.request())

    for _topic, _event_type, payload, _key in rig.publisher.sent:
        tier = classify(payload["extension"], payload["mimeType"])
        assert tier is ParseTier.HEAVY
        assert index_pool(tier) is Pool.INDEX_HEAVY
        assert gate_pool(tier) is Pool.HEAVY_PARSE
        assert parse_cost(tier, payload["sizeInBytes"]) == 1


def test_a_part_at_the_byte_cap_still_costs_one_permit() -> None:
    cap = SplitConfig().max_part_bytes

    assert cap <= XL_HEAVY_BYTES
    assert parse_cost(ParseTier.HEAVY, cap) == 1
    assert parse_cost(ParseTier.HEAVY, XL_HEAVY_BYTES + 1) == 2  # what the unsplit file would have cost


@pytest.mark.asyncio
async def test_a_part_event_names_the_size_of_the_part_not_of_the_file() -> None:
    rig = Rig(RANGES)
    await rig.seam.split(rig.request())

    sizes = {payload["sizeInBytes"] for _t, _e, payload, _k in rig.publisher.sent}

    assert sizes == {len(b"%PDF-1.4 fake")}


def test_the_event_of_a_part_that_has_no_file_yet_is_not_built_from_a_stale_node() -> None:
    _type, payload = part_event({"_key": "k", "orgId": "o", "virtualRecordId": "v"}, owner_connector_id="c")

    assert payload["storageDocumentId"] is None and payload["connectorId"] == "c"
