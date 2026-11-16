"""The graph node of one part of a split PDF, and the event that queues it."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import (
    CollectionNames,
    Connectors,
    EventTypes,
    MimeTypes,
    OriginTypes,
    ProgressStatus,
    RecordTypes,
)
from app.models.entities import FileRecord, RecordType
from app.modules.indexing.owner_connector import part_connector_id
from app.modules.indexing.split_state import ORG_FIELD
from app.utils.time_conversion import get_epoch_timestamp_in_ms

if TYPE_CHECKING:
    from collections.abc import Mapping

    from app.modules.indexing.split_state import PartSpec

RECORDS = CollectionNames.RECORDS.value


class PartFileRecord(FileRecord):
    """A part's record, written in one upsert with the ``virtualRecordId`` it is indexed under (a connector
    record gets that id when it is first indexed, but a part's is derived and must be there when its event
    is read). ``virtualRecordId`` is an ordinary record field."""

    def to_arango_base_record(self) -> dict:
        base = super().to_arango_base_record()
        if self.virtual_record_id:
            base["virtualRecordId"] = self.virtual_record_id
        return base


def part_record_name(parent_name: str, spec: PartSpec) -> str:
    return f"{parent_name} · pages {spec.page_start}–{spec.page_end}"


def part_file_record(
    spec: PartSpec, *, gen: str, parent: Mapping[str, Any], storage_document_id: str | None, size_bytes: int,
) -> FileRecord:
    """The record a part is stored as: an upload of its own under the hidden parts connector, with no edges,
    so nothing that walks a connector's or a collection's tree can reach it. It carries only ordinary record
    fields; what makes it a part is its ``contentSplitParts`` document (``split_state.part_info``)."""
    org_id = str(parent[ORG_FIELD])
    now = get_epoch_timestamp_in_ms()
    return PartFileRecord(
        id=spec.node_key,
        org_id=org_id,
        record_name=part_record_name(str(parent.get("recordName") or "document.pdf"), spec),
        record_type=RecordType.FILE,
        external_record_id=f"part:{gen}:{spec.k}",
        version=0,
        origin=OriginTypes.UPLOAD,
        connector_name=Connectors.KNOWLEDGE_BASE,
        connector_id=part_connector_id(org_id),
        virtual_record_id=spec.vrid,
        mime_type=MimeTypes.PDF.value,
        size_in_bytes=size_bytes,
        storage_document_id=storage_document_id,
        indexing_status=ProgressStatus.NOT_STARTED.value,
        parsing_status=ProgressStatus.NOT_STARTED.value,
        extraction_status=ProgressStatus.NOT_STARTED.value,
        is_internal=True,
        preview_renderable=False,
        queued_at=now,
        created_at=now,
        updated_at=now,
        is_file=True,
        extension="pdf",
    )


def part_event(part: Mapping[str, Any], *, owner_connector_id: str) -> tuple[str, dict[str, Any]]:
    """``(event_type, payload)`` of a part's ``newRecord``. The envelope names the owner connector,
    which is what admission, fairness and the connector-off gates read."""
    return EventTypes.NEW_RECORD.value, {
        "recordId": part["_key"] if "_key" in part else part["id"],
        "recordName": part.get("recordName"),
        "orgId": part.get(ORG_FIELD),
        "version": part.get("version", 0),
        "connectorName": part.get("connectorName"),
        "connectorId": owner_connector_id,
        "extension": part.get("extension") or "pdf",
        "mimeType": part.get("mimeType") or MimeTypes.PDF.value,
        "origin": part.get("origin") or OriginTypes.UPLOAD.value,
        "recordType": part.get("recordType") or RecordTypes.FILE.value,
        "virtualRecordId": part.get("virtualRecordId"),
        "sizeInBytes": part.get("sizeInBytes"),
        "storageDocumentId": part.get("storageDocumentId"),
    }
