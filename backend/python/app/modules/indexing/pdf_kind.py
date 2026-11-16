"""Whether a file is a PDF, for everything that decides if a file may be split.

The answer is the file-type registry's: the same decision the event dispatch makes (``legacy_route``) for ``Route.PDF``,
so a mime alias (``application/x-pdf``, ``text/pdf``), a mime in capitals, an extension alone and a mime alone are
read the same way by the split decision, the re-embed paths, the write scope and the stages. A file the dispatch
parses as a PDF and a file this module calls a PDF can never disagree, which would lose the parts of a file cut by
one and read by the other.
"""
from __future__ import annotations

from app.config import file_types


def is_pdf(extension: str | None, mime_type: str | None) -> bool:
    """True when the registry routes a file with this extension and mime type to the PDF parser."""
    ext = extension.lstrip(".").lower() if isinstance(extension, str) and extension else None
    dispatch = file_types.legacy_route(mime_type, ext, code_extension=ext, reads_as_code_file=False)
    return dispatch is not None and dispatch.route is file_types.Route.PDF
