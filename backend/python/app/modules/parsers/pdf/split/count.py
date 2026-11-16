"""The page count of a PDF from a pdfium-only open, the cheap first look the split decision needs.

Runs in a parse worker and imports only light modules. It never opens the file with pdfminer, which on a
large file is the step that runs out of memory (a 166 MB, 17,153 page file: 13.5 s to open, then a full
in-memory pdfium rewrite).
"""
from __future__ import annotations

import pypdfium2

from app.exceptions.indexing_exceptions import PasswordProtectedError
from app.modules.parsers.encryption import (
    PASSWORD_PROTECTED_MESSAGE,
    open_pdfium_document,
    pdfium_password_error,
)
from app.modules.parsers.pdf.split.cut import PdfSplitError


def count_pages(content: bytes) -> int:
    """Pages of *content* as pdfium sees them.

    Raises ``PasswordProtectedError`` when pdfium needs a password and ``PdfSplitError`` when it cannot
    open the file for another reason (the caller then inspects the file as it always did).
    """
    try:
        document = open_pdfium_document(content)
    except pypdfium2.PdfiumError as exc:
        if pdfium_password_error(exc):
            raise PasswordProtectedError(PASSWORD_PROTECTED_MESSAGE, details={"format": "pdf"}) from exc
        raise PdfSplitError(f"pdfium could not open the PDF: {exc}") from exc
    try:
        return len(document)
    finally:
        document.close()
