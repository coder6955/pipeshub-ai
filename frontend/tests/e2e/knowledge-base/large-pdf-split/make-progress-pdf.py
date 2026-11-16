"""Cut a page range out of a PDF, for the live progress journey (06-progress-live.spec.ts).

  python3 -I make-progress-pdf.py <source.pdf> <first-page> <last-page> <out.pdf>

A range of 501 pages is the smallest file the split engine cuts (more than 500 pages): two parts,
the second of one page, so "Indexing 0 of 2 parts" is on screen for the length of one part's parse.
Needs pypdfium2 (the version the backend pins).
"""
import sys

import pypdfium2 as pdfium


def main(argv: list[str]) -> None:
    source, first, last, out = argv[0], int(argv[1]), int(argv[2]), argv[3]
    src = pdfium.PdfDocument(source)
    if not 1 <= first <= last <= len(src):
        raise SystemExit(f"range {first}-{last} is outside 1-{len(src)}")
    dst = pdfium.PdfDocument.new()
    dst.import_pages(src, list(range(first - 1, last)))
    dst.save(out)
    print(f"{out}: {last - first + 1} pages")


if __name__ == "__main__":
    main(sys.argv[1:])
