"""Read-only differences in archived page text; no visual or legal analysis."""
from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any, Callable, Optional

MAX_CHARS = 200_000
MAX_LINES = 4_000
MAX_PAGES = 200


def lines(pages: list[dict[str, Any]]) -> list[tuple[int, str, str]]:
    if len(pages) > MAX_PAGES:
        raise ValueError("too many pages to compare; select up to 200 pages per document")
    if sum(len(p.get("text") or "") for p in pages) > MAX_CHARS:
        raise ValueError("too much text to compare; select fewer pages")
    out = []
    for page in pages:
        for raw in str(page.get("text") or "").splitlines():
            normalized = " ".join(raw.split())
            if normalized:
                out.append((int(page["page"]), raw.strip(), normalized))
    if len(out) > MAX_LINES:
        raise ValueError("too many text lines to compare; select fewer pages")
    return out


def compare(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> dict[str, Any]:
    """Align lines across page boundaries, retaining original page provenance.

    Only whitespace inside a line and empty lines are ignored. Digits,
    signs, punctuation, case, accents and repeated lines are retained.
    """
    left, right = lines(before), lines(after)
    matcher = SequenceMatcher(None, [r[2] for r in left], [r[2] for r in right], autojunk=False)
    changes = []
    for kind, i, j, a, b in matcher.get_opcodes():
        if kind == "equal":
            continue
        changes.append({"kind": kind, "before": left[i:j], "after": right[a:b]})
    unavailable = {"before": [p["page"] for p in before if not str(p.get("text") or "").strip()],
                   "after": [p["page"] for p in after if not str(p.get("text") or "").strip()]}
    complete = bool(before and after) and not any(unavailable.values())
    return {"changes": changes, "same_text": False if changes else True if complete else None,
            "complete_text_available": complete, "empty_text_pages": unavailable,
            "before_lines": len(left), "after_lines": len(right)}


def excerpt(rows: list[tuple[int, str, str]], byte_limit: int = 800,
            transform_text: Optional[Callable[[str], str]] = None) -> dict[str, Any]:
    text = "\n".join(r[1] for r in rows)
    if transform_text is not None:
        text = transform_text(text)
    raw = text.encode("utf-8")
    clipped = len(raw) > byte_limit
    pages = list(dict.fromkeys(r[0] for r in rows))
    return {"pages": pages[:10], "page_count": len(pages), "pages_truncated": len(pages) > 10,
            "text": raw[:byte_limit].decode("utf-8", "ignore") if clipped else text,
            "excerpt_truncated": clipped}
