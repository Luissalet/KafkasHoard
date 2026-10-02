"""Page ranges as people write them: «1-3,5,8-», «last», «-1» (the last page), «3-last», «odd», «even», «all».

Pages are 1-based. Every mistake raises a ``KafkaError`` with a Spanish message that says what was wrong and how to write it.
"""

from __future__ import annotations

import re

from ..errors import KafkaError
from ..util import fold

_LAST = {"last", "ultima", "ultimo", "final", "fin", "end"}
_ALL = {"all", "todas", "todo", "todos"}
_ODD = {"odd", "impar", "impares"}
_EVEN = {"even", "par", "pares"}
_RANGE = re.compile(r"^(?P<a>-?\d+|last)-(?P<b>-?\d+|last)?$")
_SINGLE = re.compile(r"^(?:-?\d+|last)$")
EXAMPLES = "Ejemplos: 3, 1-3, 2,5,8-, last, -1 (la última), 3-last, impares, pares, todas."


def _norm(text: str) -> str:
    t = fold(str(text or "")).strip().lower()
    t = re.sub(r"[‒-―−]", "-", t)                  # en dash, em dash, minus sign
    t = re.sub(r"\.\.+", "-", t)
    t = re.sub(r"(?<=[\dt])\s+(?:a|hasta)\s+(?=[\d-]|last|ultima|final|fin|end)", "-", t)      # «1 a 3», «3 hasta last»
    t = re.sub(r"\s+y\s+", ",", t)
    t = re.sub(r"\s*-\s*", "-", t)
    t = re.sub(r"\b(?:pagina|paginas|pag|pags|p)\.?\s*(?=\d|-|last)", "", t)  # «pág. 3», «p. 3»
    for word in _LAST:
        t = re.sub(rf"\b{word}\b", "last", t)
    return t


def _one(token: str, total: int, original: str) -> int:
    if token == "last":
        return total
    n = int(token)
    if n == 0:
        raise KafkaError("invalid", "La página 0 no existe: las páginas empiezan en 1.", EXAMPLES)
    if n < 0:
        n = total + n + 1
        if n < 1:
            raise KafkaError("invalid", f"«{original}» queda antes de la primera página: el PDF tiene {total} página(s).", EXAMPLES)
        return n
    if n > total:
        raise KafkaError("invalid", f"La página {n} no existe: el PDF tiene {total} página(s).", EXAMPLES)
    return n


def parse_groups(text: str, total: int) -> list[list[int]]:
    """One list of 1-based pages per comma-separated part, in the order written."""
    if total < 1:
        raise KafkaError("invalid", "El PDF no tiene páginas.")
    norm = _norm(text)
    if not norm:
        raise KafkaError("invalid", "Indica las páginas.", EXAMPLES)
    groups: list[list[int]] = []
    for token in (t for t in re.split(r"[,;\s]+", norm) if t):
        if token in _ALL:
            groups.append(list(range(1, total + 1)))
        elif token in _ODD:
            groups.append(list(range(1, total + 1, 2)))
        elif token in _EVEN:
            groups.append(list(range(2, total + 1, 2)))
        elif _SINGLE.match(token):
            groups.append([_one(token, total, token)])
        else:
            m = _RANGE.match(token)
            if not m:
                raise KafkaError("invalid", f"No entiendo «{token}» como página o rango.", EXAMPLES)
            first = _one(m.group("a"), total, token)
            last = _one(m.group("b"), total, token) if m.group("b") else total
            if first > last:
                raise KafkaError("invalid", f"El rango «{token}» está al revés: la primera página ({first}) es mayor que la última ({last}).", EXAMPLES)
            groups.append(list(range(first, last + 1)))
    if not groups:
        raise KafkaError("invalid", "Indica las páginas.", EXAMPLES)
    return groups


def parse_ranges(text: str, total: int, *, default_all: bool = False, unique: bool = False) -> list[int]:
    """Flat list of 1-based pages. Empty text means every page when ``default_all`` is set."""
    if not str(text or "").strip() and default_all:
        return list(range(1, total + 1))
    pages = [p for group in parse_groups(text, total) for p in group]
    if unique:
        seen: set[int] = set()
        pages = [p for p in pages if not (p in seen or seen.add(p))]
    return pages


def describe(pages: list[int]) -> str:
    """«1-3,5,8-9» for a list of pages (sorted, without repeats)."""
    out: list[str] = []
    items = sorted(set(pages))
    i = 0
    while i < len(items):
        j = i
        while j + 1 < len(items) and items[j + 1] == items[j] + 1:
            j += 1
        out.append(str(items[i]) if i == j else f"{items[i]}-{items[j]}")
        i = j + 1
    return ",".join(out)
