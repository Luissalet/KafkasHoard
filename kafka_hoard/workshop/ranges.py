"""Page ranges as people write them: «1-3,5,8-», «last», «-1» (the last page), «3-last», «odd», «even», «all».

The grammar and its Spanish messages are the shared ones of Hoard Link (``hoard_link.docs.pageranges``); this module only turns
its ``PageRangeError`` into the ``KafkaError`` the API, the tools and the UI already report, with the examples as the hint.
Pages are 1-based. Kafka keeps repeats by default (``unique=False``): «2,2,1» is three pages.
"""

from __future__ import annotations

from ..errors import KafkaError
from ..hoard_link.docs import pageranges as _pr
from ..hoard_link.docs.pageranges import describe

EXAMPLES = _pr.EXAMPLES_ES

__all__ = ["EXAMPLES", "describe", "parse_groups", "parse_ranges"]


def _invalid(error: _pr.PageRangeError) -> KafkaError:
    message = error.message_es
    if message.endswith(" " + _pr.EXAMPLES_ES):
        message = message[: -len(_pr.EXAMPLES_ES) - 1]
    return KafkaError("invalid", message.replace("el documento tiene", "el PDF tiene"), EXAMPLES)


def parse_groups(text: str, total: int) -> list[list[int]]:
    """One list of 1-based pages per comma-separated part, in the order written."""
    try:
        return _pr.parse_groups(text, total)
    except _pr.PageRangeError as error:
        raise _invalid(error) from error


def parse_ranges(text: str, total: int, *, default_all: bool = False, unique: bool = False) -> list[int]:
    """Flat list of 1-based pages. Empty text means every page when ``default_all`` is set."""
    try:
        return _pr.parse_ranges(text, total, default_all=default_all, unique=unique)
    except _pr.PageRangeError as error:
        raise _invalid(error) from error
