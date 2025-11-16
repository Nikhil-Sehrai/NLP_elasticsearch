# nlp/parser.py
import re
from dataclasses import dataclass
from typing import List, Optional

from es_client import get_known_indices


@dataclass
class ParsedQuery:
    indices: Optional[List[str]]
    hours: Optional[int]
    keyword: Optional[str]
    size: int


TIME_RANGE_RE = re.compile(
    r"last\s+(\d+)\s*(minutes?|mins?|hours?|hrs?|days?|day|d)",
    re.IGNORECASE,
)

LIMIT_RE = re.compile(
    r"(top|limit|show|first|last)\s+(\d+)",
    re.IGNORECASE,
)


def _parse_time_range(text: str) -> Optional[int]:
    """
    Parse things like:
      - last 24 hours
      - last 15 minutes
      - last 7 days
    Return approximate hours (int) or None.
    """
    match = TIME_RANGE_RE.search(text)
    if not match:
        return None

    value = int(match.group(1))
    unit = match.group(2).lower()

    if unit.startswith("min"):
        # round up to nearest hour
        hours = max(1, round(value / 60))
    elif unit.startswith("hour") or unit.startswith("hr"):
        hours = value
    elif unit.startswith("day") or unit == "d":
        hours = value * 24
    else:
        hours = None

    return hours


def _parse_limit(text: str, default: int = 10) -> int:
    match = LIMIT_RE.search(text)
    if not match:
        return default
    size = int(match.group(2))
    return max(1, min(size, 1000))  # basic sane bounds


def _parse_indices(text: str) -> Optional[List[str]]:
    """
    Look for explicit index mentions.
    If none found, return None ⇒ search all.
    """
    lower = text.lower()
    indices = []

    for idx in get_known_indices():
        if idx.lower() in lower:
            indices.append(idx)

    return indices or None


def _clean_keyword(text: str) -> Optional[str]:
    """
    Remove obvious boilerplate words and time/limit/index hints
    to get a simple keyword string.
    """
    lower = text.lower()

    # strip time phrases
    lower = TIME_RANGE_RE.sub(" ", lower)
    # strip limit phrases
    lower = LIMIT_RE.sub(" ", lower)

    # generic noise tokens
    noise_words = [
        "show me",
        "show",
        "list",
        "all",
        "logs",
        "events",
        "entries",
        "from",
        "the",
        "in",
        "of",
        "apache",
        "hdfs",
        "thunderbird",
    ]
    for w in noise_words:
        lower = lower.replace(w, " ")

    # normalize whitespace
    cleaned = " ".join(lower.split())
    return cleaned or None


def parse_user_query(text: str) -> ParsedQuery:
    """
    Main entry point: natural language → ParsedQuery.
    """
    hours = _parse_time_range(text)
    size = _parse_limit(text)
    indices = _parse_indices(text)
    keyword = _clean_keyword(text)

    return ParsedQuery(
        indices=indices,
        hours=hours,
        keyword=keyword,
        size=size,
    )


if __name__ == "__main__":
    examples = [
        "Show me failed login events from apache in the last 24 hours",
        "List all error logs from hdfs last 7 days top 50",
        "Show thunderbird logs about timeout",
    ]
    for q in examples:
        print(q, "→", parse_user_query(q))
