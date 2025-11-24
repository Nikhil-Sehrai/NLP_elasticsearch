# nlp/parser.py
import re
from dataclasses import dataclass
from typing import List, Optional

from es_client import get_known_indices

# spaCy helper
from .spacy_engine import add_phrase_patterns, extract_phrases, clean_keyword_text, nlp

# register known indices with the phrase matcher (so spaCy can find them)
_known_indices = get_known_indices()
if _known_indices:
    add_phrase_patterns("INDEX", _known_indices)

@dataclass
class ParsedQuery:
    indices: Optional[List[str]]
    hours: Optional[int]
    keyword: Optional[str]
    size: int


TIME_RANGE_RE = re.compile(
    r"last\s+(\d+)\s*(minutes?|mins?|hours?|hrs?|days?|day|d)\b",
    re.IGNORECASE,
)

LIMIT_RE = re.compile(
    r"\b(top|limit|show|first|last)\s+(\d+)\b",
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
    Prefer spaCy phrase matches for indices; fall back to simple substring test.
    """
    # spaCy-based extraction (registered via spacy_engine)
    doc = nlp(text)
    matches = extract_phrases(doc, "index")
    if matches:
        # normalize to canonical indices existing in get_known_indices()
        known = {k.lower(): k for k in get_known_indices()}
        resolved = []
        for m in matches:
            key = m.lower()
            if key in known:
                resolved.append(known[key])
        return resolved or None

    # fallback: substring matching
    lower = text.lower()
    indices = []
    for idx in get_known_indices():
        if idx.lower() in lower:
            indices.append(idx)
    return indices or None


def _clean_keyword(text: str) -> Optional[str]:
    """
    Use spaCy to lemmatize and remove stopwords/noise.
    Also strips time phrases, limit phrases, and index names.
    """
    # remove explicit time and limit phrases (keep original for spaCy)
    stripped = TIME_RANGE_RE.sub(" ", text)
    stripped = LIMIT_RE.sub(" ", stripped)

    # build exclude list: index names and some generic noise
    exclude_words = [
        "show", "list", "all", "logs", "events", "entries", "from", "the", "in", "of",
    ]
    exclude_words += get_known_indices()

    cleaned = clean_keyword_text(stripped, keep_stop=False, exclude=exclude_words)
    return cleaned or None


def parse_user_query(text: str) -> ParsedQuery:
    """
    Main entry point: natural language → ParsedQuery.

    Behaviour:
      - hours extracted from simple regex (same as before)
      - size extracted from simple regex (same as before)
      - indices extracted via spaCy phrase matcher OR fallback substring
      - keyword cleaned via spaCy lemmatization/stopword removal
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
        "show me 20 recent apache entries with exception stacktrace",
    ]
    for q in examples:
        print(q, "→", parse_user_query(q))
