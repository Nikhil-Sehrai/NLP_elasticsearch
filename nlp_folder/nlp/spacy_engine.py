# nlp/spacy_engine.py
"""
Lightweight spaCy wrapper used by the parser.
Provides tokenization, lemmatization, stopword filtering and a simple matcher API.
"""

from typing import List, Iterable, Tuple
import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import Doc

# If you want to reference an uploaded file (developer instruction):
# local file path (will be transformed to usable URL by your tooling)
UPLOADED_FILE_URL = "file:///mnt/data/2eb77994-3600-41ca-b795-c180dfa42bc0.png"

try:
    nlp = spacy.load("en_core_web_sm", disable=["textcat"])
except Exception:
    # If the model isn't available, raise a clear error
    raise RuntimeError(
        "spaCy model 'en_core_web_sm' not found. Please run:\n"
        "python -m spacy download en_core_web_sm"
    )

matcher = PhraseMatcher(nlp.vocab, attr="LOWER")


def add_phrase_patterns(name: str, phrases: Iterable[str]) -> None:
    """
    Register a set of phrase patterns under a name.
    Example: add_phrase_patterns("INDEX", ["apache", "hdfs", "thunderbird"])
    """
    patterns = [nlp.make_doc(p) for p in phrases]
    # Remove existing to avoid duplicates
    if name in matcher:
        matcher.remove(name)
    matcher.add(name, patterns)


def doc_tokens(doc: Doc, *, keep_stop: bool = False) -> List[str]:
    """Return tokens or lemmas filtered for punctuation."""
    tokens = []
    for t in doc:
        if t.is_punct or t.is_space:
            continue
        if not keep_stop and t.is_stop:
            continue
        tokens.append(t.lemma_.lower())
    return tokens


def extract_phrases(doc: Doc, phrase_label: str) -> List[str]:
    """Return matched phrase texts for the given phrase_label registered via add_phrase_patterns."""
    matches = matcher(doc)
    vals = []
    for match_id, start, end in matches:
        if nlp.vocab.strings[match_id] == phrase_label:
            vals.append(doc[start:end].text.lower())
    # de-duplicate preserving order
    seen = set()
    out = []
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def clean_keyword_text(text: str, *, keep_stop: bool = False, exclude: Iterable[str] = ()) -> str:
    """
    Return a cleaned keyword string:
      - lemmatized tokens
      - removed stopwords by default
      - exclude tokens/words listed in exclude (e.g., index names)
    """
    doc = nlp(text)
    excludes = set([e.lower() for e in exclude])
    tokens = []
    for t in doc:
        if t.is_punct or t.is_space:
            continue
        lemma = t.lemma_.lower()
        if lemma in excludes:
            continue
        if not keep_stop and t.is_stop:
            continue
        tokens.append(lemma)
    return " ".join(tokens).strip()
