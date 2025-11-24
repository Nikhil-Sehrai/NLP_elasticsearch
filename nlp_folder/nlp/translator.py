# nlp/translator.py
from typing import Dict, Any, List, Optional

from es_client import (
    get_known_indices,
    get_index_timestamp_field,
    get_search_fields,
)
from .parser import ParsedQuery


def build_es_query(parsed: ParsedQuery) -> Dict[str, Any]:
    """
    Turn a ParsedQuery into a pure Elasticsearch query dict.
    Does NOT execute the query.
    """
    must: List[Dict[str, Any]] = []

    indices = parsed.indices or get_known_indices()

    # time range filter
    if parsed.hours is not None:
        # all your indices share 'timestamp_iso', but we'll still resolve per index
        # and assume same field name across for simplicity.
        # If you ever have different timestamp fields, you'll want per-index queries.
        ts_field = get_index_timestamp_field(indices[0])
        must.append(
            {
                "range": {
                    ts_field: {
                        "gte": f"now-{parsed.hours}h",
                        "lte": "now",
                    }
                }
            }
        )

    # keyword search
    if parsed.keyword:
        fields = get_search_fields(indices)
        must.append(
            {
                "multi_match": {
                    "query": parsed.keyword,
                    "fields": fields,
                }
            }
        )

    if not must:
        query: Dict[str, Any] = {"match_all": {}}
    else:
        query = {"bool": {"must": must}}

    return query


def to_es_search_params(parsed: ParsedQuery) -> Dict[str, Any]:
    """
    Returns a dict with:
      - indices: List[str] or None
      - query: ES query DSL
      - size: int
    """
    indices = parsed.indices or get_known_indices()
    query = build_es_query(parsed)
    return {
        "indices": indices,
        "query": query,
        "size": parsed.size,
    }


if __name__ == "__main__":
    from .parser import parse_user_query

    q = "Show me failed login events from apache in the last 24 hours top 20"
    parsed = parse_user_query(q)
    params = to_es_search_params(parsed)
    print("Parsed:", parsed)
    print("ES params:", params)
