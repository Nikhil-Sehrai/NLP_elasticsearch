# es_client.py
from typing import Dict, Any, List, Optional
from elasticsearch import Elasticsearch

ES_URL = "http://127.0.0.1:9200"

es = Elasticsearch(ES_URL, request_timeout=30)

# Index + field configuration based on your actual mappings
INDEX_CONFIG: Dict[str, Dict[str, Any]] = {
    "apache": {
        "timestamp_field": "timestamp_iso",
        "default_search_fields": ["message", "raw_line"],
    },
    "hdfs": {
        "timestamp_field": "timestamp_iso",
        "default_search_fields": ["message", "raw_line", "level"],
    },
    "thunderbird": {
        "timestamp_field": "timestamp_iso",
        "default_search_fields": ["message", "raw_line"],
    },
}


def get_known_indices() -> List[str]:
    return list(INDEX_CONFIG.keys())


def get_index_timestamp_field(index: str) -> str:
    return INDEX_CONFIG[index]["timestamp_field"]


def get_search_fields(indices: List[str]) -> List[str]:
    """
    Union of default search fields across all selected indices.
    """
    fields = set()
    for idx in indices:
        cfg = INDEX_CONFIG.get(idx)
        if cfg:
            fields.update(cfg["default_search_fields"])
    # fallback if we somehow got nothing
    return list(fields) or ["message", "raw_line"]


def run_search(
    indices: Optional[List[str]],
    query: Dict[str, Any],
    size: int = 10,
) -> Dict[str, Any]:
    """
    Execute a search on one or more indices.
    If indices is None or empty, search across all known indices.
    """
    if not indices:
        indices = get_known_indices()

    index_param = ",".join(indices)
    body = {
        "size": size,
        "query": query,
    }
    response = es.search(index=index_param, body=body)
    return response


def match_all(indices: Optional[List[str]] = None, size: int = 10) -> Dict[str, Any]:
    """
    Simple helper: match_all query over given indices (or all).
    """
    query = {"match_all": {}}
    return run_search(indices, query, size=size)


if __name__ == "__main__":
    # quick manual self-test
    resp = match_all(size=3)
    for hit in resp["hits"]["hits"]:
        print(hit["_index"], hit["_source"].get("timestamp_iso"), hit["_source"].get("message"))
