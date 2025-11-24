# agent.py
from typing import Any, Dict

from nlp.parser import parse_user_query
from nlp.translator import to_es_search_params
from es_client import run_search


def format_hit(hit: Dict[str, Any]) -> str:
    """
    Nicely format one hit for console output.
    """
    index = hit.get("_index", "?")
    src = hit.get("_source", {})
    ts = src.get("timestamp_iso") or src.get("timestamp_epoch")
    msg = src.get("message") or src.get("raw_line")
    if msg and len(msg) > 200:
        msg = msg[:197] + "..."
    return f"[{index}] {ts} | {msg}"


def run_agent_once(user_query: str) -> None:
    # 1. Parse NL query
    parsed = parse_user_query(user_query)

    # 2. Convert to ES params
    es_params = to_es_search_params(parsed)

    # 3. Execute against Elasticsearch
    response = run_search(
        indices=es_params["indices"],
        query=es_params["query"],
        size=es_params["size"],
    )

    # 4. Show results
    hits = response.get("hits", {}).get("hits", [])
    print(f"\nReturned {len(hits)} hits (showing up to {es_params['size']}):\n")
    for hit in hits:
        print("  " + format_hit(hit))
    print()


def main():
    print("NLP → Elasticsearch Agent (Phase 2)")
    print("Connected to http://127.0.0.1:9200")
    print("Type a query, or 'exit' to quit.\n")

    while True:
        try:
            user_query = input("query> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not user_query:
            continue
        if user_query.lower() in {"exit", "quit"}:
            print("Bye!")
            break

        try:
            run_agent_once(user_query)
        except Exception as e:
            print(f"Error while processing query: {e}")

if __name__ == "__main__":
    main()
