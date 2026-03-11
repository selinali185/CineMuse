from __future__ import annotations

from typing import Iterable

from run import load_env_file
from app import run_newsapi_sync


def main() -> None:
    load_env_file()
    queries = ["science fiction", "film", "cinema"]
    counts = run_newsapi_sync(queries, pages=3, page_size=30)
    total = sum(counts.values())
    for query, num in counts.items():
        print(f"{query}: {num} articles synced.")
    print(f"Total news articles synced: {total}")


if __name__ == "__main__":
    main()
