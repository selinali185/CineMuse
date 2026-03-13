from __future__ import annotations

from run import load_env_file
from app import clean_news_table, open_sync_connection


def main() -> None:
    load_env_file()
    with open_sync_connection() as conn:
        removed = clean_news_table(conn)
    print(f"Removed {removed} non-movie news articles.")


if __name__ == "__main__":
    main()
