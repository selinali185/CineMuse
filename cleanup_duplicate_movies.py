from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Iterable


def normalize_title(title: str | None) -> str:
    return " ".join((title or "").strip().lower().split())


def deduplicate_movies(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        """
        SELECT movie_id, title, release_year, poster_url, description, tmdb_movie_id
        FROM movies
        """
    ).fetchall()
    groups: dict[tuple[str, int | None], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        key = (normalize_title(row["title"]), row["release_year"])
        groups[key].append(row)

    to_delete: list[int] = []
    for group in groups.values():
        if len(group) < 2:
            continue
        candidate = sorted(
            group,
            key=lambda r: (
                bool(r["poster_url"]),
                bool(r["description"]),
                bool(r["tmdb_movie_id"]),
                -int(r["movie_id"]),
            ),
            reverse=True,
        )[0]
        for row in group:
            if row["movie_id"] == candidate["movie_id"]:
                continue
            to_delete.append(row["movie_id"])

    if to_delete:
        conn.executemany("DELETE FROM movies WHERE movie_id = ?", [(mid,) for mid in to_delete])
        conn.commit()
    return len(to_delete)


def main() -> None:
    conn = sqlite3.connect("cinemuse.db")
    conn.row_factory = sqlite3.Row
    removed = deduplicate_movies(conn)
    conn.close()
    print(f"Removed {removed} duplicate movie entries.")


if __name__ == "__main__":
    main()
