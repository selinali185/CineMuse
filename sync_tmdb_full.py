from __future__ import annotations

from typing import Iterable

from app import app, open_sync_connection, sync_tmdb_genres, sync_tmdb_movies
from run import load_env_file


def run_batch(sources: Iterable[str], pages: int, include_directors: bool) -> None:
    print(f"Syncing {pages} pages per source, include_directors={include_directors}")
    with app.app_context():
        with open_sync_connection() as conn:
            genre_count = sync_tmdb_genres(conn)
            print(f"Genres synced: {genre_count}")
            total = 0
            for source in sources:
                movie_count = sync_tmdb_movies(
                    pages=pages,
                    source=source,
                    include_directors=include_directors,
                    db=conn,
                )
                total += movie_count
                print(f"  {source}: {movie_count} movies")
            print(f"Total movies synced in this batch: {total}")


if __name__ == "__main__":
    load_env_file()
    SOURCES = ["popular", "trending"]
    run_batch(SOURCES, pages=50, include_directors=True)
