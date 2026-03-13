from __future__ import annotations

import os

from app import app, open_sync_connection, sync_tmdb_genres, sync_tmdb_movies
from run import load_env_file


def run_batch(source: str, pages: int, include_directors: bool, start_page: int) -> None:
    print(f"Syncing {pages} pages starting from {start_page} for {source}, include_directors={include_directors}")
    with app.app_context():
        with open_sync_connection() as conn:
            genre_count = sync_tmdb_genres(conn)
            print(f"Genres synced: {genre_count}")
            movie_count = sync_tmdb_movies(
                pages=pages,
                source=source,
                include_directors=include_directors,
                start_page=start_page,
                db=conn,
            )
            print(f"  {source}: {movie_count} movies")
            print(f"Total movies synced in this batch: {movie_count}")


if __name__ == "__main__":
    load_env_file()
    SOURCE = os.environ.get("TMDB_SYNC_SOURCE", "popular")
    PAGES = int(os.environ.get("TMDB_SYNC_PAGES", "50"))
    START_PAGE = int(os.environ.get("TMDB_SYNC_START_PAGE", "1"))
    run_batch(SOURCE, pages=PAGES, include_directors=True, start_page=START_PAGE)
