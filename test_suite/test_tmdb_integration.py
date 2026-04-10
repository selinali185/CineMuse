import sqlite3

import app as cine_app


class DummyResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise cine_app.requests.exceptions.HTTPError(response=self)

    def json(self):
        return self._payload


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cine_app.ensure_movies_genres_directors_tables(conn)
    return conn


def test_fetch_tmdb_popular_movies_success(monkeypatch):
    payload = {"results": [{"id": 1, "title": "Test Movie", "vote_average": 8.0}]}

    def fake_get(*_args, **_kwargs):
        return DummyResponse(payload=payload)

    monkeypatch.setattr(cine_app.requests, "get", fake_get)
    data = cine_app.fetch_tmdb_popular_movies("key", page=1)
    assert data["results"][0]["title"] == "Test Movie"


def test_store_popular_movies_inserts_rows():
    conn = make_conn()
    payload = {
        "results": [
            {
                "id": 100,
                "title": "Stored Movie",
                "release_date": "2024-01-20",
                "vote_average": 7.8,
                "poster_path": "/a.jpg",
                "overview": "Overview",
            }
        ]
    }

    inserted = cine_app.store_popular_movies(conn, payload)
    row = conn.execute("SELECT title, release_year, rating FROM movies WHERE tmdb_movie_id = 100").fetchone()

    assert inserted == 1
    assert row["title"] == "Stored Movie"
    assert row["release_year"] == 2024
    assert row["rating"] == 3.9


def test_fetch_and_store_genres_requests(monkeypatch):
    conn = make_conn()

    def fake_get(*_args, **_kwargs):
        return DummyResponse(payload={"genres": [{"id": 28, "name": "Action"}]})

    monkeypatch.setattr(cine_app.requests, "get", fake_get)
    count = cine_app.fetch_and_store_genres_requests(conn, "key")
    row = conn.execute("SELECT genre_name, tmdb_genre_id FROM genres WHERE genre_name = 'Action'").fetchone()

    assert count == 1
    assert row["tmdb_genre_id"] == 28


def test_fetch_movie_details_and_upsert_requests_updates_movie_and_director(monkeypatch):
    conn = make_conn()
    conn.execute(
        "INSERT INTO movies (tmdb_movie_id, title, release_year, rating) VALUES (?, ?, ?, ?)",
        (200, "Old", 2000, 1.0),
    )
    movie_id = conn.execute("SELECT movie_id FROM movies WHERE tmdb_movie_id = 200").fetchone()["movie_id"]
    conn.commit()

    def fake_get(url, *_args, **_kwargs):
        if url.endswith("/movie/200"):
            return DummyResponse(
                payload={
                    "title": "Updated Movie",
                    "release_date": "2023-10-11",
                    "vote_average": 8.4,
                    "overview": "Updated overview",
                    "poster_path": "/poster.jpg",
                    "genres": [{"id": 12, "name": "Adventure"}],
                }
            )
        if url.endswith("/movie/200/credits"):
            return DummyResponse(
                payload={
                    "crew": [
                        {"id": 900, "name": "Jane Director", "job": "Director", "profile_path": "/director.jpg"}
                    ]
                }
            )
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(cine_app.requests, "get", fake_get)

    updated = cine_app.fetch_movie_details_and_upsert_requests(conn, movie_id, "key")

    assert updated is not None
    assert updated["title"] == "Updated Movie"
    assert updated["genre_name"] == "Adventure"
    assert updated["director_first_name"] == "Jane"
