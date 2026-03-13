from __future__ import annotations


import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from functools import wraps
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse, urlunparse

import requests
from difflib import SequenceMatcher
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, flash, g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "cinemuse-dev-secret")
app.config["DATABASE"] = os.path.join(app.root_path, "cinemuse.db")
app.config["TMDB_API_BASE"] = "https://api.themoviedb.org/3"
app.config["TMDB_IMAGE_BASE"] = "https://image.tmdb.org/t/p/w500"
app.config["NEWSAPI_BASE"] = "https://newsapi.org/v2"
DEFAULT_NEWS_TOPICS = ["movies", "directors", "actors", "film festivals", "film events"]
MOVIE_NEWS_KEYWORDS = (
    "movie",
    "film",
    "cinema",
    "director",
    "actor",
    "actress",
    "screening",
    "premiere",
    "trailer",
    "festival",
    "sundance",
    "tiff",
    "cannes",
    "oscar",
    "bafta",
    "academy awards",
    "box office",
    "indie",
    "streaming",
    "hollywood",
    "netflix",
    "disney",
    "pixar",
)


def is_movie_news_text(title: str, summary: str, content: str | None = None) -> bool:
    combined = " ".join(chunk for chunk in (title, summary, content or "") if chunk and chunk.strip()).lower()
    return any(keyword in combined for keyword in MOVIE_NEWS_KEYWORDS)


def normalize_news_url(url: str) -> str:
    if not url:
        return ""
    try:
        parts = urlparse(url)
    except ValueError:
        return url.strip()
    scheme = parts.scheme or "https"
    netloc = parts.netloc.lower()
    path = parts.path.rstrip("/") or "/"
    return urlunparse((scheme, netloc, path, "", "", ""))


def clean_news_table(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT news_url, news_name, news_summary, news_image_url FROM news ORDER BY news_datetime DESC"
    ).fetchall()
    kept: set[str] = set()
    removed = 0
    for row in rows:
        title = row["news_name"] or ""
        summary = row["news_summary"] or ""
        image_url = row["news_image_url"]
        norm = normalize_news_url(row["news_url"])
        keep = bool(image_url) and is_movie_news_text(title, summary)
        if norm and norm in kept:
            keep = False
        if not keep:
            conn.execute("DELETE FROM news WHERE news_url = ?", (row["news_url"],))
            removed += 1
            continue
        if norm:
            kept.add(norm)
    conn.commit()
    if removed:
        app.logger.debug("Removed %d outdated news rows", removed)
    return kept
SYNC_META_KEYS = {"news": "news_auto", "tmdb": "tmdb_auto"}
NEWS_SYNC_INTERVAL = timedelta(days=2)
TMDB_SYNC_INTERVAL = timedelta(days=3)
GENRES = ["Action", "Comedy", "Drama", "Science Fiction", "Romance", "Thriller", "Horror", "Adventure", "Documentary"]
app.config["PROFILE_UPLOAD_DIR"] = os.path.join(app.root_path, "static", "uploads", "profiles")
app.config["PROFILE_UPLOAD_URL_PREFIX"] = "/static/uploads/profiles/"
app.config["PLAYLIST_UPLOAD_DIR"] = os.path.join(app.root_path, "static", "uploads", "playlists")
app.config["PLAYLIST_UPLOAD_URL_PREFIX"] = "/static/uploads/playlists/"
app.config["ALLOWED_PROFILE_EXTENSIONS"] = {"png", "jpg", "jpeg", "webp", "gif"}
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024

_tmdb_bootstrap_attempted = False

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(app.config["DATABASE"], timeout=30)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
        g.db.execute("PRAGMA journal_mode = WAL")
        g.db.execute("PRAGMA busy_timeout = 30000")
        g.db.execute("PRAGMA synchronous = NORMAL")
    return g.db


def open_sync_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(app.config["DATABASE"], timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@app.teardown_appcontext
def close_db(_error: Exception | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


@app.template_filter("pretty_review_datetime")
def pretty_review_datetime(value: str | None) -> str:
    if not value:
        return ""
    try:
        raw = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        # Stored timestamps are UTC; render in local timezone for accurate day/time.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone()
        return local_dt.strftime("%B %d, %Y %H:%M")
    except ValueError:
        return value


@app.template_filter("rating_stars")
def rating_stars(value: float | int | None) -> str:
    if value is None:
        return ""
    try:
        rating = float(value)
    except (TypeError, ValueError):
        return ""
    rating = max(0.0, min(5.0, round(rating * 2) / 2))
    full = int(rating)
    has_half = (rating - full) >= 0.5
    empty = 5 - full - (1 if has_half else 0)
    return ("★" * full) + ("½" if has_half else "") + ("☆" * empty)


@app.template_filter("rating_star_tokens")
def rating_star_tokens(value: float | int | None) -> list[str]:
    if value is None:
        return []
    try:
        rating = float(value)
    except (TypeError, ValueError):
        return []
    rating = max(0.0, min(5.0, round(rating * 2) / 2))
    if rating <= 0:
        return []
    full = int(rating)
    has_half = (rating - full) >= 0.5
    empty = 5 - full - (1 if has_half else 0)
    return (["full"] * full) + (["half"] if has_half else []) + (["empty"] * empty)


def login_required(view):
    @wraps(view)
    def wrapped_view(**kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return view(**kwargs)

    return wrapped_view


def execute_many(sql: str) -> None:
    db = get_db()
    db.executescript(sql)
    db.commit()


def ensure_upload_dirs() -> None:
    Path(app.config["PROFILE_UPLOAD_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["PLAYLIST_UPLOAD_DIR"]).mkdir(parents=True, exist_ok=True)


def allowed_profile_file(filename: str) -> bool:
    if "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in app.config["ALLOWED_PROFILE_EXTENSIONS"]


def is_valid_email_address(email: str) -> bool:
    if not email or len(email) > 254:
        return False
    if not EMAIL_REGEX.fullmatch(email):
        return False
    local, domain = email.rsplit("@", 1)
    if local.startswith(".") or local.endswith(".") or ".." in local:
        return False
    if domain.startswith("-") or domain.endswith("-") or ".." in domain:
        return False
    return True


def save_profile_picture(file_storage, user_id: str) -> str:
    safe_name = secure_filename(file_storage.filename or "")
    if not safe_name or not allowed_profile_file(safe_name):
        raise ValueError("Unsupported image format. Allowed: png, jpg, jpeg, webp, gif.")

    ext = safe_name.rsplit(".", 1)[1].lower()
    filename = f"{user_id}_{uuid.uuid4().hex}.{ext}"
    dest_path = Path(app.config["PROFILE_UPLOAD_DIR"]) / filename
    file_storage.save(dest_path)
    return f"{app.config['PROFILE_UPLOAD_URL_PREFIX']}{filename}"


def save_playlist_cover_picture(file_storage, user_id: str) -> str:
    safe_name = secure_filename(file_storage.filename or "")
    if not safe_name or not allowed_profile_file(safe_name):
        raise ValueError("Unsupported image format. Allowed: png, jpg, jpeg, webp, gif.")

    ext = safe_name.rsplit(".", 1)[1].lower()
    filename = f"playlist_{user_id}_{uuid.uuid4().hex}.{ext}"
    dest_path = Path(app.config["PLAYLIST_UPLOAD_DIR"]) / filename
    file_storage.save(dest_path)
    return f"{app.config['PLAYLIST_UPLOAD_URL_PREFIX']}{filename}"


def log_movie_watch(user_id: str, movie_id: int, increment: int = 1) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    get_db().execute(
        """
        INSERT INTO movielogging (user_id, movie_id, times_logged, movie_log_datetime)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, movie_id)
        DO UPDATE SET
          times_logged = times_logged + excluded.times_logged,
          movie_log_datetime = excluded.movie_log_datetime
        """,
        (user_id, movie_id, increment, now),
    )
    get_db().commit()


def ensure_column(table_name: str, column_name: str, column_definition: str) -> None:
    db = get_db()
    columns = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = {column["name"] for column in columns}
    if column_name not in existing:
        db.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")
        db.commit()


def tmdb_token() -> str:
    return os.environ.get("TMDB_API_READ_ACCESS_TOKEN", "").strip()


def tmdb_api_key() -> str:
    return os.environ.get("TMDB_API_KEY", "").strip()


def is_tmdb_configured() -> bool:
    return bool(tmdb_token() or tmdb_api_key())


_tmdb_session: requests.Session | None = None


def get_tmdb_session() -> requests.Session:
    global _tmdb_session
    if _tmdb_session is None:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=Retry(total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504]),
        )
        session.mount("https://", adapter)
        _tmdb_session = session
    return _tmdb_session


def tmdb_request(path: str, params: dict[str, str | int] | None = None) -> dict:
    token = tmdb_token()
    query_params = dict(params or {})
    if not token and tmdb_api_key():
        query_params["api_key"] = tmdb_api_key()
    url = f"{app.config['TMDB_API_BASE']}{path}"
    headers = {"accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = get_tmdb_session().get(
        url, params=query_params, headers=headers, timeout=20
    )
    response.raise_for_status()
    return response.json()


def fetch_tmdb_person_bio(tmdb_person_id: int) -> str:
    try:
        payload = tmdb_request(f"/person/{tmdb_person_id}", {"language": "en-US"})
    except Exception:
        return ""
    return payload.get("biography") or ""


def newsapi_key() -> str:
    return os.environ.get("NEWSAPI_KEY", "").strip()


def is_newsapi_configured() -> bool:
    return bool(newsapi_key())


def newsapi_request(query: str, page: int = 1, page_size: int = 20) -> dict:
    if not is_newsapi_configured():
        raise RuntimeError("NEWSAPI_KEY is not set.")
    headers = {"Authorization": newsapi_key()}
    params = {"q": query, "page": page, "pageSize": page_size, "language": "en", "sortBy": "publishedAt"}
    response = requests.get(f"{app.config['NEWSAPI_BASE']}/everything", params=params, headers=headers, timeout=20)
    response.raise_for_status()
    return response.json()


def normalize_author_name(byline: str | None) -> tuple[str, str]:
    if not byline:
        return "", ""
    parts = byline.strip().split(" ", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def upsert_news_item(
    conn: sqlite3.Connection,
    *,
    url: str,
    title: str,
    summary: str,
    image_url: str | None,
    published: str,
    source_name: str,
    author_first: str,
    author_last: str,
) -> None:
    conn.execute(
        """
        INSERT INTO news (
            news_name, news_author_first_name, news_author_last_name,
            news_source, news_url, news_datetime, news_summary, news_image_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(news_url) DO UPDATE SET
            news_name = excluded.news_name,
            news_summary = excluded.news_summary,
            news_image_url = excluded.news_image_url,
            news_datetime = excluded.news_datetime
        """,
        (
            title,
            author_first,
            author_last,
            source_name,
            url,
            published,
            summary,
            image_url,
        ),
    )


def run_newsapi_sync(queries: Iterable[str], pages: int, page_size: int) -> dict[str, int]:
    if not is_newsapi_configured():
        raise RuntimeError("NEWSAPI_KEY missing")
    totals: dict[str, int] = {}
    with open_sync_connection() as conn:
        existing_norms = clean_news_table(conn)
        for query in queries:
            count = 0
            for page in range(1, pages + 1):
                payload = newsapi_request(query, page=page, page_size=page_size)
                articles = payload.get("articles") or []
                if not articles:
                    break
                for article in articles:
                    url = article.get("url")
                    if not url:
                        continue
                    title = article.get("title") or "News"
                    summary = article.get("description") or ""
                    content = article.get("content")
                    if not is_movie_news_text(title, summary, content):
                        continue
                    image_url = article.get("urlToImage")
                    if not image_url:
                        continue
                    norm = normalize_news_url(url)
                    if norm and norm in existing_norms:
                        continue
                    published = article.get("publishedAt") or datetime.utcnow().isoformat(
                        timespec="seconds"
                    )
                    author_first, author_last = normalize_author_name(article.get("author"))
                    upsert_news_item(
                        conn,
                        url=url,
                        title=title,
                        summary=summary,
                        image_url=image_url,
                        published=published,
                        source_name=article.get("source", {}).get("name", "NewsAPI"),
                        author_first=author_first,
                        author_last=author_last,
                    )
                    count += 1
                    if norm:
                        existing_norms.add(norm)
                if len(articles) < page_size:
                    break
            totals[query] = count
    return totals


def get_sync_timestamp(sync_key: str) -> datetime | None:
    row = get_db().execute(
        "SELECT last_run FROM sync_meta WHERE sync_key = ?",
        (sync_key,),
    ).fetchone()
    if not row or not row["last_run"]:
        return None
    try:
        return datetime.fromisoformat(row["last_run"])
    except ValueError:
        return None


def set_sync_timestamp(sync_key: str, timestamp: datetime | None = None) -> None:
    ts = (timestamp or datetime.utcnow()).isoformat(timespec="seconds")
    db = get_db()
    db.execute(
        """
        INSERT INTO sync_meta (sync_key, last_run)
        VALUES (?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET last_run = excluded.last_run
        """,
        (sync_key, ts),
    )
    db.commit()


def sync_due(sync_key: str, interval: timedelta) -> bool:
    last = get_sync_timestamp(sync_key)
    if not last:
        return True
    return datetime.utcnow() - last >= interval


def ensure_news_autosync() -> None:
    if not is_newsapi_configured():
        return
    if not sync_due(SYNC_META_KEYS["news"], NEWS_SYNC_INTERVAL):
        return
    try:
        run_newsapi_sync(DEFAULT_NEWS_TOPICS, pages=3, page_size=25)
        set_sync_timestamp(SYNC_META_KEYS["news"])
    except Exception as exc:
        app.logger.error("automatic news sync failed", exc_info=exc)


def ensure_tmdb_autosync() -> None:
    if not is_tmdb_configured():
        return
    if not sync_due(SYNC_META_KEYS["tmdb"], TMDB_SYNC_INTERVAL):
        return
    try:
        with open_sync_connection() as conn:
            sync_tmdb_genres(conn)
            sync_tmdb_movies(
                pages=3,
                source="popular",
                include_directors=True,
                db=conn,
            )
        set_sync_timestamp(SYNC_META_KEYS["tmdb"])
    except Exception as exc:
        app.logger.error("automatic TMDB sync failed", exc_info=exc)


def fetch_tmdb_popular_movies(api_key: str, page: int = 1) -> dict:
    """Fetch popular movies from TMDB using requests with robust error handling."""
    endpoint = f"{app.config['TMDB_API_BASE']}/movie/popular"
    try:
        response = requests.get(
            endpoint,
            params={"api_key": api_key, "page": page, "language": "en-US"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or "results" not in payload:
            raise ValueError("Unexpected TMDB response format for popular movies.")
        return payload
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response else "unknown"
        raise RuntimeError(f"TMDB popular movies HTTP error: {status}") from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("Network error while fetching TMDB popular movies.") from exc
    except ValueError as exc:
        raise RuntimeError("Invalid JSON payload from TMDB popular movies endpoint.") from exc


def ensure_movies_genres_directors_tables(conn: sqlite3.Connection) -> None:
    """Create required tables if they do not already exist."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS genres (
            genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT NOT NULL UNIQUE,
            tmdb_genre_id INTEGER UNIQUE
        );

        CREATE TABLE IF NOT EXISTS directors (
            director_id INTEGER PRIMARY KEY AUTOINCREMENT,
            director_first_name TEXT,
            director_last_name TEXT,
            tmdb_person_id INTEGER UNIQUE,
            profile_url TEXT,
            biography TEXT
        );

        CREATE TABLE IF NOT EXISTS movies (
            movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tmdb_movie_id INTEGER UNIQUE,
            title TEXT NOT NULL,
            release_year INTEGER,
            rating REAL DEFAULT 0,
            genre_id INTEGER,
            director_id INTEGER,
            poster_url TEXT,
            description TEXT,
            FOREIGN KEY (genre_id) REFERENCES genres (genre_id),
            FOREIGN KEY (director_id) REFERENCES directors (director_id)
        );
        """
    )
    conn.commit()


def store_popular_movies(conn: sqlite3.Connection, payload: dict) -> int:
    """Insert/update popular movies into local SQLite movies table."""
    ensure_movies_genres_directors_tables(conn)
    inserted = 0
    for movie in payload.get("results", []):
        tmdb_movie_id = movie.get("id")
        title = movie.get("title") or movie.get("name")
        if not tmdb_movie_id or not title:
            continue
        release_date = movie.get("release_date") or ""
        release_year = int(release_date[:4]) if len(release_date) >= 4 and release_date[:4].isdigit() else None
        rating = round(float(movie.get("vote_average", 0.0)) / 2, 1)
        conn.execute(
            """
            INSERT INTO movies (tmdb_movie_id, title, release_year, rating, poster_url, description)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(tmdb_movie_id) DO UPDATE SET
                title = excluded.title,
                release_year = excluded.release_year,
                rating = excluded.rating,
                poster_url = excluded.poster_url,
                description = excluded.description
            """,
            (
                int(tmdb_movie_id),
                title,
                release_year,
                rating,
                f"{app.config['TMDB_IMAGE_BASE']}{movie['poster_path']}" if movie.get("poster_path") else None,
                movie.get("overview", ""),
            ),
        )
        inserted += 1
    conn.commit()
    return inserted


def fetch_and_store_genres_requests(conn: sqlite3.Connection, api_key: str) -> int:
    """Fetch TMDB genres and upsert into SQLite genres table."""
    ensure_movies_genres_directors_tables(conn)
    endpoint = f"{app.config['TMDB_API_BASE']}/genre/movie/list"
    try:
        response = requests.get(
            endpoint,
            params={"api_key": api_key, "language": "en-US"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response else "unknown"
        raise RuntimeError(f"TMDB genres HTTP error: {status}") from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("Network error while fetching TMDB genres.") from exc
    except ValueError as exc:
        raise RuntimeError("Invalid JSON payload from TMDB genres endpoint.") from exc

    count = 0
    for genre in payload.get("genres", []):
        if not genre.get("id") or not genre.get("name"):
            continue
        conn.execute(
            """
            INSERT INTO genres (genre_name, tmdb_genre_id)
            VALUES (?, ?)
            ON CONFLICT(genre_name) DO UPDATE SET tmdb_genre_id = excluded.tmdb_genre_id
            """,
            (genre["name"], int(genre["id"])),
        )
        count += 1
    conn.commit()
    return count


def fetch_and_store_director_requests(conn: sqlite3.Connection, movie_id: int, api_key: str) -> int | None:
    """Fetch movie credits, extract director, and link director to movie."""
    ensure_movies_genres_directors_tables(conn)

    tmdb_movie_id_row = conn.execute(
        "SELECT tmdb_movie_id FROM movies WHERE movie_id = ?",
        (movie_id,),
    ).fetchone()
    if not tmdb_movie_id_row or not tmdb_movie_id_row["tmdb_movie_id"]:
        return None

    endpoint = f"{app.config['TMDB_API_BASE']}/movie/{tmdb_movie_id_row['tmdb_movie_id']}/credits"
    try:
        response = requests.get(
            endpoint,
            params={"api_key": api_key, "language": "en-US"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response else "unknown"
        raise RuntimeError(f"TMDB credits HTTP error: {status}") from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("Network error while fetching TMDB credits.") from exc
    except ValueError as exc:
        raise RuntimeError("Invalid JSON payload from TMDB credits endpoint.") from exc

    director = next(
        (person for person in payload.get("crew", []) if person.get("job") == "Director"),
        None,
    )
    if not director:
        return None

    full_name = (director.get("name") or "").strip()
    if not full_name:
        return None
    parts = full_name.split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""
    profile_url = (
        f"{app.config['TMDB_IMAGE_BASE']}{director['profile_path']}"
        if director.get("profile_path")
        else None
    )
    person_id = int(director["id"])

    conn.execute(
        """
        INSERT INTO directors (director_first_name, director_last_name, tmdb_person_id, profile_url, biography)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(tmdb_person_id) DO UPDATE SET
            director_first_name = excluded.director_first_name,
            director_last_name = excluded.director_last_name,
            profile_url = excluded.profile_url,
            biography = COALESCE(excluded.biography, directors.biography)
        """,
        (first_name, last_name, person_id, profile_url, biography),
    )
    director_row = conn.execute(
        "SELECT director_id FROM directors WHERE tmdb_person_id = ?",
        (person_id,),
    ).fetchone()
    if director_row:
        conn.execute(
            "UPDATE movies SET director_id = ? WHERE movie_id = ?",
            (director_row["director_id"], movie_id),
        )
    conn.commit()
    return director_row["director_id"] if director_row else None


def fetch_movie_details_and_upsert_requests(conn: sqlite3.Connection, movie_id: int, api_key: str) -> sqlite3.Row | None:
    """Fetch TMDB movie details and upsert local movie record including genre/director linkage."""
    ensure_movies_genres_directors_tables(conn)

    existing = conn.execute(
        "SELECT movie_id, tmdb_movie_id FROM movies WHERE movie_id = ?",
        (movie_id,),
    ).fetchone()
    if not existing:
        return None
    tmdb_movie_id = existing["tmdb_movie_id"]
    if not tmdb_movie_id:
        return None

    endpoint = f"{app.config['TMDB_API_BASE']}/movie/{tmdb_movie_id}"
    try:
        response = requests.get(
            endpoint,
            params={"api_key": api_key, "language": "en-US"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response else "unknown"
        raise RuntimeError(f"TMDB movie details HTTP error: {status}") from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("Network error while fetching TMDB movie details.") from exc
    except ValueError as exc:
        raise RuntimeError("Invalid JSON payload from TMDB movie details endpoint.") from exc

    genre_payload = payload.get("genres", [])
    genre_id = None
    if genre_payload:
        first_genre = genre_payload[0]
        conn.execute(
            """
            INSERT INTO genres (genre_name, tmdb_genre_id)
            VALUES (?, ?)
            ON CONFLICT(genre_name) DO UPDATE SET tmdb_genre_id = excluded.tmdb_genre_id
            """,
            (first_genre.get("name", "Unknown"), int(first_genre.get("id", 0))),
        )
        genre_row = conn.execute(
            "SELECT genre_id FROM genres WHERE tmdb_genre_id = ?",
            (int(first_genre.get("id", 0)),),
        ).fetchone()
        genre_id = genre_row["genre_id"] if genre_row else None

    release_date = payload.get("release_date") or ""
    release_year = int(release_date[:4]) if len(release_date) >= 4 and release_date[:4].isdigit() else None
    rating = round(float(payload.get("vote_average", 0.0)) / 2, 1)
    poster_url = (
        f"{app.config['TMDB_IMAGE_BASE']}{payload['poster_path']}"
        if payload.get("poster_path")
        else None
    )

    conn.execute(
        """
        UPDATE movies
        SET title = ?, release_year = ?, rating = ?, genre_id = ?, poster_url = ?, description = ?
        WHERE movie_id = ?
        """,
        (
            payload.get("title", "Untitled"),
            release_year,
            rating,
            genre_id,
            poster_url,
            payload.get("overview", ""),
            movie_id,
        ),
    )
    fetch_and_store_director_requests(conn, movie_id, api_key)

    result = conn.execute(
        """
        SELECT m.movie_id, m.title, m.release_year, m.rating, g.genre_name,
               d.director_first_name, d.director_last_name
        FROM movies m
        LEFT JOIN genres g ON g.genre_id = m.genre_id
        LEFT JOIN directors d ON d.director_id = m.director_id
        WHERE m.movie_id = ?
        """,
        (movie_id,),
    ).fetchone()
    conn.commit()
    return result


def sync_tmdb_genres(db: sqlite3.Connection | None = None) -> int:
    data = tmdb_request("/genre/movie/list", {"language": "en-US"})
    genres = data.get("genres", [])
    db = db or get_db()
    count = 0
    for genre in genres:
        name = genre.get("name")
        genre_tmdb_id = genre.get("id")
        if not name or genre_tmdb_id is None:
            continue
        db.execute(
            """
            INSERT INTO genres (genre_name, tmdb_genre_id)
            VALUES (?, ?)
            ON CONFLICT(genre_name) DO UPDATE SET tmdb_genre_id = excluded.tmdb_genre_id
            """,
            (name, genre_tmdb_id),
        )
        count += 1
    db.commit()
    return count


def resolve_genre_id_from_tmdb(genre_tmdb_id: int | None, db: sqlite3.Connection | None = None) -> int | None:
    if genre_tmdb_id is None:
        return None
    db = db or get_db()
    row = db.execute(
        "SELECT genre_id FROM genres WHERE tmdb_genre_id = ?",
        (genre_tmdb_id,),
    ).fetchone()
    if row:
        return row["genre_id"]
    db.execute(
        "INSERT INTO genres (genre_name, tmdb_genre_id) VALUES (?, ?)",
        (f"Genre {genre_tmdb_id}", genre_tmdb_id),
    )
    db.commit()
    created = db.execute("SELECT last_insert_rowid() AS id").fetchone()
    return created["id"]


def sync_tmdb_director_for_movie(tmdb_movie_id: int, db: sqlite3.Connection | None = None) -> int | None:
    db = db or get_db()
    try:
        credits = tmdb_request(f"/movie/{tmdb_movie_id}/credits", {"language": "en-US"})
    except (requests.exceptions.RequestException, ValueError):
        return None

    director = next(
        (
            person
            for person in credits.get("crew", [])
            if person.get("job") == "Director" and person.get("id")
        ),
        None,
    )
    if not director:
        return None

    full_name = (director.get("name") or "").strip()
    if not full_name:
        return None
    name_parts = full_name.split(" ", 1)
    first_name = name_parts[0]
    last_name = name_parts[1] if len(name_parts) > 1 else ""
    profile_path = director.get("profile_path")
    profile_url = (
        f"{app.config['TMDB_IMAGE_BASE']}{profile_path}" if profile_path else None
    )
    tmdb_person_id = int(director["id"])
    biography = fetch_tmdb_person_bio(tmdb_person_id)

    db.execute(
        """
        INSERT INTO directors (director_first_name, director_last_name, tmdb_person_id, profile_url, biography)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(tmdb_person_id) DO UPDATE SET
            director_first_name = excluded.director_first_name,
            director_last_name = excluded.director_last_name,
            profile_url = excluded.profile_url,
            biography = COALESCE(excluded.biography, directors.biography)
        """,
        (first_name, last_name, tmdb_person_id, profile_url, biography),
    )
    db.commit()
    row = db.execute(
        "SELECT director_id FROM directors WHERE tmdb_person_id = ?",
        (tmdb_person_id,),
    ).fetchone()
    return row["director_id"] if row else None


def sync_tmdb_movies(
    pages: int = 10,
    source: str = "popular",
    include_directors: bool = False,
    start_page: int = 1,
    db: sqlite3.Connection | None = None,
) -> int:
    db = db or get_db()
    inserted_or_updated = 0
    endpoint = "/movie/popular" if source == "popular" else "/trending/movie/week"
    pages_to_fetch = max(1, min(pages, 50))
    first_page = max(1, start_page)
    last_page = min(first_page + pages_to_fetch - 1, 1000)

    for page in range(first_page, last_page + 1):
        payload = tmdb_request(endpoint, {"language": "en-US", "page": page})
        for movie in payload.get("results", []):
            tmdb_movie_id = movie.get("id")
            title = movie.get("title") or movie.get("name")
            if tmdb_movie_id is None or not title:
                continue

            release_date = movie.get("release_date") or ""
            release_year = int(release_date[:4]) if len(release_date) >= 4 and release_date[:4].isdigit() else None
            genre_ids = movie.get("genre_ids") or []
            genre_id = resolve_genre_id_from_tmdb(genre_ids[0] if genre_ids else None, db=db)
            director_id = (
                sync_tmdb_director_for_movie(int(tmdb_movie_id), db=db)
                if include_directors
                else None
            )
            poster_path = movie.get("poster_path")
            poster_url = (
                f"{app.config['TMDB_IMAGE_BASE']}{poster_path}"
                if poster_path
                else None
            )
            rating = round(float(movie.get("vote_average", 0.0)) / 2, 1)

            db.execute(
                """
                INSERT INTO movies (
                    tmdb_movie_id, title, release_year, genre_id, director_id, rating, poster_url, description
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tmdb_movie_id) DO UPDATE SET
                    title = excluded.title,
                    release_year = excluded.release_year,
                    genre_id = excluded.genre_id,
                    director_id = excluded.director_id,
                    rating = excluded.rating,
                    poster_url = excluded.poster_url,
                    description = excluded.description
                """,
                (
                    tmdb_movie_id,
                    title,
                    release_year,
                    genre_id,
                    director_id,
                    rating,
                    poster_url,
                    movie.get("overview", ""),
                ),
            )
            inserted_or_updated += 1
    db.commit()
    return inserted_or_updated


def bootstrap_tmdb_data_if_needed() -> None:
    global _tmdb_bootstrap_attempted
    if _tmdb_bootstrap_attempted or not is_tmdb_configured():
        return
    _tmdb_bootstrap_attempted = True

    db = get_db()
    tmdb_rows = db.execute(
        "SELECT COUNT(*) AS c FROM movies WHERE tmdb_movie_id IS NOT NULL"
    ).fetchone()["c"]
    if tmdb_rows > 0:
        return

    try:
        sync_tmdb_genres()
        sync_tmdb_movies(pages=5, source="popular", include_directors=False)
    except Exception:
        # Keep app functional even if TMDB is temporarily unavailable.
        pass


def init_db() -> None:
    execute_many(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS genres (
            genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT NOT NULL UNIQUE,
            tmdb_genre_id INTEGER UNIQUE
        );

        CREATE TABLE IF NOT EXISTS directors (
            director_id INTEGER PRIMARY KEY AUTOINCREMENT,
            director_first_name TEXT,
            director_last_name TEXT,
            tmdb_person_id INTEGER UNIQUE,
            profile_url TEXT,
            biography TEXT
        );

        CREATE TABLE IF NOT EXISTS movies (
            movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tmdb_movie_id INTEGER UNIQUE,
            title TEXT NOT NULL,
            release_year INTEGER,
            genre_id INTEGER,
            rating REAL DEFAULT 0,
            director_id INTEGER,
            poster_url TEXT,
            description TEXT,
            FOREIGN KEY (genre_id) REFERENCES genres (genre_id),
            FOREIGN KEY (director_id) REFERENCES directors (director_id)
        );

        CREATE TABLE IF NOT EXISTS userauthentication (
            user_id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            biography TEXT DEFAULT '',
            profile_picture_url TEXT,
            create_time TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS usersettings (
            settings_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL UNIQUE,
            receive_news INTEGER DEFAULT 1,
            privacy_level TEXT DEFAULT 'friends',
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id)
        );

        CREATE TABLE IF NOT EXISTS userrelationships (
            follower_id TEXT NOT NULL,
            following_id TEXT NOT NULL,
            followed_date TEXT NOT NULL,
            PRIMARY KEY (follower_id, following_id),
            FOREIGN KEY (follower_id) REFERENCES userauthentication (user_id),
            FOREIGN KEY (following_id) REFERENCES userauthentication (user_id)
        );

        CREATE TABLE IF NOT EXISTS reviews (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            movie_id INTEGER NOT NULL,
            rating REAL NOT NULL,
            review_text TEXT,
            review_datetime TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id),
            FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        );

        CREATE TABLE IF NOT EXISTS likes (
            like_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            review_id INTEGER NOT NULL,
            like_datetime TEXT NOT NULL,
            UNIQUE(user_id, review_id),
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id),
            FOREIGN KEY (review_id) REFERENCES reviews (review_id)
        );

        CREATE TABLE IF NOT EXISTS movielogging (
            movielogging_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            movie_id INTEGER NOT NULL,
            times_logged INTEGER DEFAULT 1,
            movie_log_datetime TEXT NOT NULL,
            UNIQUE(user_id, movie_id),
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id),
            FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            watchlist_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            movie_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'planned',
            UNIQUE(user_id, movie_id),
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id),
            FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        );

        CREATE TABLE IF NOT EXISTS user_favorite_movies (
            user_id TEXT NOT NULL,
            slot INTEGER NOT NULL CHECK (slot BETWEEN 1 AND 3),
            movie_id INTEGER NOT NULL,
            PRIMARY KEY (user_id, slot),
            UNIQUE (user_id, movie_id),
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id),
            FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        );

        CREATE TABLE IF NOT EXISTS user_movie_likes (
            user_id TEXT NOT NULL,
            movie_id INTEGER NOT NULL,
            liked_datetime TEXT NOT NULL,
            PRIMARY KEY (user_id, movie_id),
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id),
            FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        );

        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            playlist_name TEXT NOT NULL,
            created_datetime TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id)
        );

        CREATE TABLE IF NOT EXISTS playlist_movies (
            playlist_id INTEGER NOT NULL,
            movie_id INTEGER NOT NULL,
            added_datetime TEXT,
            PRIMARY KEY (playlist_id, movie_id),
            FOREIGN KEY (playlist_id) REFERENCES playlists (playlist_id) ON DELETE CASCADE,
            FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        );

        CREATE TABLE IF NOT EXISTS searchhistory (
            search_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            search_text TEXT NOT NULL,
            search_datetime TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id)
        );

        CREATE TABLE IF NOT EXISTS news (
            news_id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_name TEXT NOT NULL,
            news_author_first_name TEXT,
            news_author_last_name TEXT,
            news_source TEXT,
            news_url TEXT,
            news_datetime TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS notifications (
            notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            notification_type TEXT NOT NULL,
            notification_text TEXT NOT NULL,
            created_datetime TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id)
        );
        """
    )
    ensure_column("genres", "tmdb_genre_id", "tmdb_genre_id INTEGER")
    ensure_column("movies", "tmdb_movie_id", "tmdb_movie_id INTEGER")
    ensure_column("directors", "tmdb_person_id", "tmdb_person_id INTEGER")
    ensure_column("directors", "profile_url", "profile_url TEXT")
    ensure_column("directors", "biography", "biography TEXT")
    ensure_column("userauthentication", "profile_picture_url", "profile_picture_url TEXT")
    ensure_column("usersettings", "receive_follow_notifications", "receive_follow_notifications INTEGER DEFAULT 1")
    ensure_column("usersettings", "receive_like_notifications", "receive_like_notifications INTEGER DEFAULT 1")
    ensure_column("playlists", "cover_image_url", "cover_image_url TEXT")
    ensure_column("playlists", "description", "description TEXT")
    ensure_column("playlist_movies", "added_datetime", "added_datetime TEXT")
    ensure_column("reviews", "liked_movie_snapshot", "liked_movie_snapshot INTEGER DEFAULT 0")
    ensure_column("searchhistory", "search_type", "search_type TEXT")
    ensure_column("searchhistory", "target_url", "target_url TEXT")
    ensure_column("searchhistory", "target_panel_url", "target_panel_url TEXT")
    ensure_column("news", "news_summary", "news_summary TEXT")
    ensure_column("news", "news_image_url", "news_image_url TEXT")
    get_db().execute(
        """
        CREATE TABLE IF NOT EXISTS sync_meta (
            sync_key TEXT PRIMARY KEY,
            last_run TEXT
        )
        """
    )
    get_db().execute(
        """
        CREATE TABLE IF NOT EXISTS review_comments (
            comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            review_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            comment_text TEXT NOT NULL,
            comment_datetime TEXT NOT NULL,
            FOREIGN KEY (review_id) REFERENCES reviews (review_id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES userauthentication (user_id)
        )
        """
    )
    get_db().execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_movies_tmdb_movie_id ON movies(tmdb_movie_id)"
    )
    get_db().execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_genres_tmdb_genre_id ON genres(tmdb_genre_id)"
    )
    get_db().execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_directors_tmdb_person_id ON directors(tmdb_person_id)"
    )
    get_db().execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_url ON news(news_url)"
    )
    get_db().commit()
    seed_data()
    bootstrap_tmdb_data_if_needed()


def seed_data() -> None:
    db = get_db()

    genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Romance", "Thriller"]
    for name in genres:
        db.execute("INSERT OR IGNORE INTO genres (genre_name) VALUES (?)", (name,))

    directors = [
        ("Denis", "Villeneuve"),
        ("Greta", "Gerwig"),
        ("Christopher", "Nolan"),
        ("Bong", "Joon-ho"),
        ("Damien", "Chazelle"),
    ]
    for first, last in directors:
        db.execute(
            "INSERT OR IGNORE INTO directors (director_first_name, director_last_name) VALUES (?, ?)",
            (first, last),
        )

    movie_count = db.execute("SELECT COUNT(*) AS c FROM movies").fetchone()["c"]
    if movie_count == 0:
        sample_movies = [
            ("Dune", 2021, "Sci-Fi", 4.6, "Denis", "Villeneuve", "Epic desert politics and destiny."),
            ("Barbie", 2023, "Comedy", 4.1, "Greta", "Gerwig", "Satire, identity, and bright visuals."),
            ("Oppenheimer", 2023, "Drama", 4.5, "Christopher", "Nolan", "Character-driven historical drama."),
            ("Parasite", 2019, "Thriller", 4.8, "Bong", "Joon-ho", "Class conflict thriller with dark humor."),
            ("La La Land", 2016, "Romance", 4.3, "Damien", "Chazelle", "A modern musical romance in LA."),
            ("Mad Max: Fury Road", 2015, "Action", 4.4, "George", "Miller", "High intensity action on the road."),
        ]

        for title, year, genre, rating, first, last, description in sample_movies:
            genre_id = db.execute(
                "SELECT genre_id FROM genres WHERE genre_name = ?", (genre,)
            ).fetchone()["genre_id"]
            director = db.execute(
                "SELECT director_id FROM directors WHERE director_first_name = ? AND director_last_name = ?",
                (first, last),
            ).fetchone()
            if director is None:
                db.execute(
                    "INSERT INTO directors (director_first_name, director_last_name) VALUES (?, ?)",
                    (first, last),
                )
                director_id = db.execute("SELECT last_insert_rowid() AS i").fetchone()["i"]
            else:
                director_id = director["director_id"]

            db.execute(
                """
                INSERT INTO movies (title, release_year, genre_id, rating, director_id, description)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, year, genre_id, rating, director_id, description),
            )


def bootstrap_database() -> None:
    ensure_upload_dirs()
    init_db()


with app.app_context():
    bootstrap_database()


def current_user() -> sqlite3.Row | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    return get_db().execute(
        "SELECT * FROM userauthentication WHERE user_id = ?", (user_id,)
    ).fetchone()


def create_notification(user_id: str, notification_type: str, text: str) -> None:
    get_db().execute(
        """
        INSERT INTO notifications (user_id, notification_type, notification_text, created_datetime)
        VALUES (?, ?, ?, ?)
        """,
        (user_id, notification_type, text, datetime.utcnow().isoformat(timespec="seconds")),
    )
    get_db().commit()


def receives_follow_notifications(user_id: str) -> bool:
    row = get_db().execute(
        "SELECT receive_follow_notifications FROM usersettings WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if not row:
        return True
    value = row["receive_follow_notifications"]
    return bool(value) if value is not None else True


def receives_like_notifications(user_id: str) -> bool:
    row = get_db().execute(
        "SELECT receive_like_notifications FROM usersettings WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if not row:
        return True
    value = row["receive_like_notifications"]
    return bool(value) if value is not None else True


@app.before_request
def ensure_dirs() -> None:
    ensure_upload_dirs()


@app.context_processor
def inject_globals():
    user = current_user()
    liked_movie_ids: set[int] = set()
    if user:
        rows = get_db().execute(
            "SELECT movie_id FROM user_movie_likes WHERE user_id = ?",
            (user["user_id"],),
        ).fetchall()
        liked_movie_ids = {row["movie_id"] for row in rows}
    return {"session_user": user, "liked_movie_ids": liked_movie_ids}


@app.route("/movie/<int:movie_id>")
@login_required
def movie_detail(movie_id: int):
    db = get_db()
    movie = db.execute(
        """
        SELECT
            m.movie_id,
            m.title,
            m.release_year,
            m.rating,
            m.poster_url,
            m.tmdb_movie_id,
            m.description,
            g.genre_id,
            g.genre_name,
            d.director_id,
            d.director_first_name,
            d.director_last_name
        FROM movies m
        LEFT JOIN genres g ON g.genre_id = m.genre_id
        LEFT JOIN directors d ON d.director_id = m.director_id
        WHERE m.movie_id = ?
        """,
        (movie_id,),
    ).fetchone()
    api_key = tmdb_api_key()
    if movie and not movie["poster_url"] and api_key:
        updated = fetch_movie_details_and_upsert_requests(db, movie_id, api_key)
        if updated:
            movie = db.execute(
                """
                SELECT
                    m.movie_id,
                    m.title,
                    m.release_year,
                    m.rating,
                    m.poster_url,
                    m.tmdb_movie_id,
                    m.description,
                    g.genre_id,
                    g.genre_name,
                    d.director_id,
                    d.director_first_name,
                    d.director_last_name
                FROM movies m
                LEFT JOIN genres g ON g.genre_id = m.genre_id
                LEFT JOIN directors d ON d.director_id = m.director_id
                WHERE m.movie_id = ?
                """,
                (movie_id,),
            ).fetchone()
    if not movie:
        flash("Movie not found.", "error")
        return redirect(url_for("home"))

    reviews = db.execute(
        """
        SELECT
            r.review_id,
            r.user_id,
            u.username,
            r.rating,
            r.review_text,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        JOIN userauthentication u ON u.user_id = r.user_id
        WHERE r.movie_id = ?
          AND TRIM(COALESCE(r.review_text, '')) <> ''
        ORDER BY r.review_datetime DESC
        LIMIT 12
        """,
        (movie_id,),
    ).fetchall()
    comments_by_review: dict[int, list[sqlite3.Row]] = {}
    review_ids = [row["review_id"] for row in reviews]
    if review_ids:
        placeholders = ",".join("?" for _ in review_ids)
        comment_rows = db.execute(
            f"""
            SELECT
                rc.comment_id,
                rc.review_id,
                rc.comment_text,
                rc.comment_datetime,
                u.username
            FROM review_comments rc
            JOIN userauthentication u ON u.user_id = rc.user_id
            WHERE rc.review_id IN ({placeholders})
            ORDER BY rc.comment_datetime ASC
            """,
            review_ids,
        ).fetchall()
        for row in comment_rows:
            comments_by_review.setdefault(row["review_id"], []).append(row)
    user_reviews = db.execute(
        """
        SELECT
            r.review_id,
            r.rating,
            r.review_text,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        WHERE r.movie_id = ?
          AND r.user_id = ?
        ORDER BY r.review_datetime DESC
        """,
        (movie_id, session["user_id"]),
    ).fetchall()
    watchlist_entry = db.execute(
        """
        SELECT status
        FROM watchlist
        WHERE user_id = ? AND movie_id = ?
        """,
        (session["user_id"], movie_id),
    ).fetchone()
    watchlist_status = watchlist_entry["status"] if watchlist_entry else None
    user_playlists = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.user_id = ?
        GROUP BY p.playlist_id
        ORDER BY p.created_datetime DESC
        """,
        (session["user_id"],),
    ).fetchall()
    user_liked_movie = db.execute(
        "SELECT 1 FROM user_movie_likes WHERE user_id = ? AND movie_id = ?",
        (session["user_id"], movie_id),
    ).fetchone() is not None
    return render_template(
        "movie_detail.html",
        movie=movie,
        reviews=reviews,
        comments_by_review=comments_by_review,
        watchlist_status=watchlist_status,
        user_playlists=user_playlists,
        user_reviews=user_reviews,
        user_liked_movie=user_liked_movie,
    )


@app.route("/movie/<int:movie_id>/actions", methods=["POST"])
@login_required
def movie_actions(movie_id: int):
    db = get_db()
    user_id = session["user_id"]
    action = request.form.get("action", "").strip()

    movie = db.execute("SELECT movie_id FROM movies WHERE movie_id = ?", (movie_id,)).fetchone()
    if not movie:
        flash("Movie not found.", "error")
        return redirect(url_for("home"))

    if action == "watchlist":
        db.execute(
            """
            INSERT INTO watchlist (user_id, movie_id, status)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, movie_id)
            DO UPDATE SET status = excluded.status
            """,
            (user_id, movie_id, "planned"),
        )
        db.commit()
        flash("Added to watchlist.", "success")

    elif action == "watched":
        log_movie_watch(user_id, movie_id, increment=1)
        db.execute(
            """
            INSERT INTO watchlist (user_id, movie_id, status)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, movie_id)
            DO UPDATE SET status = excluded.status
            """,
            (user_id, movie_id, "watched"),
        )
        db.commit()
        flash("Marked as watched.", "success")

    elif action == "review_log":
        rating_raw = request.form.get("rating", "").strip()
        rating = None
        if rating_raw:
            try:
                rating = float(rating_raw)
            except ValueError:
                flash("Invalid star rating.", "error")
                return redirect(url_for("movie_detail", movie_id=movie_id))
        review_text = request.form.get("review_text", "").strip()
        if rating is not None and (rating < 0.5 or rating > 5):
            flash("Please choose a valid star rating.", "error")
            return redirect(url_for("movie_detail", movie_id=movie_id))
        liked_movie_value = (request.form.get("liked_movie") or "").strip().lower()
        liked_movie = liked_movie_value in {"1", "on", "true", "yes"}
        if liked_movie:
            db.execute(
                """
                INSERT INTO user_movie_likes (user_id, movie_id, liked_datetime)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, movie_id)
                DO UPDATE SET liked_datetime = excluded.liked_datetime
                """,
                (user_id, movie_id, datetime.utcnow().isoformat(timespec="seconds")),
            )
        else:
            db.execute(
                "DELETE FROM user_movie_likes WHERE user_id = ? AND movie_id = ?",
                (user_id, movie_id),
            )
        db.commit()

        if review_text:
            now = datetime.utcnow().isoformat(timespec="seconds")
            db.execute(
                """
                INSERT INTO reviews (user_id, movie_id, rating, review_text, review_datetime, liked_movie_snapshot)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    movie_id,
                    rating if rating is not None else 0.0,
                    review_text,
                    now,
                    1 if liked_movie else 0,
                ),
            )
            db.commit()
        log_movie_watch(user_id, movie_id, increment=1)
        if review_text:
            flash("Review/log saved.", "success")
        else:
            flash("Log saved. Add text next time to publish a public review.", "success")

    elif action == "edit_review":
        review_id = request.form.get("review_id", type=int)
        rating_raw = request.form.get("rating", "").strip()
        rating = None
        if rating_raw:
            try:
                rating = float(rating_raw)
            except ValueError:
                flash("Invalid star rating.", "error")
                return redirect(url_for("movie_detail", movie_id=movie_id))
        review_text = request.form.get("review_text", "").strip()
        if rating is not None and (rating < 0.5 or rating > 5):
            flash("Please choose a valid star rating.", "error")
            return redirect(url_for("movie_detail", movie_id=movie_id))
        liked_movie_value = (request.form.get("liked_movie") or "").strip().lower()
        liked_movie = liked_movie_value in {"1", "on", "true", "yes"}
        if liked_movie:
            db.execute(
                """
                INSERT INTO user_movie_likes (user_id, movie_id, liked_datetime)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, movie_id)
                DO UPDATE SET liked_datetime = excluded.liked_datetime
                """,
                (user_id, movie_id, datetime.utcnow().isoformat(timespec="seconds")),
            )
        else:
            db.execute(
                "DELETE FROM user_movie_likes WHERE user_id = ? AND movie_id = ?",
                (user_id, movie_id),
            )
        db.commit()
        if review_id is None:
            flash("Missing review to edit.", "error")
            return redirect(url_for("movie_detail", movie_id=movie_id))

        now = datetime.utcnow().isoformat(timespec="seconds")
        db.execute(
            """
            UPDATE reviews
            SET rating = ?, review_text = ?, review_datetime = ?, liked_movie_snapshot = ?
            WHERE review_id = ? AND user_id = ? AND movie_id = ?
            """,
            (
                rating if rating is not None else 0.0,
                review_text,
                now,
                1 if liked_movie else 0,
                review_id,
                user_id,
                movie_id,
            ),
        )
        db.commit()
        flash("Review updated.", "success")

    elif action == "add_to_playlists":
        raw_ids = request.form.getlist("playlist_ids")
        playlist_ids: list[int] = []
        for raw in raw_ids:
            try:
                playlist_ids.append(int(raw))
            except ValueError:
                continue
        if not playlist_ids:
            flash("Select at least one playlist.", "error")
            return redirect(url_for("movie_detail", movie_id=movie_id))

        placeholders = ",".join("?" for _ in playlist_ids)
        owned_rows = db.execute(
            f"""
            SELECT playlist_id
            FROM playlists
            WHERE user_id = ? AND playlist_id IN ({placeholders})
            """,
            (user_id, *playlist_ids),
        ).fetchall()
        owned_ids = [row["playlist_id"] for row in owned_rows]
        if not owned_ids:
            flash("No valid playlists selected.", "error")
            return redirect(url_for("movie_detail", movie_id=movie_id))

        now = datetime.utcnow().isoformat(timespec="seconds")
        db.executemany(
            """
            INSERT OR IGNORE INTO playlist_movies (playlist_id, movie_id, added_datetime)
            VALUES (?, ?, ?)
            """,
            [(playlist_id, movie_id, now) for playlist_id in owned_ids],
        )
        db.commit()
        flash("Movie added to selected playlists.", "success")

    else:
        flash("Unknown action.", "error")

    return redirect(url_for("movie_detail", movie_id=movie_id))


@app.route("/director/<int:director_id>")
@login_required
def director_detail(director_id: int):
    db = get_db()
    director = db.execute(
        """
        SELECT director_id, director_first_name, director_last_name, profile_url, biography
        FROM directors
        WHERE director_id = ?
        """,
        (director_id,),
    ).fetchone()
    if not director:
        flash("Director not found.", "error")
        return redirect(url_for("home"))
    if director and not director["biography"]:
        person = db.execute(
            "SELECT tmdb_person_id FROM directors WHERE director_id = ?",
            (director_id,),
        ).fetchone()
        if person and person["tmdb_person_id"]:
            bio = fetch_tmdb_person_bio(person["tmdb_person_id"])
            if bio:
                db.execute(
                    "UPDATE directors SET biography = ? WHERE director_id = ?",
                    (bio, director_id),
                )
                db.commit()
                director = db.execute(
                    """
                    SELECT director_id, director_first_name, director_last_name, profile_url, biography
                    FROM directors
                    WHERE director_id = ?
                    """,
                    (director_id,),
                ).fetchone()

    movies = db.execute(
        """
        SELECT movie_id, title, release_year, poster_url, rating
        FROM movies
        WHERE director_id = ?
        ORDER BY release_year DESC, rating DESC
        """,
        (director_id,),
    ).fetchall()
    return render_template("director_detail.html", director=director, movies=movies)


@app.route("/movies/<int:movie_id>/panel")
@login_required
def movie_panel(movie_id: int):
    db = get_db()
    movie = db.execute(
        """
        SELECT
            m.movie_id,
            m.title,
            m.release_year,
            m.rating,
            m.poster_url,
            m.description,
            g.genre_id,
            g.genre_name,
            d.director_id,
            d.director_first_name,
            d.director_last_name
        FROM movies m
        LEFT JOIN genres g ON g.genre_id = m.genre_id
        LEFT JOIN directors d ON d.director_id = m.director_id
        WHERE m.movie_id = ?
        """,
        (movie_id,),
    ).fetchone()
    if not movie:
        return "<p>Movie not found.</p>", 404
    return render_template("movie_panel.html", movie=movie)


@app.route("/directors/<int:director_id>/panel")
@login_required
def director_panel(director_id: int):
    db = get_db()
    director = db.execute(
        """
        SELECT director_id, director_first_name, director_last_name, profile_url, biography
        FROM directors
        WHERE director_id = ?
        """,
        (director_id,),
    ).fetchone()
    if not director:
        return "<p>Director not found.</p>", 404

    movies = db.execute(
        """
        SELECT movie_id, title, release_year, poster_url, rating
        FROM movies
        WHERE director_id = ?
        ORDER BY release_year DESC, rating DESC
        LIMIT 3
        """,
        (director_id,),
    ).fetchall()
    return render_template("director_panel.html", director=director, movies=movies)


@app.route("/genre/<int:genre_id>")
@login_required
def genre_detail(genre_id: int):
    db = get_db()
    genre = db.execute(
        "SELECT genre_id, genre_name FROM genres WHERE genre_id = ?",
        (genre_id,),
    ).fetchone()
    if not genre:
        flash("Genre not found.", "error")
        return redirect(url_for("home"))

    movies = db.execute(
        """
        SELECT m.movie_id, m.title, m.release_year, m.poster_url, m.rating, d.director_id,
               d.director_first_name, d.director_last_name
        FROM movies m
        LEFT JOIN directors d ON d.director_id = m.director_id
        WHERE m.genre_id = ?
        ORDER BY m.rating DESC, m.release_year DESC
        """,
        (genre_id,),
    ).fetchall()
    return render_template("genre_detail.html", genre=genre, movies=movies)


def row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


@app.route("/movies")
@login_required
def movies_api():
    db = get_db()
    api_key = tmdb_api_key()
    page = max(request.args.get("page", default=1, type=int), 1)
    per_page = max(1, min(request.args.get("per_page", default=50, type=int), 250))

    if request.args.get("sync") == "1" and api_key:
        pages_to_sync = max(1, min(request.args.get("pages", default=5, type=int), 50))
        for tmdb_page in range(1, pages_to_sync + 1):
            payload = fetch_tmdb_popular_movies(api_key=api_key, page=tmdb_page)
            store_popular_movies(db, payload)

    offset = (page - 1) * per_page

    movies = db.execute(
        """
        SELECT m.movie_id, m.title, m.release_year, m.rating, m.poster_url, g.genre_name
        FROM movies m
        LEFT JOIN genres g ON g.genre_id = m.genre_id
        ORDER BY m.release_year DESC, m.rating DESC
        LIMIT ? OFFSET ?
        """,
        (per_page, offset),
    ).fetchall()
    return jsonify([dict(row) for row in movies])


@app.route("/movies/<int:movie_id>")
@login_required
def movie_api(movie_id: int):
    db = get_db()
    api_key = tmdb_api_key()

    if request.args.get("sync") == "1" and api_key:
        fetch_movie_details_and_upsert_requests(db, movie_id, api_key)

    movie = db.execute(
        """
        SELECT m.movie_id, m.title, m.release_year, m.rating, m.poster_url, m.description,
               g.genre_id, g.genre_name, d.director_id, d.director_first_name, d.director_last_name
        FROM movies m
        LEFT JOIN genres g ON g.genre_id = m.genre_id
        LEFT JOIN directors d ON d.director_id = m.director_id
        WHERE m.movie_id = ?
        """,
        (movie_id,),
    ).fetchone()
    if not movie:
        return jsonify({"error": "Movie not found"}), 404
    return jsonify(dict(movie))


@app.route("/genres")
@login_required
def genres_api():
    db = get_db()
    api_key = tmdb_api_key()
    if request.args.get("sync") == "1" and api_key:
        fetch_and_store_genres_requests(db, api_key)

    genres = db.execute(
        "SELECT genre_id, genre_name, tmdb_genre_id FROM genres ORDER BY genre_name"
    ).fetchall()
    return jsonify([dict(row) for row in genres])


@app.route("/directors")
@login_required
def directors_api():
    directors = get_db().execute(
        """
        SELECT director_id, director_first_name, director_last_name, profile_url
        FROM directors
        ORDER BY director_last_name, director_first_name
        """
    ).fetchall()
    return jsonify([dict(row) for row in directors])


@app.route("/directors/<int:director_id>")
@login_required
def director_api(director_id: int):
    director = get_db().execute(
        """
        SELECT director_id, director_first_name, director_last_name, profile_url, biography
        FROM directors
        WHERE director_id = ?
        """,
        (director_id,),
    ).fetchone()
    if not director:
        return jsonify({"error": "Director not found"}), 404
    return jsonify(dict(director))


@app.route("/movies/page")
@login_required
def movies_page():
    page = max(request.args.get("page", default=1, type=int), 1)
    per_page = 24
    offset = (page - 1) * per_page
    db = get_db()

    total = db.execute("SELECT COUNT(*) AS c FROM movies").fetchone()["c"]
    movies = db.execute(
        """
        SELECT movie_id, title, release_year, rating, poster_url
        FROM movies
        ORDER BY release_year DESC, rating DESC
        LIMIT ? OFFSET ?
        """,
        (per_page, offset),
    ).fetchall()
    has_next = (offset + per_page) < total
    return render_template(
        "movie_list.html",
        movies=movies,
        page=page,
        has_next=has_next,
        total=total,
    )


@app.route("/movies/<int:movie_id>/page")
@login_required
def movie_page(movie_id: int):
    return redirect(url_for("movie_detail", movie_id=movie_id))


@app.route("/")
def home():
    user = current_user()
    if not user:
        return render_template("landing.html")

    ensure_news_autosync()
    ensure_tmdb_autosync()

    db = get_db()
    recommended = db.execute(
        """
        SELECT
            m.movie_id,
            m.title,
            m.poster_url,
            COALESCE(SUM(ml.times_logged), 0) AS watched_count
        FROM movies m
        JOIN movielogging ml ON ml.movie_id = m.movie_id
        WHERE datetime(ml.movie_log_datetime) >= datetime('now', '-7 days')
        GROUP BY m.movie_id
        ORDER BY watched_count DESC, m.title ASC
        LIMIT 10
        """
    ).fetchall()

    friend_activity = db.execute(
        """
        SELECT
            r.review_id,
            u.username,
            m.movie_id,
            m.title,
            m.poster_url,
            r.rating,
            r.review_text,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        JOIN userauthentication u ON u.user_id = r.user_id
        JOIN movies m ON m.movie_id = r.movie_id
        JOIN userrelationships ur_forward
          ON ur_forward.following_id = r.user_id
         AND ur_forward.follower_id = ?
        JOIN userrelationships ur_back
          ON ur_back.follower_id = r.user_id
         AND ur_back.following_id = ?
        WHERE r.user_id != ?
          AND datetime(r.review_datetime) >= datetime('now', '-14 days')
        ORDER BY r.review_datetime DESC
        LIMIT 12
        """
        ,
        (user["user_id"], user["user_id"], user["user_id"]),
    ).fetchall()

    news_items = db.execute(
        """
        SELECT news_id, news_name, news_source, news_url, news_summary, news_image_url
        FROM news
        WHERE TRIM(COALESCE(news_url, '')) <> ''
        ORDER BY news_datetime DESC
        LIMIT 20
        """
    ).fetchall()

    playlists = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, p.cover_image_url, u.username,
               COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        JOIN userauthentication u ON u.user_id = p.user_id
        JOIN userrelationships ur_forward
          ON ur_forward.following_id = p.user_id
         AND ur_forward.follower_id = ?
        JOIN userrelationships ur_back
          ON ur_back.follower_id = p.user_id
         AND ur_back.following_id = ?
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.user_id != ?
        GROUP BY p.playlist_id
        ORDER BY p.created_datetime DESC
        LIMIT 6
        """
        ,
        (user["user_id"], user["user_id"], user["user_id"]),
    ).fetchall()

    return render_template(
        "dashboard.html",
        recommended=recommended,
        friend_activity=friend_activity,
        news_items=news_items,
        playlists=playlists,
    )


@app.route("/playlists/friends")
@login_required
def friends_playlists():
    db = get_db()
    user_id = session["user_id"]
    items = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, p.cover_image_url, p.created_datetime, u.username,
               COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        JOIN userauthentication u ON u.user_id = p.user_id
        JOIN userrelationships ur_forward
          ON ur_forward.following_id = p.user_id
         AND ur_forward.follower_id = ?
        JOIN userrelationships ur_back
          ON ur_back.follower_id = p.user_id
         AND ur_back.following_id = ?
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.user_id != ?
        GROUP BY p.playlist_id
        ORDER BY p.created_datetime DESC
        """
        ,
        (user_id, user_id, user_id),
    ).fetchall()
    return render_template("friends_playlists.html", items=items)


@app.route("/reviews/<int:review_id>/panel")
@login_required
def review_panel(review_id: int):
    db = get_db()
    review = db.execute(
        """
        SELECT
            r.review_id,
            r.user_id,
            u.username,
            m.movie_id,
            m.title,
            m.poster_url,
            r.rating,
            r.review_text,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        JOIN userauthentication u ON u.user_id = r.user_id
        JOIN movies m ON m.movie_id = r.movie_id
        WHERE r.review_id = ?
        """,
        (review_id,),
    ).fetchone()
    if not review:
        return "<p>Review not found.</p>", 404
    return render_template("review_panel.html", review=review)


@app.route("/signup", methods=["GET", "POST"])
def signup():
    combined_flow = request.args.get("combined") == "1" or request.form.get("combined") == "1"

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        username = request.form.get("username", "").strip().lower()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        receive_news = 1 if request.form.get("receive_news") else 0

        if not all([name, username, email, password]):
            flash("Please fill in all required fields.", "error")
            if combined_flow:
                return redirect(url_for("signup", combined=1))
            return redirect(url_for("signup"))

        db = get_db()
        existing = db.execute(
            "SELECT user_id FROM userauthentication WHERE username = ? OR email = ?",
            (username, email),
        ).fetchone()
        if existing:
            flash("Username or email already exists.", "error")
            if combined_flow:
                return redirect(url_for("signup", combined=1))
            return redirect(url_for("signup"))

        user_id = str(uuid.uuid4())
        db.execute(
            """
            INSERT INTO userauthentication (user_id, email, password, username, name, biography, create_time)
            VALUES (?, ?, ?, ?, ?, '', ?)
            """,
            (
                user_id,
                email,
                generate_password_hash(password),
                username,
                name,
                datetime.utcnow().isoformat(timespec="seconds"),
            ),
        )
        db.execute(
            "INSERT INTO usersettings (user_id, receive_news) VALUES (?, ?)",
            (user_id, receive_news),
        )
        db.commit()

        session["user_id"] = user_id
        create_notification(user_id, "system", "Welcome to CineMuse.")
        flash("Account created successfully.", "success")
        return redirect(url_for("home"))

    return render_template("auth.html", mode="signup", show_login_followup=combined_flow)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        user = get_db().execute(
            "SELECT * FROM userauthentication WHERE username = ?", (username,)
        ).fetchone()

        if not user or not check_password_hash(user["password"], password):
            flash("Invalid username or password.", "error")
            return redirect(url_for("login"))

        session["user_id"] = user["user_id"]
        flash("Welcome back.", "success")
        return redirect(url_for("home"))

    return render_template("auth.html", mode="login")


@app.route("/logout", methods=["GET", "POST"])
def logout():
    if "user_id" not in session:
        return redirect(url_for("home"))

    if request.method == "POST":
        session.clear()
        return redirect(url_for("logged_out"))

    return render_template("logout.html")


@app.route("/logged-out")
def logged_out():
    return render_template("logged_out.html")


@app.route("/profile")
@login_required
def profile():
    db = get_db()
    user = current_user()

    favorite_rows = db.execute(
        """
        SELECT uf.slot, m.movie_id, m.title, m.poster_url
        FROM user_favorite_movies uf
        JOIN movies m ON m.movie_id = uf.movie_id
        WHERE uf.user_id = ?
        ORDER BY uf.slot
        """,
        (user["user_id"],),
    ).fetchall()
    favorite_by_slot: dict[int, sqlite3.Row | None] = {1: None, 2: None, 3: None}
    for row in favorite_rows:
        favorite_by_slot[row["slot"]] = row
    favorite_slots = [(slot, favorite_by_slot[slot]) for slot in (1, 2, 3)]
    favorite_movie_choices = db.execute(
        "SELECT movie_id, title, poster_url FROM movies ORDER BY title LIMIT 500"
    ).fetchall()

    recent_activity = db.execute(
        """
        SELECT
            m.movie_id,
            m.title,
            m.poster_url,
            r.rating,
            r.review_text,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        JOIN movies m ON m.movie_id = r.movie_id
        WHERE r.user_id = ?
        ORDER BY r.review_datetime DESC
        LIMIT 3
        """,
        (user["user_id"],),
    ).fetchall()

    watchlist_items = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url, w.status
        FROM watchlist w
        JOIN movies m ON m.movie_id = w.movie_id
        WHERE w.user_id = ?
        ORDER BY w.watchlist_id DESC
        LIMIT 3
        """,
        (user["user_id"],),
    ).fetchall()

    playlists = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, p.cover_image_url, COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.user_id = ?
        GROUP BY p.playlist_id
        ORDER BY p.created_datetime DESC
        LIMIT 6
        """,
        (user["user_id"],),
    ).fetchall()
    liked_movies = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url, uml.liked_datetime
        FROM user_movie_likes uml
        JOIN movies m ON m.movie_id = uml.movie_id
        WHERE uml.user_id = ?
        ORDER BY uml.liked_datetime DESC
        LIMIT 3
        """,
        (user["user_id"],),
    ).fetchall()

    stats = db.execute(
        """
        SELECT
            COALESCE(SUM(times_logged), 0) AS total_watched,
            COALESCE(SUM(CASE
                WHEN strftime('%Y', movie_log_datetime) = strftime('%Y', 'now')
                THEN times_logged
                ELSE 0 END), 0) AS watched_this_year
        FROM movielogging
        WHERE user_id = ?
        """,
        (user["user_id"],),
    ).fetchone()

    follower_count = db.execute(
        "SELECT COUNT(*) AS c FROM userrelationships WHERE following_id = ?",
        (user["user_id"],),
    ).fetchone()["c"]
    following_count = db.execute(
        "SELECT COUNT(*) AS c FROM userrelationships WHERE follower_id = ?",
        (user["user_id"],),
    ).fetchone()["c"]

    return render_template(
        "profile.html",
        user=user,
        favorite_slots=favorite_slots,
        favorite_movie_choices=favorite_movie_choices,
        recent_activity=recent_activity,
        watchlist_items=watchlist_items,
        playlists=playlists,
        liked_movies=liked_movies,
        stats=stats,
        follower_count=follower_count,
        following_count=following_count,
    )


@app.route("/users/<string:username>")
@login_required
def user_profile(username: str):
    db = get_db()
    viewer_id = session["user_id"]
    target = db.execute(
        """
        SELECT user_id, username, name, biography, profile_picture_url
        FROM userauthentication
        WHERE LOWER(username) = ?
        """,
        (username.strip().lower(),),
    ).fetchone()
    if not target:
        flash("User not found.", "error")
        return redirect(url_for("search", type="users"))

    follower_count = db.execute(
        "SELECT COUNT(*) AS c FROM userrelationships WHERE following_id = ?",
        (target["user_id"],),
    ).fetchone()["c"]
    following_count = db.execute(
        "SELECT COUNT(*) AS c FROM userrelationships WHERE follower_id = ?",
        (target["user_id"],),
    ).fetchone()["c"]
    favorite_rows = db.execute(
        """
        SELECT uf.slot, m.movie_id, m.title, m.poster_url
        FROM user_favorite_movies uf
        JOIN movies m ON m.movie_id = uf.movie_id
        WHERE uf.user_id = ?
        ORDER BY uf.slot
        """,
        (target["user_id"],),
    ).fetchall()
    favorite_by_slot: dict[int, sqlite3.Row | None] = {1: None, 2: None, 3: None}
    for row in favorite_rows:
        favorite_by_slot[row["slot"]] = row
    favorite_slots = [(slot, favorite_by_slot[slot]) for slot in (1, 2, 3)]

    recent_activity = db.execute(
        """
        SELECT
            m.movie_id,
            m.title,
            m.poster_url,
            r.rating,
            r.review_text,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        JOIN movies m ON m.movie_id = r.movie_id
        WHERE r.user_id = ?
        ORDER BY r.review_datetime DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    watchlist_items = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url, w.status
        FROM watchlist w
        JOIN movies m ON m.movie_id = w.movie_id
        WHERE w.user_id = ?
        ORDER BY w.watchlist_id DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    playlists = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, p.cover_image_url, COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.user_id = ?
        GROUP BY p.playlist_id
        ORDER BY p.created_datetime DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    liked_movies = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url, uml.liked_datetime
        FROM user_movie_likes uml
        JOIN movies m ON m.movie_id = uml.movie_id
        WHERE uml.user_id = ?
        ORDER BY uml.liked_datetime DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    is_following = db.execute(
        """
        SELECT 1
        FROM userrelationships
        WHERE follower_id = ? AND following_id = ?
        """,
        (viewer_id, target["user_id"]),
    ).fetchone() is not None
    can_follow = viewer_id != target["user_id"]

    return render_template(
        "user_profile.html",
        profile_user=target,
        follower_count=follower_count,
        following_count=following_count,
        favorite_slots=favorite_slots,
        recent_activity=recent_activity,
        watchlist_items=watchlist_items,
        playlists=playlists,
        liked_movies=liked_movies,
        is_following=is_following,
        can_follow=can_follow,
    )


@app.route("/users/<string:username>/panel")
@login_required
def user_profile_panel(username: str):
    db = get_db()
    viewer_id = session["user_id"]
    target = db.execute(
        """
        SELECT user_id, username, name, biography, profile_picture_url
        FROM userauthentication
        WHERE LOWER(username) = ?
        """,
        (username.strip().lower(),),
    ).fetchone()
    if not target:
        return "<p>User not found.</p>", 404

    follower_count = db.execute(
        "SELECT COUNT(*) AS c FROM userrelationships WHERE following_id = ?",
        (target["user_id"],),
    ).fetchone()["c"]
    following_count = db.execute(
        "SELECT COUNT(*) AS c FROM userrelationships WHERE follower_id = ?",
        (target["user_id"],),
    ).fetchone()["c"]
    recent_activity = db.execute(
        """
        SELECT
            m.movie_id,
            m.title,
            m.poster_url,
            r.rating,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        JOIN movies m ON m.movie_id = r.movie_id
        WHERE r.user_id = ?
        ORDER BY r.review_datetime DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    favorite_rows = db.execute(
        """
        SELECT uf.slot, m.movie_id, m.title, m.poster_url
        FROM user_favorite_movies uf
        JOIN movies m ON m.movie_id = uf.movie_id
        WHERE uf.user_id = ?
        ORDER BY uf.slot
        """,
        (target["user_id"],),
    ).fetchall()
    favorite_by_slot: dict[int, sqlite3.Row | None] = {1: None, 2: None, 3: None}
    for row in favorite_rows:
        favorite_by_slot[row["slot"]] = row
    favorite_slots = [(slot, favorite_by_slot[slot]) for slot in (1, 2, 3)]
    watchlist_items = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url, w.status
        FROM watchlist w
        JOIN movies m ON m.movie_id = w.movie_id
        WHERE w.user_id = ?
        ORDER BY w.watchlist_id DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    playlists = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, p.cover_image_url, COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.user_id = ?
        GROUP BY p.playlist_id
        ORDER BY p.created_datetime DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    liked_movies = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url, uml.liked_datetime
        FROM user_movie_likes uml
        JOIN movies m ON m.movie_id = uml.movie_id
        WHERE uml.user_id = ?
        ORDER BY uml.liked_datetime DESC
        LIMIT 3
        """,
        (target["user_id"],),
    ).fetchall()
    is_following = db.execute(
        """
        SELECT 1
        FROM userrelationships
        WHERE follower_id = ? AND following_id = ?
        """,
        (viewer_id, target["user_id"]),
    ).fetchone() is not None
    can_follow = viewer_id != target["user_id"]

    return render_template(
        "user_profile_panel.html",
        profile_user=target,
        follower_count=follower_count,
        following_count=following_count,
        recent_activity=recent_activity,
        favorite_slots=favorite_slots,
        watchlist_items=watchlist_items,
        playlists=playlists,
        liked_movies=liked_movies,
        is_following=is_following,
        can_follow=can_follow,
    )


@app.route("/users/<string:username>/follow-toggle", methods=["POST"])
@login_required
def toggle_follow_user(username: str):
    db = get_db()
    viewer_id = session["user_id"]
    viewer = db.execute(
        "SELECT username FROM userauthentication WHERE user_id = ?",
        (viewer_id,),
    ).fetchone()
    viewer_username = viewer["username"] if viewer else "Someone"
    target = db.execute(
        "SELECT user_id FROM userauthentication WHERE LOWER(username) = ?",
        (username.strip().lower(),),
    ).fetchone()
    if not target:
        return jsonify({"ok": False, "error": "User not found"}), 404
    target_id = target["user_id"]
    if target_id == viewer_id:
        return jsonify({"ok": False, "error": "Cannot follow yourself"}), 400

    existing = db.execute(
        """
        SELECT 1
        FROM userrelationships
        WHERE follower_id = ? AND following_id = ?
        """,
        (viewer_id, target_id),
    ).fetchone()
    if existing:
        db.execute(
            "DELETE FROM userrelationships WHERE follower_id = ? AND following_id = ?",
            (viewer_id, target_id),
        )
        is_following = False
    else:
        target_follows_viewer = db.execute(
            """
            SELECT 1
            FROM userrelationships
            WHERE follower_id = ? AND following_id = ?
            """,
            (target_id, viewer_id),
        ).fetchone() is not None
        db.execute(
            """
            INSERT OR IGNORE INTO userrelationships (follower_id, following_id, followed_date)
            VALUES (?, ?, ?)
            """,
            (viewer_id, target_id, datetime.utcnow().isoformat(timespec="seconds")),
        )
        is_following = True
    db.commit()

    if is_following and receives_follow_notifications(target_id):
        if target_follows_viewer:
            text = f"{viewer_username} has followed you back."
        else:
            text = f"{viewer_username} started following you."
        create_notification(target_id, "follow", text)

    follower_count = db.execute(
        "SELECT COUNT(*) AS c FROM userrelationships WHERE following_id = ?",
        (target_id,),
    ).fetchone()["c"]
    return jsonify({"ok": True, "is_following": is_following, "follower_count": follower_count})


@app.route("/profile/favorites", methods=["POST"])
@login_required
def update_profile_favorites():
    db = get_db()
    slot = request.form.get("slot", type=int)
    movie_id = request.form.get("movie_id", type=int)
    if slot not in {1, 2, 3} or movie_id is None:
        flash("Invalid favorite movie selection.", "error")
        return redirect(url_for("profile"))

    movie = db.execute("SELECT movie_id FROM movies WHERE movie_id = ?", (movie_id,)).fetchone()
    if not movie:
        flash("Selected movie does not exist.", "error")
        return redirect(url_for("profile"))

    # Enforce uniqueness across the 3 slots before assigning chosen slot.
    db.execute(
        "DELETE FROM user_favorite_movies WHERE user_id = ? AND movie_id = ?",
        (session["user_id"], movie_id),
    )
    db.execute(
        """
        INSERT INTO user_favorite_movies (user_id, slot, movie_id)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, slot)
        DO UPDATE SET movie_id = excluded.movie_id
        """,
        (session["user_id"], slot, movie_id),
    )
    db.commit()
    flash("Favourite films updated.", "success")
    return redirect(url_for("profile"))


@app.route("/movies/<int:movie_id>/toggle-like", methods=["POST"])
@login_required
def toggle_movie_like(movie_id: int):
    db = get_db()
    user_id = session["user_id"]

    exists = db.execute("SELECT movie_id FROM movies WHERE movie_id = ?", (movie_id,)).fetchone()
    if not exists:
        flash("Movie not found.", "error")
        return redirect(url_for("home"))

    liked = db.execute(
        "SELECT 1 FROM user_movie_likes WHERE user_id = ? AND movie_id = ?",
        (user_id, movie_id),
    ).fetchone()
    if liked:
        db.execute(
            "DELETE FROM user_movie_likes WHERE user_id = ? AND movie_id = ?",
            (user_id, movie_id),
        )
    else:
        db.execute(
            """
            INSERT INTO user_movie_likes (user_id, movie_id, liked_datetime)
            VALUES (?, ?, ?)
            """,
            (user_id, movie_id, datetime.utcnow().isoformat(timespec="seconds")),
        )
    db.commit()

    next_url = request.form.get("next", "").strip()
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(request.referrer or url_for("home"))


@app.route("/reviews", methods=["GET", "POST"])
@login_required
def reviews():
    db = get_db()
    user = current_user()

    if request.method == "POST":
        movie_id = request.form.get("movie_id", type=int)
        rating = request.form.get("rating", type=float)
        review_text = request.form.get("review_text", "").strip()

        if movie_id is None or rating is None:
            flash("Please select a movie and rating.", "error")
            return redirect(url_for("reviews"))

        now = datetime.utcnow().isoformat(timespec="seconds")
        db.execute(
            """
            INSERT INTO reviews (user_id, movie_id, rating, review_text, review_datetime, liked_movie_snapshot)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user["user_id"], movie_id, rating, review_text, now, 0),
        )
        db.execute(
            """
            INSERT INTO movielogging (user_id, movie_id, times_logged, movie_log_datetime)
            VALUES (?, ?, 1, ?)
            ON CONFLICT(user_id, movie_id)
            DO UPDATE SET
              times_logged = times_logged + 1,
              movie_log_datetime = excluded.movie_log_datetime
            """,
            (user["user_id"], movie_id, now),
        )
        db.commit()
        flash("Review posted.", "success")
        return redirect(url_for("reviews"))

    movies = db.execute(
        "SELECT movie_id, title, release_year FROM movies ORDER BY title"
    ).fetchall()
    all_reviews = db.execute(
        """
        SELECT r.review_id, u.username, m.movie_id, m.title, r.rating, r.review_text, r.review_datetime,
               SUM(CASE WHEN l.like_id IS NOT NULL THEN 1 ELSE 0 END) AS like_count
        FROM reviews r
        JOIN userauthentication u ON u.user_id = r.user_id
        JOIN movies m ON m.movie_id = r.movie_id
        LEFT JOIN likes l ON l.review_id = r.review_id
        GROUP BY r.review_id
        ORDER BY r.review_datetime DESC
        """
    ).fetchall()
    return render_template("reviews.html", movies=movies, reviews=all_reviews)


@app.route("/reviews/<int:review_id>/delete", methods=["POST"])
@login_required
def delete_review(review_id: int):
    db = get_db()
    db.execute(
        "DELETE FROM reviews WHERE review_id = ? AND user_id = ?",
        (review_id, session["user_id"]),
    )
    db.commit()
    flash("Review deleted.", "success")
    return redirect(url_for("reviews"))


@app.route("/reviews/<int:review_id>/like", methods=["POST"])
@login_required
def like_review(review_id: int):
    db = get_db()
    actor = db.execute(
        "SELECT username FROM userauthentication WHERE user_id = ?",
        (session["user_id"],),
    ).fetchone()
    owner = db.execute(
        """
        SELECT r.user_id, m.title
        FROM reviews r
        LEFT JOIN movies m ON m.movie_id = r.movie_id
        WHERE r.review_id = ?
        """,
        (review_id,),
    ).fetchone()
    try:
        db.execute(
            "INSERT INTO likes (user_id, review_id, like_datetime) VALUES (?, ?, ?)",
            (session["user_id"], review_id, datetime.utcnow().isoformat(timespec="seconds")),
        )
        db.commit()
        if (
            owner
            and owner["user_id"] != session["user_id"]
            and receives_like_notifications(owner["user_id"])
        ):
            actor_name = actor["username"] if actor else "Someone"
            movie_title = owner["title"] or "your movie review"
            create_notification(
                owner["user_id"],
                "review_like",
                f"{actor_name} liked your review on {movie_title}.",
            )
    except sqlite3.IntegrityError:
        pass
    return redirect(url_for("reviews"))


@app.route("/reviews/<int:review_id>/comment", methods=["POST"])
@login_required
def comment_review(review_id: int):
    db = get_db()
    user_id = session["user_id"]
    comment_text = request.form.get("comment_text", "").strip()
    next_url = request.form.get("next", "").strip()

    if not comment_text:
        flash("Comment cannot be empty.", "error")
        if next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("reviews"))

    review = db.execute(
        """
        SELECT r.review_id, r.user_id, r.movie_id, m.title
        FROM reviews r
        LEFT JOIN movies m ON m.movie_id = r.movie_id
        WHERE r.review_id = ?
        """,
        (review_id,),
    ).fetchone()
    if not review:
        flash("Review not found.", "error")
        if next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("reviews"))

    db.execute(
        """
        INSERT INTO review_comments (review_id, user_id, comment_text, comment_datetime)
        VALUES (?, ?, ?, ?)
        """,
        (review_id, user_id, comment_text, datetime.utcnow().isoformat(timespec="seconds")),
    )
    db.commit()

    if review["user_id"] != user_id and receives_like_notifications(review["user_id"]):
        actor = db.execute(
            "SELECT username FROM userauthentication WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        actor_name = actor["username"] if actor else "Someone"
        movie_title = review["title"] or "your movie review"
        create_notification(
            review["user_id"],
            "review_comment",
            f"{actor_name} commented on your review on {movie_title}.",
        )

    flash("Comment added.", "success")
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("movie_detail", movie_id=review["movie_id"]))


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    db = get_db()
    user = current_user()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        biography = request.form.get("biography", "").strip()
        username = request.form.get("username", "").strip().lower()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        receive_news = 1 if request.form.get("receive_news") else 0
        receive_follow_notifications = 1 if request.form.get("receive_follow_notifications") else 0
        receive_like_notifications = 1 if request.form.get("receive_like_notifications") else 0
        profile_picture_url = user["profile_picture_url"]

        if not name or not username or not email:
            flash("Name, username, and email are required.", "error")
            return redirect(url_for("settings"))
        if not is_valid_email_address(email):
            flash("Please enter a real, valid email address.", "error")
            return redirect(url_for("settings"))

        uploaded_picture = request.files.get("profile_picture")
        if uploaded_picture and uploaded_picture.filename:
            try:
                profile_picture_url = save_profile_picture(uploaded_picture, user["user_id"])
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("settings"))

        try:
            if password:
                db.execute(
                    """
                    UPDATE userauthentication
                    SET name = ?, biography = ?, username = ?, email = ?, password = ?, profile_picture_url = ?
                    WHERE user_id = ?
                    """,
                    (
                        name,
                        biography,
                        username,
                        email,
                        generate_password_hash(password),
                        profile_picture_url,
                        user["user_id"],
                    ),
                )
            else:
                db.execute(
                    """
                    UPDATE userauthentication
                    SET name = ?, biography = ?, username = ?, email = ?, profile_picture_url = ?
                    WHERE user_id = ?
                    """,
                    (name, biography, username, email, profile_picture_url, user["user_id"]),
                )
            db.execute(
                """
                INSERT INTO usersettings (
                    user_id,
                    receive_news,
                    receive_follow_notifications,
                    receive_like_notifications
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id)
                DO UPDATE SET
                    receive_news = excluded.receive_news,
                    receive_follow_notifications = excluded.receive_follow_notifications,
                    receive_like_notifications = excluded.receive_like_notifications
                """,
                (
                    user["user_id"],
                    receive_news,
                    receive_follow_notifications,
                    receive_like_notifications,
                ),
            )
            db.commit()
        except sqlite3.IntegrityError:
            flash("Username or email is already in use.", "error")
            return redirect(url_for("settings"))

        flash("Settings saved.", "success")
        return redirect(url_for("settings"))

    setting = db.execute(
        "SELECT * FROM usersettings WHERE user_id = ?", (user["user_id"],)
    ).fetchone()
    return render_template("settings.html", user=user, setting=setting)


@app.route("/activity")
@login_required
def activity():
    items = get_db().execute(
        """
        SELECT
            r.review_id,
            m.movie_id,
            m.title,
            m.poster_url,
            r.rating,
            r.review_text,
            r.review_datetime,
            COALESCE(r.liked_movie_snapshot, 0) AS liked_movie
        FROM reviews r
        JOIN movies m ON m.movie_id = r.movie_id
        WHERE r.user_id = ?
        ORDER BY r.review_datetime DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template("activity.html", items=items)


@app.route("/activity/review/edit", methods=["POST"])
@login_required
def edit_activity_review():
    db = get_db()
    user_id = session["user_id"]
    review_id = request.form.get("review_id", type=int)
    movie_id = request.form.get("movie_id", type=int)
    rating_raw = request.form.get("rating", "").strip()
    review_text = request.form.get("review_text", "").strip()
    liked_movie_value = (request.form.get("liked_movie") or "").strip().lower()
    liked_movie = liked_movie_value in {"1", "on", "true", "yes"}

    if review_id is None or movie_id is None:
        flash("Missing review data.", "error")
        return redirect(url_for("activity"))

    rating = None
    if rating_raw:
        try:
            rating = float(rating_raw)
        except ValueError:
            flash("Invalid star rating.", "error")
            return redirect(url_for("activity"))
    if rating is not None and (rating < 0.5 or rating > 5):
        flash("Please choose a valid star rating.", "error")
        return redirect(url_for("activity"))

    if liked_movie:
        db.execute(
            """
            INSERT INTO user_movie_likes (user_id, movie_id, liked_datetime)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, movie_id)
            DO UPDATE SET liked_datetime = excluded.liked_datetime
            """,
            (user_id, movie_id, datetime.utcnow().isoformat(timespec="seconds")),
        )
    else:
        db.execute(
            "DELETE FROM user_movie_likes WHERE user_id = ? AND movie_id = ?",
            (user_id, movie_id),
        )

    db.execute(
        """
        UPDATE reviews
        SET rating = ?, review_text = ?, review_datetime = ?, liked_movie_snapshot = ?
        WHERE review_id = ? AND user_id = ? AND movie_id = ?
        """,
        (
            rating if rating is not None else 0.0,
            review_text,
            datetime.utcnow().isoformat(timespec="seconds"),
            1 if liked_movie else 0,
            review_id,
            user_id,
            movie_id,
        ),
    )
    db.commit()
    flash("Review updated.", "success")
    return redirect(url_for("activity"))


@app.route("/logged-movies")
@login_required
def logged_movies():
    items = get_db().execute(
        """
        SELECT m.movie_id, m.title, ml.times_logged, ml.movie_log_datetime
        FROM movielogging ml
        JOIN movies m ON m.movie_id = ml.movie_id
        WHERE ml.user_id = ?
        ORDER BY ml.movie_log_datetime DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template("logged_movies.html", items=items, title="All Logged Movies")


@app.route("/logged-movies/current-year")
@login_required
def logged_movies_current_year():
    items = get_db().execute(
        """
        SELECT m.movie_id, m.title, ml.times_logged, ml.movie_log_datetime
        FROM movielogging ml
        JOIN movies m ON m.movie_id = ml.movie_id
        WHERE ml.user_id = ?
          AND strftime('%Y', ml.movie_log_datetime) = strftime('%Y', 'now')
        ORDER BY ml.movie_log_datetime DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template(
        "logged_movies.html", items=items, title="Logged Movies This Year"
    )


@app.route("/watchlist", methods=["GET", "POST"])
@login_required
def watchlist():
    db = get_db()

    if request.method == "POST":
        movie_id = request.form.get("movie_id", type=int)
        status = request.form.get("status", "planned")
        db.execute(
            """
            INSERT INTO watchlist (user_id, movie_id, status)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, movie_id)
            DO UPDATE SET status = excluded.status
            """,
            (session["user_id"], movie_id, status),
        )
        db.commit()
        flash("Watchlist updated.", "success")
        next_url = request.form.get("next", "").strip()
        if next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("watchlist"))

    movies = db.execute("SELECT movie_id, title FROM movies ORDER BY title").fetchall()
    items = db.execute(
        """
        SELECT m.movie_id, m.title, w.status
        FROM watchlist w
        JOIN movies m ON m.movie_id = w.movie_id
        WHERE w.user_id = ?
        ORDER BY w.watchlist_id DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template("watchlist.html", movies=movies, items=items)


@app.route("/likes")
@login_required
def likes():
    items = get_db().execute(
        """
        SELECT m.movie_id, m.title, m.poster_url, uml.liked_datetime
        FROM user_movie_likes uml
        JOIN movies m ON m.movie_id = uml.movie_id
        WHERE uml.user_id = ?
        ORDER BY uml.liked_datetime DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template("likes.html", items=items)


@app.route("/following")
@login_required
def following():
    db = get_db()
    items = db.execute(
        """
        SELECT u.username, u.name, u.profile_picture_url, ur.followed_date
        FROM userrelationships ur
        JOIN userauthentication u ON u.user_id = ur.following_id
        WHERE ur.follower_id = ?
        ORDER BY ur.followed_date DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template("following.html", items=items)


@app.route("/followers")
@login_required
def followers():
    items = get_db().execute(
        """
        SELECT u.username, u.name, u.profile_picture_url, ur.followed_date
        FROM userrelationships ur
        JOIN userauthentication u ON u.user_id = ur.follower_id
        WHERE ur.following_id = ?
        ORDER BY ur.followed_date DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template("followers.html", items=items)


@app.route("/users/<string:username>/followers")
@login_required
def user_followers(username: str):
    db = get_db()
    target = db.execute(
        """
        SELECT user_id, username, name
        FROM userauthentication
        WHERE LOWER(username) = ?
        """,
        (username.strip().lower(),),
    ).fetchone()
    if not target:
        flash("User not found.", "error")
        return redirect(url_for("search", type="users"))

    items = db.execute(
        """
        SELECT u.username, u.name, u.profile_picture_url, ur.followed_date
        FROM userrelationships ur
        JOIN userauthentication u ON u.user_id = ur.follower_id
        WHERE ur.following_id = ?
        ORDER BY ur.followed_date DESC
        """,
        (target["user_id"],),
    ).fetchall()
    return render_template(
        "user_connections.html",
        profile_user=target,
        items=items,
        section_title="Followers",
        empty_message="No followers yet.",
    )


@app.route("/users/<string:username>/followers/panel")
@login_required
def user_followers_panel(username: str):
    db = get_db()
    target = db.execute(
        """
        SELECT user_id, username, name
        FROM userauthentication
        WHERE LOWER(username) = ?
        """,
        (username.strip().lower(),),
    ).fetchone()
    if not target:
        return "<p>User not found.</p>", 404

    items = db.execute(
        """
        SELECT u.username, u.name, u.profile_picture_url, ur.followed_date
        FROM userrelationships ur
        JOIN userauthentication u ON u.user_id = ur.follower_id
        WHERE ur.following_id = ?
        ORDER BY ur.followed_date DESC
        """,
        (target["user_id"],),
    ).fetchall()
    return render_template(
        "user_connections_panel.html",
        profile_user=target,
        items=items,
        section_title="Followers",
        empty_message="No followers yet.",
    )


@app.route("/users/<string:username>/following")
@login_required
def user_following(username: str):
    db = get_db()
    target = db.execute(
        """
        SELECT user_id, username, name
        FROM userauthentication
        WHERE LOWER(username) = ?
        """,
        (username.strip().lower(),),
    ).fetchone()
    if not target:
        flash("User not found.", "error")
        return redirect(url_for("search", type="users"))

    items = db.execute(
        """
        SELECT u.username, u.name, u.profile_picture_url, ur.followed_date
        FROM userrelationships ur
        JOIN userauthentication u ON u.user_id = ur.following_id
        WHERE ur.follower_id = ?
        ORDER BY ur.followed_date DESC
        """,
        (target["user_id"],),
    ).fetchall()
    return render_template(
        "user_connections.html",
        profile_user=target,
        items=items,
        section_title="Following",
        empty_message="Not following anyone yet.",
    )


@app.route("/users/<string:username>/following/panel")
@login_required
def user_following_panel(username: str):
    db = get_db()
    target = db.execute(
        """
        SELECT user_id, username, name
        FROM userauthentication
        WHERE LOWER(username) = ?
        """,
        (username.strip().lower(),),
    ).fetchone()
    if not target:
        return "<p>User not found.</p>", 404

    items = db.execute(
        """
        SELECT u.username, u.name, u.profile_picture_url, ur.followed_date
        FROM userrelationships ur
        JOIN userauthentication u ON u.user_id = ur.following_id
        WHERE ur.follower_id = ?
        ORDER BY ur.followed_date DESC
        """,
        (target["user_id"],),
    ).fetchall()
    return render_template(
        "user_connections_panel.html",
        profile_user=target,
        items=items,
        section_title="Following",
        empty_message="Not following anyone yet.",
    )


@app.route("/recommendations")
@login_required
def recommendations():
    db = get_db()
    genre = request.args.get("genre", "").strip()
    query = request.args.get("query", "").strip()

    chosen_genre = genre

    sql = (
        "SELECT m.movie_id, m.title, m.release_year, g.genre_id, g.genre_name, m.rating FROM movies m "
        "LEFT JOIN genres g ON g.genre_id = m.genre_id WHERE 1=1"
    )
    params: list[str] = []
    if chosen_genre:
        sql += " AND g.genre_name = ?"
        params.append(chosen_genre)
    if query:
        sql += " AND LOWER(m.title) LIKE ?"
        params.append(f"%{query.lower()}%")
    sql += " ORDER BY m.rating DESC LIMIT 25"

    movies = db.execute(sql, params).fetchall()
    return render_template(
        "recommendations.html", movies=movies, selected_genre=chosen_genre, query=query
    )



@app.route("/sync/tmdb")
@login_required
def sync_tmdb():
    if not is_tmdb_configured():
        flash("TMDB credentials missing. Set TMDB_API_READ_ACCESS_TOKEN or TMDB_API_KEY.", "error")
        return redirect(url_for("settings"))

    pages = request.args.get("pages", default=10, type=int)
    start_page = request.args.get("start_page", default=1, type=int)
    source = request.args.get("source", default="popular", type=str)
    include_directors = request.args.get("include_directors", default=0, type=int) == 1
    try:
        with open_sync_connection() as conn:
            genre_count = sync_tmdb_genres(conn)
            movie_count = sync_tmdb_movies(
                pages=pages,
                source=source,
                include_directors=include_directors,
                start_page=start_page,
                db=conn,
            )
        flash(
            f"TMDB sync completed. Genres synced: {genre_count}, movies synced: {movie_count}.",
            "success",
        )
    except Exception as exc:
        flash(f"TMDB sync failed: {exc}", "error")
    return redirect(url_for("home"))


@app.route("/search/history/<int:search_id>/delete", methods=["POST"])
@login_required
def delete_search_history(search_id):
    db = get_db()
    db.execute(
        "DELETE FROM searchhistory WHERE search_id = ? AND user_id = ?",
        (search_id, session["user_id"]),
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/sync/news")
@login_required
def sync_news():
    if not is_newsapi_configured():
        flash("NewsAPI key missing. Set NEWSAPI_KEY in .env.", "error")
        return redirect(url_for("home"))

    pages = request.args.get("pages", default=5, type=int)
    page_size = request.args.get("page_size", default=30, type=int)
    topics = request.args.getlist("query")
    if not topics:
        topics = DEFAULT_NEWS_TOPICS

    try:
        counts = run_newsapi_sync(topics, pages, page_size)
        total = sum(counts.values())
        if total:
            flash(f"News sync completed: {total} articles.", "success")
        else:
            flash("News sync completed, no new articles found.", "info")
    except Exception as exc:
        flash(f"News sync failed: {exc}", "error")
    return redirect(url_for("home"))


@app.route("/playlists", methods=["GET", "POST"])
@login_required
def playlists():
    db = get_db()

    if request.method == "POST":
        action = request.form.get("action")
        if action == "create":
            playlist_name = request.form.get("playlist_name", "").strip()
            if playlist_name:
                db.execute(
                    "INSERT INTO playlists (user_id, playlist_name, created_datetime) VALUES (?, ?, ?)",
                    (session["user_id"], playlist_name, datetime.utcnow().isoformat(timespec="seconds")),
                )
                db.commit()
                flash("Playlist created.", "success")
        elif action == "add_movie":
            playlist_id = request.form.get("playlist_id", type=int)
            movie_id = request.form.get("movie_id", type=int)
            db.execute(
                """
                INSERT OR IGNORE INTO playlist_movies (playlist_id, movie_id, added_datetime)
                VALUES (?, ?, ?)
                """,
                (playlist_id, movie_id, datetime.utcnow().isoformat(timespec="seconds")),
            )
            db.commit()
            flash("Movie added to playlist.", "success")
        return redirect(url_for("playlists"))

    playlists_data = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, p.created_datetime,
               COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.user_id = ?
        GROUP BY p.playlist_id
        ORDER BY p.created_datetime DESC
        """,
        (session["user_id"],),
    ).fetchall()

    playlist_movies = db.execute(
        """
        SELECT p.playlist_name, m.movie_id, m.title
        FROM playlists p
        JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        JOIN movies m ON m.movie_id = pm.movie_id
        WHERE p.user_id = ?
        ORDER BY p.playlist_id DESC
        """,
        (session["user_id"],),
    ).fetchall()

    movies = db.execute("SELECT movie_id, title FROM movies ORDER BY title").fetchall()
    return render_template(
        "playlists.html", playlists=playlists_data, playlist_movies=playlist_movies, movies=movies
    )


@app.route("/playlists/<int:playlist_id>", methods=["GET", "POST"])
@login_required
def playlist_detail(playlist_id: int):
    db = get_db()
    user_id = session["user_id"]

    playlist = db.execute(
        """
        SELECT p.playlist_id, p.playlist_name, p.cover_image_url, p.description, p.created_datetime,
               COUNT(pm.movie_id) AS movie_count
        FROM playlists p
        LEFT JOIN playlist_movies pm ON pm.playlist_id = p.playlist_id
        WHERE p.playlist_id = ? AND p.user_id = ?
        GROUP BY p.playlist_id
        """,
        (playlist_id, user_id),
    ).fetchone()
    if not playlist:
        flash("Playlist not found.", "error")
        return redirect(url_for("playlists"))

    if request.method == "POST":
        action = request.form.get("action", "add_movies").strip()
        if action == "edit_playlist":
            playlist_name = request.form.get("playlist_name", "").strip()
            description = request.form.get("description", "").strip()
            if not playlist_name:
                flash("Playlist title is required.", "error")
                return redirect(url_for("playlist_detail", playlist_id=playlist_id))
            if len(description) > 250:
                flash("Playlist description can be at most 250 characters.", "error")
                return redirect(url_for("playlist_detail", playlist_id=playlist_id))

            cover_image_url = playlist["cover_image_url"]
            uploaded_cover = request.files.get("cover_image")
            if uploaded_cover and uploaded_cover.filename:
                try:
                    cover_image_url = save_playlist_cover_picture(uploaded_cover, user_id)
                except ValueError as exc:
                    flash(str(exc), "error")
                    return redirect(url_for("playlist_detail", playlist_id=playlist_id))

            db.execute(
                """
                UPDATE playlists
                SET playlist_name = ?, description = ?, cover_image_url = ?
                WHERE playlist_id = ? AND user_id = ?
                """,
                (playlist_name, description, cover_image_url, playlist_id, user_id),
            )
            db.commit()
            flash("Playlist updated.", "success")
            return redirect(url_for("playlist_detail", playlist_id=playlist_id))
        elif action == "delete_playlist":
            db.execute(
                "DELETE FROM playlists WHERE playlist_id = ? AND user_id = ?",
                (playlist_id, user_id),
            )
            db.commit()
            flash("Playlist deleted.", "success")
            return redirect(url_for("playlists"))

        selected_movie_ids = request.form.getlist("movie_ids")
        selected_movie_ids_int: list[int] = []
        for raw_id in selected_movie_ids:
            try:
                selected_movie_ids_int.append(int(raw_id))
            except ValueError:
                continue

        if not selected_movie_ids_int:
            flash("Select at least one movie to add.", "error")
            return redirect(url_for("playlist_detail", playlist_id=playlist_id))

        now = datetime.utcnow().isoformat(timespec="seconds")
        db.executemany(
            """
            INSERT OR IGNORE INTO playlist_movies (playlist_id, movie_id, added_datetime)
            VALUES (?, ?, ?)
            """,
            [(playlist_id, movie_id, now) for movie_id in selected_movie_ids_int],
        )
        db.commit()
        flash("Movies added to playlist.", "success")
        return redirect(url_for("playlist_detail", playlist_id=playlist_id))

    playlist_movies = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url
        FROM playlist_movies pm
        JOIN movies m ON m.movie_id = pm.movie_id
        WHERE pm.playlist_id = ?
        ORDER BY
          CASE WHEN pm.added_datetime IS NULL THEN 1 ELSE 0 END,
          pm.added_datetime DESC,
          pm.rowid DESC
        """,
        (playlist_id,),
    ).fetchall()
    addable_movies = db.execute(
        """
        SELECT m.movie_id, m.title, m.poster_url
        FROM movies m
        LEFT JOIN playlist_movies pm
          ON pm.movie_id = m.movie_id
         AND pm.playlist_id = ?
        WHERE pm.movie_id IS NULL
        ORDER BY m.title
        LIMIT 500
        """,
        (playlist_id,),
    ).fetchall()

    return render_template(
        "playlist_detail.html",
        playlist=playlist,
        playlist_movies=playlist_movies,
        addable_movies=addable_movies,
    )


@app.route("/playlists/new", methods=["GET", "POST"])
@login_required
def create_playlist():
    db = get_db()

    if request.method == "POST":
        playlist_name = request.form.get("playlist_name", "").strip()
        selected_movie_ids = request.form.getlist("movie_ids")
        selected_movie_ids_int: list[int] = []
        for raw_id in selected_movie_ids:
            try:
                selected_movie_ids_int.append(int(raw_id))
            except ValueError:
                continue

        if not playlist_name:
            flash("Playlist name is required.", "error")
            return redirect(url_for("create_playlist"))

        cover_image_url = None
        uploaded_cover = request.files.get("cover_image")
        if uploaded_cover and uploaded_cover.filename:
            try:
                cover_image_url = save_playlist_cover_picture(uploaded_cover, session["user_id"])
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("create_playlist"))

        created_datetime = datetime.utcnow().isoformat(timespec="seconds")
        db.execute(
            """
            INSERT INTO playlists (user_id, playlist_name, created_datetime, cover_image_url)
            VALUES (?, ?, ?, ?)
            """,
            (session["user_id"], playlist_name, created_datetime, cover_image_url),
        )
        playlist_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        if selected_movie_ids_int:
            now = datetime.utcnow().isoformat(timespec="seconds")
            db.executemany(
                """
                INSERT OR IGNORE INTO playlist_movies (playlist_id, movie_id, added_datetime)
                VALUES (?, ?, ?)
                """,
                [(playlist_id, movie_id, now) for movie_id in selected_movie_ids_int],
            )

        db.commit()
        flash("Playlist created.", "success")
        return redirect(url_for("playlists"))

    movies = db.execute(
        "SELECT movie_id, title, poster_url FROM movies ORDER BY title LIMIT 500"
    ).fetchall()
    return render_template("create_playlist.html", movies=movies)


@app.route("/inbox")
@login_required
def inbox():
    items = get_db().execute(
        """
        SELECT notification_type, notification_text, created_datetime
        FROM notifications
        WHERE user_id = ?
        ORDER BY created_datetime DESC
        """,
        (session["user_id"],),
    ).fetchall()
    return render_template("inbox.html", items=items)


@app.route("/events")
@login_required
def events():
    source = request.args.get("source", "").strip().lower()
    db = get_db()
    if source:
        items = db.execute(
            "SELECT * FROM news WHERE LOWER(news_source) LIKE ? ORDER BY news_datetime DESC",
            (f"%{source}%",),
        ).fetchall()
    else:
        items = db.execute("SELECT * FROM news ORDER BY news_datetime DESC").fetchall()
    return render_template("events.html", items=items, source=source)


@app.route("/mail")
@login_required
def mail():
    db = get_db()
    section = request.args.get("section", "notifications").strip().lower()
    if section not in {"notifications", "news"}:
        section = "notifications"

    notifications = []
    news_items = []
    if section == "notifications":
        notifications = db.execute(
            """
            SELECT notification_type, notification_text, created_datetime
            FROM notifications
            WHERE user_id = ?
            ORDER BY created_datetime DESC
            """,
            (session["user_id"],),
        ).fetchall()
    else:
        news_items = db.execute(
            "SELECT * FROM news ORDER BY news_datetime DESC"
        ).fetchall()

    return render_template(
        "mail.html",
        section=section,
        notifications=notifications,
        news_items=news_items,
    )


@app.route("/search/suggest")
@login_required
def search_suggest():
    db = get_db()
    q = request.args.get("q", "").strip()
    if len(q) < 3:
        return jsonify({"movies": [], "directors": [], "users": []})

    needle = f"%{q.lower()}%"
    movies = db.execute(
        """
        SELECT movie_id, title, release_year
        FROM movies
        WHERE LOWER(title) LIKE ?
        ORDER BY rating DESC, release_year DESC
        LIMIT 6
        """,
        (needle,),
    ).fetchall()
    directors = db.execute(
        """
        SELECT director_id, director_first_name, director_last_name
        FROM directors
        WHERE LOWER(director_first_name || ' ' || director_last_name) LIKE ?
        ORDER BY director_last_name, director_first_name
        LIMIT 6
        """,
        (needle,),
    ).fetchall()
    users = db.execute(
        """
        SELECT username, name
        FROM userauthentication
        WHERE LOWER(username) LIKE ? OR LOWER(name) LIKE ?
        ORDER BY username
        LIMIT 6
        """,
        (needle, needle),
    ).fetchall()
    return jsonify(
        {
            "movies": [
                {

                    "label": f"{row['title']} ({row['release_year']})"
                    if row["release_year"]
                    else row["title"],
                    "target_url": url_for("movie_detail", movie_id=row["movie_id"]),
                    "panel_url": url_for("movie_panel", movie_id=row["movie_id"]),
                }
                for row in movies
            ],
            "directors": [
                {
                    "label": f"{row['director_first_name']} {row['director_last_name']}".strip(),
                    "target_url": url_for("director_detail", director_id=row["director_id"]),
                    "panel_url": url_for("director_panel", director_id=row["director_id"]),
                }
                for row in directors
            ],
            "users": [
                {
                    "label": f"@{row['username']} · {row['name']}",
                    "target_url": url_for("user_profile", username=row["username"]),
                    "panel_url": url_for("user_profile_panel", username=row["username"]),
                }
                for row in users
            ],
        }
    )


@app.route("/search")
@login_required
def search():
    db = get_db()
    q = request.args.get("q", "").strip()
    filter_type = request.args.get("type", "movies")

    results = []
    history_target_url: str | None = None
    history_target_panel_url: str | None = None
    if q:
        if filter_type == "users":
            results = db.execute(
                "SELECT username, name, biography FROM userauthentication WHERE LOWER(username) LIKE ?",
                (f"%{q.lower()}%",),
            ).fetchall()
            if results:
                target = next((r for r in results if r["username"].lower() == q.lower()), results[0])
                history_target_url = url_for("user_profile", username=target["username"])
                history_target_panel_url = url_for("user_profile_panel", username=target["username"])
        elif filter_type == "directors":
            results = db.execute(
                """
                SELECT director_id, director_first_name, director_last_name, profile_url, biography
                FROM directors
                WHERE LOWER(director_first_name || ' ' || director_last_name) LIKE ?
                ORDER BY (profile_url IS NOT NULL) DESC, director_last_name, director_first_name
                """,
                (f"%{q.lower()}%",),
            ).fetchall()
            seen_directors: set[str] = set()
            deduped: list[sqlite3.Row] = []
            for row in results:
                name_key = f"{row['director_first_name']} {row['director_last_name']}".strip().lower()
                if name_key in seen_directors:
                    continue
                seen_directors.add(name_key)
                deduped.append(row)
            results = deduped
            if results:
                target = next(
                    (
                        r
                        for r in results
                        if f"{r['director_first_name']} {r['director_last_name']}".strip().lower() == q.lower()
                    ),
                    results[0],
                )
                history_target_url = url_for("director_detail", director_id=target["director_id"])
                history_target_panel_url = url_for("director_panel", director_id=target["director_id"])
        elif filter_type == "genres":
            results = db.execute(
                """
                SELECT genre_id, genre_name
                FROM genres
                WHERE LOWER(genre_name) LIKE ?
                ORDER BY genre_name
                LIMIT 50
                """,
                (f"%{q.lower()}%",),
            ).fetchall()
            if results:
                target = next(
                    (r for r in results if r["genre_name"].lower() == q.lower()),
                    results[0],
                )
                history_target_url = url_for("genre_detail", genre_id=target["genre_id"])
                history_target_panel_url = None
        else:
            needle = q.lower()
            like_query = f"%{needle}%"
            candidates = db.execute(
                """
                SELECT m.movie_id, m.title, g.genre_id, g.genre_name, m.release_year, m.poster_url
                FROM movies m
                LEFT JOIN genres g ON g.genre_id = m.genre_id
                WHERE LOWER(m.title) LIKE ?
                ORDER BY m.rating DESC, m.release_year DESC
                LIMIT 200
                """,
                (like_query,),
            ).fetchall()
            if not candidates:
                candidates = db.execute(
                    """
                    SELECT m.movie_id, m.title, g.genre_id, g.genre_name, m.release_year, m.poster_url
                    FROM movies m
                    LEFT JOIN genres g ON g.genre_id = m.genre_id
                    ORDER BY m.rating DESC, m.release_year DESC
                    LIMIT 500
                    """
                ).fetchall()

            scored = []
            for row in candidates:
                title = (row["title"] or "").lower()
                ratio = SequenceMatcher(None, needle, title).ratio()
                scored.append((row, ratio))
            scored.sort(key=lambda pair: pair[1], reverse=True)

            # Prefer rows where majority of query words appear in title
            query_words = [word for word in needle.split() if word]
            def word_match(row):
                title = (row["title"] or "").lower()
                return sum(1 for w in query_words if w in title) / max(len(query_words), 1)

            filtered = [
                row for row, ratio in scored
                if ratio >= 0.45 or word_match(row) >= 0.6
            ]
            if not filtered:
                filtered = [row for row, _ in scored][:25]
            results = filtered
            if results:
                target = next((r for r in results if r["title"].lower() == q.lower()), results[0])
                history_target_url = url_for("movie_detail", movie_id=target["movie_id"])
                history_target_panel_url = url_for("movie_panel", movie_id=target["movie_id"])
        db.execute(
            """
            DELETE FROM searchhistory
            WHERE user_id = ? AND LOWER(search_text) = ? AND COALESCE(search_type, 'movies') = ?
            """,
            (session["user_id"], q.lower(), filter_type),
        )
        db.execute(
            """
            INSERT INTO searchhistory (
                user_id, search_text, search_datetime, search_type, target_url, target_panel_url
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                session["user_id"],
                q,
                datetime.utcnow().isoformat(timespec="seconds"),
                filter_type,
                history_target_url,
                history_target_panel_url,
            ),
        )
        db.commit()

    history = db.execute(
        """
        SELECT sh.search_id, sh.search_text, COALESCE(sh.search_type, 'movies') AS search_type, sh.target_url, sh.target_panel_url
        FROM searchhistory sh
        JOIN (
            SELECT
                LOWER(search_text) AS search_key,
                COALESCE(search_type, 'movies') AS search_type_key,
                MAX(search_id) AS latest_search_id
            FROM searchhistory
            WHERE user_id = ?
            GROUP BY LOWER(search_text), COALESCE(search_type, 'movies')
        ) latest ON latest.latest_search_id = sh.search_id
        WHERE sh.user_id = ?
          AND sh.target_url IS NOT NULL
        ORDER BY sh.search_id DESC
        LIMIT 8
        """,
        (session["user_id"], session["user_id"]),
    ).fetchall()

    return render_template(
        "search.html", q=q, filter_type=filter_type, results=results, history=history
    )


if __name__ == "__main__":
    from run import main

    main()
