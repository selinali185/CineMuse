from __future__ import annotations

import sqlite3

DB = "cinemuse.db"
KEYWORDS = ["movie", "film", "director", "actor", "festival", "event", "premiere", "screening", "box office"]


def matches_keyword(text: str | None) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(keyword in lower for keyword in KEYWORDS)


def main() -> None:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT news_id, news_name, news_summary FROM news")
    rows = cur.fetchall()
    to_delete = []
    for news_id, title, summary in rows:
        if matches_keyword(title) or matches_keyword(summary):
            continue
        to_delete.append(news_id)
    if to_delete:
        placeholder = ",".join("?" * len(to_delete))
        cur.execute(f"DELETE FROM news WHERE news_id IN ({placeholder})", to_delete)
        conn.commit()
    print(f"Removed {len(to_delete)} irrelevant news articles; remaining={conn.execute('SELECT COUNT(*) FROM news').fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    main()
