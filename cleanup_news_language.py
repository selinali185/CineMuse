from __future__ import annotations

import sqlite3

DB_PATH = "cinemuse.db"


def is_ascii_text(text: str | None) -> bool:
    if not text:
        return False
    total = 0
    ascii_count = 0
    for ch in text:
        if ch.isalnum() or ch.isspace() or ch in ".,;:'\"?!-()":
            ascii_count += 1
        total += 1
    if total == 0:
        return False
    return (ascii_count / total) >= 0.85


def delete_non_english() -> None:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT news_id, news_name, news_summary FROM news")
    rows = cursor.fetchall()
    to_delete = [
        row[0]
        for row in rows
        if not (is_ascii_text(row[1]) or is_ascii_text(row[2]))
    ]
    if to_delete:
        placeholder = ",".join("?" * len(to_delete))
        cursor.execute(f"DELETE FROM news WHERE news_id IN ({placeholder})", to_delete)
        conn.commit()
    conn.close()
    print(f"Removed {len(to_delete)} non-English articles.")


if __name__ == "__main__":
    delete_non_english()
