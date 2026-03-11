import sqlite3

DB_PATH = "cinemuse.db"
KEYWORDS = ["movie", "film", "director", "actor", "festival", "event"]


def filter_clause(column: str) -> str:
    return " AND ".join([f"LOWER({column}) NOT LIKE ?" for _ in KEYWORDS])


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM news")
    before = cursor.fetchone()[0]
    clause = " AND ".join(
        [
            f"(LOWER(news_name) NOT LIKE ? AND LOWER(news_summary) NOT LIKE ?)",
        ]
        * len(KEYWORDS)
    )
    params = []
    for kw in KEYWORDS:
        params.extend([f"%{kw}%", f"%{kw}%"])
    sql = f"DELETE FROM news WHERE {clause}"
    cursor.execute(sql, params)
    deleted = cursor.rowcount
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM news")
    after = cursor.fetchone()[0]
    conn.close()
    print(f"Removed {deleted} news rows; before={before}, after={after}")


if __name__ == "__main__":
    main()
