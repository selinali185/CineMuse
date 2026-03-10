from app import app, init_db


def setup_database() -> None:
    """Initialize CineMuse database schema using the app's canonical init_db()."""
    with app.app_context():
        init_db()
    print("Success! 'cinemuse.db' schema is initialized.")


if __name__ == "__main__":
    setup_database()
