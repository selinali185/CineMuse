import os
from pathlib import Path


def load_env_file() -> None:
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_run_config() -> tuple[str, int, bool]:
    host = os.environ.get("FLASK_RUN_HOST", "127.0.0.1")
    port = int(os.environ.get("FLASK_RUN_PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "1").lower() in {"1", "true", "yes", "on"}
    return host, port, debug


def main() -> None:
    load_env_file()
    from app import app

    host, port, debug = get_run_config()
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
