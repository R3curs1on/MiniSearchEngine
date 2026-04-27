"""
Shared MySQL connection helpers for the crawler and indexer.
"""

import os
from pathlib import Path
from typing import Any

try:
    import mysql.connector as mysql_connector
except ImportError:  # pragma: no cover - exercised when the DB driver is unavailable
    mysql_connector = None


def load_dotenv() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        os.environ.setdefault(key, value)


load_dotenv()


def get_mysql_config() -> dict:

    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    missing = [name for name, value in {
        "MYSQL_USER": user,
        "MYSQL_PASSWORD": password,
        "MYSQL_DATABASE": database,
    }.items() if not value]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            f"Missing required MySQL configuration: {names}. "
            "Copy .env.example to .env and set real credentials for a dedicated MySQL user."
        )

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }


def get_db() -> Any:
    if mysql_connector is None:
        raise RuntimeError(
            "mysql-connector-python is required for crawler and indexer operations."
        )

    return mysql_connector.connect(
        **get_mysql_config(),
        autocommit=False,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )
