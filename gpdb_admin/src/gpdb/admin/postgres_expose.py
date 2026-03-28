"""Helper functions for exposing captive Postgres via TCP."""

from __future__ import annotations

import secrets
import subprocess
from pathlib import Path

from gpdb import GPGraph
from pixeltable_pgserver.utils import POSTGRES_BIN_PATH
from sqlalchemy import text
from sqlalchemy.sql import quoted_name


def generate_postgres_credentials() -> tuple[str, str]:
    """Generate random username and password for Postgres access."""
    username = f"gpdb_user_{secrets.token_hex(4)}"
    password = secrets.token_urlsafe(32)
    return username, password


def configure_pg_hba(pgdata: Path, username: str) -> None:
    """Configure pg_hba.conf to allow the generated user via TCP.

    Appends or ensures an entry for the user with scram-sha-256 auth from 0.0.0.0/0.
    """
    pg_hba_path = pgdata / "pg_hba.conf"

    # Read existing content
    if pg_hba_path.exists():
        content = pg_hba_path.read_text()
    else:
        content = ""

    # Check if entry already exists
    entry = f"host    all             {username}            0.0.0.0/0               scram-sha-256"
    if entry in content:
        return

    # Append the new entry
    with pg_hba_path.open("a") as f:
        f.write(f"\n{entry}\n")


async def create_postgres_user(uri: str, username: str, password: str) -> None:
    """Create a Postgres user with the given credentials.

    Executes CREATE ROLE and ALTER ROLE via SQL.
    """
    db = GPGraph(uri)
    try:
        async with db.sqla_engine.begin() as conn:
            user = quoted_name(username, True)
            escaped_pw = password.replace("'", "''")
            await conn.execute(
                text(f"CREATE ROLE {user} WITH LOGIN PASSWORD '{escaped_pw}'"),
            )
            await conn.execute(text(f"GRANT CONNECT ON DATABASE postgres TO {user}"))
            await conn.execute(text(f"GRANT ALL ON SCHEMA public TO {user}"))
            await conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {user}"))
            await conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {user}"))
    finally:
        await db.sqla_engine.dispose()


def reload_postgres_config(pgdata: Path) -> None:
    """Reload Postgres configuration after pg_hba.conf changes.

    Executes pg_ctl reload using the bundled pg_ctl binary.
    """
    pg_ctl = POSTGRES_BIN_PATH / "pg_ctl"
    subprocess.run(
        [str(pg_ctl), "-D", str(pgdata), "reload"],
        check=True,
        capture_output=True,
    )
