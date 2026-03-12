import shutil
import tempfile
from pathlib import Path

import pytest
from pixeltable_pgserver import PostgresServer


@pytest.fixture(scope="session")
def pg_server():
    """Start a temporary PostgreSQL server for the test session."""
    try:
        pgdata_str = tempfile.mkdtemp()
        pgdata = Path(pgdata_str)
    except OSError:
        pgdata = Path("./.test_pgdata").resolve()
        if pgdata.exists():
            shutil.rmtree(pgdata)
        pgdata.mkdir(parents=True, exist_ok=True)

    server = PostgresServer(pgdata)
    with server:
        yield server

    if pgdata.exists():
        shutil.rmtree(pgdata)

