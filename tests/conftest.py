from unittest.mock import MagicMock

import pytest


@pytest.fixture
def pg_conn():
    """
    Mock Postgres connection supporting `with pg_conn.cursor() as cursor:`.
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    return conn
