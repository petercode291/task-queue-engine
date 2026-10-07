"""Shared pytest setup: point the engine at a throwaway database before it is imported."""
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

# conftest.py is loaded before any test module, so this runs before task_engine is imported.
TEST_DB = Path(tempfile.mkdtemp(prefix="task_engine_tests_")) / "test_tasks.db"
os.environ["TASK_DB"] = str(TEST_DB)


@pytest.fixture(autouse=True)
def fresh_db() -> Iterator[None]:
    """Every test starts and ends with an empty database."""
    TEST_DB.unlink(missing_ok=True)
    yield
    TEST_DB.unlink(missing_ok=True)
