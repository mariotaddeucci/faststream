"""SQLite broker tests."""

import pytest


@pytest.fixture
def queue_name():
    """Test queue name."""
    return "test_queue"
