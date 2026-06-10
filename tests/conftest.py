"""Shared fixtures for Robusta Queue tests."""

import os
import shutil
import tempfile

import pytest


@pytest.fixture
def data_dir():
    """Create a temporary directory for test shard data."""
    tmpdir = tempfile.mkdtemp(prefix="queue_max_test_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def queue(data_dir):
    """Create a Queue instance configured with small shard count."""
    from queue_max import Queue

    q = Queue(
        shards=3,
        rate_limit=1000,  # High limit to avoid throttling in tests
        max_retries=3,
        data_dir=data_dir,
    )
    yield q


@pytest.fixture
def sample_payload():
    """Return a standard test payload."""
    return {"task": "test", "data": "hello"}


@pytest.fixture
def process_function():
    """Return a simple process function for testing."""
    def process(payload):
        return payload.get("data", "ok")
    return process


@pytest.fixture
def failing_process_function():
    """Return a process function that always fails."""
    def process(payload):
        raise ValueError("Simulated failure")
    return process
