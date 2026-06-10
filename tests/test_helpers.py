"""Tests for utility helpers."""

import pytest

from robusta_queue.utils.helpers import (
    backoff_delay,
    determine_shard,
    is_retryable_error,
    validate_payload,
    validate_priority,
)


class TestValidatePayload:
    def test_valid_dict(self):
        payload = {"key": "value"}
        assert validate_payload(payload) == payload

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="Payload must be a dict"):
            validate_payload("not a dict")

    def test_non_serializable(self):
        with pytest.raises(ValueError, match="JSON-serializable"):
            validate_payload({"obj": object()})


class TestValidatePriority:
    def test_valid_priorities(self):
        assert validate_priority(0) == 0
        assert validate_priority(1) == 1
        assert validate_priority(2) == 2

    def test_invalid_priority(self):
        with pytest.raises(ValueError):
            validate_priority(3)
        with pytest.raises(ValueError):
            validate_priority(-1)


class TestBackoffDelay:
    def test_first_attempt(self):
        delay = backoff_delay(1, base_delay=60)
        assert 48 <= delay <= 72  # 60 ±20%

    def test_second_attempt(self):
        delay = backoff_delay(2, base_delay=60)
        assert 96 <= delay <= 144  # 120 ±20%

    def test_third_attempt(self):
        delay = backoff_delay(3, base_delay=60)
        assert 192 <= delay <= 288  # 240 ±20%

    def test_max_delay(self):
        delay = backoff_delay(10, base_delay=60, max_delay=3600)
        assert delay <= 3600


class TestDetermineShard:
    def test_with_pagina_id(self):
        shard = determine_shard(42, 6)
        assert shard == 42 % 6

    def test_consistency(self):
        assert determine_shard(100, 6) == determine_shard(100, 6)

    def test_without_pagina_id(self):
        for _ in range(100):
            shard = determine_shard(None, 6)
            assert 0 <= shard < 6


class TestIsRetryableError:
    def test_retryable_errors(self):
        assert is_retryable_error(TimeoutError("timeout"))
        assert is_retryable_error(Exception("500 Internal Server Error"))
        assert is_retryable_error(Exception("connection refused"))
        assert is_retryable_error(Exception("429 Too Many Requests"))

    def test_permanent_errors(self):
        assert not is_retryable_error(Exception("400 Bad Request"))
        assert not is_retryable_error(Exception("401 Unauthorized"))
        assert not is_retryable_error(Exception("403 Forbidden"))
        assert not is_retryable_error(Exception("404 Not Found"))
