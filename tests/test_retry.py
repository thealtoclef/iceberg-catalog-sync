"""Tests for retry decorator logic."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from iceberg_catalog_sync.config import RetryConfig
from iceberg_catalog_sync.retry import _is_retryable, make_retry_decorator


class TestIsRetryable:
    def test_429_in_message(self):
        assert _is_retryable(Exception("HTTP 429 Too Many Requests"))

    def test_503_in_message(self):
        assert _is_retryable(Exception("503 Service Unavailable"))

    def test_status_code_attribute_429(self):
        exc = Exception("rate limited")
        exc.status_code = 429
        assert _is_retryable(exc)

    def test_status_code_attribute_503(self):
        exc = Exception("unavailable")
        exc.status_code = 503
        assert _is_retryable(exc)

    def test_not_retryable(self):
        assert not _is_retryable(Exception("404 Not Found"))

    def test_not_retryable_500(self):
        assert not _is_retryable(Exception("500 Internal Server Error"))


class TestMakeRetryDecorator:
    def test_retries_on_429(self):
        config = RetryConfig(max_attempts=3, base_delay_seconds=0.01, max_delay_seconds=0.1)
        decorator = make_retry_decorator(config)

        call_count = 0

        @decorator
        def flaky_fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("HTTP 429 Too Many Requests")
            return "ok"

        assert flaky_fn() == "ok"
        assert call_count == 3

    def test_gives_up_after_max_attempts(self):
        config = RetryConfig(max_attempts=2, base_delay_seconds=0.01, max_delay_seconds=0.1)
        decorator = make_retry_decorator(config)

        @decorator
        def always_fails():
            raise Exception("HTTP 429")

        with pytest.raises(Exception, match="429"):
            always_fails()

    def test_no_retry_on_non_retryable(self):
        config = RetryConfig(max_attempts=3, base_delay_seconds=0.01, max_delay_seconds=0.1)
        decorator = make_retry_decorator(config)

        call_count = 0

        @decorator
        def not_retryable():
            nonlocal call_count
            call_count += 1
            raise Exception("404 Not Found")

        with pytest.raises(Exception, match="404"):
            not_retryable()
        assert call_count == 1

    def test_retries_counter_incremented(self):
        config = RetryConfig(max_attempts=3, base_delay_seconds=0.01, max_delay_seconds=0.1)
        counter = MagicMock()
        decorator = make_retry_decorator(config, retries_counter=counter, operation="test_op")

        call_count = 0

        @decorator
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("429")
            return "done"

        flaky()
        counter.add.assert_called_with(1, {"operation": "test_op"})
