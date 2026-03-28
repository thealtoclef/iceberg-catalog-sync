"""Tenacity-based retry decorator for rate-limited catalog operations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, TypeVar

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from opentelemetry.metrics import Counter

    from iceberg_catalog_sync.config import RetryConfig

logger = logging.getLogger("iceberg_catalog_sync")

F = TypeVar("F", bound=Callable)


def _is_retryable(exc: BaseException) -> bool:
    """Check if an exception is retryable (HTTP 429 or 503)."""
    exc_str = str(exc)
    status_code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    if status_code in (429, 503):
        return True
    return "429" in exc_str or "503" in exc_str


def make_retry_decorator(
    config: RetryConfig,
    retries_counter: Counter | None = None,
    operation: str = "catalog_operation",
) -> Callable[[F], F]:
    """Create a tenacity retry decorator from config."""

    def before_sleep(retry_state: RetryCallState) -> None:
        attempt = retry_state.attempt_number
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "Retry attempt %d for %s: %s",
            attempt,
            operation,
            exc,
        )
        if retries_counter is not None:
            retries_counter.add(1, {"operation": operation})

    return retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(config.max_attempts),
        wait=wait_exponential(
            multiplier=config.base_delay_seconds,
            max=config.max_delay_seconds,
        ),
        before_sleep=before_sleep,
        reraise=True,
    )
