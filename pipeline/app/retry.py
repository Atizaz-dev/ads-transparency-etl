from __future__ import annotations

import logging
import random
from typing import Callable, TypeVar

import httpx
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from .config import settings

logger = logging.getLogger(__name__)
T = TypeVar("T")


class TransientSourceError(Exception):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _is_transient(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            TransientSourceError,
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
        ),
    )


def _wait_strategy(retry_state: RetryCallState) -> float:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, TransientSourceError) and exc.retry_after is not None:
        return max(exc.retry_after, 0.05) + random.uniform(0.05, 0.25)

    return wait_exponential_jitter(
        initial=settings.http_base_delay_ms / 1000,
        max=settings.http_max_delay_ms / 1000,
    )(retry_state)


def _before_sleep(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "transient source failure; attempt=%s error=%s",
        retry_state.attempt_number,
        exc,
    )


def with_source_retries(fn: Callable[..., T]) -> Callable[..., T]:
    return retry(
        reraise=True,
        stop=stop_after_attempt(settings.http_max_retries),
        wait=_wait_strategy,
        retry=retry_if_exception(_is_transient),
        before_sleep=_before_sleep,
    )(fn)
