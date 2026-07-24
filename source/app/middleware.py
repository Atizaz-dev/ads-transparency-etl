from __future__ import annotations

import asyncio
import random
import threading
import time
from collections import deque

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from .config import settings


class RateLimiter:
    def __init__(self, rate_per_second: float) -> None:
        self.rate = rate_per_second
        self.window = 1.0
        self.hits: deque[float] = deque()
        self.lock = threading.Lock()

    def allow(self) -> tuple[bool, float]:
        now = time.monotonic()
        with self.lock:
            while self.hits and now - self.hits[0] >= self.window:
                self.hits.popleft()
            if len(self.hits) >= self.rate:
                retry_after = self.window - (now - self.hits[0])
                return False, max(retry_after, 0.05)
            self.hits.append(now)
            return True, 0.0


rate_limiter = RateLimiter(settings.rate_limit_rps)
_rng = random.Random()


class SourceBehaviorMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path == "/health":
            return await call_next(request)

        allowed, retry_after = rate_limiter.allow()
        if not allowed:
            return JSONResponse(
                status_code=429,
                content={"detail": "rate limit exceeded"},
                headers={"Retry-After": f"{retry_after:.3f}"},
            )

        if _rng.random() < settings.transient_error_rate:
            if _rng.random() < 0.4:
                await asyncio.sleep(0.35 + _rng.random() * 0.4)
                return JSONResponse(
                    status_code=504,
                    content={"detail": "upstream timeout"},
                )
            return JSONResponse(
                status_code=503,
                content={"detail": "temporary unavailable"},
            )

        return await call_next(request)
