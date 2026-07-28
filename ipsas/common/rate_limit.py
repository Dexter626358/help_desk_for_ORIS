"""Простой in-memory rate limit и лимит одновременных POST-операций."""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque


@dataclass
class GuardDecision:
    allowed: bool
    reason: str = ""
    retry_after_s: int = 0


class RequestGuard:
    """Защита от перегрузки публичного UI без внешних зависимостей."""

    def __init__(
        self,
        *,
        max_concurrent: int = 4,
        rate_limit: int = 30,
        rate_window_s: float = 60.0,
    ) -> None:
        self.max_concurrent = max(1, int(max_concurrent))
        self.rate_limit = max(1, int(rate_limit))
        self.rate_window_s = float(rate_window_s)
        self._lock = threading.Lock()
        self._inflight = 0
        self._hits: dict[str, Deque[float]] = defaultdict(deque)

    def try_acquire(self, client_key: str) -> GuardDecision:
        now = time.monotonic()
        with self._lock:
            hits = self._hits[client_key]
            while hits and now - hits[0] > self.rate_window_s:
                hits.popleft()
            if len(hits) >= self.rate_limit:
                retry = int(max(1, self.rate_window_s - (now - hits[0])))
                return GuardDecision(
                    False,
                    reason="Слишком много запросов. Подождите немного и повторите.",
                    retry_after_s=retry,
                )
            if self._inflight >= self.max_concurrent:
                return GuardDecision(
                    False,
                    reason="Сервер занят обработкой других задач. Повторите через минуту.",
                    retry_after_s=30,
                )
            hits.append(now)
            self._inflight += 1
            return GuardDecision(True)

    def release(self) -> None:
        with self._lock:
            if self._inflight > 0:
                self._inflight -= 1


_guard: RequestGuard | None = None
_guard_lock = threading.Lock()


def get_request_guard(
    *,
    max_concurrent: int = 4,
    rate_limit: int = 30,
    rate_window_s: float = 60.0,
) -> RequestGuard:
    global _guard
    with _guard_lock:
        if _guard is None:
            _guard = RequestGuard(
                max_concurrent=max_concurrent,
                rate_limit=rate_limit,
                rate_window_s=rate_window_s,
            )
        return _guard


def reset_request_guard() -> None:
    global _guard
    with _guard_lock:
        _guard = None
