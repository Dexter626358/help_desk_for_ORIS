"""Ограниченный пул фоновых задач (вместо неограниченных daemon-Thread)."""

from __future__ import annotations

import atexit
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Optional, TypeVar

from ipsas.config.settings import get_settings
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class JobQueueFullError(RuntimeError):
    """Очередь фоновых задач заполнена."""


class BoundedJobExecutor:
    """ThreadPoolExecutor с лимитом одновременных задач + длины очереди."""

    def __init__(self, *, max_workers: int, max_queued: int) -> None:
        self.max_workers = max(1, int(max_workers))
        self.max_queued = max(0, int(max_queued))
        # Слоты = рабочие + ожидающие в очереди
        self._slots = threading.BoundedSemaphore(self.max_workers + self.max_queued)
        self._pool = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="ipsas-job",
        )
        self._closed = False

    def submit(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
        if self._closed:
            raise RuntimeError("Job executor is shut down")
        if not self._slots.acquire(blocking=False):
            raise JobQueueFullError(
                "Слишком много фоновых задач. Подождите и повторите."
            )

        def _wrapped() -> T:
            try:
                return fn(*args, **kwargs)
            finally:
                self._slots.release()

        try:
            return self._pool.submit(_wrapped)
        except Exception:
            self._slots.release()
            raise

    def shutdown(self, wait: bool = False) -> None:
        self._closed = True
        self._pool.shutdown(wait=wait, cancel_futures=True)


_executor: Optional[BoundedJobExecutor] = None
_lock = threading.Lock()


def get_job_executor() -> BoundedJobExecutor:
    global _executor
    with _lock:
        if _executor is None:
            settings = get_settings()
            _executor = BoundedJobExecutor(
                max_workers=settings.max_concurrent_jobs,
                max_queued=settings.max_job_queue,
            )
            atexit.register(_shutdown_executor)
            logger.info(
                "Job executor started: workers=%s queue=%s",
                settings.max_concurrent_jobs,
                settings.max_job_queue,
            )
        return _executor


def reset_job_executor() -> None:
    """Сброс singleton (тесты)."""
    global _executor
    with _lock:
        if _executor is not None:
            _executor.shutdown(wait=False)
            _executor = None


def _shutdown_executor() -> None:
    global _executor
    with _lock:
        if _executor is not None:
            _executor.shutdown(wait=False)
            _executor = None


def submit_background(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
    return get_job_executor().submit(fn, *args, **kwargs)
