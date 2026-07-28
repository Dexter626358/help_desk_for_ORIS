"""Compatibility shim → ``ipsas.jobs.issue_metadata``."""

from ipsas.jobs.issue_metadata import (
    cleanup_expired_tasks,
    task_get,
    task_pop,
    task_set,
    task_ttl_seconds,
)

__all__ = [
    "cleanup_expired_tasks",
    "task_get",
    "task_pop",
    "task_set",
    "task_ttl_seconds",
]
