"""REMOVED 23 Jul 2026 — dead code.

Celery was never wired to generation (stub task, no worker, no broker on the
Space). Generation concurrency is handled by the admission-control layer in
app/api/routes.py (v5.1). This tombstone exists only because the remote
sync cannot delete files — safe to delete locally.
"""
