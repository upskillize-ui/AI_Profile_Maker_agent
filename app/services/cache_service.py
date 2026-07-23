"""
Cache Service — Redis-backed caching for profile HTML.
Falls back gracefully if Redis is not running.

v2 — LAZY RECONNECT: the old version pinged Redis exactly once at import
time and disabled caching for the life of the process if that ping failed
(e.g. Redis restarting a second after the app booted). Now:

  • No connection attempt at import — first use connects on demand.
  • After a failure, reconnection is re-attempted at most once every
    RECONNECT_INTERVAL seconds (60s) so a down Redis costs one cheap
    timestamp check per call, not a blocking connect per call.
  • Every operation is no-op-safe without Redis and never raises to
    callers. A connection that dies mid-flight is dropped and retried
    on the next eligible call.

Public API is unchanged: get / set / delete / get_profile_html /
set_profile_html / invalidate_profile.
"""

import json
import logging
import time
import threading
from typing import Optional, Any
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Re-attempt a failed Redis connection at most this often (seconds).
RECONNECT_INTERVAL = 60.0

_client = None                 # live redis client, or None
_last_attempt: float = 0.0     # monotonic timestamp of the last FAILED attempt
_lock = threading.Lock()       # guards connect attempts across threads


def _get_client():
    """Return a live Redis client or None. Never raises.

    Lazily connects on first use; after a failure, waits RECONNECT_INTERVAL
    before trying again so a down Redis stays cheap."""
    global _client, _last_attempt

    if _client is not None:
        return _client

    now = time.monotonic()
    if _last_attempt and (now - _last_attempt) < RECONNECT_INTERVAL:
        return None  # failed recently — don't hammer

    with _lock:
        # Re-check under the lock (another thread may have just connected
        # or just failed).
        if _client is not None:
            return _client
        now = time.monotonic()
        if _last_attempt and (now - _last_attempt) < RECONNECT_INTERVAL:
            return None
        try:
            import redis
            client = redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            client.ping()
            _client = client
            _last_attempt = 0.0
            logger.info("Redis connected — caching enabled")
            return _client
        except Exception:
            _client = None
            _last_attempt = time.monotonic()
            logger.warning(
                "Redis not available — caching disabled, will retry in %.0fs "
                "(this is fine for dev)", RECONNECT_INTERVAL,
            )
            return None


def _drop_client():
    """Forget a client that failed mid-operation so the next eligible call
    reconnects instead of reusing a dead socket."""
    global _client, _last_attempt
    _client = None
    _last_attempt = time.monotonic()


class CacheService:

    @staticmethod
    def get(key: str) -> Optional[Any]:
        client = _get_client()
        if not client:
            return None
        try:
            val = client.get(key)
        except Exception:
            _drop_client()
            return None
        try:
            return json.loads(val) if val else None
        except Exception:
            return None  # corrupt entry — not a connection problem

    @staticmethod
    def set(key: str, value: Any, ttl: int = None):
        client = _get_client()
        if not client:
            return
        try:
            client.setex(key, ttl or settings.PROFILE_CACHE_TTL, json.dumps(value, default=str))
        except Exception:
            _drop_client()

    @staticmethod
    def delete(key: str):
        client = _get_client()
        if not client:
            return
        try:
            client.delete(key)
        except Exception:
            _drop_client()

    @staticmethod
    def get_profile_html(slug: str) -> Optional[str]:
        return CacheService.get(f"profile:html:{slug}")

    @staticmethod
    def set_profile_html(slug: str, html: str):
        CacheService.set(f"profile:html:{slug}", html)

    @staticmethod
    def invalidate_profile(slug: str):
        CacheService.delete(f"profile:html:{slug}")
        CacheService.delete(f"profile:data:{slug}")
