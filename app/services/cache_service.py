"""
Cache Service — Redis-backed caching for profile HTML.
Falls back gracefully if Redis is not running.
"""

import json
import logging
from typing import Optional, Any
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

try:
    import redis
    _client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    _client.ping()
except Exception:
    _client = None
    logger.warning("Redis not available — caching disabled (this is fine for dev)")


class CacheService:

    @staticmethod
    def get(key: str) -> Optional[Any]:
        if not _client:
            return None
        try:
            val = _client.get(key)
            return json.loads(val) if val else None
        except Exception:
            return None

    @staticmethod
    def set(key: str, value: Any, ttl: int = None):
        if not _client:
            return
        try:
            _client.setex(key, ttl or settings.PROFILE_CACHE_TTL, json.dumps(value, default=str))
        except Exception:
            pass

    @staticmethod
    def delete(key: str):
        if not _client:
            return
        try:
            _client.delete(key)
        except Exception:
            pass

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
