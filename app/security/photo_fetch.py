"""Bounded, allowlisted profile-photo loader for public OG cards.

Only HTTPS hosts explicitly configured in PROFILE_IMAGE_ALLOWED_HOSTS are accepted.
Network-level egress controls are still necessary to defend against DNS rebinding.
"""
from io import BytesIO
import ipaddress
import logging
import os
import socket
from urllib.parse import urlsplit

import httpx
from PIL import Image, UnidentifiedImageError

logger = logging.getLogger(__name__)
MAX_IMAGE_BYTES = 2 * 1024 * 1024
MAX_IMAGE_PIXELS = 8_000_000


def _allowed_host(url: str) -> bool:
    try:
        parsed = urlsplit(url or "")
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username
                or parsed.password or parsed.port not in (None, 443)):
            return False
        host = parsed.hostname.lower().rstrip(".")
        allowed = {
            value.strip().lower().rstrip(".")
            for value in os.getenv("PROFILE_IMAGE_ALLOWED_HOSTS", "").split(",")
            if value.strip()
        }
        return bool(allowed) and host in allowed
    except ValueError:
        return False


def _public_dns_only(host: str) -> bool:
    try:
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        return bool(addresses) and all(
            ipaddress.ip_address(item[4][0]).is_global for item in addresses
        )
    except (OSError, ValueError):
        return False


def fetch_profile_image(url: str):
    """Return a bounded Pillow RGB image or None; never follow redirects."""
    if not _allowed_host(url):
        return None
    host = urlsplit(url).hostname
    if not _public_dns_only(host):
        return None
    try:
        with httpx.Client(
            timeout=httpx.Timeout(5.0, connect=3.0),
            follow_redirects=False,
            trust_env=False,
        ) as client:
            with client.stream("GET", url, headers={"Accept": "image/*"}) as response:
                if response.status_code != 200:
                    return None
                if not response.headers.get("content-type", "").lower().startswith("image/"):
                    return None
                stated = response.headers.get("content-length")
                if stated and int(stated) > MAX_IMAGE_BYTES:
                    return None
                chunks, size = [], 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_IMAGE_BYTES:
                        return None
                    chunks.append(chunk)
        with Image.open(BytesIO(b"".join(chunks))) as img:
            if img.width * img.height > MAX_IMAGE_PIXELS:
                return None
            return img.convert("RGB")
    except (httpx.HTTPError, OSError, ValueError, UnidentifiedImageError,
            Image.DecompressionBombError):
        logger.info("Profile image blocked or unavailable; using initials")
        return None
