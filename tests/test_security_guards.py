"""Offline tests for ProfileIQ external image security controls.

No live URL, LMS access, student data or provider API key is required.
"""
from io import BytesIO
import os
from unittest.mock import patch

from PIL import Image

from app.security import photo_fetch


def test_photo_denied_until_allowlist_is_configured():
    with patch.dict(os.environ, {"PROFILE_IMAGE_ALLOWED_HOSTS": ""}):
        assert photo_fetch.fetch_profile_image("https://example.com/a.png") is None


def test_only_exact_https_host_is_accepted():
    with patch.dict(os.environ, {"PROFILE_IMAGE_ALLOWED_HOSTS": "assets.upskillize.com"}):
        assert photo_fetch._allowed_host("https://assets.upskillize.com/a.png")
        for url in (
            "http://assets.upskillize.com/a.png",
            "https://assets.upskillize.com.evil.test/a.png",
            "https://127.0.0.1/a.png",
            "https://user:pass@assets.upskillize.com/a.png",
            "https://assets.upskillize.com:8080/a.png",
        ):
            assert not photo_fetch._allowed_host(url), url


def test_private_dns_address_is_rejected():
    with patch.object(
        photo_fetch.socket, "getaddrinfo",
        return_value=[(2, 1, 6, "", ("127.0.0.1", 443))],
    ):
        assert not photo_fetch._public_dns_only("assets.upskillize.com")


def test_valid_photo_loads_and_oversized_stream_fails():
    class Response:
        def __init__(self, code, headers, chunks):
            self.status_code, self.headers, self.chunks = code, headers, chunks

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def iter_bytes(self):
            yield from self.chunks

    class Client:
        def __init__(self, response, **kwargs):
            self.response = response

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            return self.response

    output = BytesIO()
    Image.new("RGB", (4, 4)).save(output, format="PNG")
    fixtures = (
        (Response(200, {"content-type": "image/png"}, [output.getvalue()]), True),
        (Response(302, {"location": "http://127.0.0.1/"}, []), False),
        (Response(200, {"content-type": "image/png"}, [b"x" * (photo_fetch.MAX_IMAGE_BYTES + 1)]), False),
    )
    for response, should_load in fixtures:
        with (
            patch.dict(os.environ, {"PROFILE_IMAGE_ALLOWED_HOSTS": "assets.upskillize.com"}),
            patch.object(photo_fetch, "_public_dns_only", return_value=True),
            patch.object(photo_fetch.httpx, "Client",
                         side_effect=lambda **kwargs: Client(response, **kwargs)),
        ):
            loaded = photo_fetch.fetch_profile_image("https://assets.upskillize.com/p.png")
            assert (loaded is not None) == should_load
