"""
ProfileIQ — Load test (Locust) sized for the 2-3k student launch.

Simulates the real launch-day traffic mix on READ paths:
  - students opening their own profile        (/profile/me, JWT auth)
  - recruiters/friends opening share links    (/profile/share/{token})
  - health checks                             (/health)

It deliberately does NOT hammer POST /profile/generate — that endpoint
makes paid LLM calls. Generation capacity is validated separately by the
E2E suite (one force_regenerate per test student).

Setup (PowerShell):
  pip install locust PyJWT
  $env:JWT_SECRET       = "<same as HF Space secret>"
  $env:TEST_STUDENT_IDS = "74,87,94"
  $env:SHARE_TOKENS     = "<comma-separated live share tokens, optional>"

Run (web UI):
  locust -f tests/locustfile.py --host https://upskill25-ai-enhancer.hf.space
  # open http://localhost:8089 → users: 200, spawn rate: 10

Run (headless, CI-style — 200 users, 5 minutes, CSV report):
  locust -f tests/locustfile.py --host https://upskill25-ai-enhancer.hf.space `
         --headless -u 200 -r 10 -t 5m --csv tests/load_report

Interpretation for launch day:
  2-3k students over a day ≈ peak ~200 concurrent readers. Pass criteria:
    p95 latency  < 2s on /profile/me
    p95 latency  < 1.5s on /profile/share
    error rate   < 1%
  If /profile/me p95 blows up: raise pool_size in db_models.get_engine and
  run uvicorn with more workers (see launch report §Scale).
"""
import os
import random
import time

import jwt
from locust import HttpUser, task, between

JWT_SECRET = os.environ.get("JWT_SECRET", "")
STUDENT_IDS = [int(x) for x in
               os.environ.get("TEST_STUDENT_IDS", "74,87,94").split(",") if x.strip()]
SHARE_TOKENS = [t for t in os.environ.get("SHARE_TOKENS", "").split(",") if t.strip()]


def _token(student_id: int) -> str:
    return jwt.encode(
        {"id": student_id, "role": "student",
         "email": f"load{student_id}@upskillize.com",
         "iat": int(time.time()), "exp": int(time.time()) + 7200},
        JWT_SECRET, algorithm="HS256")


class StudentReader(HttpUser):
    """A student who logs in and views their own profile page."""
    weight = 6
    wait_time = between(2, 8)

    def on_start(self):
        sid = random.choice(STUDENT_IDS)
        self.headers = ({"Authorization": f"Bearer {_token(sid)}"}
                        if JWT_SECRET else {})

    @task(8)
    def view_own_profile(self):
        if not self.headers:
            return
        self.client.get("/api/v1/profile/me", headers=self.headers,
                        name="/profile/me")

    @task(1)
    def health(self):
        self.client.get("/health", name="/health")


class ShareLinkVisitor(HttpUser):
    """A recruiter/friend opening a shared profile URL (no auth)."""
    weight = 3
    wait_time = between(1, 5)

    @task
    def view_shared(self):
        if not SHARE_TOKENS:
            return
        tok = random.choice(SHARE_TOKENS)
        self.client.get(f"/api/v1/profile/share/{tok}",
                        name="/profile/share/[token]")


class BadActor(HttpUser):
    """Verifies the security posture holds under load: enumeration attempts
    must keep returning 404/401 quickly, not degrade into 500s."""
    weight = 1
    wait_time = between(3, 10)

    @task(2)
    def probe_dead_public(self):
        with self.client.get("/api/v1/profile/public/guess-slug",
                             name="[dead] /profile/public", catch_response=True) as r:
            r.success() if r.status_code in (404, 405) else r.failure(f"{r.status_code}")

    @task(2)
    def probe_bad_token(self):
        with self.client.get("/api/v1/profile/share/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                             name="[probe] bad share token", catch_response=True) as r:
            r.success() if r.status_code == 404 else r.failure(f"{r.status_code}")

    @task(1)
    def probe_unauth_me(self):
        with self.client.get("/api/v1/profile/me",
                             name="[probe] unauth /me", catch_response=True) as r:
            r.success() if r.status_code == 401 else r.failure(f"{r.status_code}")
