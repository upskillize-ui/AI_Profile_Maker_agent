"""
ProfileIQ — LIVE end-to-end tests against the deployed HF Space + real DB.

What it does, per test student:
  1. Mints a student JWT locally from JWT_SECRET (same HS256 the Space uses).
  2. Security sweep: deleted endpoints stay dead, auth is enforced,
     bad share tokens 404, IDOR override is ignored.
  3. Regenerates the profile (force_regenerate=true) through the real API.
  4. Fetches /profile/me and checks structure: locked tab order, Punctuality
     tab, best-5 caps, Explore LMS URL.
  5. GROUND TRUTH: recomputes every snapshot axis straight from the Aiven DB
     using the locked rules (best attempt per item → category average;
     ÷8 overall) and compares against the numbers baked into rendered_html.

Environment (set in PowerShell before running):
  $env:PROFILEIQ_BASE_URL = "https://upskill25-ai-enhancer.hf.space"   # default
  $env:JWT_SECRET         = "<same value as the HF Space secret>"
  $env:DATABASE_URL       = "<same Aiven MySQL URL as the HF Space>"   # for ground-truth checks
  $env:TEST_STUDENT_IDS   = "74,87,94"                                  # default

Run:  python -m pytest tests/test_live_e2e.py -v
Tests that need a missing env var SKIP (they do not fail) — so you can run
the API layer without DATABASE_URL, or skip everything offline.

NOTE: this suite REGENERATES the test students' profiles on production.
That is the point (it exercises the real pipeline) — only run it with the
three designated test students.
"""
import os
import re
import time

import pytest

requests = pytest.importorskip("requests")
jwt = pytest.importorskip("jwt")

BASE = os.environ.get("PROFILEIQ_BASE_URL",
                      "https://upskill25-profile-iq.hf.space").rstrip("/")
API = f"{BASE}/api/v1"
JWT_SECRET = os.environ.get("JWT_SECRET", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# conftest.py sets offline placeholders so the OFFLINE suite can import
# app modules. Those placeholders must never be mistaken for real
# credentials here — treat them as "not set" so live tests SKIP loudly
# instead of failing with 401s / sqlite errors.
if JWT_SECRET == "offline-test-secret":
    JWT_SECRET = ""
if DATABASE_URL.startswith("sqlite"):
    DATABASE_URL = ""
STUDENT_IDS = [int(x) for x in
               os.environ.get("TEST_STUDENT_IDS", "74,87,94").split(",")
               if x.strip()]

needs_jwt = pytest.mark.skipif(not JWT_SECRET, reason="JWT_SECRET not set")
needs_db = pytest.mark.skipif(not (JWT_SECRET and DATABASE_URL),
                              reason="JWT_SECRET and/or DATABASE_URL not set")

LOCKED_ORDER = ["courses", "assignments", "casestudies", "assessments",
                "mock-test", "industry", "mock-int", "capstones",
                "punctuality", "hack", "cohort"]

# Radar axis order as rendered (renderer PERF_AXES) → collector axis keys
AXIS_RENDER_ORDER = ["assignment", "assessment", "mock_test", "industry",
                     "interview", "capstone", "case_study", "punctuality"]


def _token(student_id: int, role: str = "student") -> str:
    return jwt.encode(
        {"id": student_id, "role": role, "email": f"test{student_id}@upskillize.com",
         "iat": int(time.time()), "exp": int(time.time()) + 3600},
        JWT_SECRET, algorithm="HS256")


def _auth(student_id: int) -> dict:
    return {"Authorization": f"Bearer {_token(student_id)}"}


def _get_me(student_id: int) -> dict:
    r = requests.get(f"{API}/profile/me", headers=_auth(student_id), timeout=60)
    assert r.status_code == 200, f"/profile/me → {r.status_code}: {r.text[:300]}"
    return r.json()


# ═══════════════════════════════════════════════════════════════════
# 1. Security sweep (no regeneration needed)
# ═══════════════════════════════════════════════════════════════════

class TestSecurity:

    def test_deleted_public_endpoint_stays_dead(self):
        r = requests.get(f"{API}/profile/public/any-slug-at-all", timeout=30)
        assert r.status_code in (404, 405), r.status_code

    def test_deleted_download_endpoint_stays_dead(self):
        r = requests.get(f"{API}/profile/download/any-slug-at-all", timeout=30)
        assert r.status_code in (404, 405), r.status_code

    def test_generate_requires_auth(self):
        r = requests.post(f"{API}/profile/generate", json={}, timeout=30)
        assert r.status_code == 401, r.status_code

    def test_me_requires_auth(self):
        r = requests.get(f"{API}/profile/me", timeout=30)
        assert r.status_code == 401, r.status_code

    def test_bad_share_token_404(self):
        r = requests.get(f"{API}/profile/share/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                         timeout=30)
        assert r.status_code == 404, r.status_code

    def test_short_share_token_404(self):
        r = requests.get(f"{API}/profile/share/short", timeout=30)
        assert r.status_code == 404, r.status_code

    def test_corporate_endpoint_requires_auth(self):
        r = requests.get(f"{API}/profile/corporate/{STUDENT_IDS[0]}", timeout=30)
        assert r.status_code == 401, r.status_code

    @needs_jwt
    def test_student_token_rejected_on_corporate_endpoint(self):
        # A student JWT must NOT pass the corporate gate (401/403/404 all
        # acceptable — anything but 200 HTML).
        r = requests.get(f"{API}/profile/corporate/{STUDENT_IDS[0]}",
                         headers=_auth(STUDENT_IDS[0]), timeout=30)
        assert r.status_code in (401, 403, 404), r.status_code

    @needs_jwt
    def test_idor_override_is_ignored(self):
        """Caller A sends body.student_id = B → profile generated must be A's."""
        a, b = STUDENT_IDS[0], STUDENT_IDS[1]
        r = requests.post(f"{API}/profile/generate",
                          headers=_auth(a),
                          json={"student_id": b, "force_regenerate": False},
                          timeout=180)
        assert r.status_code == 200, f"{r.status_code}: {r.text[:300]}"
        me = _get_me(a)
        # slug embeds the true student_id: <name>-<id>-<hex>
        assert f"-{a}-" in (me.get("slug") or ""), \
            f"slug {me.get('slug')} does not belong to caller {a}"


# ═══════════════════════════════════════════════════════════════════
# 2. Regeneration + structural checks per test student
# ═══════════════════════════════════════════════════════════════════

@needs_jwt
@pytest.mark.parametrize("student_id", STUDENT_IDS)
class TestGeneratedProfile:

    def test_regenerate_succeeds(self, student_id):
        r = requests.post(f"{API}/profile/generate",
                          headers=_auth(student_id),
                          json={"force_regenerate": True},
                          timeout=300)
        assert r.status_code == 200, f"{r.status_code}: {r.text[:500]}"
        body = r.json()
        assert body.get("status") == "completed", body.get("status")
        assert "student_name" in body

    def test_me_returns_rendered_html(self, student_id):
        me = _get_me(student_id)
        html = me.get("rendered_html") or ""
        assert len(html) > 5000, "rendered_html missing or suspiciously small"

    def test_locked_tab_order(self, student_id):
        html = _get_me(student_id)["rendered_html"]
        # Buttons only (panels are divs) — and only the perf tab group.
        ids = re.findall(
            r'<button[^>]*data-tab-group="perf" data-tab-id="([a-z-]+)"', html)
        assert ids == LOCKED_ORDER, ids

    def test_punctuality_tab_present(self, student_id):
        html = _get_me(student_id)["rendered_html"]
        assert 'data-tab-id="punctuality"' in html
        assert ("Coming Soon" in html) or ("Your Punctuality Score reflects" in html)

    def test_best5_cap_on_every_ranked_tab(self, student_id):
        html = _get_me(student_id)["rendered_html"]
        for tab in ("capstones", "casestudies", "assignments", "assessments",
                    "mock-int", "mock-test", "industry", "hack"):
            m = re.search(
                rf'itab-panel" data-tab-group="perf" data-tab-id="{tab}">'
                r'(?:(?!itab-panel).)*', html, re.S)
            if not m:
                continue
            n = m.group(0).count('class="perf-rank"')
            assert n <= 5, f"tab {tab} shows {n} rows (> 5)"

    def test_explore_lms_login_url(self, student_id):
        html = _get_me(student_id)["rendered_html"]
        assert 'https://lms.upskillize.com/login' in html

    def test_overall_average_is_strict_div8(self, student_id):
        """Average in the radar footer == sum(8 axis numbers) ÷ 8."""
        html = _get_me(student_id)["rendered_html"]
        axis_scores = [int(x) for x in
                       re.findall(r'class="dna-label-num">(\d+)</text>', html)]
        assert len(axis_scores) == 8, f"expected 8 axes, got {len(axis_scores)}"
        footer = re.search(r'dna-foot-val">(\d+)</span>', html)
        assert footer, "average footer not found"
        expected = round(sum(axis_scores) / 8)
        got = int(footer.group(1))
        assert abs(got - expected) <= 1, \
            f"average {got} != /8 of axes {axis_scores} (expected ~{expected})"


# ═══════════════════════════════════════════════════════════════════
# 3. Ground truth — recompute axes from the DB and compare
# ═══════════════════════════════════════════════════════════════════

@needs_db
@pytest.mark.parametrize("student_id", STUDENT_IDS)
class TestGroundTruth:
    """Recomputes expected axis values straight from MySQL with the locked
    rules and compares them to the numbers rendered in the profile."""

    @pytest.fixture()
    def db(self):
        from sqlalchemy import create_engine
        url = DATABASE_URL
        if url.startswith("mysql://"):
            url = "mysql+pymysql://" + url[len("mysql://"):]
        engine = create_engine(url, pool_pre_ping=True)
        conn = engine.connect()
        yield conn
        conn.close()
        engine.dispose()

    @staticmethod
    def _scalar(db, sql, **params):
        from sqlalchemy import text
        row = db.execute(text(sql), params).first()
        return None if row is None or row[0] is None else float(row[0])

    def _expected_axes(self, db, student_id):
        """Locked rule in SQL: best attempt per item, then AVG per category."""
        ax = {}
        # Group by TOPIC — TestGen mints a new test_id per attempt, so
        # test_id can never be the grouping key.
        ax["mock_test"] = self._scalar(db, """
            SELECT AVG(best) FROM (
                SELECT MAX(score_percentage) AS best FROM test_history
                WHERE student_id = :uid
                GROUP BY topic
            ) t""", uid=str(student_id))
        ax["assessment"] = self._scalar(db, """
            SELECT AVG(best) FROM (
                SELECT MAX(score / NULLIF(total_marks,0) * 100) AS best
                FROM quiz_attempts WHERE student_id = :uid GROUP BY quiz_id
            ) t""", uid=student_id)
        ax["assignment"] = self._scalar(db, """
            SELECT AVG(best) FROM (
                SELECT MAX(grade) AS best FROM assignment_submissions
                WHERE student_id = :uid AND grade IS NOT NULL
                GROUP BY assignment_id
            ) t""", uid=student_id)
        ax["case_study"] = self._scalar(db, """
            SELECT AVG(best) FROM (
                SELECT MAX(grade) AS best FROM case_study_submissions
                WHERE student_id = :uid AND grade IS NOT NULL
                GROUP BY case_study_id
            ) t""", uid=student_id)
        ax["capstone"] = self._scalar(db, """
            SELECT AVG(best) FROM (
                SELECT MAX(grade / NULLIF(COALESCE(total_marks,100),0) * 100) AS best
                FROM capstones
                WHERE student_id = :uid AND grade IS NOT NULL
                GROUP BY id
            ) t""", uid=student_id)
        ax["industry"] = self._scalar(db, """
            SELECT AVG(best) FROM (
                SELECT MAX(score) AS best FROM industry_session_submissions
                WHERE student_id = :uid AND score IS NOT NULL
                GROUP BY session_id
            ) t""", uid=student_id)
        return ax

    def test_rendered_axes_match_db(self, db, student_id):
        me = _get_me(student_id)
        html = me["rendered_html"]
        rendered = [int(x) for x in
                    re.findall(r'class="dna-label-num">(\d+)</text>', html)]
        assert len(rendered) == 8
        rendered_by_key = dict(zip(AXIS_RENDER_ORDER, rendered))

        expected = self._expected_axes(db, student_id)
        mismatches = []
        for key, exp in expected.items():
            got = rendered_by_key[key]
            exp_r = 0 if exp is None else round(exp)
            if abs(got - exp_r) > 1:   # ±1 for int rounding
                mismatches.append(f"{key}: rendered {got}, DB says {exp_r}")
        assert not mismatches, "; ".join(mismatches)

    def test_mock_test_axis_beats_naive_average(self, db, student_id):
        """Regression for the original '24 vs best 100' bug: the axis must
        never be BELOW the all-attempts average when retries exist."""
        best_avg = self._expected_axes(db, student_id)["mock_test"]
        naive = self._scalar(db, """
            SELECT AVG(score_percentage) FROM test_history
            WHERE student_id = :uid""", uid=str(student_id))
        if best_avg is None or naive is None:
            pytest.skip("student has no mock tests")
        assert best_avg >= naive - 0.01


# ═══════════════════════════════════════════════════════════════════
# 4. Share-link lifecycle (create → fetch → revoke → dead)
# ═══════════════════════════════════════════════════════════════════

@needs_jwt
class TestShareLifecycle:

    def test_full_cycle(self):
        sid = STUDENT_IDS[0]
        r = requests.post(f"{API}/profile/share/create",
                          headers=_auth(sid), timeout=60)
        assert r.status_code == 200, r.text[:300]
        url = r.json()["share_url"]
        assert "/profile/share/" in url

        pub = requests.get(url, timeout=60)
        assert pub.status_code == 200 and "<html" in pub.text.lower()

        rv = requests.post(f"{API}/profile/share/revoke",
                           headers=_auth(sid), timeout=60)
        assert rv.status_code == 200

        dead = requests.get(url, timeout=60)
        assert dead.status_code == 404, "revoked share URL must be dead"
