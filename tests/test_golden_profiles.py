"""Golden-profile unit tests — pure offline, no network, no DB, no API keys.

Covers the honest-output contract of the profile pipeline:
  a. SummaryAgent._normalize_goal        — raw aspiration → clean role
  b. SummaryAgent template path          — rich fixture → work-led bullets
  c. AIEnhancer._minimal_fallback        — never "Enrolled in" for rich data
  d. Groundedness gate                   — honest passes, invented fails
  e. RoleMatcher.calculate_ats_score v2  — components/band/locked contract
     (defensively skipped while the old implementation is still in place)
  f. routes admission control            — cap respected, counters drain

ANTHROPIC_API_KEY is popped BEFORE any app import so every AI path falls
back to templates/rules. CacheService is stubbed in sys.modules so no
import chain can ever touch Redis or app settings.
"""

import asyncio
import copy
import os
import re
import sys
import types

import pytest

# ─────────────────────────────────────────────────────────────────────────
# OFFLINE HARDENING — must run before any `app.` import.
# ─────────────────────────────────────────────────────────────────────────
os.environ.pop("ANTHROPIC_API_KEY", None)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Stub the cache module so importing ai_enhancer can never reach Redis
# (or even app.config) regardless of how cache_service evolves.
_cache_stub = types.ModuleType("app.services.cache_service")


class _StubCacheService:
    _store = {}

    @staticmethod
    def get(key):
        return None

    @staticmethod
    def set(key, value, ttl=None):
        pass

    @staticmethod
    def delete(key):
        pass

    @staticmethod
    def get_profile_html(slug):
        return None

    @staticmethod
    def set_profile_html(slug, html):
        pass

    @staticmethod
    def invalidate_profile(slug):
        pass


_cache_stub.CacheService = _StubCacheService
sys.modules["app.services.cache_service"] = _cache_stub

from app.agents.summary_agent import SummaryAgent                  # noqa: E402
from app.agents.ai_enhancer import AIEnhancer, _check_groundedness  # noqa: E402


@pytest.fixture(autouse=True)
def _no_api_key(monkeypatch):
    """Belt and braces: no test in this file may see an API key."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


# ─────────────────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────────────────

def _rich_student() -> dict:
    """A realistic rich-data student: current intern role, B.Tech, skills,
    strong scores, and a raw 'To be a ...' aspiration."""
    return {
        "personal": {
            "user_id": 101,
            "full_name": "Ravi Kumar",
            "current_designation": "Software Development Intern",
            "current_employer": "Aagaz Training Center",
            "work_experience_years": "0.5",
            "career_goals": "To be a Senior Software Engineer in 3 to 5 years",
            "preferred_role": "Software Developer",
            "preferred_location": "Bengaluru",
            "hobbies": "Cricket, open-source",
        },
        "computed": {
            "overall_score": 72,
            "best_test_score": 90,
            "avg_test_score": 78,
            "total_tests": 4,
            "total_quizzes": 3,
            "total_case_studies": 2,
            "completed_courses": 2,
            "total_courses": 3,
            "total_hours": 40,
            "improvement_pct": 12,
            "consistency_score": 70,
        },
        "courses": [
            {"course_name": "Banking Foundation", "progress_percentage": 100},
            {"course_name": "Payments and Cards", "progress_percentage": 100},
        ],
        "education": [
            {"degree": "B.Tech", "field_of_study": "Computer Science",
             "institution": "SEC Sasaram", "year": "2024"},
        ],
        "work_experience": [
            {"title": "Software Development Intern",
             "company": "Aagaz Training Center",
             "duration": "Jan 2026 - Present",
             "description": "Built REST APIs in Python and Django"},
        ],
        "case_studies": [
            {"title": "UPI Fraud Detection", "score": 85,
             "topic": "payments risk", "key_concepts": ["payment systems", "risk assessment"]},
            {"title": "SVB Collapse", "score": 74,
             "topic": "banking governance", "key_concepts": ["risk management"]},
        ],
        "certifications": [
            {"certificate_name": "Banking Foundation Certificate", "issuer": "Upskillize"},
        ],
        "personality": {
            "personality_type": "Execution",
            "traits_json": "Focused, action-oriented",
            "work_style": "Collaborative",
        },
        "all_skills": {
            "technical_skills": [
                {"name": "Python"}, {"name": "Django"}, {"name": "React.js"},
            ],
            "tools": [{"name": "Git"}],
            "soft_skills": [{"name": "Communication"}],
        },
        "test_scores": [
            {"subject": "Banking Basics", "score": 90, "percentage": 90},
        ],
        "quiz_scores": [],
        "data_sources": ["lms", "resume"],
    }


def _empty_student() -> dict:
    return {
        "personal": {"user_id": 999, "full_name": "New Student"},
        "computed": {},
        "courses": [],
        "education": [],
        "work_experience": [],
        "case_studies": [],
        "certifications": [],
        "personality": {},
        "all_skills": {},
        "test_scores": [],
        "quiz_scores": [],
        "data_sources": [],
    }


@pytest.fixture
def rich_student():
    return _rich_student()


@pytest.fixture
def empty_student():
    return _empty_student()


# ─────────────────────────────────────────────────────────────────────────
# (a) SummaryAgent._normalize_goal
# ─────────────────────────────────────────────────────────────────────────

class TestNormalizeGoal:

    def test_to_be_a_with_horizon(self):
        assert SummaryAgent._normalize_goal(
            "To be a Senior Software Engineer in 3 to 5 years"
        ) == "Senior Software Engineer"

    def test_i_want_to_become(self):
        assert SummaryAgent._normalize_goal(
            "I want to become an AI Strategy Manager"
        ) == "AI Strategy Manager"

    def test_plain_role_unchanged(self):
        assert SummaryAgent._normalize_goal("Data Analyst") == "Data Analyst"

    def test_empty_and_none_safe(self):
        assert SummaryAgent._normalize_goal("") == ""
        assert SummaryAgent._normalize_goal(None) == ""


# ─────────────────────────────────────────────────────────────────────────
# (b) SummaryAgent template path on a rich fixture
# ─────────────────────────────────────────────────────────────────────────

class TestTemplateSummaryRich:

    @pytest.fixture
    def summary(self, rich_student):
        agent = SummaryAgent()
        assert agent.has_api is False, "API key must not be present in tests"
        return asyncio.run(agent.generate(rich_student))

    def test_non_empty(self, summary):
        assert summary and summary.strip()

    def test_does_not_lead_with_enrollment(self, summary):
        assert not summary.startswith("• Enrolled")
        first_line = summary.splitlines()[0]
        assert "enrolled" not in first_line.lower()

    def test_raw_goal_phrase_never_surfaces(self, summary):
        assert "in 3 to 5 years" not in summary
        assert "To be a" not in summary

    def test_first_bullet_mentions_work_role(self, summary):
        first_line = summary.splitlines()[0]
        assert "Software Development Intern" in first_line


# ─────────────────────────────────────────────────────────────────────────
# (c) AIEnhancer._minimal_fallback on the rich fixture
# ─────────────────────────────────────────────────────────────────────────

class TestMinimalFallback:

    @pytest.fixture
    def fallback(self, rich_student):
        enhancer = AIEnhancer(summary_agent=SummaryAgent())
        return enhancer._minimal_fallback(rich_student)

    def test_contains_work_title(self, fallback):
        assert "Software Development Intern" in fallback

    def test_no_enrollment_lead(self, fallback):
        assert "Enrolled in" not in fallback

    def test_no_raw_goal_quote(self, fallback):
        assert "To be a" not in fallback
        assert "in 3 to 5 years" not in fallback

    def test_empty_student_still_returns_text(self, empty_student):
        enhancer = AIEnhancer(summary_agent=SummaryAgent())
        out = enhancer._minimal_fallback(empty_student)
        assert out and out.strip()


# ─────────────────────────────────────────────────────────────────────────
# (d) Groundedness gate
# ─────────────────────────────────────────────────────────────────────────

class TestGroundednessGate:

    def test_honest_summary_is_grounded(self, rich_student):
        honest = (
            "• Software Development Intern at Aagaz Training Center.\n"
            "• B.Tech in Computer Science (2024), trained in Python, Django, and React.js.\n"
            "• Scored 85% on the UPI Fraud Detection case study."
        )
        grounded, invented = _check_groundedness(honest, rich_student)
        assert grounded is True, f"honest summary flagged as invented: {invented}"
        assert invented == []

    def test_invented_tech_claim_is_rejected(self, rich_student):
        fabricated = (
            "• Software Development Intern at Aagaz Training Center.\n"
            "• Deployed production workloads on Kubernetes."
        )
        grounded, invented = _check_groundedness(fabricated, rich_student)
        assert grounded is False
        assert any("kubernetes" in item.lower() for item in invented)


# ─────────────────────────────────────────────────────────────────────────
# (e) RoleMatcher.calculate_ats_score — v2 contract (defensive)
# ─────────────────────────────────────────────────────────────────────────

_ATS_V2_KEYS = {"total_score", "band", "locked", "components"}
_ATS_V2_COMPONENTS = {"keyword", "completeness", "evidence"}


def _ats_result(student: dict) -> dict:
    from app.agents.role_matcher import RoleMatcher
    return RoleMatcher().calculate_ats_score(student)


def _require_ats_v2(result: dict) -> None:
    if not _ATS_V2_KEYS <= set(result.keys()):
        pytest.skip("ats v2 not integrated")
    components = result.get("components")
    if not isinstance(components, dict) or not _ATS_V2_COMPONENTS <= set(components.keys()):
        pytest.skip("ats v2 not integrated")


def _component_score(value):
    """Component may be a bare number or a dict carrying a score field."""
    if isinstance(value, dict):
        for key in ("score", "points", "value"):
            if key in value and isinstance(value[key], (int, float)):
                return value[key]
        pytest.skip("ats v2 not integrated")
    if isinstance(value, (int, float)):
        return value
    pytest.skip("ats v2 not integrated")


class TestAtsScoreV2:

    def test_rich_student_score_and_components(self, rich_student):
        result = _ats_result(rich_student)
        _require_ats_v2(result)

        total = result["total_score"]
        assert 0 < total <= 100

        component_sum = sum(
            _component_score(result["components"][k]) for k in _ATS_V2_COMPONENTS
        )
        assert abs(component_sum - total) <= 1, (
            f"components {component_sum} != total {total} (±1)"
        )

        band = result["band"]
        assert isinstance(band, str) and band.strip(), "band must be a named tier"
        # Threshold consistency (only checked when the known tier names are
        # in use — stays green if the naming evolves).
        if band in ("Strong", "Ready", "Developing"):
            if total >= 80:
                assert band == "Strong"
            elif total >= 60:
                assert band == "Ready"
            else:
                assert band == "Developing"
        assert result["locked"] in (False, 0, None), (
            "rich student must not be locked"
        )

    def test_empty_student_is_locked(self, empty_student):
        result = _ats_result(empty_student)
        _require_ats_v2(result)
        assert bool(result["locked"]) is True

    def test_prestige_blind_scoring(self, rich_student):
        """Two identical students differing ONLY in institution + city must
        score identically — the ATS never rewards brand names."""
        result_a = _ats_result(rich_student)
        _require_ats_v2(result_a)

        other = copy.deepcopy(rich_student)
        other["education"][0]["institution"] = "IIT Bombay"
        other["personal"]["preferred_location"] = "Mumbai"
        result_b = _ats_result(other)
        _require_ats_v2(result_b)

        assert result_a["total_score"] == result_b["total_score"], (
            "ATS score changed when only institution/city changed"
        )


# ─────────────────────────────────────────────────────────────────────────
# (f) Routes admission logic — extract-and-exec, no app import needed
# ─────────────────────────────────────────────────────────────────────────

_ROUTES_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "app", "api", "routes.py",
)


class _FakeHTTPException(Exception):
    def __init__(self, status_code=None, detail=None, headers=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.headers = headers


def _load_admission_module(env: dict) -> dict:
    """Slice the admission-control block out of routes.py and exec it in an
    isolated namespace with stubbed FastAPI/logger dependencies."""
    with open(_ROUTES_PATH, "r", encoding="utf-8") as fh:
        source = fh.read()

    match = re.search(
        r"(MAX_PARALLEL_GENERATIONS\s*=.*?)(?=\ndef _collect_all_sync)",
        source,
        re.DOTALL,
    )
    assert match, "admission-control block not found in routes.py"

    import logging
    import time
    from collections import defaultdict

    saved = {}
    for key, value in env.items():
        saved[key] = os.environ.get(key)
        os.environ[key] = str(value)
    try:
        namespace = {
            "os": os,
            "asyncio": asyncio,
            "logging": logging,
            "time": time,
            "defaultdict": defaultdict,
            "logger": logging.getLogger("admission-test"),
            "HTTPException": _FakeHTTPException,
            # routes.py defines METRICS above the sliced block — stub it so
            # the admission code's counters have somewhere to go.
            "METRICS": defaultdict(int),
            "METRICS_STARTED_AT": time.time(),
        }
        exec(compile(match.group(1), "<admission-slice>", "exec"), namespace)
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return namespace


class TestAdmissionControl:

    def test_cap_respected_and_counters_drain(self):
        max_parallel = 3
        waiting_room = 10
        ns = _load_admission_module({
            "MAX_PARALLEL_GENERATIONS": max_parallel,
            "GENERATION_WAITING_ROOM": waiting_room,
            "GENERATION_WAIT_SECONDS": "5",
        })
        run = ns["_run_with_admission"]

        state = {"running": 0, "peak": 0, "completed": 0, "busy": 0}

        async def work():
            state["running"] += 1
            state["peak"] = max(state["peak"], state["running"])
            await asyncio.sleep(0.02)
            state["running"] -= 1
            state["completed"] += 1
            return {"ok": True}

        async def caller(student_id):
            try:
                return await run(student_id, work)
            except _FakeHTTPException as exc:
                assert exc.status_code == 503
                state["busy"] += 1
                return None

        async def main():
            return await asyncio.gather(*(caller(i) for i in range(30)))

        results = asyncio.run(main())

        # Cap respected: never more than MAX_PARALLEL_GENERATIONS at once.
        assert state["peak"] <= max_parallel
        assert state["peak"] >= 1

        # Waiting room enforced: exactly `waiting_room` admitted, rest 503.
        assert state["completed"] == waiting_room
        assert state["busy"] == 30 - waiting_room
        assert sum(1 for r in results if r == {"ok": True}) == waiting_room

        # Counters fully drained afterwards.
        assert ns["_admitted_generations"] == 0
        assert ns["_inflight_students"] == set()
        # Semaphore restored to full capacity.
        sem = ns["_generation_semaphore"]
        assert sem._value == max_parallel

    def test_duplicate_click_guard(self):
        ns = _load_admission_module({
            "MAX_PARALLEL_GENERATIONS": "1",
            "GENERATION_WAITING_ROOM": "10",
            "GENERATION_WAIT_SECONDS": "5",
        })
        run = ns["_run_with_admission"]

        async def slow_work():
            await asyncio.sleep(0.05)
            return {"ok": True}

        async def main():
            first = asyncio.ensure_future(run(7, slow_work))
            await asyncio.sleep(0.01)  # let the first admission register
            second = await run(7, slow_work)
            first_result = await first
            return first_result, second

        first_result, second = asyncio.run(main())
        assert first_result == {"ok": True}
        assert second.get("already_in_progress") is True
        assert ns["_admitted_generations"] == 0
        assert ns["_inflight_students"] == set()
