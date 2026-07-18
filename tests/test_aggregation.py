"""
ProfileIQ — OFFLINE aggregation & rendering tests (no DB, no network).

Locks in the confirmed aggregation rules so a future edit that silently
breaks them fails CI instead of shipping:

  RULE 1  Axis value    = average of the BEST attempt per item.
  RULE 2  Average Score = sum of all 8 axes ÷ 8 (empty axis counts as 0).
  RULE 3  Tab rankings  = best attempt per item, top 5 (courses: all).
  RULE 4  Tab order     = Courses → Assignments → Case Studies → Assessments
                          → Mock Tests → Industry Sessions → Mock Interviews
                          → Capstones → Punctuality → Hackathons → Cohort.

Run:  python -m pytest tests/test_aggregation.py -v
"""
import asyncio
import re

import pytest

from app.services.profile_renderer import (
    ProfileRenderer,
    _compute_perf_snapshot,
    _dedupe_best_rows,
    _combine_assessments,
    _enrich_courses,
    _compute_activity_counts,
)


# ═══════════════════════════════════════════════════════════════════
# Fixtures — synthetic students shaped like real collector output
# ═══════════════════════════════════════════════════════════════════

def _mock_tests_with_retries():
    """TestGen reality: every attempt gets a NEW test_id. Topic 'Banking
    Basics' retried 4× (best 100); AML and KYC single attempts. Grouping
    must be by TOPIC — grouping by test_id would see 6 distinct tests."""
    return (
        [{"test_id": 100 + i, "topic": "Banking Basics", "title": "Banking Basics",
          "score": s, "percentage": s, "total_marks": 100}
         for i, s in enumerate((10, 25, 40, 100))]
        + [{"test_id": 200, "topic": "AML", "title": "AML",
            "score": 30, "percentage": 30, "total_marks": 100}]
        + [{"test_id": 300, "topic": "KYC", "title": "KYC",
            "score": 20, "percentage": 20, "total_marks": 100}]
    )


def _base_student(**overrides):
    capstones = [
        {"capstone_id": 1, "title": "Fraud & AML Pattern Detection",
         "score": 73, "ai_score": 73, "faculty_score": 73, "max_score": 100},
        {"capstone_id": 2, "title": "Digital Lending Workflow", "score": None},
        {"capstone_id": 3, "title": "NeoBank Onboarding", "score": None},
        {"capstone_id": 4, "title": "Coop Bank Modernisation", "score": None},
    ]
    interviews = (
        [{"session_id": f"s{i}", "title": "Business Analyst", "score": sc,
          "duration_min": 30, "duration_minutes": 30,
          "submitted_at": "2026-06-01T10:00:00"}
         for i, sc in enumerate([80, 75, 60])]
        + [{"session_id": f"u{i}", "title": "Ops Round", "score": None}
           for i in range(8)]
    )
    assessments = [
        {"attempt_id": 1, "quiz_id": 7, "title": "Module 1 Quiz",
         "exam_name": "Module 1 Quiz", "score": 60, "percentage": 60,
         "total_marks": 100, "course_name": "BFSI Foundations",
         "passed": True, "submitted_at": "2026-05-01T00:00:00"},
        {"attempt_id": 2, "quiz_id": 7, "title": "Module 1 Quiz",
         "exam_name": "Module 1 Quiz", "score": 90, "percentage": 90,
         "total_marks": 100, "course_name": "BFSI Foundations",
         "passed": True, "submitted_at": "2026-05-02T00:00:00"},
        {"attempt_id": 3, "quiz_id": 8, "title": "Module 2 Quiz",
         "exam_name": "Module 2 Quiz", "score": 50, "percentage": 50,
         "total_marks": 100, "course_name": "BFSI Foundations",
         "passed": False, "submitted_at": "2026-05-05T00:00:00"},
    ]
    courses = [
        {"enrollment_id": 1, "course_id": 11, "course_name": "BFSI Foundations",
         "status": "completed", "completion_status": "completed",
         "progress": 100.0, "progress_percentage": 100.0,
         "enrolled_at": "2026-04-01T00:00:00",
         "completed_at": "2026-06-20T00:00:00", "category": "BFSI"},
        {"enrollment_id": 2, "course_id": 12, "course_name": "Digital Banking",
         "status": "in_progress", "completion_status": "in_progress",
         "progress": 45.0, "progress_percentage": 45.0,
         "enrolled_at": "2026-05-10T00:00:00", "completed_at": "",
         "category": "BFSI"},
    ]
    data = {
        "personal": {"full_name": "Ranjana Kumari", "email": "r@x.com",
                     "city": "Bangalore", "state": "Karnataka", "country": "IN"},
        "courses": courses,
        "assignments": [], "case_studies": [],
        "capstones": capstones, "capstone_projects": capstones,
        "industry_sessions": [], "industry_interactions": [],
        "mock_tests": _mock_tests_with_retries(),
        "mock_interviews": interviews,
        "assessments": assessments, "test_scores": assessments,
        "attendance": {"sessions_attended": 0, "sessions_total": 0,
                       "attendance_percent": None, "recent_sessions": []},
        "punctuality": {"score": None, "band": None,
                        "events_counted": 0, "status": "table_missing"},
        "personality": {}, "certifications": [], "computed": {"user_id": 87},
    }
    data.update(overrides)
    return data


PROFILE_DATA = {
    "headline": "Business Analyst - BFSI",
    "professional_summary": "Test summary.",
    "skills_data": {}, "performance_data": {}, "education_data": [],
    "work_experience": [], "projects_data": [], "certifications_data": [],
    "role_matches": [], "ats_data": {},
}


def _render(student_data):
    return ProfileRenderer().render(student_data, dict(PROFILE_DATA),
                                    "test-slug", "private")


# ═══════════════════════════════════════════════════════════════════
# RULE 1 — best attempt per item
# ═══════════════════════════════════════════════════════════════════

class TestBestAttemptDedup:

    def test_retried_test_collapses_to_best(self):
        out = _dedupe_best_rows(_mock_tests_with_retries(), "topic", "title")
        assert len(out) == 3                   # 3 topics, NOT 6 attempt ids
        assert out[0]["score"] == 100          # best attempt survives
        assert out[0]["attempt_count"] == 4    # retries counted

    def test_testgen_unique_test_ids_still_collapse(self):
        """Regression: TestGen mints a new test_id per attempt — grouping
        by test_id made 32 attempts look like 32 tests (chip bug)."""
        rows = _mock_tests_with_retries()
        assert len({r["test_id"] for r in rows}) == 6  # all ids distinct
        assert len(_dedupe_best_rows(rows, "topic", "title")) == 3

    def test_axis_uses_best_attempts_not_all(self):
        snap = _compute_perf_snapshot(_base_student(), dict(PROFILE_DATA), {})
        axes = {a["key"]: a["score"] for a in snap["axes"]}
        # avg(best 100, 30, 20) = 50 — NOT avg of all 6 attempts (37.5)
        assert axes["mock_test"] == 50
        assert axes["assessment"] == 70        # avg(best 90, 50)
        assert axes["interview"] == 72         # avg(80,75,60); unscored ignored
        assert axes["capstone"] == 73          # only scored capstone

    def test_unscored_rows_sink_not_crash(self):
        rows = [{"session_id": "a", "title": "X", "score": None},
                {"session_id": "b", "title": "Y", "score": 55}]
        out = _dedupe_best_rows(rows, "session_id", "title")
        assert out[0]["score"] == 55 and out[1]["score"] is None

    def test_same_title_different_ids_stay_separate(self):
        rows = [{"quiz_id": 1, "title": "Quiz", "score": 40},
                {"quiz_id": 2, "title": "Quiz", "score": 80}]
        assert len(_dedupe_best_rows(rows, "quiz_id", "title")) == 2


# ═══════════════════════════════════════════════════════════════════
# RULE 2 — strict ÷8 average
# ═══════════════════════════════════════════════════════════════════

class TestDivideByEight:

    def test_empty_axes_count_as_zero(self):
        snap = _compute_perf_snapshot(_base_student(), dict(PROFILE_DATA), {})
        # (0+70+50+0+72+73+0+0)/8 = 33.125 → 33
        assert snap["average"] == 33

    def test_punctuality_included_when_present(self):
        sd = _base_student()
        sd["punctuality"] = {"score": 78, "band": "Reliable",
                             "events_counted": 30, "status": "available"}
        snap = _compute_perf_snapshot(sd, dict(PROFILE_DATA), {})
        assert snap["average"] == round((50 + 72 + 70 + 73 + 78) / 8)  # 43

    def test_all_empty_student_scores_zero(self):
        sd = _base_student(courses=[], mock_tests=[], mock_interviews=[],
                           assessments=[], test_scores=[], capstones=[],
                           capstone_projects=[])
        snap = _compute_perf_snapshot(sd, dict(PROFILE_DATA), {})
        assert snap["average"] == 0

    def test_rings_match_axis_count(self):
        snap = _compute_perf_snapshot(_base_student(), dict(PROFILE_DATA), {})
        assert len(snap["rings"]) == 4
        for ring in snap["rings"]:
            assert len(ring.split(" ")) == 8   # octagon, not heptagon


# ═══════════════════════════════════════════════════════════════════
# RULE 3 — tab content: best 5, all courses, dedup badges
# ═══════════════════════════════════════════════════════════════════

class TestTabContent:

    def test_combined_assessments_dedup(self):
        comb = _combine_assessments(_base_student())
        assert len(comb) == 2
        assert comb[0]["percentage"] == 90 and comb[0]["attempt_count"] == 2

    def test_enriched_courses_expose_template_keys(self):
        courses = _enrich_courses(_base_student())
        assert courses[0]["completion_status"] == "completed"
        assert courses[0]["progress_percentage"] == 100.0
        assert courses[0]["quiz_count"] == 3          # from assessments now

    def test_activity_counts_are_distinct_items(self):
        sd = _base_student()
        comb = _combine_assessments(sd)
        ranked = {
            "capstones": _dedupe_best_rows(sd["capstone_projects"], "capstone_id", "title"),
            "case_studies": [], "assignments": [], "assessments": comb,
            "mock_tests": _dedupe_best_rows(sd["mock_tests"], "topic", "title"),
            "mock_interviews": _dedupe_best_rows(sd["mock_interviews"], "session_id", "title"),
            "industry": [],
        }
        counts = _compute_activity_counts(sd, ranked)
        assert counts["mock_tests"] == 3       # 3 distinct tests, not 6 attempts
        assert counts["assessments"] == 2      # 2 quizzes, not 3 attempts

    def test_rendered_tabs_cap_at_five(self):
        # 9 distinct passing mock tests → panel shows exactly 5 + "+4 more".
        # Scores >= 60 so the v12.8 score floor keeps them all.
        sd = _base_student()
        sd["mock_tests"] = [
            {"test_id": i, "topic": f"Topic {i}", "title": f"Topic {i}",
             "score": 70 + i, "percentage": 70 + i, "total_marks": 100}
            for i in range(9)
        ]
        html = _render(sd)
        panel = re.search(
            r'data-tab-id="mock-test">(?:(?!itab-panel).)*', html, re.S).group(0)
        assert panel.count('class="perf-rank"') == 5
        assert "+ 4 more mock tests" in panel

    def test_attempt_badges_render(self):
        html = _render(_base_student())
        assert "4 attempts · best score" in html   # retried mock test (best 100)
        assert "2 attempts · best score" in html   # retaken quiz (best 90)

    def test_score_floor_hides_below_60(self):
        # v12.8: only work scoring >= 60 appears in Top Performance tabs.
        sd = _base_student()
        sd["mock_tests"] = [
            {"test_id": 1, "topic": "AlphaTest", "title": "AlphaTest",
             "score": 90, "percentage": 90, "total_marks": 100},
            {"test_id": 2, "topic": "BetaTest", "title": "BetaTest",
             "score": 55, "percentage": 55, "total_marks": 100},
            {"test_id": 3, "topic": "GammaTest", "title": "GammaTest",
             "score": 0, "percentage": 0, "total_marks": 100},
        ]
        html = _render(sd)
        panel = re.search(
            r'data-tab-id="mock-test">(?:(?!itab-panel).)*', html, re.S).group(0)
        assert "AlphaTest" in panel          # 90 -> shown
        assert "BetaTest" not in panel       # 55 -> hidden
        assert "GammaTest" not in panel      # 0  -> hidden

    def test_command_bar_has_no_count_numbers(self):
        # User rule: chips read "Enrolled Courses", not "3 Enrolled Courses".
        html = _render(_base_student())
        cbar = re.search(r'<div class="command-bar">.*?</div>\s*</div>', html, re.S)
        assert cbar, "command bar not found"
        assert 'class="cmd-chip-num">' not in cbar.group(0)   # count circles gone
        assert "Enrolled Courses" in cbar.group(0)            # labels stay

    def test_all_courses_shown_uncapped(self):
        sd = _base_student()
        sd["courses"] = [
            {"enrollment_id": i, "course_id": i, "course_name": f"Course {i}",
             "status": "in_progress", "completion_status": "in_progress",
             "progress": 10.0 * (i % 10), "progress_percentage": 10.0 * (i % 10),
             "enrolled_at": "2026-05-01T00:00:00", "completed_at": "",
             "category": "BFSI"}
            for i in range(1, 14)
        ]
        html = _render(sd)
        panel = re.search(
            r'data-tab-id="courses">(?:(?!itab-panel).)*', html, re.S).group(0)
        assert panel.count('class="perf-rank"') == 13


# ═══════════════════════════════════════════════════════════════════
# RULE 4 — locked 11-tab order + Punctuality tab
# ═══════════════════════════════════════════════════════════════════

LOCKED_ORDER = ["courses", "assignments", "casestudies", "assessments",
                "mock-test", "industry", "mock-int", "capstones",
                "punctuality", "hack", "cohort"]


class TestTabOrderAndPunctuality:

    def test_locked_tab_order(self):
        html = _render(_base_student())
        # Buttons only (panels are divs) — and only the perf tab group, since
        # the Work & Learning section has its own itab-bar.
        ids = re.findall(
            r'<button[^>]*data-tab-group="perf" data-tab-id="([a-z-]+)"', html)
        assert ids == LOCKED_ORDER

    def test_punctuality_coming_soon_when_no_data(self):
        html = _render(_base_student())
        assert "Coming Soon" in html
        assert "Reliability in attending live sessions" in html
        assert "Recent behavior weighted more than older activity" in html

    def test_punctuality_score_card_when_available(self):
        sd = _base_student()
        sd["punctuality"] = {"score": 78, "band": "Reliable",
                             "events_counted": 30, "status": "available"}
        html = _render(sd)
        assert '78<span class="punct-score-suffix">/100</span>' in html
        assert "Reliable" in html
        assert "Your Punctuality Score reflects:" in html
        assert "Coming Soon" not in html.split('data-tab-id="punctuality"')[1] \
                                        .split("itab-panel")[0]

    def test_radar_punctuality_vertex_links_to_tab(self):
        html = _render(_base_student())
        assert "goToPerf('punctuality')" in html

    def test_explore_lms_points_at_login(self):
        html = _render(_base_student())
        assert 'href="https://lms.upskillize.com/login"' in html
        assert "Explore LMS" in html


# ═══════════════════════════════════════════════════════════════════
# Collector-side math (same rules, JSON path)
# ═══════════════════════════════════════════════════════════════════

class TestCollectorMath:

    @pytest.fixture()
    def dc(self):
        from app.services.data_collector import DataCollector
        return DataCollector(db=None)

    def test_snapshot_and_metrics(self, dc):
        mock_tests = _mock_tests_with_retries()
        assessments = [{"quiz_id": 7, "title": "M1", "score": 60},
                       {"quiz_id": 7, "title": "M1", "score": 90},
                       {"quiz_id": 8, "title": "M2", "score": 50}]
        capstones = [{"capstone_id": 1, "title": "Fraud", "score": 73},
                     {"capstone_id": 2, "title": "DLW", "score": None}]
        snap = dc._compute_snapshot(
            assignments=[], case_studies=[], capstones=capstones,
            industry_sessions=[], mock_tests=mock_tests, mock_interviews=[],
            assessments=assessments,
            punctuality={"score": None, "status": "table_missing"})
        assert snap["axes"]["mock_tests"] == 50.0
        assert snap["axes"]["assessments"] == 70.0
        assert snap["average"] == round((50 + 70 + 73) / 8, 1)

        courses = [{"course_name": "A", "completion_status": "completed",
                    "progress_percentage": 100.0}]
        m = dc._compute_metrics(87, snap, courses=courses,
                                mock_tests=mock_tests, assessments=assessments,
                                case_studies=[], assignments=[],
                                capstones=capstones, industry_sessions=[],
                                mock_interviews=[])
        assert m["best_test_score"] == 100
        assert m["avg_test_score"] == 50.0
        assert m["total_tests"] == 3
        assert m["completed_courses"] == 1
        assert m["top_subjects"][0] == ("Banking Basics", 100.0)

    def test_empty_payload_constructs(self, dc):
        p = dc._empty_payload(99)
        assert p["performance_snapshot"]["average"] == 0.0
        assert p["punctuality"]["status"] == "no_data"


# ═══════════════════════════════════════════════════════════════════
# Groundedness gate calibration (ai_enhancer)
# Regression for prod 17 Jul 2026: "All tiers failed for user_id=87" —
# the flat 3-word tolerance rejected every normal summary.
# ═══════════════════════════════════════════════════════════════════

class TestGroundednessGate:

    SOURCE = {
        "personal": {"full_name": "Ranjana Kumari",
                     "career_goals": "Business Analyst roles in banking",
                     "key_skills": "Excel, SQL, Communication"},
        "courses": [{"course_name": "BFSI Foundations", "category": "BFSI",
                     "description": "Core banking concepts"},
                    {"course_name": "Digital Banking", "category": "BFSI",
                     "description": "Payments and digital channels"}],
        "mock_interviews": [{"title": "Business Analyst", "role": "Business Analyst",
                             "company": "HDFC", "level": "entry"}],
        "mock_tests": [{"topic": "Banking Basics", "title": "Banking Basics",
                        "band": "Advanced"}],
        "capstones": [{"title": "Fraud & AML Pattern Detection",
                       "course_name": "BFSI Foundations"}],
        "certifications": [], "personality": {},
    }

    def test_realistic_honest_summary_passes(self):
        from app.agents.ai_enhancer import _check_groundedness
        summary = (
            "• Enrolled in BFSI Foundations and Digital Banking on Upskillize.\n"
            "• Scored highest on the Fraud & AML Pattern Detection capstone.\n"
            "• Practiced Business Analyst mock interviews, including an HDFC scenario.\n"
            "• Reached the Advanced band on Banking Basics practice tests.\n"
            "• Targeting Business Analyst roles in banking."
        )
        grounded, invented = _check_groundedness(summary, self.SOURCE)
        assert grounded, f"honest summary rejected; invented={invented}"

    def test_connective_english_never_trips_gate(self):
        """Regression for prod 17 Jul (2nd round): words like 'across',
        'spanning', 'suggests', 'both' were flagged as inventions and every
        tier failed. The v1.2 targeted gate must ignore connective prose."""
        from app.agents.ai_enhancer import _check_groundedness
        summary = (
            "• Progress across both enrolled courses suggests a focus spanning "
            "banking concepts and digital channels, moving toward analyst work.\n"
            "• Performance reflects early-stage learning rather than deep "
            "specialization, though results within case work are strongest.\n"
            "• Targeting Business Analyst roles in banking."
        )
        grounded, invented = _check_groundedness(summary, self.SOURCE)
        assert grounded, f"connective prose rejected; invented={invented}"

    def test_wholesale_invention_still_fails(self):
        from app.agents.ai_enhancer import _check_groundedness
        summary = (
            "• Seasoned Kubernetes architect with Terraform, Golang and AWS expertise.\n"
            "• Led offshore engineering squads delivering microservices platforms.\n"
            "• Certified Scrum practitioner driving agile transformation initiatives.\n"
            "• Machine learning specialist deploying TensorFlow pipelines to production.\n"
            "• Blockchain consultant advising hedge funds on derivatives strategy."
        )
        grounded, invented = _check_groundedness(summary, self.SOURCE)
        assert not grounded, "fabricated summary passed the gate"
        assert any(i.startswith("tech:") for i in invented), invented

    def test_invented_score_fails_small_counts_pass(self):
        from app.agents.ai_enhancer import _check_groundedness
        # "2 of 4" is arithmetic (allowed); "scored 92%" is a fake metric
        ok = "• Completed 2 of 4 enrolled courses on Upskillize."
        grounded, _ = _check_groundedness(ok, self.SOURCE)
        assert grounded
        fake = "• Scored 92% on the Fraud & AML Pattern Detection capstone."
        grounded, invented = _check_groundedness(fake, self.SOURCE)
        assert not grounded and any(i.startswith("number:") for i in invented)

    def test_invented_employer_fails(self):
        from app.agents.ai_enhancer import _check_groundedness
        summary = "• Currently working as an analyst at Goldman Sachs in Mumbai."
        grounded, invented = _check_groundedness(summary, self.SOURCE)
        assert not grounded and any(i.startswith("name:") for i in invented)

    def test_interview_vocab_is_harvested(self):
        from app.agents.ai_enhancer import _extract_source_vocabulary
        vocab = _extract_source_vocabulary(self.SOURCE)
        assert "hdfc" in vocab          # mock_interviews company now indexed
        assert "advanced" in vocab      # mock test band now indexed


# ═══════════════════════════════════════════════════════════════════
# Beyond Work section — personality / hobbies / languages key mapping
# Regression for prod 17 Jul: card locked + "Add hobbies" despite data.
# ═══════════════════════════════════════════════════════════════════

class TestBeyondWork:

    def test_personality_traits_map_from_collector_shape(self):
        from app.agents.profile_orchestrator import ProfileOrchestrator
        merged = {"personality": {
            "personality_type": "Integrity",
            "traits": ["principled", "self-directed", "independent"],
            "strengths": ["accuracy", "consistency"],
            "summary": "Works best with clear rules.",
            "status": "available",
        }}
        out = ProfileOrchestrator._personality(None, merged)
        assert out["personality_type"] == "Integrity"
        assert "principled" in out["traits"]          # card unlocks
        assert out["work_style"]                       # strengths fallback

    def test_hobbies_and_languages_render_from_personal_fields(self):
        sd = _base_student()
        sd["personal"]["hobbies"] = "Cricket, Stock Market Analysis; Reading"
        sd["personal"]["languages_known"] = "English, Hindi, Kannada"
        html = _render(sd)
        assert "Cricket" in html and "Stock Market Analysis" in html
        assert "Add hobbies on your LMS profile" not in html
        assert "English · Hindi · Kannada" in html

    def test_minimal_fallback_no_double_period(self):
        from app.agents.ai_enhancer import AIEnhancer
        enh = AIEnhancer(summary_agent=None)
        out = enh._minimal_fallback({
            "personal": {"career_goals": "To be a Senior Software Engineer in 3 to 5 years."},
            "courses": [{"course_name": "Payments & Cards"}],
        })
        assert ".." not in out


# ═══════════════════════════════════════════════════════════════════
# Summary content rules (template fallback, no API) — v11.1
# Regression: no course tallies, no "attempts", no aggregate score,
# no "interning at Upskillize"; lead with real experience.
# ═══════════════════════════════════════════════════════════════════

class TestSummaryContent:

    def _agent(self):
        import os
        os.environ.pop("ANTHROPIC_API_KEY", None)
        from app.agents.summary_agent import SummaryAgent
        a = SummaryAgent()
        a.has_api = False   # force template fallback (deterministic, no LLM)
        return a

    def _data(self):
        return {
            "personal": {"full_name": "Ranjana Kumari",
                         "current_designation": "Software Developer",
                         "current_employer": "TCS",
                         "career_goals": "Senior Software Engineer in 3 to 5 years",
                         "key_skills": "Python, SQL"},
            "computed": {"overall_score": 24, "best_test_score": 100,
                         "total_quizzes": 11, "completed_courses": 2,
                         "total_courses": 3, "total_case_studies": 0,
                         "consistency_score": 80, "improvement_pct": 18},
            "courses": [{"course_name": "BFSI Foundations"},
                        {"course_name": "Payments & Cards"}],
            "education": [{"degree": "B.Tech", "field_of_study": "CSE",
                           "institution": "NIT"}],
            "work_experience": [{"title": "Software Developer", "company": "TCS"}],
            "case_studies": [], "certifications": [], "personality": {},
            "all_skills": {},
        }

    def test_no_banned_patterns(self):
        agent = self._agent()
        out = asyncio.new_event_loop().run_until_complete(
            agent.generate(self._data())).lower()
        assert "of 3" not in out and "2 of" not in out      # no course tally
        assert "attempt" not in out                          # no attempts
        assert "24%" not in out and "overall score" not in out
        assert "interning at upskillize" not in out
        assert "still in progress" not in out and "consistency" not in out

    def test_leads_with_real_experience(self):
        agent = self._agent()
        out = asyncio.new_event_loop().run_until_complete(
            agent.generate(self._data()))
        first = out.strip().splitlines()[0].lower()
        assert "software developer" in first and "tcs" in first

    def test_grounding_vocab_includes_experience_and_education(self):
        from app.agents.ai_enhancer import _extract_source_vocabulary
        vocab = _extract_source_vocabulary({
            "personal": {}, "education": [{"degree": "B.Tech", "field_of_study": "CSE",
                                           "institution": "NIT Patna"}],
            "work_experience": [{"title": "Backend Developer", "company": "Infosys"}],
            "all_skills": {"technical_skills": [{"name": "Django"}]},
        })
        assert "patna" in vocab and "infosys" in vocab and "django" in vocab


# ═══════════════════════════════════════════════════════════════════
# Routes — response shape (pure function, no DB)
# ═══════════════════════════════════════════════════════════════════

def test_profile_response_includes_student_name():
    from app.api.routes import _profile_response

    class FakeStatus:
        value = "completed"

    class FakeProfile:
        id = 1
        slug = "andrew-carter-74-abc"
        student_name = "Andrew Carter"
        status = FakeStatus()
        visible_to_corporates = True
        share_token = None
        student_id = 74
        total_views = 0
        updated_at = "2026-07-17"

    resp = _profile_response(FakeProfile())
    assert resp["student_name"] == "Andrew Carter"
    assert resp["visible_to_corporates"] is True
    assert resp["share_url"] is None


# ═══════════════════════════════════════════════════════════════════
# v13 — Personalized descriptions + 2-role hero headline (Beyond Work,
# certifications, achievements). Locks the fixes the user requested:
#   • hobby chips title-cased ("watching movie." → "Watching Movies")
#   • same hobby → DIFFERENT line per student (variety seed)
#   • Integrity card never shows the false "locked" message when a
#     personality type exists
#   • hero headline = exactly two roles (BFSI + real profession)
# ═══════════════════════════════════════════════════════════════════

class TestBeyondWorkDescriptions:
    def test_hobby_title_case(self):
        from app.agents.ai_polisher import _title_case_hobby
        assert _title_case_hobby("watching movie.") == "Watching Movies"
        assert _title_case_hobby("reading") == "Reading"
        assert _title_case_hobby("  playing cricket ") == "Playing Cricket"
        assert _title_case_hobby("") == ""

    def test_same_hobby_different_line_per_student(self):
        from app.agents.ai_polisher import _fallback_beyond_work, _variety_seed
        a = _fallback_beyond_work("Ranjana Kumari", ["Reading"], "SDE",
                                  "Integrity", "", _variety_seed("Ranjana Kumari", 52))
        b = _fallback_beyond_work("Amit Sharma", ["Reading"], "Analyst",
                                  "Adaptability", "", _variety_seed("Amit Sharma", 77))
        assert a["hobby_cards"][0]["line"] != b["hobby_cards"][0]["line"]
        assert a["hobby_cards"][0]["line"].strip()

    def test_personality_line_present_when_type_exists(self):
        from app.agents.ai_polisher import _fallback_beyond_work, _variety_seed
        bw = _fallback_beyond_work("Ranjana Kumari", ["Reading"],
                                   "Senior Software Engineer", "Integrity", "",
                                   _variety_seed("Ranjana Kumari", 1))
        # FIRST PERSON — never the candidate's name or third person
        assert bw["personality_line"].strip()
        assert "Ranjana" not in bw["personality_line"]
        assert " her " not in bw["personality_line"] and " she " not in bw["personality_line"].lower()
        assert "my" in bw["personality_line"].lower() or "i " in bw["personality_line"].lower()
        # hobby + goal lines also first person, no name
        assert "Ranjana" not in bw["hobby_cards"][0]["line"]
        assert "Ranjana" not in bw["career_goal_line"]

    def test_cert_and_achievement_fallback_lines(self):
        from app.agents.ai_polisher import _fallback_cert_line, _fallback_achv_line
        assert "Naresh" in _fallback_cert_line("Full Stack Python", "Naresh i Technologies")
        assert _fallback_achv_line("Top Case Study", "Distinction").strip()


class TestTwoRoleHeadline:
    def _orch(self):
        from app.agents.profile_orchestrator import ProfileOrchestrator
        return ProfileOrchestrator.__new__(ProfileOrchestrator)

    def test_fresher_cs_gets_bfsi_plus_tech(self):
        rm = [{"role_title": "Digital Banking Associate", "category": "FinTech", "match_percentage": 82},
              {"role_title": "Full Stack Developer", "category": "Technology", "match_percentage": 70}]
        h = self._orch()._two_role_headline(
            rm, {"employment_type": "student", "field_of_study": "Computer Science"},
            {"education": [{"degree": "B.Tech", "field_of_study": "CS"}]}, "")
        parts = [p.strip() for p in h.split("|")]
        assert len(parts) == 2
        assert "Digital Banking Associate" in parts[0]
        assert "Developer" in parts[1]

    def test_working_professional_uses_designation(self):
        rm = [{"role_title": "Business Analyst - BFSI", "category": "Banking", "match_percentage": 80}]
        h = self._orch()._two_role_headline(
            rm, {"current_designation": "Software Engineer", "employment_type": "Full-time"},
            {"education": []}, "")
        assert "Software Engineer" in h and "|" in h

    def test_always_at_most_two_roles(self):
        h = self._orch()._two_role_headline([], {}, {}, "A | B | C | D")
        assert len(h.split("|")) == 2


class TestDescriptionsRender:
    def _render(self, profile_data, hobbies="Reading, watching movie."):
        from app.services.profile_renderer import ProfileRenderer
        student_data = {"personal": {"full_name": "Ranjana Kumari", "user_id": 52,
                        "hobbies": hobbies, "field_of_study": "Computer Science"},
                        "courses": [], "computed": {}}
        base = {"headline": "Digital Banking Associate | Software Developer",
                "personality_data": {"personality_type": "Integrity", "traits": ""},
                "performance_data": {}, "projects_data": [], "education_data": []}
        base.update(profile_data)
        return ProfileRenderer().render(student_data, base, "t")

    def test_no_false_lock_when_description_present(self):
        html = self._render({"beyond_work": {
            "personality_line": "Assessed as an Integrity profile — principled and self-directed.",
            "hobby_cards": [{"name": "Reading", "line": "Reading sharpens focus."}]}})
        assert "Complete the psychometric test" not in html
        assert "Assessed as an Integrity profile" in html

    def test_hobby_chips_titlecased_in_fallback(self):
        # no beyond_work provided → renderer derives title-cased chips
        html = self._render({})
        assert 'hobby-tag">Watching Movies' in html

    def test_cert_description_renders(self):
        html = self._render({
            "certifications_data": [{"certificate_name": "Full Stack Python", "issued_at": "2025"}],
            "cert_descriptions": {"full stack python": "Validated Python web development training."}})
        assert "Validated Python web development training." in html