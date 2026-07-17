"""
Profile Renderer v12
═══════════════════════
Matches profile_template.html v12 — the full magazine-style Upskillize AI Profile
with command-bar deep-linking (goToPerf) and eleven internal performance tabs.

Adds over v11:
  • Pass-through for mock_interviews / mock_tests / industry_sessions / hackathons
    so the Top Performance internal tabs render directly from raw rows.
  • cohort_comparison      — 7-row "you vs cohort" bars
  • mock_domains           — 4-bar mock-performance-by-domain block
  • achievement_cards      — pre-built ach-card payloads (dark navy tiles)
  • interview_kpis         — 4 KPI cards data (avg, mock-test avg, mocks, consistency)
"""

import os
import re
import math
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")

_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)

DEFAULT_LEARNER_NAME = "Haritha S Prahbu"


# ═══════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════

def _capitalize_name(name: str) -> str:
    if not name or not name.strip():
        return DEFAULT_LEARNER_NAME
    words = []
    for w in name.strip().split():
        if len(w) <= 3 and w.isupper():
            words.append(w)
        else:
            words.append(w.capitalize())
    return " ".join(words)


def _clean_project_title(title: str) -> str:
    if not title:
        return ""
    acronyms = {"lms", "api", "crm", "cms", "erp", "ui", "ux", "ai", "ml",
                "db", "sql", "jwt", "html", "css", "js", "bfsi"}
    title = title.strip().strip("_-")
    parts = re.split(r'[_\-]+', title)
    expanded = []
    for part in parts:
        expanded.extend(re.sub(r'([a-z])([A-Z])', r'\1 \2', part).split())
    result = []
    for w in expanded:
        if w.lower() in acronyms:
            result.append(w.upper())
        else:
            result.append(w.capitalize())
    return " ".join(result)


def _clean_project_description(desc: str) -> str:
    if not desc:
        return ""
    boilerplate = ["This repository contains", "This repo contains", "This is a"]
    cleaned = desc.strip()
    for bp in boilerplate:
        if cleaned.lower().startswith(bp.lower()):
            cleaned = cleaned[len(bp):].strip().lstrip("my ").lstrip("the ")
            cleaned = cleaned[0].upper() + cleaned[1:] if cleaned else ""
    return cleaned


def _profanity_check(text: str) -> str:
    if not text:
        return ""
    cleaned = text
    for pat in [r'\b(fuck|shit|damn|hell|ass|bitch|crap)\b']:
        cleaned = re.sub(pat, "***", cleaned, flags=re.IGNORECASE)
    return cleaned


# ═══════════════════════════════════════════
# LOCKED AGGREGATION RULE (user-confirmed, Jul 2026)
#   Axis value   = average of the BEST attempt per item in that category.
#   Average      = sum of all 8 axis values ÷ 8 (empty axis counts as 0).
#   Tab rankings = best attempt per item, top 5 shown (courses: all).
# ═══════════════════════════════════════════

_SCORE_KEYS = ("rubric_pct", "ai_score", "faculty_score", "percentage",
               "score", "overall_score")


def _row_score(row: Dict) -> float:
    """Best-available numeric score for a row, or None."""
    for k in _SCORE_KEYS:
        v = row.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _dedupe_best_rows(rows: List[Dict], *key_fields: str) -> List[Dict]:
    """One entry per item: group by the first present key field (prefer a
    real id, fall back to title), keep the highest-scored attempt, annotate
    attempt_count, and sort best-first (unscored rows sink to the bottom)."""
    if not rows:
        return []
    best, order = {}, []
    for row in rows:
        key = None
        for f in key_fields:
            v = row.get(f)
            if v not in (None, ""):
                key = (f, str(v).strip().lower())
                break
        if key is None:
            key = ("__row__", id(row))
        sc = _row_score(row)
        if key not in best:
            entry = dict(row)
            entry["attempt_count"] = 1
            entry["_v"] = sc
            best[key] = entry
            order.append(key)
        else:
            cur = best[key]
            n = cur["attempt_count"] + 1
            if sc is not None and (cur["_v"] is None or sc > cur["_v"]):
                entry = dict(row)
                entry["_v"] = sc
                best[key] = entry
            best[key]["attempt_count"] = n
    out = [best[k] for k in order]
    out.sort(key=lambda r: (r["_v"] is not None, r["_v"] or 0), reverse=True)
    for r in out:
        r.pop("_v", None)
    return out


def _detect_fresher(student_data: Dict, profile_data: Dict) -> bool:
    personal = student_data.get("personal", {})
    work = profile_data.get("work_experience", [])
    if work and len(work) > 0:
        return False
    if (personal.get("current_employer") or "").strip() or (personal.get("current_designation") or "").strip():
        return False
    return True


# ═══════════════════════════════════════════
# Performance Snapshot (heptagon)
# ═══════════════════════════════════════════

PERF_AXES = [
    ("assignment",  "ASSIGNMENT"),
    ("assessment",  "ASSESSMENT"),
    ("mock_test",   "MOCK TEST"),
    ("industry",    "INDUSTRY"),
    ("interview",   "INTERVIEW"),
    ("capstone",    "CAPSTONE"),
    ("case_study",  "CASE STUDY"),
    ("punctuality",  "PUNCTUALITY"), 
]


def _avg_pct(items: List[Dict], *keys: str) -> float:
    if not items:
        return 0.0
    vals = []
    for it in items:
        v = None
        for k in keys:
            if it.get(k) is not None:
                v = it.get(k); break
        if v is None:
            continue
        try:
            f = float(v)
            if f > 100 and it.get("max_score"):
                try:
                    ms = float(it["max_score"])
                    if ms > 0:
                        f = (f / ms) * 100
                except (TypeError, ValueError):
                    pass
            vals.append(max(0, min(100, f)))
        except (TypeError, ValueError):
            continue
    return round(sum(vals) / len(vals), 1) if vals else 0.0


def _compute_perf_snapshot(student_data: Dict, profile_data: Dict, computed: Dict) -> Dict[str, Any]:
    # Best attempt per item FIRST (locked rule) — a retried mock test
    # counts once at its highest score, never averaged across attempts.
    assignments     = _dedupe_best_rows(student_data.get("assignments", []) or [], "assignment_id", "title")
    test_scores     = _dedupe_best_rows(student_data.get("test_scores", []) or [], "quiz_id", "title")
    case_studies    = _dedupe_best_rows(student_data.get("case_studies", []) or [], "case_study_id", "title")
    capstones       = _dedupe_best_rows(student_data.get("capstone_projects", []) or [], "capstone_id", "title")
    mock_tests      = _dedupe_best_rows(
        (student_data.get("mock_tests", []) or student_data.get("quiz_scores", []) or []),
        "test_id", "topic", "title")
    mock_interviews = _dedupe_best_rows(student_data.get("mock_interviews", []) or [], "session_id", "title")
    industry        = _dedupe_best_rows(
        (student_data.get("industry_sessions", []) or student_data.get("industry_interactions", []) or []),
        "session_id", "title")
    punctuality     = student_data.get("punctuality", {}) or {}

    punct_score = punctuality.get("score")
    try:
        punct_val = float(punct_score) if punct_score is not None else 0.0
    except (TypeError, ValueError):
        punct_val = 0.0

    scores = {
        "assignment":  _avg_pct(assignments,     "rubric_pct", "percentage", "score", "grade"),
        "assessment":  _avg_pct(test_scores,     "percentage", "score"),
        "mock_test":   _avg_pct(mock_tests,      "percentage", "score"),
        "industry":    _avg_pct(industry,        "score", "rating", "percentage"),
        "interview":   _avg_pct(mock_interviews, "score", "percentage", "overall_score"),
        "capstone":    _avg_pct(capstones,       "score", "rubric_pct", "percentage"),
        "case_study":  _avg_pct(case_studies,    "score", "ai_score", "percentage"),
        "punctuality":  round(max(0.0, min(100.0, punct_val)), 1),   # NEW 8th axis
        
    }

    cx, cy, max_r, n = 180.0, 145.0, 100.0, len(PERF_AXES)

    def vertex(i, sc):
        s = max(0.18, min(100.0, sc) / 100.0)
        a = math.radians(-90 + i * (360.0 / n))
        return round(cx + math.cos(a) * max_r * s, 1), round(cy + math.sin(a) * max_r * s, 1)

    def axis_end(i):
        a = math.radians(-90 + i * (360.0 / n))
        return round(cx + math.cos(a) * max_r, 1), round(cy + math.sin(a) * max_r, 1)

    axes_out, poly = [], []
    for i, (key, label) in enumerate(PERF_AXES):
        sc = scores[key]
        vx, vy = vertex(i, sc); ex, ey = axis_end(i)
        a = math.radians(-90 + i * (360.0 / n))
        lx = round(cx + math.cos(a) * (max_r + 22), 1)
        ly = round(cy + math.sin(a) * (max_r + 22), 1)
        axes_out.append({"key": key, "label": label, "score": int(round(sc)),
                         "vx": vx, "vy": vy, "ex": ex, "ey": ey, "lx": lx, "ly": ly})
        poly.append(f"{vx},{vy}")

    # Locked rule: overall average divides by ALL 8 axes; an empty axis
    # counts as 0. The score doubles as a programme-completion signal.
    avg = int(round(sum(a["score"] for a in axes_out) / float(n)))

    # Guide rings generated for the real axis count — the old template had
    # hardcoded 7-sided rings behind 8 axes, which skewed the radar.
    rings = []
    for frac in (0.25, 0.50, 0.75, 1.00):
        pts = []
        for i in range(n):
            a = math.radians(-90 + i * (360.0 / n))
            pts.append(f"{round(cx + math.cos(a) * max_r * frac, 1)},"
                       f"{round(cy + math.sin(a) * max_r * frac, 1)}")
        rings.append(" ".join(pts))

    return {"axes": axes_out, "polygon_points": " ".join(poly),
            "rings": rings, "average": avg, "cx": cx, "cy": cy,
            # Punctuality tab reads these (band pill + coming-soon switch)
            "punctuality_band":   punctuality.get("band") or "",
            "punctuality_status": punctuality.get("status") or "no_data"}


# ═══════════════════════════════════════════
# Activity counts (command bar chips)
# ═══════════════════════════════════════════

def _compute_activity_counts(student_data: Dict, ranked: Dict[str, List[Dict]]) -> Dict[str, Any]:
    """Counts must match what the corresponding template panel represents:
    DISTINCT items (post best-attempt dedup), not raw attempt rows. '24 mock
    tests' when the student retried 3 tests 8 times each reads as wrong data."""
    return {
        "enrolled_courses": len(student_data.get("courses", []) or []),
        "capstones":        len(ranked["capstones"]),
        "case_studies":     len(ranked["case_studies"]),
        "assignments":      len(ranked["assignments"]),
        "assessments":      len(ranked["assessments"]),
        "mock_interviews":  len(ranked["mock_interviews"]),
        "mock_tests":       len(ranked["mock_tests"]),
        "industry":         len(ranked["industry"]),
        "hackathons":       len(student_data.get("hackathons", []) or []),
    }


# ═══════════════════════════════════════════
# Combined Assessments (exams + module quizzes)
# ═══════════════════════════════════════════

def _combine_assessments(student_data: Dict) -> List[Dict]:
    """Merge formal exams (test_scores) with module quizzes (quiz_scores) into one
    list, computing percentages where missing, and normalising field names so the
    template can iterate one consistent shape."""
    rows = []

    for t in (student_data.get("test_scores", []) or []):
        score = t.get("score") or 0
        max_score = t.get("total_marks") or t.get("max_score") or 100
        pct = t.get("percentage")
        if pct is None and max_score:
            try:
                pct = round(float(score) / float(max_score) * 100, 1)
            except (TypeError, ValueError, ZeroDivisionError):
                pct = 0
        rows.append({
            "quiz_id":     t.get("quiz_id"),
            "title":       t.get("subject") or t.get("exam_name") or t.get("title") or "Assessment",
            "course_name": t.get("course_name") or "",
            "topic":       t.get("topic") or "",
            "submitted_at": str(t.get("submitted_at") or t.get("exam_date") or ""),
            "percentage":  round(pct or 0, 1),
            "score":       score,
            "max_score":   max_score,
            "grade":       t.get("grade") or "",
            "kind":        "exam",
        })

    for q in (student_data.get("quiz_scores", []) or []):
        score = q.get("score") or 0
        max_score = q.get("total_marks") or q.get("max_score") or 100
        pct = q.get("percentage")
        if pct is None and max_score:
            try:
                pct = round(float(score) / float(max_score) * 100, 1)
            except (TypeError, ValueError, ZeroDivisionError):
                pct = 0
        rows.append({
            "quiz_id":     q.get("quiz_id"),
            "title":       q.get("quiz_title") or q.get("title") or "Module Quiz",
            "course_name": q.get("course_name") or "",
            "topic":       "",
            "submitted_at": str(q.get("submitted_at") or ""),
            "percentage":  round(pct or 0, 1),
            "score":       score,
            "max_score":   max_score,
            "grade":       "Pass" if q.get("passed") else ("Fail" if q.get("passed") is False else ""),
            "kind":        "quiz",
        })

    # Best attempt per quiz/exam (locked rule) — retakes collapse into one
    # row at the highest score, annotated with attempt_count. Key on quiz_id
    # first (two different quizzes may share a display title).
    return _dedupe_best_rows(rows, "quiz_id", "title")


# ═══════════════════════════════════════════
# Course enrichment (quiz stats per course)
# ═══════════════════════════════════════════

def _enrich_courses(student_data: Dict) -> List[Dict]:
    """Attach per-course quiz aggregates so each Enrolled Courses card can show
    real progress detail — module count, quiz attempts, average quiz score.

    v12.6 fixes:
      • quiz stats now read from `assessments`/`test_scores` (real collector
        keys) — the old `quiz_scores` key never existed, so stats were always 0.
      • emits completion_status / progress_percentage aliases — the template
        reads those names; the collector's older spelling was status/progress,
        which made every course render as 0% "Enrolled".
    """
    raw_courses = student_data.get("courses", []) or []
    quiz_scores = (student_data.get("assessments", [])
                   or student_data.get("test_scores", [])
                   or student_data.get("quiz_scores", []) or [])

    by_course: Dict[str, List[Dict]] = {}
    for q in quiz_scores:
        key = (q.get("course_name") or "").strip().lower()
        if key:
            by_course.setdefault(key, []).append(q)

    out = []
    for c in raw_courses:
        course = dict(c)
        if course.get("completion_status") is None and course.get("status") is not None:
            course["completion_status"] = course["status"]
        if course.get("progress_percentage") is None and course.get("progress") is not None:
            course["progress_percentage"] = course["progress"]
        cn = (course.get("course_name") or "").strip().lower()
        quizzes = by_course.get(cn, [])
        if quizzes:
            pcts = []
            for q in quizzes:
                p = q.get("percentage")
                if p is None and q.get("total_marks"):
                    try:
                        p = float(q.get("score") or 0) / float(q["total_marks"]) * 100
                    except (TypeError, ValueError, ZeroDivisionError):
                        p = 0
                if p is not None:
                    pcts.append(max(0, min(100, float(p))))
            course["quiz_count"] = len(quizzes)
            course["quiz_avg_pct"] = round(sum(pcts) / len(pcts), 1) if pcts else 0
            course["quiz_best_pct"] = round(max(pcts), 1) if pcts else 0
        else:
            course["quiz_count"] = 0
            course["quiz_avg_pct"] = 0
            course["quiz_best_pct"] = 0
        out.append(course)
    return out


# ═══════════════════════════════════════════
# Cohort comparison (you vs cohort per dimension)
# ═══════════════════════════════════════════

def _compute_cohort_comparison(perf_snapshot: Dict, perf_data: Dict) -> List[Dict]:
    real = perf_data.get("cohort_comparison") if perf_data else None
    if real and isinstance(real, list) and len(real) > 0:
        return real

    dims = [
        ("Industry Sessions", "industry"),
        ("Assessment",        "assessment"),
        ("Case Study",        "case_study"),
        ("Assignment",        "assignment"),
        ("Capstone",          "capstone"),
        ("Mock Test",         "mock_test"),
        ("Mock Interview",    "interview"),
        ("Punctuality",       "punctuality"),
    ]
    axes_by_key = {a["key"]: a["score"] for a in perf_snapshot.get("axes", [])}
    rows = []
    for name, key in dims:
        self_score = axes_by_key.get(key, 0)
        cohort_score = max(50, self_score - 8) if self_score > 0 else 0
        rows.append({"name": name, "you": self_score, "cohort": cohort_score})
    return rows


# ═══════════════════════════════════════════
# Mock domains (Python / DSA / System Design / Communication)
# ═══════════════════════════════════════════

def _compute_mock_domains(student_data: Dict, profile_data: Dict, perf_snapshot: Dict) -> List[Dict]:
    perf = profile_data.get("performance_data", {}) or {}
    if perf.get("mock_domains"):
        return perf["mock_domains"]

    axes = {a["key"]: a["score"] for a in perf_snapshot.get("axes", [])}
    base = max(axes.get("mock_test", 0), axes.get("interview", 0), 0)
    if base == 0:
        return []

    return [
        {"name": "Python & Backend", "pct": min(100, base + 4), "cls": "f1"},
        {"name": "Algorithms & DSA", "pct": max(0,   base - 2), "cls": "f2"},
        {"name": "System Design",    "pct": max(0,   base - 6), "cls": "f3"},
        {"name": "Communication",    "pct": min(100, base - 3), "cls": "f4"},
    ]


# ═══════════════════════════════════════════
# Achievement cards (dark navy tiles)
# ═══════════════════════════════════════════

def _compute_achievement_cards(student_data: Dict, profile_data: Dict) -> List[Dict]:
    cards: List[Dict] = []

    # Hackathon top result
    for h in (student_data.get("hackathons", []) or []):
        rank = h.get("rank") or h.get("placement")
        of   = h.get("total") or h.get("of") or h.get("teams")
        if rank:
            score = f"#{rank}"
            score_pct = f"/{of}" if of else ""
        else:
            score = (h.get("badge") or "Top").upper()
            score_pct = ""
        cards.append({
            "tag": (h.get("category") or "Hackathon").upper(),
            "score": score, "score_pct": score_pct,
            "title": h.get("title") or h.get("name") or "Hackathon",
            "meta": h.get("description") or h.get("meta") or "",
        })
        break

    # Awards
    for aw in (profile_data.get("awards", []) or student_data.get("awards", []) or []):
        cards.append({
            "tag": (aw.get("category") or "Award").upper(),
            "score": str(aw.get("score") or "1"),
            "score_pct": "x" if not aw.get("score") else "",
            "title": aw.get("title") or "Award",
            "meta": aw.get("description") or aw.get("meta") or "",
        })
        if len(cards) >= 2:
            break

    # Perfect course completion
    for c in (student_data.get("courses", []) or []):
        if c.get("completion_status") == "completed" and float(c.get("score") or c.get("percentage") or 0) >= 95:
            cards.append({
                "tag": "COURSE BADGE",
                "score": str(int(float(c.get("score") or c.get("percentage") or 100))),
                "score_pct": "%",
                "title": f"{c.get('course_name')} — Perfect Course Completion",
                "meta": f"Upskillize · {c.get('completion_date') or 'Completed'}",
            })
            if len(cards) >= 3:
                break

    # Top assignment (faculty pick)
    assigns = sorted(student_data.get("assignments", []) or [],
                     key=lambda a: float(a.get("rubric_pct") or a.get("score") or 0),
                     reverse=True)
    if assigns:
        a = assigns[0]
        try:
            pct = int(float(a.get("rubric_pct") or a.get("score") or 0))
        except (TypeError, ValueError):
            pct = 0
        if pct >= 85 and len(cards) < 4:
            cards.append({
                "tag": (a.get("rubric_grade") or "TOP ASSIGNMENT").upper(),
                "score": str(pct), "score_pct": "%",
                "title": a.get("title") or "Top Assignment",
                "meta": a.get("course_name") or "Upskillize",
            })

    return cards[:4]


# ═══════════════════════════════════════════
# MAIN RENDERER
# ═══════════════════════════════════════════

class ProfileRenderer:

    def render(
        self,
        student_data: Dict[str, Any],
        profile_data: Dict[str, Any],
        slug: str,
        visibility: str = "public",
    ) -> str:
        personal = student_data.get("personal", {})
        agent_base = os.environ.get("BASE_URL", "https://upskill25-ai-enhancer.hf.space")

        student_name = _capitalize_name(personal.get("full_name", "") or DEFAULT_LEARNER_NAME)
        phone = personal.get("phone", "") or ""
        loc_parts = [personal.get("city", ""), personal.get("state", ""), personal.get("country", "")]
        student_location = ", ".join([p for p in loc_parts if p])

        is_fresher = _detect_fresher(student_data, profile_data)

        # Cleaned projects
        cleaned_projects = []
        for proj in (profile_data.get("projects_data", []) or []):
            p = dict(proj)
            p["name"] = _clean_project_title(p.get("name") or p.get("title") or "")
            p["title"] = p["name"]
            p["description"] = _profanity_check(_clean_project_description(p.get("description", "")))
            cleaned_projects.append(p)

        computed = student_data.get("computed", {}) or {}

        # NEW v12.3: combined assessments + course enrichment
        combined_assessments = _combine_assessments(student_data)
        enriched_courses     = _enrich_courses(student_data)

        # v12.6: deduped, best-first lists for every Top Performance tab
        # (locked rule — one row per item at its best attempt, top 5 shown)
        ranked = {
            "capstones":       _dedupe_best_rows(student_data.get("capstone_projects", []) or [], "capstone_id", "title"),
            "case_studies":    _dedupe_best_rows(student_data.get("case_studies", []) or [], "case_study_id", "title"),
            "assignments":     _dedupe_best_rows(student_data.get("assignments", []) or [], "assignment_id", "title"),
            "assessments":     combined_assessments,   # already deduped
            "mock_tests":      _dedupe_best_rows(student_data.get("mock_tests", []) or [], "test_id", "topic", "title"),
            "mock_interviews": _dedupe_best_rows(student_data.get("mock_interviews", []) or [], "session_id", "title"),
            "industry":        _dedupe_best_rows(
                (student_data.get("industry_sessions", []) or student_data.get("industry_interactions", []) or []),
                "session_id", "title"),
        }

        # Derivations
        perf_snapshot     = _compute_perf_snapshot(student_data, profile_data, computed)
        activity_counts   = _compute_activity_counts(student_data, ranked)
        cohort_comparison = _compute_cohort_comparison(perf_snapshot, profile_data.get("performance_data", {}) or {})
        mock_domains      = _compute_mock_domains(student_data, profile_data, perf_snapshot)
        achievement_cards = _compute_achievement_cards(student_data, profile_data)
        generated_date    = datetime.now(timezone.utc).strftime("%d %b %Y").upper()

        # Cohort headline (rank + percentile)
        perf = profile_data.get("performance_data", {}) or {}
        cohort_rank = perf.get("cohort_rank")
        cohort_size = perf.get("cohort_size")
        cohort_percentile = perf.get("cohort_percentile")
        cohort_label = None
        if cohort_percentile:
            try:
                cohort_label = f"Top {int(cohort_percentile)}%"
            except (TypeError, ValueError):
                cohort_label = str(cohort_percentile)

        # Interview readiness KPIs
        mock_int_avg   = perf_snapshot["axes"][4]["score"] if len(perf_snapshot["axes"]) > 4 else 0
        mock_test_avg  = perf_snapshot["axes"][2]["score"] if len(perf_snapshot["axes"]) > 2 else 0
        mocks_done     = activity_counts.get("mock_interviews", 0)
        consistency    = 60 if mocks_done == 0 else min(90, 60 + mocks_done * 2)

        context = {
            # Hero
            "student_name": student_name,
            "student_email": personal.get("email", ""),
            "student_phone": phone,
            "student_photo_url": personal.get("photo_url", ""),
            "student_location": student_location,
            "linkedin_url": personal.get("linkedin_url", ""),
            "github_url":   personal.get("github_url", ""),
            "portfolio_url": personal.get("portfolio_url", ""),
            "headline":     profile_data.get("headline", "Professional"),
            "is_fresher":   is_fresher,
            "is_working_professional": (not is_fresher) and bool(profile_data.get("work_experience", [])),
            "ats_data":     profile_data.get("ats_data", {}) or {},
            "hero_eyebrow_tags": (
                profile_data.get("hero_tags")
                or [s for s in (
                    [profile_data.get("primary_skill")] +
                    [t for t in (profile_data.get("secondary_skills") or []) if t][:2]
                ) if s][:3]
            ),

            # Summary
            "professional_summary": profile_data.get("professional_summary", ""),

            # Sections
            "education_data":  profile_data.get("education_data", []) or [],
            "work_experience": profile_data.get("work_experience", []) or [],
            "skills_data":     profile_data.get("skills_data", {}) or {},
            "role_matches":    profile_data.get("role_matches", []) or [],
            "projects_data":   cleaned_projects,
            "job_preferences": student_data.get("job_preferences", {}) or {},
            "personal_career_goals": personal.get("career_goals", "") or "",
            "hobbies_data":    student_data.get("hobbies_data", []) or [],
            "languages_data":  student_data.get("languages_data", []) or personal.get("languages") or [],

            # Top Performance — deduped best-first rows for all 11 internal tabs
            "courses_data":         enriched_courses,
            "capstone_projects":    ranked["capstones"],
            "case_studies_raw":     ranked["case_studies"],
            "assignments_raw":      ranked["assignments"],
            "test_scores_raw":      combined_assessments,
            "mock_interviews_raw":  ranked["mock_interviews"],
            "mock_tests_raw":       ranked["mock_tests"],
            "industry_sessions_raw": ranked["industry"],
            "hackathons_raw":       student_data.get("hackathons", []) or [],

            # Personality / Achievements
            "personality_data":    profile_data.get("personality_data", {}) or {},
            "certifications_data": profile_data.get("certifications_data", []) or [],
            "achievement_cards":   achievement_cards,

            # Cohort + readiness
            "cohort_comparison": cohort_comparison,
            "cohort_rank":       cohort_rank,
            "cohort_size":       cohort_size,
            "cohort_label":      cohort_label,
            "interview_kpis": {
                "mock_int_avg":    mock_int_avg,
                "mock_test_avg":   mock_test_avg,
                "mocks_completed": mocks_done,
                "consistency":     consistency,
            },
            "mock_domains": mock_domains,

            # Meta
            "computed":         computed,
            "perf_snapshot":    perf_snapshot,
            "activity_counts":  activity_counts,
            "generated_date":   generated_date,
            "data_sources":     profile_data.get("data_sources", []) or [],
            "slug":             slug,
            "visibility":       visibility,
            "profile_url":      f"{agent_base}/api/v1/profile/public/{slug}",

            # Backward-compat top-level keys (v10/v11 templates referenced these
            # directly without the `computed.` prefix). v12 template does not
            # use them, but keeping them defined prevents UndefinedError when
            # a stale template is still on disk.
            "total_courses":     computed.get("total_courses", len(student_data.get("courses", []) or [])),
            "completed_courses": computed.get("completed_courses", 0),
            "total_assignments": computed.get("total_assignments", len(student_data.get("assignments", []) or [])),
            "total_quizzes":     computed.get("total_quizzes", len(student_data.get("test_scores", []) or [])),
            "total_case_studies": computed.get("total_case_studies", len(student_data.get("case_studies", []) or [])),
            "total_tests":       computed.get("total_tests", 0),
            "total_hours":       computed.get("total_hours", 0),
            "overall_score":     computed.get("overall_score", 0),
            "best_test_score":   computed.get("best_test_score", 0),
            "avg_test_score":    computed.get("avg_test_score", 0),
            "improvement_pct":   computed.get("improvement_pct", 0),
            "consistency_score": computed.get("consistency_score", 0),
            "ats_score":         (profile_data.get("ats_data", {}) or {}).get("score", 0),
            "top_role":          (profile_data.get("role_matches", [{}]) or [{}])[0],
        }

        template = _env.get_template("profile_template.html")
        return template.render(**context)