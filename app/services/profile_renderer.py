"""
Profile Renderer v10
═══════════════════════
Passes ALL data to the template including:
- LMS course progress, best scores, case studies, assignments, capstone
- Personality from psychometric test
- Job preferences, semester results, certificates
- Data validation: name capitalization, project title cleanup
- Best-score logic: only show highest score per topic
Template variables match the new profile_template.html v10 exactly.
"""

import os
import re
import logging
from typing import Dict, Any, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")

_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)


# ═══════════════════════════════════════════
# DATA VALIDATION HELPERS
# ═══════════════════════════════════════════

def _capitalize_name(name: str) -> str:
    """Fix 'shivani Singh' → 'Shivani Singh', keep acronyms like IIT."""
    if not name:
        return "Student"
    words = []
    for w in name.strip().split():
        if len(w) <= 3 and w.isupper():
            words.append(w)  # Keep acronyms like "IIT", "SEC"
        else:
            words.append(w.capitalize())
    return " ".join(words)


def _clean_project_title(title: str) -> str:
    """Fix ugly GitHub repo names: 'Lms_portal' → 'LMS Portal',
    'django_Practices' → 'Django Practices', 'pyProject' → 'Py Project'."""
    if not title:
        return ""
    # Known acronyms to uppercase
    acronyms = {"lms", "api", "crm", "cms", "erp", "ui", "ux", "ai", "ml", "db", "sql", "jwt", "html", "css", "js"}
    # Remove leading/trailing underscores and hyphens
    title = title.strip().strip("_-")
    # Split on underscores, hyphens, and camelCase
    parts = re.split(r'[_\-]+', title)
    # Further split camelCase: "pyProject" → ["py", "Project"]
    expanded = []
    for part in parts:
        expanded.extend(re.sub(r'([a-z])([A-Z])', r'\1 \2', part).split())
    # Capitalize each word, uppercase known acronyms
    result = []
    for w in expanded:
        if w.lower() in acronyms:
            result.append(w.upper())
        else:
            result.append(w.capitalize())
    return " ".join(result)


def _clean_project_description(desc: str) -> str:
    """Clean up GitHub-copied descriptions."""
    if not desc:
        return ""
    # Remove common GitHub boilerplate
    boilerplate = [
        "This repository contains",
        "This repo contains",
        "This is a",
    ]
    cleaned = desc.strip()
    for bp in boilerplate:
        if cleaned.lower().startswith(bp.lower()):
            cleaned = cleaned[len(bp):].strip().lstrip("my ").lstrip("the ")
            cleaned = cleaned[0].upper() + cleaned[1:] if cleaned else ""
    return cleaned


def _best_scores(scores: List[Dict]) -> List[Dict]:
    """Keep only the BEST score per topic/subject.
    If a student scores 70 then 85 then 60 on 'Banking Foundations',
    only show 85. Groups by (subject or topic or exam_name)."""
    if not scores:
        return []
    best = {}
    for s in scores:
        key = (s.get("subject") or s.get("topic") or s.get("course_name") or "General").strip().lower()
        score_val = s.get("percentage") or s.get("score") or 0
        try:
            score_val = float(score_val)
        except (TypeError, ValueError):
            score_val = 0
        if key not in best or score_val > best[key].get("_score_val", 0):
            entry = dict(s)
            entry["_score_val"] = score_val
            best[key] = entry
    # Sort by score descending, remove internal key
    result = sorted(best.values(), key=lambda x: x.get("_score_val", 0), reverse=True)
    for r in result:
        r.pop("_score_val", None)
    return result


def _best_case_studies(studies: List[Dict], limit: int = 2) -> List[Dict]:
    """Return top N case studies by ai_score."""
    if not studies:
        return []
    sorted_cs = sorted(studies, key=lambda x: float(x.get("score") or x.get("ai_score") or 0), reverse=True)
    return sorted_cs[:limit]


def _best_assignments(assignments: List[Dict], limit: int = 2) -> List[Dict]:
    """Return top N assignments by grade/score."""
    if not assignments:
        return []
    def score_val(a):
        g = a.get("score") or a.get("grade") or "0"
        try:
            return float(g)
        except (TypeError, ValueError):
            return 0
    sorted_a = sorted(assignments, key=score_val, reverse=True)
    return sorted_a[:limit]


def _detect_fresher(student_data: Dict, profile_data: Dict) -> bool:
    """Auto-detect if student is a fresher (no real work experience)."""
    personal = student_data.get("personal", {})
    work = profile_data.get("work_experience", [])
    employer = personal.get("current_employer", "") or ""
    designation = personal.get("current_designation", "") or ""
    if work and len(work) > 0:
        return False
    if employer.strip() or designation.strip():
        return False
    return True


def _profanity_check(text: str) -> str:
    """Basic profanity/unprofessional content filter."""
    if not text:
        return ""
    # Very basic — just flag obviously unprofessional content
    bad_patterns = [
        r'\b(fuck|shit|damn|hell|ass|bitch|crap)\b',
    ]
    cleaned = text
    for pat in bad_patterns:
        cleaned = re.sub(pat, "***", cleaned, flags=re.IGNORECASE)
    return cleaned


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

        # ── Data Validation ──────────────────────────
        student_name = _capitalize_name(personal.get("full_name", "Student"))
        phone = personal.get("phone", "") or ""

        # Location
        loc_parts = [personal.get("city", ""), personal.get("state", ""), personal.get("country", "")]
        student_location = ", ".join([p for p in loc_parts if p])

        # ── Fresher Detection ────────────────────────
        is_fresher = _detect_fresher(student_data, profile_data)

        # ── Best Scores (keep only highest per topic) ──
        raw_test_scores = student_data.get("test_scores", [])
        best_test_scores = _best_scores(raw_test_scores)

        # ── Best Case Studies (top 2) ────────────────
        raw_case_studies = student_data.get("case_studies", [])
        best_cases = _best_case_studies(raw_case_studies, limit=2)

        # ── Best Assignments (top 2) ─────────────────
        raw_assignments = student_data.get("assignments", [])
        best_assigns = _best_assignments(raw_assignments, limit=2)

        # ── Clean Projects ───────────────────────────
        raw_projects = profile_data.get("projects_data", [])
        cleaned_projects = []
        for proj in raw_projects:
            p = dict(proj)
            p["name"] = _clean_project_title(p.get("name") or p.get("title") or "")
            p["title"] = p["name"]
            p["description"] = _profanity_check(
                _clean_project_description(p.get("description", ""))
            )
            cleaned_projects.append(p)

        # ── Capstone (best one) ──────────────────────
        capstone_projects = student_data.get("capstone_projects", [])
        best_capstone = capstone_projects[0] if capstone_projects else None

        # ── Courses with progress ────────────────────
        courses = student_data.get("courses", [])

        # ── Semester results ─────────────────────────
        semester_results = student_data.get("semester_results", [])

        # ── Job preferences ──────────────────────────
        job_preferences = student_data.get("job_preferences", {})

        # ── Certificates ─────────────────────────────
        certifications = profile_data.get("certifications_data", [])

        # ── Personality ──────────────────────────────
        personality_data = profile_data.get("personality_data", {})

        # ── Computed metrics ─────────────────────────
        computed = student_data.get("computed", {})

        context = {
            # ═══ HERO SECTION ═══
            "student_name": student_name,
            "student_email": personal.get("email", ""),
            "student_phone": phone,
            "student_photo_url": personal.get("photo_url", ""),
            "student_location": student_location,
            "linkedin_url": personal.get("linkedin_url", ""),
            "github_url": personal.get("github_url", ""),
            "portfolio_url": personal.get("portfolio_url", ""),
            "headline": profile_data.get("headline", "Professional"),
            "is_fresher": is_fresher,
            "ats_data": profile_data.get("ats_data", {}),

            # ═══ PROFESSIONAL SUMMARY ═══
            "professional_summary": profile_data.get("professional_summary", ""),

            # ═══ LMS COURSE PROGRESS (NEW) ═══
            "courses_data": courses,
            "total_courses": len(courses),
            "completed_courses": sum(1 for c in courses if c.get("completion_status") == "completed"),

            # ═══ ASSESSMENT SCORES — BEST ONLY (NEW) ═══
            "best_test_scores": best_test_scores,
            "total_assessments": computed.get("total_quizzes", 0),

            # ═══ BEST CASE STUDIES (NEW) ═══
            "best_case_studies": best_cases,
            "total_case_studies": computed.get("total_case_studies", 0),

            # ═══ BEST ASSIGNMENTS (NEW) ═══
            "best_assignments": best_assigns,
            "total_assignments": len(raw_assignments),

            # ═══ CAPSTONE PROJECT (NEW) ═══
            "capstone_project": best_capstone,

            # ═══ CERTIFICATES ═══
            "certifications_data": certifications,

            # ═══ SEMESTER / ACADEMIC RESULTS (NEW) ═══
            "semester_results": semester_results,

            # ═══ PERSONALITY & TRAITS (NEW) ═══
            "personality_data": personality_data,

            # ═══ EDUCATION ═══
            "education_data": profile_data.get("education_data", []),

            # ═══ WORK EXPERIENCE ═══
            "work_experience": profile_data.get("work_experience", []),

            # ═══ SKILLS ═══
            "skills_data": profile_data.get("skills_data", {}),

            # ═══ ELIGIBLE JOB ROLES ═══
            "role_matches": profile_data.get("role_matches", []),

            # ═══ PROJECTS (cleaned) ═══
            "projects_data": cleaned_projects,

            # ═══ JOB PREFERENCES (NEW) ═══
            "job_preferences": job_preferences,

            # ═══ GITHUB PROFILE ═══
            "github_profile": profile_data.get("github_profile", {}),

            # ═══ PERFORMANCE METRICS ═══
            "performance_data": profile_data.get("performance_data", {}),

            # ═══ COMPUTED METRICS ═══
            "computed": computed,

            # ═══ ACHIEVEMENTS ═══
            "top_achievements": profile_data.get("top_achievements", []),
            "case_study_highlights": profile_data.get("case_study_highlights", []),
            "test_highlights": profile_data.get("test_highlights", []),
            "assignment_highlights": profile_data.get("assignment_highlights", []),

            # ═══ STATEMENTS ═══
            "growth_statement": profile_data.get("growth_statement", ""),
            "consistency_statement": profile_data.get("consistency_statement", ""),
            "engagement_statement": profile_data.get("engagement_statement", ""),

            # ═══ DATA SOURCES & META ═══
            "data_sources": profile_data.get("data_sources", []),
            "slug": slug,
            "visibility": visibility,
            "profile_url": f"{agent_base}/api/v1/profile/public/{slug}",
        }

        template = _env.get_template("profile_template.html")
        return template.render(**context)