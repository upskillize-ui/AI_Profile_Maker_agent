"""
Profile Renderer — REVISED
═══════════════════════════
Changes:
  - Removed hardcoded "PGCDF", "FinTech Professional", "Cohort 2026"
  - Headline derived from actual enrolled courses
  - Program derived from batch or first course
  - Cohort derived from batch start date
  - Passes student_data to template for course/quiz rendering
"""

import os
import logging
from typing import Dict, Any
from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")

_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)


class ProfileRenderer:
    """Renders student profile data into recruiter-ready HTML."""

    def render(
        self,
        student_data: Dict[str, Any],
        profile_data: Dict[str, Any],
        slug: str,
        visibility: str = "public",
    ) -> str:
        personal = student_data.get("personal", {})
        computed = student_data.get("computed", {})
        courses = student_data.get("courses", [])
        batch = student_data.get("batch_info", {})

        # ── FIXED: Derive headline from REAL courses ──
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]
        if course_names:
            headline = " | ".join(course_names[:2]) + " Learner"
        else:
            headline = "Upskillize Learner"

        # ── FIXED: Derive program from batch or actual course ──
        if batch.get("batch_name"):
            program_name = batch["batch_name"]
        elif course_names:
            program_name = course_names[0]
        else:
            program_name = "Upskillize Program"

        # ── FIXED: Derive cohort from batch date ──
        cohort = ""
        if batch.get("start_date"):
            cohort = str(batch["start_date"])[:4]

        context = {
            # Student info — all from real data
            "student_name": (personal.get("full_name") or "Student").strip(),
            "student_email": personal.get("email", ""),
            "student_photo_url": personal.get("photo_url", ""),
            "student_headline": headline,
            "student_linkedin": personal.get("linkedin_url", ""),
            "student_city": personal.get("city", ""),
            "student_state": personal.get("state", ""),
            "program_name": program_name,
            "cohort": cohort,

            # AI sections
            "summary": profile_data.get("professional_summary", ""),
            "skills": profile_data.get("skills_data", {}),
            "performance": profile_data.get("performance_data", {}),
            "journey": profile_data.get("journey_data", {}),
            "personality": profile_data.get("personality_data", {}),
            "case_studies": profile_data.get("case_studies_data", []),
            "testgen": profile_data.get("testgen_data", {}),
            "projects": profile_data.get("projects_data", []),
            "certifications": profile_data.get("certifications_data", []),
            "ats_keywords": profile_data.get("ats_keywords", []),

            # Quick metrics
            "overall_score": computed.get("overall_score", 0),
            "best_test": computed.get("best_test_score", 0),
            "case_avg": computed.get("avg_case_study_score", 0),
            "improvement": computed.get("improvement_pct", 0),
            "total_hours": computed.get("total_hours", 0),
            "consistency": computed.get("consistency_score", 85),

            # Raw student data for template (courses, quizzes etc.)
            "student_data": student_data,

            # Meta
            "slug": slug,
            "visibility": visibility,
            "profile_url": f"https://upskillize.com/profile/{slug}",
        }

        template = _env.get_template("profile_template.html")
        return template.render(**context)
