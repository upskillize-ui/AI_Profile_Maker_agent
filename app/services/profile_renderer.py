"""
Profile Renderer v9
═══════════════════
Passes ALL data sources to the template:
- LMS data (courses, scores, case studies, capstone, semester results)
- Resume data (education, work experience, skills)
- GitHub data
- LinkedIn data
- Psychometric data (personality, traits, work style)
- Job preferences
- Auto-detects fresher vs working professional
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

    def render(
        self,
        student_data: Dict[str, Any],
        profile_data: Dict[str, Any],
        slug: str,
        visibility: str = "public",
    ) -> str:
        personal = student_data.get("personal", {})
        agent_base = os.environ.get("BASE_URL", "https://upskill25-ai-enhancer.hf.space")

        # Build location string from city/state/country
        loc_parts = [personal.get("city", ""), personal.get("state", ""), personal.get("country", "")]
        student_location = ", ".join([p for p in loc_parts if p])

        # ── Detect fresher vs working professional ──
        work_experience = profile_data.get("work_experience", [])
        work_years = personal.get("work_experience_years", "") or ""
        current_employer = personal.get("current_employer", "") or ""

        is_working_professional = bool(
            work_experience or current_employer or
            (work_years and str(work_years) not in ("0", "", "fresher", "Fresher"))
        )
        is_fresher = not is_working_professional

        context = {
            # ── Student basic info ──
            "student_name": (personal.get("full_name") or "Student").strip(),
            "student_email": personal.get("email", ""),
            "student_phone": personal.get("phone", ""),
            "student_photo_url": personal.get("photo_url", ""),
            "student_location": student_location,
            "linkedin_url": personal.get("linkedin_url", ""),
            "github_url": personal.get("github_url", ""),
            "portfolio_url": personal.get("portfolio_url", ""),

            # ── Career stage detection ──
            "is_fresher": is_fresher,
            "is_working_professional": is_working_professional,

            # ── Headline & summary ──
            "headline": profile_data.get("headline", "Professional"),
            "professional_summary": profile_data.get("professional_summary", ""),

            # ── Skills (merged from all sources) ──
            "skills_data": profile_data.get("skills_data", {}),

            # ── Performance & metrics ──
            "performance_data": profile_data.get("performance_data", {}),
            "top_achievements": profile_data.get("top_achievements", []),

            # ── Case studies & tests ──
            "case_study_highlights": profile_data.get("case_study_highlights", []),
            "test_highlights": profile_data.get("test_highlights", []),

            # ── Job role matches & ATS ──
            "role_matches": profile_data.get("role_matches", []),
            "ats_data": profile_data.get("ats_data", {}),

            # ── Personality (from psychometric) ──
            "personality_data": profile_data.get("personality_data", {}),

            # ── Optional growth statements ──
            "growth_statement": profile_data.get("growth_statement", ""),
            "consistency_statement": profile_data.get("consistency_statement", ""),
            "engagement_statement": profile_data.get("engagement_statement", ""),

            # ── Education & Work Experience ──
            "education_data": profile_data.get("education_data", []),
            "work_experience": work_experience,

            # ── Projects ──
            "projects_data": profile_data.get("projects_data", []),

            # ── GitHub profile ──
            "github_profile": profile_data.get("github_profile", {}),

            # ── Certifications ──
            "certifications_data": profile_data.get("certifications_data", []),

            # ── NEW v9: Capstone projects ──
            "capstone_projects": student_data.get("capstone_projects", []),

            # ── NEW v9: Semester / final results ──
            "semester_results": student_data.get("semester_results", []),

            # ── NEW v9: Job preferences ──
            "job_preferences": student_data.get("job_preferences", {}),

            # ── LMS sections ──
            "courses": student_data.get("courses", []),
            "courses_data":     profile_data.get("courses_data", student_data.get("courses", [])),
            "assignments_data": profile_data.get("assignments_data", student_data.get("assignments", [])),
            "attendance_data":  profile_data.get("attendance_data", student_data.get("attendance", {})),

            # ── Data sources tracking ──
            "data_sources": profile_data.get("data_sources", []),

            # ── Meta ──
            "slug": slug,
            "visibility": visibility,
            "profile_url": f"{agent_base}/api/v1/profile/public/{slug}",
        }

        template = _env.get_template("profile_template.html")
        return template.render(**context)
