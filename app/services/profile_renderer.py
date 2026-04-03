"""
Profile Renderer v4
═══════════════════
Passes ALL v4 data to the template: achievements, role matches,
ATS score, education, work experience, GitHub profile.
Template variables match the new profile_template.html exactly.
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

        context = {
            # Student info
            "student_name": (personal.get("full_name") or "Student").strip(),
            "student_email": personal.get("email", ""),
            "student_photo_url": personal.get("photo_url", ""),
            "linkedin_url": personal.get("linkedin_url", ""),
            "github_url": personal.get("github_url", ""),
            "portfolio_url": personal.get("portfolio_url", ""),

            # v4: Headline from role matcher (not course names)
            "headline": profile_data.get("headline", "Financial Services Professional"),

            # v4: Professional summary (AI-generated or template)
            "professional_summary": profile_data.get("professional_summary", ""),

            # v4: Skills (merged from LMS + resume + GitHub)
            "skills_data": profile_data.get("skills_data", {}),

            # v4: Performance metrics
            "performance_data": profile_data.get("performance_data", {}),

            # v4: Top achievements (reframed from real data)
            "top_achievements": profile_data.get("top_achievements", []),

            # v4: Case study highlights (reframed professionally)
            "case_study_highlights": profile_data.get("case_study_highlights", []),

            # v4: Test highlights (reframed professionally)
            "test_highlights": profile_data.get("test_highlights", []),

            # v4: Role matches (eligible job roles)
            "role_matches": profile_data.get("role_matches", []),

            # v4: ATS score data
            "ats_data": profile_data.get("ats_data", {}),
            "ats_keywords": profile_data.get("ats_keywords", []),

            # v4: Personality from psychometric
            "personality_data": profile_data.get("personality_data", {}),

            # v4: Statements (growth, consistency, engagement)
            "growth_statement": profile_data.get("growth_statement", ""),
            "consistency_statement": profile_data.get("consistency_statement", ""),
            "engagement_statement": profile_data.get("engagement_statement", ""),

            # v4: Education (from resume)
            "education_data": profile_data.get("education_data", []),

            # v4: Work experience (from resume)
            "work_experience": profile_data.get("work_experience", []),

            # v4: Projects (merged LMS + resume + GitHub)
            "projects_data": profile_data.get("projects_data", []),

            # v4: GitHub profile
            "github_profile": profile_data.get("github_profile", {}),

            # v4: Certifications (merged)
            "certifications_data": profile_data.get("certifications_data", []),

            # Courses (for the courses section if kept)
            "courses": student_data.get("courses", []),

            # Data sources used
            "data_sources": profile_data.get("data_sources", ["lms"]),

            # Meta
            "slug": slug,
            "visibility": visibility,
            "profile_url": f"{agent_base}/api/v1/profile/public/{slug}",
        }

        template = _env.get_template("profile_template.html")
        return template.render(**context)
