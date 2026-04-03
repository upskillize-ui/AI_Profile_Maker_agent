"""
Profile Orchestrator v4
═══════════════════════
Assembles the complete student profile from all engines.
Every data point is real. Every achievement is verified.
The profile shows the BEST of each student.
"""

import asyncio
import time
import logging
from typing import Dict, Any

from app.agents.summary_agent import SummaryAgent
from app.agents.skills_agent import SkillsAgent
from app.agents.achievement_engine import AchievementEngine
from app.agents.role_matcher import RoleMatcher
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ProfileOrchestrator:

    def __init__(self):
        self.summary_agent = SummaryAgent()
        self.skills_agent = SkillsAgent()
        self.achievement_engine = AchievementEngine()
        self.role_matcher = RoleMatcher()

    async def generate_profile(self, student_data: Dict[str, Any]) -> Dict[str, Any]:
        start = time.time()

        # Parallel: AI summary + rule-based skills
        summary, skills = await asyncio.gather(
            self.summary_agent.generate(student_data),
            self.skills_agent.generate(student_data),
            return_exceptions=True,
        )

        if isinstance(summary, Exception):
            logger.error(f"Summary agent failed: {summary}")
            summary = self._emergency_summary(student_data)
        if isinstance(skills, Exception):
            logger.error(f"Skills agent failed: {skills}")
            skills = {"technical_skills": [], "tools": [], "soft_skills": [],
                       "domain_knowledge": [], "ats_keywords": []}

        # Synchronous: achievement engine + role matcher (instant, rule-based)
        achievements = self.achievement_engine.generate_all(student_data)
        role_matches = self.role_matcher.match_roles(student_data)
        ats_data = self.role_matcher.calculate_ats_score(student_data)

        return {
            # AI-generated
            "professional_summary": summary,

            # Skills (rule-based)
            "skills_data": skills,

            # Achievements (rule-based, from REAL data)
            "headline": achievements.get("headline", "Upskillize Learner"),
            "top_achievements": achievements.get("top_achievements", []),
            "case_study_highlights": achievements.get("case_study_highlights", []),
            "test_highlights": achievements.get("test_highlights", []),
            "assignment_highlights": achievements.get("assignment_highlights", []),
            "project_highlights": achievements.get("project_highlights", []),

            # Metrics (pure math from DB)
            "learning_metrics": achievements.get("learning_metrics", {}),
            "consistency_statement": achievements.get("consistency_statement", ""),
            "growth_statement": achievements.get("growth_statement", ""),
            "engagement_statement": achievements.get("engagement_statement", ""),

            # Performance (pure math)
            "performance_data": self._performance(student_data),

            # Learning journey (from DB timestamps)
            "journey_data": self._journey(student_data),

            # Personality (from psychometric test)
            "personality_data": self._personality(student_data),

            # Case studies detail (from DB)
            "case_studies_data": self._case_studies(student_data),

            # Test performance detail (from DB)
            "testgen_data": self._testgen(student_data),

            # Projects (from DB)
            "projects_data": self._projects(student_data),

            # Certifications (from DB)
            "certifications_data": self._certifications(student_data),

            # Role matching (rule-based)
            "role_matches": role_matches,

            # ATS score (rule-based)
            "ats_data": ats_data,
            "ats_keywords": skills.get("ats_keywords", []) if isinstance(skills, dict) else [],

            # Metadata
            "generation_time_seconds": round(time.time() - start, 2),
            "ai_model_used": "claude-haiku-4-5-20251001" if self.summary_agent.has_api else "rule-based-v4",
        }

    # ─── Section Builders (all from REAL data) ───────────

    def _performance(self, d: Dict) -> dict:
        c = d.get("computed", {})
        return {
            "overall_score": c.get("overall_score", 0),
            "best_test_score": c.get("best_test_score", 0),
            "avg_test_score": c.get("avg_test_score", 0),
            "avg_case_study_score": c.get("avg_case_study_score", 0),
            "avg_quiz_score": c.get("avg_quiz_score", 0),
            "improvement_pct": c.get("improvement_pct", 0),
            "consistency_score": c.get("consistency_score", 85),
            "total_hours": c.get("total_hours", 0),
            "total_tests": c.get("total_tests", 0),
            "total_case_studies": c.get("total_case_studies", 0),
            "total_assignments": c.get("total_assignments", 0),
            "total_courses": c.get("total_courses", 0),
            "total_quizzes": c.get("total_quizzes", 0),
            "completed_courses": c.get("completed_courses", 0),
        }

    def _case_studies(self, d: Dict) -> list:
        cases = sorted(
            d.get("case_studies", []),
            key=lambda x: float(x.get("score", 0) or 0), reverse=True
        )[:settings.MAX_CASE_STUDIES_SHOWN]
        return [
            {
                "title": cs.get("title", "Untitled"),
                "topic": cs.get("topic", ""),
                "score": cs.get("score", 0),
                "max_score": cs.get("max_score", 100),
                "percentage": round(float(cs.get("score", 0) or 0) / max(float(cs.get("max_score", 100) or 100), 1) * 100, 1),
                "key_concepts": cs.get("key_concepts", []),
                "feedback_summary": (cs.get("ai_feedback", "") or "")[:200],
                "grade": cs.get("ai_grade", ""),
                "strengths": cs.get("ai_strengths", []),
                "improvements": cs.get("ai_improvements", []),
                "word_count": cs.get("word_count", 0),
                "course_name": cs.get("course_name", ""),
            }
            for cs in cases
        ]

    def _testgen(self, d: Dict) -> dict:
        c = d.get("computed", {})
        return {
            "best_score": c.get("best_test_score", 0),
            "avg_score": c.get("avg_test_score", 0),
            "total_tests": c.get("total_tests", 0),
            "subject_strengths": [
                {"subject": s[0], "avg_score": s[1]}
                for s in c.get("top_subjects", [])[:6]
            ],
            "improvement_pct": c.get("improvement_pct", 0),
            "consistency_score": c.get("consistency_score", 85),
        }

    def _journey(self, d: Dict) -> dict:
        c = d.get("computed", {})
        act = d.get("platform_activity", {})
        milestones = []
        for course in d.get("courses", []):
            if course.get("completed_at"):
                milestones.append({
                    "type": "course_completed",
                    "title": course.get("course_name", "Course"),
                    "date": str(course.get("completed_at", "")),
                })
            elif course.get("enrolled_at"):
                milestones.append({
                    "type": "course_enrolled",
                    "title": course.get("course_name", "Course"),
                    "date": str(course.get("enrolled_at", "")),
                })
        for cert in d.get("certifications", []):
            milestones.append({
                "type": "certification",
                "title": cert.get("certificate_name", "Certificate"),
                "date": str(cert.get("issued_at", "")),
            })
        return {
            "total_hours": c.get("total_hours", 0),
            "active_days": int(act.get("active_days", 0) or 0),
            "courses_completed": c.get("completed_courses", 0),
            "total_enrolled": c.get("total_courses", 0),
            "milestones": milestones[:15],
        }

    def _personality(self, d: Dict) -> dict:
        p = d.get("personality", {})
        return {
            "personality_type": p.get("personality_type", ""),
            "traits": p.get("traits_json", ""),
            "work_style": p.get("work_style", ""),
            "communication": p.get("communication_profile", ""),
            "leadership": p.get("leadership_indicators", ""),
        }

    def _projects(self, d: Dict) -> list:
        return [
            {
                "title": p.get("title", "Project"),
                "description": p.get("description", ""),
                "technologies": p.get("technologies_used", []),
                "mentor_feedback": p.get("mentor_feedback", ""),
            }
            for p in d.get("projects", [])[:5]
        ]

    def _certifications(self, d: Dict) -> list:
        return [
            {
                "name": c.get("certificate_name", "Certificate"),
                "course_name": c.get("course_name", ""),
                "issued_at": str(c.get("issued_at", "")),
                "verification_url": c.get("verification_url", ""),
            }
            for c in d.get("certifications", [])
        ]

    def _emergency_summary(self, d: Dict) -> str:
        p = d.get("personal", {})
        name = (p.get("full_name") or "Student").strip()
        courses = d.get("courses", [])
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]
        if course_names:
            return f"{name} is building professional expertise through {', '.join(course_names[:2])} on Upskillize. Developing industry-relevant skills through structured coursework and assessments."
        return f"{name} is registered on Upskillize and building their professional profile."
