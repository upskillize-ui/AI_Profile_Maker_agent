"""
Profile Orchestrator — REVISED
═══════════════════════════════
Changes:
  - Fallback summary no longer says "FinTech professional" for everyone
  - Fallback skills no longer invents "BFSI" skills when student has none
  - All data derived from actual student activity
"""

import asyncio
import time
import logging
from typing import Dict, Any

from app.agents.summary_agent import SummaryAgent
from app.agents.skills_agent import SkillsAgent
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ProfileOrchestrator:

    def __init__(self):
        self.summary_agent = SummaryAgent()
        self.skills_agent = SkillsAgent()

    async def generate_profile(self, student_data: Dict[str, Any]) -> Dict[str, Any]:
        start = time.time()

        summary, skills = await asyncio.gather(
            self.summary_agent.generate(student_data),
            self.skills_agent.generate(student_data),
            return_exceptions=True,
        )

        if isinstance(summary, Exception):
            logger.error(f"Summary agent failed: {summary}")
            summary = self._fallback_summary(student_data)
        if isinstance(skills, Exception):
            logger.error(f"Skills agent failed: {skills}")
            skills = self._fallback_skills(student_data)

        return {
            "professional_summary": summary,
            "skills_data": skills,
            "performance_data": self._performance(student_data),
            "journey_data": self._journey(student_data),
            "personality_data": self._personality(student_data),
            "case_studies_data": self._case_studies(student_data),
            "testgen_data": self._testgen(student_data),
            "projects_data": self._projects(student_data),
            "certifications_data": self._certifications(student_data),
            "ats_keywords": skills.get("ats_keywords", []) if isinstance(skills, dict) else [],
            "generation_time_seconds": round(time.time() - start, 2),
            "ai_model_used": settings.AI_MODEL,
        }

    # ─── Section builders ────────────────────────────────

    def _performance(self, d: Dict) -> dict:
        c = d.get("computed", {})
        return {
            "overall_score": c.get("overall_score", 0),
            "best_test_score": c.get("best_test_score", 0),
            "avg_test_score": c.get("avg_test_score", 0),
            "avg_case_study_score": c.get("avg_case_study_score", 0),
            "improvement_pct": c.get("improvement_pct", 0),
            "consistency_score": c.get("consistency_score", 85),
            "total_hours": c.get("total_hours", 0),
            "total_tests": c.get("total_tests", 0),
            "total_case_studies": c.get("total_case_studies", 0),
            "total_assignments": c.get("total_assignments", 0),
            "total_courses": c.get("total_courses", 0),
            "total_quizzes": c.get("total_quizzes", 0),
        }

    def _case_studies(self, d: Dict) -> list:
        cases = sorted(
            d.get("case_studies", []), key=lambda x: x.get("score", 0), reverse=True
        )[: settings.MAX_CASE_STUDIES_SHOWN]
        return [
            {
                "title": cs.get("title", "Untitled"),
                "topic": cs.get("topic", ""),
                "score": cs.get("score", 0),
                "max_score": cs.get("max_score", 100),
                "percentage": round(cs.get("score", 0) / max(cs.get("max_score", 100), 1) * 100, 1),
                "key_concepts": cs.get("key_concepts", []),
                "feedback_summary": (cs.get("ai_feedback", "") or "")[:200],
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
        for cert in d.get("certifications", []):
            milestones.append({
                "type": "certification",
                "title": cert.get("certificate_name", "Certificate"),
                "date": str(cert.get("issued_at", "")),
            })
        return {
            "total_hours": c.get("total_hours", 0),
            "active_days": act.get("active_days", 0),
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

    # ─── FIXED Fallbacks ────────────────────────────────

    def _fallback_summary(self, d: Dict) -> str:
        """Honest summary using ONLY real data."""
        p = d.get("personal", {})
        c = d.get("computed", {})
        courses = d.get("courses", [])
        name = (p.get("full_name") or "Student").strip()
        course_names = [co.get("course_name", "") for co in courses if co.get("course_name")]
        course_text = ", ".join(course_names[:3]) if course_names else "the Upskillize platform"

        total_quizzes = c.get("total_quizzes", 0)
        overall = c.get("overall_score", 0)

        if overall > 50:
            return (
                f"{name} is actively building expertise through {course_text} on Upskillize. "
                f"Achieved an overall score of {overall}% with "
                f"{total_quizzes} assessment{'s' if total_quizzes != 1 else ''} completed."
            )

        quiz_text = f" Completed {total_quizzes} quiz{'zes' if total_quizzes != 1 else ''}." if total_quizzes > 0 else ""
        return (
            f"{name} is currently enrolled in {course_text} on Upskillize.{quiz_text} "
            f"Building foundational knowledge through structured coursework and assessments."
        )

    def _fallback_skills(self, d: Dict) -> dict:
        """Skills from ACTUAL data only. No invented skills."""
        top = d.get("computed", {}).get("top_subjects", [])
        courses = d.get("courses", [])

        technical = [
            {"name": s[0], "score": int(s[1]), "evidence": f"Test avg: {s[1]}%"}
            for s in top[:4]
        ]

        domain = []
        for co in courses[:4]:
            cname = co.get("course_name", "")
            if cname:
                domain.append({"name": cname, "score": 50, "evidence": "Enrolled"})

        return {
            "technical_skills": technical,
            "tools": [],
            "soft_skills": [],
            "domain_knowledge": domain,
            "ats_keywords": [],
        }
