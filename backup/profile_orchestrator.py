"""
Profile Orchestrator v4 — FIXED
════════════════════════════════
- Handles resume_url (downloads PDF) AND resume_text
- Fetches GitHub data in parallel
- Merges all sources before generating
"""

import asyncio
import time
import logging
from typing import Dict, Any

from app.agents.summary_agent import SummaryAgent
from app.agents.skills_agent import SkillsAgent
from app.agents.achievement_engine import AchievementEngine
from app.agents.role_matcher import RoleMatcher
from app.services.resume_parser import ResumeParser
from app.services.github_fetcher import GitHubFetcher
from app.services.data_merger import DataMerger
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ProfileOrchestrator:

    def __init__(self):
        self.summary_agent = SummaryAgent()
        self.skills_agent = SkillsAgent()
        self.achievement_engine = AchievementEngine()
        self.role_matcher = RoleMatcher()
        self.resume_parser = ResumeParser()
        self.github_fetcher = GitHubFetcher()
        self.data_merger = DataMerger()

    async def generate_profile(self, student_data: Dict[str, Any]) -> Dict[str, Any]:
        start = time.time()
        personal = student_data.get("personal", {})

        # ── Step 1: Get resume text (from DB text or download from URL) ──
        resume_text = personal.get("resume_text") or ""

        if not resume_text and personal.get("resume_url"):
            resume_text = await self._download_resume(personal["resume_url"])

        # Also check if student filled skills/bio in LMS profile directly
        lms_skills_text = personal.get("key_skills") or personal.get("skills") or ""
        lms_bio = personal.get("about_me") or ""
        if lms_skills_text and not resume_text:
            # Use LMS profile fields as mini-resume
            resume_text = f"""
Name: {personal.get('full_name', '')}
Headline: {personal.get('current_designation', '')}
Skills: {lms_skills_text}
Bio: {lms_bio}
Education: {personal.get('education_level', '')} {personal.get('field_of_study', '')} from {personal.get('institution', '')} ({personal.get('graduation_year', '')})
Experience: {personal.get('work_experience_years', '')} years at {personal.get('current_employer', '')}
"""

        github_url = personal.get("github_url") or ""

        # ── Step 2: Fetch external data (parallel) ──
        resume_data, github_data = {}, {}
        try:
            tasks = []
            if resume_text:
                tasks.append(("resume", self.resume_parser.parse(resume_text)))
            if github_url:
                tasks.append(("github", self.github_fetcher.fetch(github_url)))

            if tasks:
                results = await asyncio.gather(
                    *[t[1] for t in tasks],
                    return_exceptions=True,
                )
                for i, (name, _) in enumerate(tasks):
                    if isinstance(results[i], Exception):
                        logger.warning(f"{name} fetch failed: {results[i]}")
                    elif name == "resume":
                        resume_data = results[i]
                    elif name == "github":
                        github_data = results[i]
        except Exception as e:
            logger.warning(f"External data fetch failed: {e}")

        # ── Step 3: Merge all data sources ──
        merged_data = self.data_merger.merge(student_data, resume_data, github_data)

        # ── Step 4: Generate AI summary + rule-based skills (parallel) ──
        summary, skills = await asyncio.gather(
            self.summary_agent.generate(merged_data),
            self.skills_agent.generate(merged_data),
            return_exceptions=True,
        )

        if isinstance(summary, Exception):
            logger.error(f"Summary agent failed: {summary}")
            summary = self._emergency_summary(merged_data)
        if isinstance(skills, Exception):
            logger.error(f"Skills agent failed: {skills}")
            skills = {"technical_skills": [], "tools": [], "soft_skills": [],
                       "domain_knowledge": [], "ats_keywords": []}

        # ── Step 5: Merge AI skills with multi-source skills ──
        all_skills = merged_data.get("all_skills", {})
        if all_skills:
            combined_technical = self._combine_skill_lists(
                skills.get("technical_skills", []),
                all_skills.get("technical_skills", [])
            )
            combined_soft = self._combine_skill_lists(
                skills.get("soft_skills", []),
                all_skills.get("soft_skills", [])
            )
            skills["technical_skills"] = combined_technical[:12]
            skills["soft_skills"] = combined_soft[:6]
            if all_skills.get("tools"):
                skills["tools"] = all_skills["tools"][:6]

        # ── Step 6: Role matching + ATS ──
        role_matches = self.role_matcher.match_roles(merged_data)
        ats_data = self.role_matcher.calculate_ats_score(merged_data)

        # ── Step 7: Achievement engine ──
        achievements = self.achievement_engine.generate_all(merged_data, role_matches)

        return {
            "professional_summary": summary,
            "skills_data": skills,
            "headline": achievements.get("headline", "Financial Services Professional"),
            "top_achievements": achievements.get("top_achievements", []),
            "case_study_highlights": achievements.get("case_study_highlights", []),
            "test_highlights": achievements.get("test_highlights", []),
            "assignment_highlights": achievements.get("assignment_highlights", []),
            "project_highlights": achievements.get("project_highlights", []),
            "learning_metrics": achievements.get("learning_metrics", {}),
            "consistency_statement": achievements.get("consistency_statement", ""),
            "growth_statement": achievements.get("growth_statement", ""),
            "engagement_statement": achievements.get("engagement_statement", ""),
            "performance_data": self._performance(merged_data),
            "education_data": merged_data.get("education", []),
            "work_experience": merged_data.get("work_experience", []),
            "journey_data": self._journey(merged_data),
            "personality_data": self._personality(merged_data),
            "case_studies_data": self._case_studies(merged_data),
            "testgen_data": self._testgen(merged_data),
            "projects_data": merged_data.get("projects", [])[:5],
            "github_profile": merged_data.get("github_profile", {}),
            "certifications_data": merged_data.get("certifications", []),
            "role_matches": role_matches,
            "ats_data": ats_data,
            "ats_keywords": skills.get("ats_keywords", []) if isinstance(skills, dict) else [],
            "data_sources": merged_data.get("data_sources", ["lms"]),
            "courses_data":      student_data.get("courses", []),
            "assignments_data":  student_data.get("assignments", []),
            "attendance_data":   student_data.get("attendance", {}),
            "generation_time_seconds": round(time.time() - start, 2),
            "ai_model_used": "claude-haiku-4-5-20251001" if self.summary_agent.has_api else "rule-based-v4",
        }

    async def _download_resume(self, url: str) -> str:
        """Download resume PDF from URL and extract text."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.warning(f"Resume download failed: HTTP {resp.status_code}")
                    return ""

                # Save to temp file and extract text
                import tempfile
                import os
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                    f.write(resp.content)
                    tmp_path = f.name

                try:
                    # Try PyPDF2 first
                    try:
                        from PyPDF2 import PdfReader
                        reader = PdfReader(tmp_path)
                        text = "\n".join(page.extract_text() or "" for page in reader.pages)
                        if text.strip():
                            logger.info(f"Resume extracted: {len(text)} chars via PyPDF2")
                            return text
                    except ImportError:
                        pass

                    # Try pdfplumber
                    try:
                        import pdfplumber
                        with pdfplumber.open(tmp_path) as pdf:
                            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                            if text.strip():
                                logger.info(f"Resume extracted: {len(text)} chars via pdfplumber")
                                return text
                    except ImportError:
                        pass

                    # Try pdfminer
                    try:
                        from pdfminer.high_level import extract_text as pdfminer_extract
                        text = pdfminer_extract(tmp_path)
                        if text.strip():
                            logger.info(f"Resume extracted: {len(text)} chars via pdfminer")
                            return text
                    except ImportError:
                        pass

                    logger.warning("No PDF extraction library available (install PyPDF2, pdfplumber, or pdfminer)")
                    return ""
                finally:
                    os.unlink(tmp_path)

        except Exception as e:
            logger.warning(f"Resume download/extract failed: {e}")
            return ""

    def _combine_skill_lists(self, list_a: list, list_b: list) -> list:
        combined = {}
        for skill in list_a + list_b:
            key = skill.get("name", "").lower()
            if key:
                if key not in combined or skill.get("score", 0) > combined[key].get("score", 0):
                    combined[key] = skill
        return sorted(combined.values(), key=lambda x: x.get("score", 0), reverse=True)

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
        cases = sorted(d.get("case_studies", []), key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:settings.MAX_CASE_STUDIES_SHOWN]
        return [{"title": cs.get("title", ""), "score": cs.get("score", 0), "max_score": cs.get("max_score", 100),
                 "percentage": round(float(cs.get("score", 0) or 0) / max(float(cs.get("max_score", 100) or 100), 1) * 100, 1),
                 "key_concepts": cs.get("key_concepts", []), "grade": cs.get("ai_grade", ""),
                 "feedback_summary": (cs.get("ai_feedback", "") or "")[:200]} for cs in cases]

    def _testgen(self, d: Dict) -> dict:
        c = d.get("computed", {})
        return {"best_score": c.get("best_test_score", 0), "avg_score": c.get("avg_test_score", 0),
                "total_tests": c.get("total_tests", 0),
                "subject_strengths": [{"subject": s[0], "avg_score": s[1]} for s in c.get("top_subjects", [])[:6]]}

    def _journey(self, d: Dict) -> dict:
        c = d.get("computed", {})
        milestones = []
        for course in d.get("courses", []):
            if course.get("completed_at"):
                milestones.append({"type": "course_completed", "title": course.get("course_name", ""), "date": str(course.get("completed_at", ""))})
        for cert in d.get("certifications", []):
            milestones.append({"type": "certification", "title": cert.get("certificate_name", ""), "date": str(cert.get("issued_at", ""))})
        return {"total_hours": c.get("total_hours", 0), "active_days": int(d.get("platform_activity", {}).get("active_days", 0) or 0),
                "courses_completed": c.get("completed_courses", 0), "total_enrolled": c.get("total_courses", 0), "milestones": milestones[:15]}

    def _personality(self, d: Dict) -> dict:
        p = d.get("personality", {})
        return {"personality_type": p.get("personality_type", ""), "traits": p.get("traits_json", ""),
                "work_style": p.get("work_style", ""), "communication": p.get("communication_profile", ""),
                "leadership": p.get("leadership_indicators", "")}

    def _emergency_summary(self, d: Dict) -> str:
        name = (d.get("personal", {}).get("full_name") or "Student").strip()
        headline = d.get("personal", {}).get("current_designation", "")
        if headline:
            return f"{name} — {headline}. Building professional expertise through structured learning and hands-on projects."
        return f"{name} is building their professional profile through verified coursework and assessments."