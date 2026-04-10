"""
Profile Orchestrator v6 — Hashed partial regeneration
═════════════════════════════════════════════════════
- Handles resume_url (downloads PDF) AND resume_text
- Fetches GitHub + LinkedIn data in parallel (wired up — was missing in v4)
- Merges all sources (LMS + Resume + GitHub + LinkedIn) before generating
- NEW: Per-section data hashing → only changed sections regenerate on
  force_regenerate, saving Haiku cost and giving instant UX when nothing changed
- NEW: regenerate_partial(...) returns {updated_sections, profile_data}
"""

import asyncio
import hashlib
import json
import time
import logging
from typing import Dict, Any, List, Optional

from app.agents.summary_agent import SummaryAgent
from app.agents.skills_agent import SkillsAgent
from app.agents.achievement_engine import AchievementEngine
from app.agents.role_matcher import RoleMatcher
from app.services.resume_parser import ResumeParser
from app.services.github_fetcher import GitHubFetcher
from app.services.linkedin_fetcher import LinkedInFetcher
from app.services.data_merger import DataMerger
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


# ─── Section keys used in performance_data._meta.section_hashes ───
SECTION_SUMMARY      = "summary"
SECTION_SKILLS       = "skills"
SECTION_ROLES        = "roles"
SECTION_ACHIEVEMENTS = "achievements"
SECTION_PERSONALITY  = "personality"
SECTION_PROJECTS     = "projects"
SECTION_EDUCATION    = "education"
SECTION_EXPERIENCE   = "experience"

ALL_SECTIONS = [
    SECTION_SUMMARY, SECTION_SKILLS, SECTION_ROLES, SECTION_ACHIEVEMENTS,
    SECTION_PERSONALITY, SECTION_PROJECTS, SECTION_EDUCATION, SECTION_EXPERIENCE,
]


class ProfileOrchestrator:

    def __init__(self):
        self.summary_agent = SummaryAgent()
        self.skills_agent = SkillsAgent()
        self.achievement_engine = AchievementEngine()
        self.role_matcher = RoleMatcher()
        self.resume_parser = ResumeParser()
        self.github_fetcher = GitHubFetcher()
        self.linkedin_fetcher = LinkedInFetcher()
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
        linkedin_url = personal.get("linkedin_url") or ""

        # ── Step 2: Fetch external data (parallel) ──
        resume_data, github_data, linkedin_data = {}, {}, {}
        try:
            tasks = []
            if resume_text:
                tasks.append(("resume", self.resume_parser.parse(resume_text)))
            if github_url:
                tasks.append(("github", self.github_fetcher.fetch(github_url)))
            if linkedin_url:
                tasks.append(("linkedin", self.linkedin_fetcher.fetch(linkedin_url)))

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
                    elif name == "linkedin":
                        linkedin_data = results[i]
        except Exception as e:
            logger.warning(f"External data fetch failed: {e}")

        # ── Step 3: Merge all data sources (now includes LinkedIn) ──
        merged_data = self.data_merger.merge(student_data, resume_data, github_data, linkedin_data)

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

        # ── Step 8: Compute per-section data hashes for partial regen tracking ──
        section_hashes = self._compute_section_hashes(merged_data)

        performance = self._performance(merged_data)
        # Embed _meta inside performance_data so it gets saved to the existing JSON column
        # — no DB schema change needed.
        performance["_meta"] = {
            "section_hashes": section_hashes,
            "last_full_regen": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "data_sources": merged_data.get("data_sources", ["lms"]),
        }

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
            "performance_data": performance,
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
            # ats_keywords kept in payload for back-compat, but template no longer renders them as a section
            "ats_keywords": skills.get("ats_keywords", []) if isinstance(skills, dict) else [],
            "data_sources": merged_data.get("data_sources", ["lms"]),
            "courses_data":      student_data.get("courses", []),
            "assignments_data":  student_data.get("assignments", []),
            "attendance_data":   student_data.get("attendance", {}),
            "generation_time_seconds": round(time.time() - start, 2),
            "ai_model_used": "claude-haiku-4-5-20251001" if self.summary_agent.has_api else "rule-based-v6",
        }

    # ══════════════════════════════════════════════════════════════════
    # PARTIAL REGENERATION — only refresh sections whose source data changed
    # ══════════════════════════════════════════════════════════════════

    def _hash_obj(self, obj: Any) -> str:
        """Stable short hash of any JSON-serializable object."""
        try:
            blob = json.dumps(obj, sort_keys=True, default=str)
        except Exception:
            blob = str(obj)
        return hashlib.md5(blob.encode("utf-8")).hexdigest()[:12]

    def _compute_section_hashes(self, merged_data: Dict[str, Any]) -> Dict[str, str]:
        """Compute one hash per section, based on the SOURCE data that
        section depends on. If the source hash matches the previous run,
        the section can be skipped on regenerate."""
        personal = merged_data.get("personal", {}) or {}
        computed = merged_data.get("computed", {}) or {}
        all_skills = merged_data.get("all_skills", {}) or {}

        return {
            SECTION_SUMMARY: self._hash_obj({
                "name": personal.get("full_name"),
                "edu":  merged_data.get("education", []),
                "work": merged_data.get("work_experience", []),
                "linkedin_headline": personal.get("linkedin_headline"),
                "linkedin_summary":  personal.get("linkedin_summary"),
                "career_goals":      personal.get("career_goals"),
                "preferred_role":    personal.get("preferred_role"),
                "overall_score":     computed.get("overall_score"),
                "completed_courses": computed.get("completed_courses"),
                "course_names": [c.get("course_name") for c in merged_data.get("courses", [])],
                "tech_skills":  [s.get("name") for s in all_skills.get("technical_skills", [])[:10]],
            }),
            SECTION_SKILLS: self._hash_obj({
                "courses":     [c.get("course_name") for c in merged_data.get("courses", [])],
                "tech_skills": all_skills.get("technical_skills", []),
                "tools":       all_skills.get("tools", []),
                "soft_skills": all_skills.get("soft_skills", []),
                "github_languages": (merged_data.get("github_profile") or {}).get("languages", {}),
                "top_subjects": computed.get("top_subjects", []),
            }),
            SECTION_ROLES: self._hash_obj({
                "edu":  merged_data.get("education", []),
                "work": merged_data.get("work_experience", []),
                "tech_skills": [s.get("name") for s in all_skills.get("technical_skills", [])],
                "course_names": [c.get("course_name") for c in merged_data.get("courses", [])],
                "completed_courses": computed.get("completed_courses"),
                "overall_score": computed.get("overall_score"),
            }),
            SECTION_ACHIEVEMENTS: self._hash_obj({
                "test_scores":  merged_data.get("test_scores", []),
                "case_studies": merged_data.get("case_studies", []),
                "assignments":  merged_data.get("assignments", []),
                "quiz_scores":  merged_data.get("quiz_scores", []),
                "improvement":  computed.get("improvement_pct"),
                "consistency":  computed.get("consistency_score"),
            }),
            SECTION_PERSONALITY: self._hash_obj(merged_data.get("personality", {})),
            SECTION_PROJECTS: self._hash_obj({
                "projects":  merged_data.get("projects", []),
                "github_repos": (merged_data.get("github_profile") or {}).get("top_repos", []),
            }),
            SECTION_EDUCATION:  self._hash_obj(merged_data.get("education", [])),
            SECTION_EXPERIENCE: self._hash_obj(merged_data.get("work_experience", [])),
        }

    def _diff_sections(self, old_hashes: Dict[str, str], new_hashes: Dict[str, str]) -> List[str]:
        """Return the list of sections whose hash changed (or are new)."""
        if not old_hashes:
            return list(new_hashes.keys())
        return [k for k, v in new_hashes.items() if old_hashes.get(k) != v]

    async def regenerate_partial(
        self,
        student_data: Dict[str, Any],
        existing_profile_data: Dict[str, Any],
        existing_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Regenerate ONLY the sections whose source data has changed.

        Returns a dict with:
          - profile_data: the updated profile (existing data merged with new sections)
          - updated_sections: list of section names that actually changed
          - was_no_op: True if nothing changed (frontend can show "already up to date")
        """
        start = time.time()
        personal = student_data.get("personal", {})

        # ── Re-fetch external sources (resume / github / linkedin) ──
        resume_text = personal.get("resume_text") or ""
        if not resume_text and personal.get("resume_url"):
            resume_text = await self._download_resume(personal["resume_url"])

        lms_skills_text = personal.get("key_skills") or personal.get("skills") or ""
        lms_bio = personal.get("about_me") or ""
        if lms_skills_text and not resume_text:
            resume_text = (
                f"Name: {personal.get('full_name', '')}\n"
                f"Headline: {personal.get('current_designation', '')}\n"
                f"Skills: {lms_skills_text}\n"
                f"Bio: {lms_bio}\n"
                f"Education: {personal.get('education_level', '')} {personal.get('field_of_study', '')} "
                f"from {personal.get('institution', '')} ({personal.get('graduation_year', '')})\n"
                f"Experience: {personal.get('work_experience_years', '')} years at {personal.get('current_employer', '')}\n"
            )

        github_url = personal.get("github_url") or ""
        linkedin_url = personal.get("linkedin_url") or ""

        resume_data, github_data, linkedin_data = {}, {}, {}
        try:
            tasks = []
            if resume_text:
                tasks.append(("resume", self.resume_parser.parse(resume_text)))
            if github_url:
                tasks.append(("github", self.github_fetcher.fetch(github_url)))
            if linkedin_url:
                tasks.append(("linkedin", self.linkedin_fetcher.fetch(linkedin_url)))
            if tasks:
                results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
                for i, (name, _) in enumerate(tasks):
                    if isinstance(results[i], Exception):
                        logger.warning(f"{name} fetch failed: {results[i]}")
                    elif name == "resume":   resume_data = results[i]
                    elif name == "github":   github_data = results[i]
                    elif name == "linkedin": linkedin_data = results[i]
        except Exception as e:
            logger.warning(f"External fetch failed in partial regen: {e}")

        merged_data = self.data_merger.merge(student_data, resume_data, github_data, linkedin_data)

        # ── Compute new hashes and diff against the old ones ──
        new_hashes = self._compute_section_hashes(merged_data)
        old_hashes = (existing_meta or {}).get("section_hashes", {}) if existing_meta else {}
        changed = self._diff_sections(old_hashes, new_hashes)

        # ── Fast path: nothing changed → return existing profile, $0 cost ──
        if not changed:
            logger.info(f"Partial regen: no sections changed, returning cached profile (0ms agent calls)")
            return {
                "profile_data": existing_profile_data,
                "updated_sections": [],
                "was_no_op": True,
                "regen_time_seconds": round(time.time() - start, 3),
            }

        logger.info(f"Partial regen: {len(changed)} section(s) changed: {changed}")

        # ── Always need merged role_matches if anything dependent changed ──
        # Roles + summary + achievements may all be affected — recompute lazily
        updated = dict(existing_profile_data)  # shallow copy

        needs_roles = any(s in changed for s in (SECTION_ROLES, SECTION_SKILLS, SECTION_EDUCATION, SECTION_EXPERIENCE))
        role_matches = updated.get("role_matches", [])
        ats_data     = updated.get("ats_data", {})
        if needs_roles:
            role_matches = self.role_matcher.match_roles(merged_data)
            ats_data     = self.role_matcher.calculate_ats_score(merged_data)
            updated["role_matches"] = role_matches
            updated["ats_data"]     = ats_data

        # ── Skills (rule-based, fast) ──
        if SECTION_SKILLS in changed:
            try:
                skills = await self.skills_agent.generate(merged_data)
            except Exception as e:
                logger.error(f"Skills agent failed in partial regen: {e}")
                skills = updated.get("skills_data", {})
            # Merge with multi-source skills (same logic as full regen)
            all_skills = merged_data.get("all_skills", {})
            if all_skills:
                combined_tech = self._combine_skill_lists(
                    skills.get("technical_skills", []),
                    all_skills.get("technical_skills", []),
                )
                combined_soft = self._combine_skill_lists(
                    skills.get("soft_skills", []),
                    all_skills.get("soft_skills", []),
                )
                skills["technical_skills"] = combined_tech[:12]
                skills["soft_skills"] = combined_soft[:6]
                if all_skills.get("tools"):
                    skills["tools"] = all_skills["tools"][:6]
            updated["skills_data"] = skills
            updated["ats_keywords"] = skills.get("ats_keywords", []) if isinstance(skills, dict) else []

        # ── Summary (the only call that costs Haiku money) ──
        if SECTION_SUMMARY in changed:
            try:
                summary = await self.summary_agent.generate(merged_data)
            except Exception as e:
                logger.error(f"Summary agent failed in partial regen: {e}")
                summary = updated.get("professional_summary", "") or self._emergency_summary(merged_data)
            updated["professional_summary"] = summary

        # ── Achievements (rule-based, fast) ──
        if SECTION_ACHIEVEMENTS in changed or needs_roles:
            achievements = self.achievement_engine.generate_all(merged_data, role_matches)
            updated["headline"]              = achievements.get("headline", updated.get("headline", "Professional"))
            updated["top_achievements"]      = achievements.get("top_achievements", [])
            updated["case_study_highlights"] = achievements.get("case_study_highlights", [])
            updated["test_highlights"]       = achievements.get("test_highlights", [])
            updated["assignment_highlights"] = achievements.get("assignment_highlights", [])
            updated["project_highlights"]    = achievements.get("project_highlights", [])
            updated["learning_metrics"]      = achievements.get("learning_metrics", {})
            updated["consistency_statement"] = achievements.get("consistency_statement", "")
            updated["growth_statement"]      = achievements.get("growth_statement", "")
            updated["engagement_statement"]  = achievements.get("engagement_statement", "")

        # ── Personality / Projects / Education / Experience are pure data passthroughs ──
        if SECTION_PERSONALITY in changed:
            updated["personality_data"] = self._personality(merged_data)
        if SECTION_PROJECTS in changed:
            updated["projects_data"] = merged_data.get("projects", [])[:5]
            updated["github_profile"] = merged_data.get("github_profile", {})
        if SECTION_EDUCATION in changed:
            updated["education_data"] = merged_data.get("education", [])
        if SECTION_EXPERIENCE in changed:
            updated["work_experience"] = merged_data.get("work_experience", [])

        # ── Always refresh performance metrics + meta hashes ──
        performance = self._performance(merged_data)
        performance["_meta"] = {
            "section_hashes": new_hashes,
            "last_full_regen": (existing_meta or {}).get("last_full_regen", time.strftime("%Y-%m-%dT%H:%M:%S")),
            "last_partial_regen": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "data_sources": merged_data.get("data_sources", ["lms"]),
        }
        updated["performance_data"] = performance
        updated["data_sources"] = merged_data.get("data_sources", ["lms"])
        updated["generation_time_seconds"] = round(time.time() - start, 2)

        return {
            "profile_data": updated,
            "updated_sections": changed,
            "was_no_op": False,
            "regen_time_seconds": round(time.time() - start, 3),
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