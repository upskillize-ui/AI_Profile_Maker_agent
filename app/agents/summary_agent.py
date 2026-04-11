"""
Summary Agent v7 — Bullet Points + Unique Per Student
══════════════════════════════════════════════════════
Generates a 4-6 bullet point professional summary that highlights what's
ACTUALLY unique and impressive about each student. Pulls from:

- LMS courses, scores, assignments, case studies, certifications
- LMS profile fields: current employer, designation, work years
- Resume: education, work experience, skills
- LinkedIn: headline, summary, skills
- Psychometric: personality type, traits, work style
- Computed metrics: best scores, completion rates, training hours

Each bullet must say something DIFFERENT and SPECIFIC. No generic filler.
The first bullet is always a strong positioning statement. Subsequent
bullets cover education, experience, technical capabilities, achievements,
and career focus — but ONLY when real data backs each one.
"""

import os
import hashlib
import logging
import httpx
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class SummaryAgent:

    def __init__(self):
        self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.has_api = bool(self.api_key)

    async def generate(self, student_data: Dict[str, Any]) -> str:
        personal = student_data.get("personal", {})
        computed = student_data.get("computed", {})
        courses = student_data.get("courses", [])
        education = student_data.get("education", [])
        work_experience = student_data.get("work_experience", [])
        case_studies = student_data.get("case_studies", [])
        certifications = student_data.get("certifications", [])
        personality = student_data.get("personality", {})
        all_skills = student_data.get("all_skills", {}) or {}

        name = (personal.get("full_name") or "Student").strip()
        first_name = name.split()[0] if name else "The candidate"

        # Build a rich data context
        ctx = self._build_context(
            personal, computed, courses, education,
            work_experience, case_studies, certifications,
            personality, all_skills, student_data,
        )

        if self.has_api:
            try:
                return await self._ai_bullet_summary(name, first_name, ctx)
            except Exception as e:
                logger.warning(f"AI summary failed, using fallback: {e}")

        return self._template_bullet_summary(name, first_name, ctx)

    # ─── Context builder ──────────────────────────────────────

    def _build_context(self, personal, computed, courses, education,
                       work_experience, case_studies, certifications,
                       personality, all_skills, student_data) -> Dict[str, Any]:
        """Build a structured context with all real data points."""

        # Education
        edu_str = ""
        if education:
            e = education[0]
            degree = e.get("degree", "")
            inst = e.get("institution", "")
            field = e.get("field_of_study", "")
            year = e.get("year", "")
            parts = []
            if degree:
                parts.append(degree)
            if field and field.lower() not in (degree or "").lower():
                parts.append(f"in {field}")
            if inst:
                parts.append(f"from {inst}")
            if year:
                parts.append(f"({year})")
            edu_str = " ".join(parts)

        # Work experience — collect ALL not just first
        work_strs = []
        for w in work_experience[:3]:
            title = w.get("title", "")
            company = w.get("company", "")
            duration = w.get("duration", "")
            if title and company:
                ws = f"{title} at {company}"
                if duration:
                    ws += f" ({duration})"
                work_strs.append(ws)
            elif title:
                work_strs.append(title)

        # Top technical skills with sources
        top_skills = []
        for sk in all_skills.get("technical_skills", [])[:8]:
            if isinstance(sk, dict) and sk.get("name"):
                top_skills.append(sk["name"])

        # Top courses (not all of them)
        top_courses = [c.get("course_name", "") for c in courses if c.get("course_name")][:4]

        # Best case study
        best_case = ""
        if case_studies:
            sorted_cs = sorted(case_studies, key=lambda x: float(x.get("score", 0) or 0), reverse=True)
            if sorted_cs:
                top_case = sorted_cs[0]
                title = top_case.get("title", "")
                score = top_case.get("score", 0)
                if title and score:
                    best_case = f'"{title}" ({score}%)'

        # Certifications
        cert_names = []
        for c in certifications[:5]:
            n = c.get("certificate_name") or c.get("name", "")
            if n:
                cert_names.append(n)

        # Domain
        domain = self._derive_domain(top_courses, education, work_experience, personal)

        # Career goals & preferred role from LMS
        career_goals = personal.get("career_goals", "") or ""
        preferred_role = personal.get("preferred_role", "") or ""
        current_designation = personal.get("current_designation", "") or ""
        current_employer = personal.get("current_employer", "") or ""
        work_years = personal.get("work_experience_years", "") or ""

        # LinkedIn
        linkedin_headline = personal.get("linkedin_headline", "") or ""
        linkedin_summary = personal.get("linkedin_summary", "") or ""

        return {
            "domain":               domain,
            "edu_str":              edu_str,
            "work_strs":            work_strs,
            "top_skills":           top_skills,
            "top_courses":          top_courses,
            "best_case":            best_case,
            "cert_names":           cert_names,
            "career_goals":         career_goals,
            "preferred_role":       preferred_role,
            "current_designation":  current_designation,
            "current_employer":     current_employer,
            "work_years":           work_years,
            "linkedin_headline":    linkedin_headline,
            "linkedin_summary":     linkedin_summary,
            "personality_type":     personality.get("personality_type", ""),
            "personality_traits":   personality.get("traits_json", ""),
            "work_style":           personality.get("work_style", ""),
            "overall_score":        computed.get("overall_score", 0),
            "best_test_score":      computed.get("best_test_score", 0),
            "avg_test_score":       computed.get("avg_test_score", 0),
            "total_assessments": (
                computed.get("total_tests", 0)
                + computed.get("total_quizzes", 0)
                + computed.get("total_case_studies", 0)
            ),
            "total_case_studies":   computed.get("total_case_studies", 0),
            "completed_courses":    computed.get("completed_courses", 0),
            "total_courses":        computed.get("total_courses", 0),
            "training_hours":       computed.get("total_hours", 0),
            "improvement_pct":      computed.get("improvement_pct", 0),
            "consistency":          computed.get("consistency_score", 0),
        }

    # ─── AI Generation ────────────────────────────────────────

    async def _ai_bullet_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        """Ask Claude Haiku to generate 4-6 unique bullets from the context."""

        # Build a clean data dump for the prompt
        data_lines = [f"Candidate Name: {name}"]
        if ctx["edu_str"]:
            data_lines.append(f"Education: {ctx['edu_str']}")
        if ctx["work_strs"]:
            data_lines.append(f"Work Experience: {' | '.join(ctx['work_strs'])}")
        if ctx["current_designation"] or ctx["current_employer"]:
            cd = ctx["current_designation"]
            ce = ctx["current_employer"]
            wy = ctx["work_years"]
            cur = f"Current: {cd}" if cd else ""
            if ce:
                cur += f" at {ce}"
            if wy:
                cur += f" ({wy} years exp)"
            data_lines.append(cur)
        if ctx["top_skills"]:
            data_lines.append(f"Top Skills: {', '.join(ctx['top_skills'])}")
        if ctx["top_courses"]:
            data_lines.append(f"Areas of Focus: {', '.join(ctx['top_courses'])}")
        if ctx["best_case"]:
            data_lines.append(f"Best Case Study: {ctx['best_case']}")
        if ctx["cert_names"]:
            data_lines.append(f"Certifications: {', '.join(ctx['cert_names'])}")
        if ctx["overall_score"] > 0:
            data_lines.append(f"Overall Performance Score: {ctx['overall_score']}%")
        if ctx["best_test_score"] > 0:
            data_lines.append(f"Best Assessment Score: {ctx['best_test_score']}%")
        if ctx["total_assessments"] > 0:
            data_lines.append(f"Total Assessments Completed: {ctx['total_assessments']}")
        if ctx["completed_courses"] > 0:
            data_lines.append(f"Courses Completed: {ctx['completed_courses']} of {ctx['total_courses']}")
        if ctx["training_hours"] > 0:
            data_lines.append(f"Training Hours: {ctx['training_hours']}")
        if ctx["personality_type"]:
            data_lines.append(f"Personality Type (Psychometric): {ctx['personality_type']}")
        if ctx["personality_traits"]:
            data_lines.append(f"Key Traits: {ctx['personality_traits']}")
        if ctx["work_style"]:
            data_lines.append(f"Work Style: {ctx['work_style']}")
        if ctx["linkedin_headline"]:
            data_lines.append(f"LinkedIn Headline: {ctx['linkedin_headline']}")
        if ctx["career_goals"]:
            data_lines.append(f"Career Goals: {ctx['career_goals']}")
        if ctx["preferred_role"]:
            data_lines.append(f"Preferred Role: {ctx['preferred_role']}")
        if ctx["domain"]:
            data_lines.append(f"Industry Domain: {ctx['domain']}")

        data_str = "\n".join(data_lines)

        prompt = f"""You are writing a professional summary for a candidate's recruiter-facing profile. The summary will be displayed as bullet points on a job profile page.

CANDIDATE DATA:
{data_str}

WRITE A 4-6 BULLET POINT SUMMARY following these STRICT rules:

FORMAT:
- Output ONLY bullet points, one per line
- Each line MUST start with "• " (bullet character + space)
- No headings, no "Professional Summary:", no markdown, no preamble
- No introduction paragraph before the bullets
- Each bullet should be 1-2 sentences maximum

CONTENT RULES:
1. Each bullet must say something DIFFERENT — never repeat information
2. Each bullet must reference SPECIFIC data from the candidate (real numbers, real names, real skills)
3. Use the candidate's first name "{first_name}" only in the FIRST bullet
4. Bullet 1 = positioning statement: who they are + their strongest credential (education OR current role OR top skill)
5. Bullet 2 = academic foundation OR work experience (whichever is stronger), with specific institution/company names
6. Bullet 3 = technical capabilities — list 3-5 actual skills they have, grouped by domain
7. Bullet 4 = achievements — best assessment score, case study, certification, or measurable metric (only if data exists)
8. Bullet 5 = work style or personality from psychometric test (only if data is provided)
9. Bullet 6 = career focus / what they're seeking next (only if career goals or preferred role provided)
10. SKIP any bullet where you don't have real data — better to have 4 strong bullets than 6 weak ones
11. NEVER mention "Upskillize", "LMS", "platform", or any training provider name
12. NEVER use generic filler like "passionate about", "dedicated learner", "eager to grow", "aspiring professional", "building foundational skills"
13. NEVER make up data not in the input
14. Sound IMPRESSIVE but factually accurate — like a senior recruiter wrote it
15. Use action verbs and specific industry terminology

EXAMPLE OUTPUT FORMAT:
• Maya Chen is a B.Tech Computer Science graduate from VIT Vellore with hands-on experience in full-stack web development and data analytics.
• Completed an Industry-Focused internship as a Frontend Developer at TechMint Solutions, contributing to production React applications used by 15K+ users.
• Technical proficiency spans Python, React, Node.js, MongoDB, and AWS — backed by 8 verified assessments at an average score of 87%.
• Earned distinction on the "E-commerce Recommendation Engine" case study (94%) and holds Google Data Analytics Professional Certificate.
• Identified as an Analytical Strategist through psychometric assessment — methodical, self-directed, and strong in cross-functional collaboration.
• Targeting Junior Software Developer or Data Analyst roles in product-based companies with growth-oriented engineering teams.

NOW WRITE THE SUMMARY:"""

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 600,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            data = response.json()
            text = data["content"][0]["text"].strip()

            # Clean the output: ensure each line starts with • and remove any preamble
            lines = text.split("\n")
            clean_lines = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # Strip markdown headers
                if line.startswith("#"):
                    continue
                # Convert various bullet styles to •
                if line.startswith("- "):
                    line = "• " + line[2:]
                elif line.startswith("* "):
                    line = "• " + line[2:]
                elif not line.startswith("•"):
                    line = "• " + line
                clean_lines.append(line)
            return "\n".join(clean_lines)

    # ─── Template Fallback (when no API key) ──────────────────

    def _template_bullet_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        """Generate bullet summary from templates when no API is available.
        Each bullet is conditionally added — only if real data backs it."""
        bullets = []

        # Bullet 1: Positioning — always present
        positioning = self._make_positioning_bullet(first_name, ctx)
        if positioning:
            bullets.append(positioning)

        # Bullet 2: Education / Background
        edu_bullet = self._make_education_bullet(ctx)
        if edu_bullet:
            bullets.append(edu_bullet)

        # Bullet 3: Work experience
        work_bullet = self._make_work_bullet(ctx)
        if work_bullet:
            bullets.append(work_bullet)

        # Bullet 4: Technical skills
        skills_bullet = self._make_skills_bullet(ctx)
        if skills_bullet:
            bullets.append(skills_bullet)

        # Bullet 5: Achievements
        ach_bullet = self._make_achievements_bullet(ctx)
        if ach_bullet:
            bullets.append(ach_bullet)

        # Bullet 6: Personality from psychometric
        pers_bullet = self._make_personality_bullet(ctx)
        if pers_bullet:
            bullets.append(pers_bullet)

        # Bullet 7: Career focus
        career_bullet = self._make_career_bullet(ctx)
        if career_bullet:
            bullets.append(career_bullet)

        # Cap at 6 bullets max
        return "\n".join(bullets[:6])

    def _make_positioning_bullet(self, first_name: str, ctx: Dict) -> str:
        domain = ctx["domain"]
        # Use most impressive credential available
        if ctx["current_designation"] and ctx["current_employer"]:
            return f"• {first_name} is a {ctx['current_designation']} at {ctx['current_employer']} with focused expertise in {domain}."
        elif ctx["edu_str"] and ctx["top_skills"]:
            return f"• {first_name} is a {ctx['edu_str']} graduate building specialized expertise in {domain} with hands-on capability in {', '.join(ctx['top_skills'][:3])}."
        elif ctx["edu_str"]:
            return f"• {first_name} is a {ctx['edu_str']} graduate positioned to launch a career in {domain}."
        elif ctx["current_designation"]:
            return f"• {first_name} is a {ctx['current_designation']} developing advanced expertise in {domain}."
        elif ctx["linkedin_headline"]:
            return f"• {first_name}: {ctx['linkedin_headline']}"
        else:
            return f"• {first_name} is an emerging professional building structured expertise in {domain}."

    def _make_education_bullet(self, ctx: Dict) -> str:
        if not ctx["edu_str"]:
            return ""
        # Don't repeat if already in positioning
        return f"• Academic foundation: {ctx['edu_str']}, providing the analytical and conceptual base for a career in {ctx['domain']}."

    def _make_work_bullet(self, ctx: Dict) -> str:
        if not ctx["work_strs"]:
            return ""
        if len(ctx["work_strs"]) == 1:
            return f"• Hands-on industry exposure as {ctx['work_strs'][0]}, applying classroom learning to real-world business challenges."
        else:
            return f"• Multi-role professional experience including {' and '.join(ctx['work_strs'][:2])} — building versatile, cross-functional capabilities."

    def _make_skills_bullet(self, ctx: Dict) -> str:
        if not ctx["top_skills"]:
            return ""
        skills = ctx["top_skills"][:5]
        if ctx["best_test_score"] > 0:
            return f"• Core technical capabilities span {', '.join(skills)} — backed by {ctx['total_assessments']} verified assessments with a peak score of {ctx['best_test_score']}%."
        return f"• Core technical capabilities include {', '.join(skills)} — developed through structured coursework and self-directed practice."

    def _make_achievements_bullet(self, ctx: Dict) -> str:
        parts = []
        if ctx["best_case"]:
            parts.append(f"top case study analysis on {ctx['best_case']}")
        if ctx["completed_courses"] > 0:
            parts.append(f"{ctx['completed_courses']} certified course completion{'s' if ctx['completed_courses'] != 1 else ''}")
        if ctx["cert_names"]:
            parts.append(f"earned {ctx['cert_names'][0]}")
        if ctx["overall_score"] >= 70:
            parts.append(f"{ctx['overall_score']}% overall performance")
        if not parts:
            return ""
        return f"• Notable achievements: {'; '.join(parts[:3])}."

    def _make_personality_bullet(self, ctx: Dict) -> str:
        if not ctx["personality_type"] or ctx["personality_type"] in ("Getting Started", ""):
            return ""
        traits = ctx["personality_traits"]
        ws = ctx["work_style"]
        result = f"• Identified as a {ctx['personality_type']} through psychometric assessment"
        if traits:
            result += f" — {traits.lower()}"
        if ws:
            result += f"; {ws.lower()} work style"
        result += "."
        return result

    def _make_career_bullet(self, ctx: Dict) -> str:
        if ctx["preferred_role"]:
            return f"• Actively targeting {ctx['preferred_role']} opportunities where analytical strengths and {ctx['domain']} foundation can drive measurable impact."
        elif ctx["career_goals"]:
            cg = ctx["career_goals"][:150]
            return f"• Career focus: {cg}"
        return ""

    # ─── Domain detection (unchanged from v6) ─────────────────

    def _derive_domain(self, course_names: list, education=None, work_experience=None, personal=None) -> str:
        if education is None: education = []
        if work_experience is None: work_experience = []
        if personal is None: personal = {}

        all_text_parts = list(course_names)
        for edu in education:
            all_text_parts.append(edu.get("degree", ""))
            all_text_parts.append(edu.get("field_of_study", ""))
            all_text_parts.append(edu.get("institution", ""))
        for work in work_experience:
            all_text_parts.append(work.get("title", ""))
            all_text_parts.append(work.get("company", ""))
            all_text_parts.append(work.get("description", ""))
        all_text_parts.append(personal.get("career_goals", "") or "")
        all_text_parts.append(personal.get("preferred_role", "") or "")
        all_text_parts.append(personal.get("current_designation", "") or "")
        all_text_parts.append(personal.get("linkedin_headline", "") or "")
        all_text_parts.append(personal.get("key_skills", "") or "")

        text = " ".join(all_text_parts).lower()
        if not text.strip():
            return "Financial Services"

        if "business analy" in text or "business intelligence" in text:
            return "Business Analysis & Analytics"
        elif "ux" in text or "user experience" in text or "user interface" in text or "ui design" in text:
            return "UX/UI Design & Digital Product"
        elif "data analy" in text or "data scien" in text or "power bi" in text or "tableau" in text:
            return "Data Analytics & Business Intelligence"
        elif "web develop" in text or "full stack" in text or "frontend" in text or "backend" in text:
            return "Software Development & Engineering"
        elif "digital market" in text or "marketing" in text or "seo" in text:
            return "Digital Marketing & Strategy"
        elif "fintech" in text or "digital bank" in text:
            return "FinTech & Digital Banking"
        elif "operations executive" in text or "branch operations" in text or "core banking" in text:
            return "Banking Operations & Financial Services"
        elif "e-commerce" in text or "ecommerce" in text:
            return "E-Commerce & Digital Business"
        elif "payment" in text or "card" in text or "upi" in text:
            return "Payment Systems & Digital Transactions"
        elif "banking" in text or "b.com" in text or "bcom" in text or "commerce" in text:
            return "Banking & Financial Services"
        elif "insurance" in text:
            return "Insurance & Risk"
        elif "risk" in text or "compliance" in text:
            return "Risk & Compliance"
        elif "finance" in text:
            return "Finance & Financial Services"
        elif "python" in text or "java" in text or "programming" in text:
            return "Software Development"
        elif "design" in text:
            return "Design & Creative Technology"
        return "Financial Services"
