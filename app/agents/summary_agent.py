"""
Summary Agent v8 — MBA-Level Articulation
═══════════════════════════════════════════
Generates a 4-6 bullet point professional summary with recruiter-grade
language. Reads like a McKinsey cover letter meets a Bloomberg bio —
authoritative, specific, and distinguished.

Key upgrades from v7:
- MBA-caliber vocabulary: "cross-functional" not "team player"
- Quantified positioning: lead with measurable credentials
- Structured narrative arc: credential → capability → impact → trajectory
- Zero filler: no "passionate about", "eager to learn", "aspiring"
- Indian hiring market awareness: BFSI terminology, tier awareness
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

        # about_me / bio
        about_me = personal.get("about_me", "") or personal.get("bio", "") or ""

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
            "about_me":             about_me,
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

    # ─── AI Generation (MBA-Level Prompt) ─────────────────────

    async def _ai_bullet_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        """Ask Claude to generate 4-6 MBA-caliber professional summary bullets."""

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
        if ctx.get("about_me"):
            data_lines.append(f"About Me (student's own words): {ctx['about_me']}")
        if ctx["domain"]:
            data_lines.append(f"Industry Domain: {ctx['domain']}")

        data_str = "\n".join(data_lines)

        prompt = f"""You are a senior executive recruiter drafting a professional summary for a candidate's public profile page. Write with the precision of a McKinsey bio and the authority of a Bloomberg executive profile.

CANDIDATE DATA:
{data_str}

GENERATE A 4-6 BULLET POINT SUMMARY following these rules:

FORMAT:
- Output ONLY bullet points, one per line
- Each line MUST start with "• " (bullet character + space)
- No headings, no preamble, no markdown, no "Professional Summary:"
- Each bullet: 1-2 concise sentences maximum

NARRATIVE ARC (follow this structure):
1. POSITIONING (required): Lead with the candidate's strongest credential. Use "{first_name}" only here. Frame as: "[Name] is a [credential] with [distinguishing capability]." If working professional: lead with role + tenure. If fresher: lead with degree + institution + domain focus.

2. ACADEMIC or PROFESSIONAL FOUNDATION (if data exists): Reference specific institution, degree, or employer names. Contextualize the credential — don't just list it. Example: "Holds a B.Tech in Computer Science from VIT Vellore, providing quantitative and systems-thinking foundations for technology roles."

3. TECHNICAL COMPETENCIES (if skills exist): Group skills by function, not random list. Use industry-standard phrasing. Example: "Technical proficiency encompasses backend development (Python, Django, FastAPI), data infrastructure (MySQL, MongoDB), and frontend engineering (React, TypeScript) — validated across 8 structured assessments."

4. DEMONSTRATED OUTCOMES (if achievements exist): Lead with the strongest metric. Combine case study scores, certifications, and assessment results into a single achievement-density bullet. Example: "Earned distinction on the 'Credit Risk Modeling' case analysis (94%) and completed the Certified Banking & Finance Analyst programme, demonstrating applied analytical rigour."

5. BEHAVIOURAL PROFILE (only if psychometric data provided): Frame the personality type as a workplace asset, not a label. Example: "Psychometric profiling identifies a Structured Analyst disposition — characterized by methodical problem decomposition, evidence-based decision-making, and high task ownership in cross-functional settings."

6. CAREER TRAJECTORY (only if career goals or preferred role provided): Frame as strategic intent, not wishful thinking. Example: "Positioned for Business Analyst or Product Associate roles in BFSI organizations where analytical depth, domain-specific training, and structured problem-solving drive operational decision-making."

LANGUAGE STANDARDS:
- Write at MBA/executive level — authoritative, precise, zero filler
- NEVER use: "passionate about", "dedicated learner", "eager to grow", "aspiring professional", "building foundational skills", "hands-on experience", "keen interest"
- PREFER: "demonstrated proficiency", "validated through", "positioned for", "complemented by", "underpinned by", "spanning", "encompassing"
- Use active voice and precise industry terminology
- Every claim must reference SPECIFIC data from the input — real names, real numbers, real skills
- NEVER mention "Upskillize", "LMS", "platform", or any training provider name
- NEVER fabricate data not present in the input
- SKIP any bullet where real data doesn't exist — 4 strong bullets outperform 6 diluted ones

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

    # ─── Template Fallback (MBA-Level) ────────────────────────

    def _template_bullet_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        """Generate MBA-caliber bullet summary when no API is available.
        Each bullet is conditionally added — only if real data backs it."""
        bullets = []

        # Bullet 1: Positioning — always present
        positioning = self._make_positioning_bullet(first_name, ctx)
        if positioning:
            bullets.append(positioning)

        # Bullet 2: Academic / Professional foundation
        edu_bullet = self._make_education_bullet(ctx)
        if edu_bullet:
            bullets.append(edu_bullet)

        # Bullet 3: Work experience
        work_bullet = self._make_work_bullet(ctx)
        if work_bullet:
            bullets.append(work_bullet)

        # Bullet 4: Technical competencies
        skills_bullet = self._make_skills_bullet(ctx)
        if skills_bullet:
            bullets.append(skills_bullet)

        # Bullet 5: Demonstrated outcomes
        ach_bullet = self._make_achievements_bullet(ctx)
        if ach_bullet:
            bullets.append(ach_bullet)

        # Bullet 6: Behavioural profile
        pers_bullet = self._make_personality_bullet(ctx)
        if pers_bullet:
            bullets.append(pers_bullet)

        # Bullet 7: Career trajectory
        career_bullet = self._make_career_bullet(ctx)
        if career_bullet:
            bullets.append(career_bullet)

        # Cap at 6 bullets max
        return "\n".join(bullets[:6])

    def _make_positioning_bullet(self, first_name: str, ctx: Dict) -> str:
        domain = ctx["domain"]
        if ctx["current_designation"] and ctx["current_employer"]:
            yrs = f" with {ctx['work_years']} years of experience" if ctx["work_years"] else ""
            return f"• {first_name} is a {ctx['current_designation']} at {ctx['current_employer']}{yrs}, specializing in {domain}."
        elif ctx["edu_str"] and ctx["top_skills"]:
            return f"• {first_name} is a {ctx['edu_str']} graduate with demonstrated proficiency in {', '.join(ctx['top_skills'][:3])}, positioned for entry-level roles in {domain}."
        elif ctx["edu_str"]:
            return f"• {first_name} is a {ctx['edu_str']} graduate with structured training in {domain}, prepared to contribute to industry-facing analytical and technical functions."
        elif ctx["current_designation"]:
            return f"• {first_name} is a {ctx['current_designation']} advancing domain-specific expertise in {domain}."
        elif ctx["linkedin_headline"]:
            return f"• {first_name}: {ctx['linkedin_headline']}"
        else:
            return f"• {first_name} is an emerging {domain} professional with structured, credential-backed training across core domain competencies."

    def _make_education_bullet(self, ctx: Dict) -> str:
        if not ctx["edu_str"]:
            return ""
        return f"• Holds a {ctx['edu_str']}, providing the quantitative and conceptual foundations underpinning a career in {ctx['domain']}."

    def _make_work_bullet(self, ctx: Dict) -> str:
        if not ctx["work_strs"]:
            return ""
        if len(ctx["work_strs"]) == 1:
            return f"• Professional exposure as {ctx['work_strs'][0]}, applying structured methodologies to real-world operational and business challenges."
        else:
            return f"• Cross-functional professional experience spanning {' and '.join(ctx['work_strs'][:2])}, building versatile competencies across multiple organizational contexts."

    def _make_skills_bullet(self, ctx: Dict) -> str:
        if not ctx["top_skills"]:
            return ""
        skills = ctx["top_skills"][:5]
        if ctx["best_test_score"] > 0:
            return f"• Technical proficiency encompasses {', '.join(skills)} — validated across {ctx['total_assessments']} structured assessments with a peak performance of {ctx['best_test_score']}%."
        return f"• Technical competencies span {', '.join(skills)}, developed through structured coursework and applied project implementation."

    def _make_achievements_bullet(self, ctx: Dict) -> str:
        parts = []
        if ctx["best_case"]:
            parts.append(f"earned distinction on the {ctx['best_case']} case analysis")
        if ctx["completed_courses"] > 0:
            parts.append(f"completed {ctx['completed_courses']} certified programme{'s' if ctx['completed_courses'] != 1 else ''}")
        if ctx["cert_names"]:
            parts.append(f"holds the {ctx['cert_names'][0]} credential")
        if ctx["overall_score"] >= 70:
            parts.append(f"maintained a {ctx['overall_score']}% aggregate performance benchmark")
        if not parts:
            return ""
        joined = "; ".join(parts[:3])
        return f"• Demonstrated outcomes include {joined} — reflecting applied analytical rigour and sustained engagement."

    def _make_personality_bullet(self, ctx: Dict) -> str:
        if not ctx["personality_type"] or ctx["personality_type"] in ("Getting Started", ""):
            return ""
        result = f"• Psychometric profiling identifies a {ctx['personality_type']} disposition"
        traits = ctx["personality_traits"]
        ws = ctx["work_style"]
        if traits:
            result += f" — characterized by {traits.lower()}"
        if ws:
            result += f" and a {ws.lower()} orientation in team settings"
        result += "."
        return result

    def _make_career_bullet(self, ctx: Dict) -> str:
        if ctx["preferred_role"]:
            return f"• Positioned for {ctx['preferred_role']} opportunities in organizations where analytical depth, domain training, and structured problem-solving drive measurable operational impact."
        elif ctx["career_goals"]:
            cg = ctx["career_goals"][:150]
            return f"• Strategic career focus: {cg}"
        return ""

    # ─── Domain detection ─────────────────────────────────────

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