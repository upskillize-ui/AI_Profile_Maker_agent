"""
Summary Agent v9 — Recruiter-grade bullets (Sonnet)
═══════════════════════════════════════════════════
Generates 5-7 PUNCHY bullet points that highlight what makes a candidate
UNIQUE — not generic. Each bullet ≤22 words, drawn from a different angle
(distinctive combination, cherry-pick achievement, shipped work,
domain edge, psychometric signal, current trajectory, student's own voice).

v9 fixes from v8:
  • Single, complete f-string prompt (v8 had two prompts pasted together,
    broken indentation, and lost {data_str} injection — file wouldn't import)
  • {data_str} is properly injected so Sonnet sees the candidate data
  • Bumped max_tokens 600 → 900 for richer output
  • Banned-phrase list expanded
  • Entity normalization (Uuskillize → Upskillize) handled in-prompt
"""

import os
import logging
import httpx
from typing import Dict, Any

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

        ctx = self._build_context(
            personal, computed, courses, education,
            work_experience, case_studies, certifications,
            personality, all_skills, student_data,
        )

        if self.has_api:
            try:
                return await self._ai_bullet_summary(name, first_name, ctx)
            except Exception as e:
                logger.warning(f"AI summary failed, using template fallback: {e}")

        return self._template_bullet_summary(name, first_name, ctx)

    # ─── Context builder ──────────────────────────────────────

    def _build_context(self, personal, computed, courses, education,
                       work_experience, case_studies, certifications,
                       personality, all_skills, student_data) -> Dict[str, Any]:

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

        top_skills = []
        for sk in all_skills.get("technical_skills", [])[:8]:
            if isinstance(sk, dict) and sk.get("name"):
                top_skills.append(sk["name"])

        top_courses = [c.get("course_name", "") for c in courses if c.get("course_name")][:4]

        best_case = ""
        if case_studies:
            sorted_cs = sorted(case_studies, key=lambda x: float(x.get("score", 0) or 0), reverse=True)
            if sorted_cs:
                top_case = sorted_cs[0]
                title = top_case.get("title", "")
                score = top_case.get("score", 0)
                if title and score:
                    best_case = f'"{title}" ({score}%)'

        cert_names = []
        for c in certifications[:5]:
            n = c.get("certificate_name") or c.get("name", "")
            if n:
                cert_names.append(n)

        domain = self._derive_domain(top_courses, education, work_experience, personal)

        career_goals = personal.get("career_goals", "") or ""
        preferred_role = personal.get("preferred_role", "") or ""
        current_designation = personal.get("current_designation", "") or ""
        current_employer = personal.get("current_employer", "") or ""
        work_years = personal.get("work_experience_years", "") or ""
        about_me = personal.get("about_me", "") or personal.get("bio", "") or ""
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
            "personality_traits":   personality.get("traits_json", "") or personality.get("traits", ""),
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

    # ─── AI generation (Sonnet, single clean prompt) ──────────

    async def _ai_bullet_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        """Single Sonnet call, single clean f-string prompt."""

        # Build a structured data dump
        data_lines = [f"Candidate Name: {name}"]
        if ctx["edu_str"]:
            data_lines.append(f"Education: {ctx['edu_str']}")
        if ctx["work_strs"]:
            data_lines.append(f"Work Experience: {' | '.join(ctx['work_strs'])}")
        if ctx["current_designation"] or ctx["current_employer"]:
            cd = ctx["current_designation"]
            ce = ctx["current_employer"]
            wy = ctx["work_years"]
            cur = f"Current: {cd}" if cd else "Current: "
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

        prompt = f"""<role>
You are a senior placement specialist at India's top campus-to-corporate firm, with 15+ years placing analysts into BFSI, FinTech, and product roles. Recruiters scan the first 2 bullets in 8 seconds and decide whether to keep reading. Front-load accordingly.
</role>

<task>
Read the candidate data and produce 5–7 bullets for the "Professional Summary" section of this candidate's Upskillize portfolio. Order by hiring-decision weight — strongest first.
</task>

<candidate_data>
{data_str}
</candidate_data>

<input_schema_notes>
Data may include any subset of: Personal (name, location, About Me), Address, Additional Info, Resume, LinkedIn, GitHub, Psychometric profile (Integrity / Synergy / Driver / Strategist), Job Preferences, and Upskillize coursework with scores and assessor grades. Sections may be sparse or missing — work only with what is present.
</input_schema_notes>

<the_one_rule>
Every bullet must be traceable to a specific fact in the candidate data — a number, a named project, a named entity, a score, a grade, a stack, or a quoted line. If you cannot point to the exact source fact, cut the bullet. No abstractions, no adjectives without evidence, no recruiter clichés like "passionate", "dedicated", "results-driven", "positioned for".
</the_one_rule>

<priority_ordering>
Order bullets by hiring-decision weight, not by section order in the data.

BULLETS 1–2 — the shortlist decision. Must contain a number, a shipped artifact, OR a rare crossover. Pick from:
  • SHIPPED WORK — something real, used by real people, with a usage number
  • CHERRY-PICK ACHIEVEMENT — top score, top grade, named recognition
  • DISTINCTIVE COMBINATION — a rare, defensible crossover

BULLETS 3–4 — credibility:
  • DOMAIN / TECH EDGE — stack or BFSI/FinTech fluency in plain English
  • CURRENT TRAJECTORY — program, certification, specialization in progress

BULLETS 5–7 — texture:
  • PSYCHOMETRIC SIGNAL — personality type translated into a workplace asset
  • STUDENT'S OWN VOICE — a meaningful line from About Me
  • SECONDARY ACHIEVEMENT — a second strong score or project

If a top-tier bullet is weak, promote a stronger lower-tier bullet up. Recruiter eyes never wait.
</priority_ordering>

<reasoning_steps>
Do this internally, do not print:
1. Extract 8–10 specific facts from the data — scores, projects, stacks, named entities, lines from About Me.
2. Rank by hiring-decision weight: shipped + numbers > top scores > rare combination > domain depth > trajectory > personality > voice.
3. Lock the top 2 facts as bullets 1 and 2. Each must carry a number, a shipped artifact, or a rare crossover.
4. Fill remaining bullets in descending weight, one angle each, no repeats.
5. Word-count every bullet. >22 words = rewrite shorter or cut.
6. Final check: would a recruiter who reads only bullets 1–2 want to open the resume? If no, reorder.
</reasoning_steps>

<hard_rules>
1. Each bullet = 1 sentence, 12–22 words.
2. Bullet 1 must contain a number, a shipped artifact name, or a rare-combination claim. Bullet 2 must be a different angle, equally hard-evidenced.
3. Start each bullet with a noun phrase, action verb, or specific noun. Never "She is…", "He has…", "{first_name} is…".
4. Use real specifics — real names, real percentages, real course names. Never invent.
5. No two bullets may restate the same point.
6. Skip a weak bullet. 5 strong bullets beat 7 diluted ones.
7. Normalize known-entity typos silently. "Uuskillize" / "Upskilize" / "Upskillze" → "Upskillize". Program names: PGDFDB, ADFBA, CBAF, CFBM, EAPrep, CAPM, ACAPM, "Data to Decisions".
8. If a section is empty, skip it silently.
</hard_rules>

<good_examples>
Notice how bullets 1–2 land the hire-decision punch immediately:

- Built the Razorpay payment integration powering enrollments for 400+ Upskillize students in production.
- Scored 87% on the RBI Banking Foundation case study, graded A — Very Good by rubric assessor.
- B.Com graduate who taught herself React, Node and FastAPI to ship production payment code.
- Currently sharpening BFSI specialization through ADFBA while shipping LMS features in production.
- Integrity-type psychometric signals a methodical, low-supervision teammate strong on compliance work.
</good_examples>

<bad_examples>
✗ "Passionate about technology and positioned for analytical roles."
   → No source fact. Pure abstraction. Cut.
✗ "Ranjana holds a Bachelor's degree in CS from BEU, providing foundational depth in systems architecture and analytical problem-solving aligned with data-intensive roles."
   → 24 words. Credentials without crossover or output. Starts with name.
✗ Opening with "Integrity-type psychometric signals a methodical teammate…"
   → Personality is texture, not shortlist signal. Wasted top slot.
✗ Opening with "Currently pursuing the ADFBA program at Upskillize…"
   → Trajectory without achievement reads as "still learning". Bury it lower.
</bad_examples>

<output_format>
Output ONLY bullet lines, ordered highest-to-lowest hiring-decision weight. Each line starts with "• " (bullet + space). No heading, no preamble, no markdown fences, no closing remark. 5 to 7 bullets total.
</output_format>

Now produce the bullets, strongest first."""

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 900,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            data = response.json()
            text = data["content"][0]["text"].strip()

            # Normalize: ensure each line starts with "• "
            lines = text.split("\n")
            clean_lines = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#"):
                    continue
                if line.startswith("- "):
                    line = "• " + line[2:]
                elif line.startswith("* "):
                    line = "• " + line[2:]
                elif not line.startswith("•"):
                    line = "• " + line
                clean_lines.append(line)

            return "\n".join(clean_lines)

    # ─── Template fallback (no API available) ────────────────

    def _template_bullet_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        """Generate bullet summary when API is unavailable.
        Each bullet only added if real data backs it — no fabrication."""
        bullets = []

        positioning = self._make_positioning_bullet(first_name, ctx)
        if positioning:
            bullets.append(positioning)

        edu_bullet = self._make_education_bullet(ctx)
        if edu_bullet:
            bullets.append(edu_bullet)

        work_bullet = self._make_work_bullet(ctx)
        if work_bullet:
            bullets.append(work_bullet)

        skills_bullet = self._make_skills_bullet(ctx)
        if skills_bullet:
            bullets.append(skills_bullet)

        ach_bullet = self._make_achievements_bullet(ctx)
        if ach_bullet:
            bullets.append(ach_bullet)

        pers_bullet = self._make_personality_bullet(ctx)
        if pers_bullet:
            bullets.append(pers_bullet)

        career_bullet = self._make_career_bullet(ctx)
        if career_bullet:
            bullets.append(career_bullet)

        return "\n".join(bullets[:6])

    def _make_positioning_bullet(self, first_name: str, ctx: Dict) -> str:
        domain = ctx["domain"]
        if ctx["current_designation"] and ctx["current_employer"]:
            yrs = f" with {ctx['work_years']} years experience" if ctx["work_years"] else ""
            return f"• {first_name} works as {ctx['current_designation']} at {ctx['current_employer']}{yrs}, focused on {domain}."
        elif ctx["edu_str"] and ctx["top_skills"]:
            return f"• {ctx['edu_str']} graduate with hands-on skills in {', '.join(ctx['top_skills'][:3])}, targeting entry-level {domain} roles."
        elif ctx["edu_str"]:
            return f"• {ctx['edu_str']} graduate, building toward a career in {domain}."
        elif ctx["linkedin_headline"]:
            return f"• {ctx['linkedin_headline']}"
        else:
            return f"• Emerging {domain} professional with credential-backed training."

    def _make_education_bullet(self, ctx: Dict) -> str:
        if not ctx["edu_str"]:
            return ""
        return f"• Holds a {ctx['edu_str']} — quantitative foundation for a {ctx['domain']} career."

    def _make_work_bullet(self, ctx: Dict) -> str:
        if not ctx["work_strs"]:
            return ""
        if len(ctx["work_strs"]) == 1:
            return f"• Hands-on industry exposure as {ctx['work_strs'][0]}."
        else:
            return f"• Cross-functional experience across {' and '.join(ctx['work_strs'][:2])}."

    def _make_skills_bullet(self, ctx: Dict) -> str:
        if not ctx["top_skills"]:
            return ""
        skills = ctx["top_skills"][:5]
        if ctx["best_test_score"] > 0:
            return f"• Working command of {', '.join(skills)}; peak assessment score {ctx['best_test_score']}%."
        return f"• Working command of {', '.join(skills)} from coursework and applied projects."

    def _make_achievements_bullet(self, ctx: Dict) -> str:
        parts = []
        if ctx["best_case"]:
            parts.append(f"top score on the {ctx['best_case']} case study")
        if ctx["completed_courses"] > 0:
            parts.append(f"{ctx['completed_courses']} certified programme{'s' if ctx['completed_courses'] != 1 else ''}")
        if ctx["cert_names"]:
            parts.append(f"holds {ctx['cert_names'][0]}")
        if ctx["overall_score"] >= 70:
            parts.append(f"{ctx['overall_score']}% aggregate performance")
        if not parts:
            return ""
        joined = "; ".join(parts[:3])
        return f"• Track record: {joined}."

    def _make_personality_bullet(self, ctx: Dict) -> str:
        if not ctx["personality_type"] or ctx["personality_type"] in ("Getting Started", ""):
            return ""
        result = f"• {ctx['personality_type']}-type psychometric"
        traits = ctx["personality_traits"]
        ws = ctx["work_style"]
        if traits:
            result += f" — {traits.lower()}"
        if ws:
            result += f", {ws.lower()} in team settings"
        result += "."
        return result

    def _make_career_bullet(self, ctx: Dict) -> str:
        if ctx["preferred_role"]:
            return f"• Targeting {ctx['preferred_role']} roles in BFSI organizations."
        elif ctx["career_goals"]:
            cg = ctx["career_goals"][:120]
            return f"• Career goal: {cg}"
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