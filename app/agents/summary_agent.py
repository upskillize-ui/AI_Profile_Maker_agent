"""
Summary Agent v11 — Dynamic, honest, recruiter-grade bullets
═════════════════════════════════════════════════════════════
Generates 3–5 bullet points grounded entirely in real student data.
No hallucination, no inflation, no marketing language.

v11 fixes from v10:
  • Dynamic prompt: sections activate/deactivate based on available data.
    Sparse-data students get a shorter, focused prompt. Rich-data students
    get the full structure. No wasted tokens on empty sections.
  • Smart angle selection: _determine_lead_angle() picks the strongest
    data signal, not random rotation. Seed still varies phrasing within
    the chosen angle.
  • Real examples embedded in prompt for each student archetype
    (working professional, fresher with scores, sparse/new student,
    career-pivot student).
  • Banned-phrase list is explicit and expanded — catches common AI
    inflation patterns before they reach output.
  • Template fallback rewritten: sentences combine related facts
    naturally instead of one-fact-per-bullet.
  • _clean_bullets() now also strips banned marketing phrases as a
    final safety net.
"""

import os
import hashlib
import logging
import httpx
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# ── Phrases that must never appear in output ──────────────────
BANNED_PHRASES = [
    "passionate about", "dedicated to", "motivated by",
    "results-driven", "hard-working", "self-starter",
    "go-getter", "detail-oriented", "team player",
    "positioned for", "poised to", "uniquely qualified",
    "rare combination", "distinctive crossover", "unique blend",
    "sustained commitment", "deep specialisation", "deep expertise",
    "measurable output", "measurable impact", "proven track record",
    "performance highlights", "key results", "standout result",
    "industry-validated", "industry-ready", "job-ready",
    "currently sharpening", "actively building expertise",
    "emerging professional", "aspiring professional",
    "dynamic professional", "versatile professional",
    "strong foundation", "solid foundation", "robust foundation",
    "well-rounded", "well-positioned", "well-equipped",
    "cutting-edge", "state-of-the-art", "next-generation",
    "leveraging", "synergy", "synergistic",
    "holistic", "comprehensive understanding",
]


class SummaryAgent:

    def __init__(self):
        self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.has_api = bool(self.api_key)

    # ═══════════════════════════════════════════════════════════
    #  PUBLIC ENTRY POINT
    # ═══════════════════════════════════════════════════════════

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

        ctx["_seed"] = self._variety_seed(name, ctx)
        ctx["_archetype"] = self._detect_archetype(ctx)
        ctx["_lead_angle"] = self._determine_lead_angle(ctx)
        ctx["_differentiators"] = self._find_differentiators(ctx)

        if self.has_api:
            try:
                return await self._ai_summary(name, first_name, ctx)
            except Exception as e:
                logger.warning("AI summary failed, falling back to template: %s", e)

        return self._template_summary(name, first_name, ctx)

    # ═══════════════════════════════════════════════════════════
    #  CONTEXT BUILDER
    # ═══════════════════════════════════════════════════════════

    def _build_context(self, personal, computed, courses, education,
                       work_experience, case_studies, certifications,
                       personality, all_skills, student_data) -> Dict[str, Any]:

        edu_str = ""
        if education:
            e = education[0]
            parts = []
            if e.get("degree"):
                parts.append(e["degree"])
            field = e.get("field_of_study", "")
            if field and field.lower() not in (e.get("degree") or "").lower():
                parts.append(f"in {field}")
            if e.get("institution"):
                parts.append(f"from {e['institution']}")
            if e.get("year"):
                parts.append(f"({e['year']})")
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

        top_courses = [
            c.get("course_name", "") for c in courses if c.get("course_name")
        ][:4]

        best_case_title = ""
        best_case_score = 0
        best_case = ""
        if case_studies:
            sorted_cs = sorted(
                case_studies,
                key=lambda x: float(x.get("score", 0) or 0),
                reverse=True,
            )
            if sorted_cs:
                top = sorted_cs[0]
                best_case_title = top.get("title", "")
                best_case_score = float(top.get("score", 0) or 0)
                if best_case_title and best_case_score:
                    best_case = f'"{best_case_title}" ({best_case_score:.0f}%)'

        cert_names = []
        for c in certifications[:5]:
            n = c.get("certificate_name") or c.get("name", "")
            if n:
                cert_names.append(n)

        domain = self._derive_domain(top_courses, education, work_experience, personal)
        hobbies = personal.get("hobbies", "") or personal.get("interests", "") or ""
        about_me = personal.get("about_me", "") or personal.get("bio", "") or ""

        return {
            "domain": domain, "edu_str": edu_str,
            "edu_degree": education[0].get("degree", "") if education else "",
            "edu_field": education[0].get("field_of_study", "") if education else "",
            "edu_institution": education[0].get("institution", "") if education else "",
            "work_strs": work_strs, "work_count": len(work_experience),
            "top_skills": top_skills, "top_courses": top_courses,
            "best_case": best_case, "best_case_title": best_case_title,
            "best_case_score": best_case_score, "cert_names": cert_names,
            "career_goals": personal.get("career_goals", "") or "",
            "preferred_role": personal.get("preferred_role", "") or "",
            "current_designation": personal.get("current_designation", "") or "",
            "current_employer": personal.get("current_employer", "") or "",
            "work_years": personal.get("work_experience_years", "") or "",
            "about_me": about_me, "hobbies": hobbies,
            "linkedin_headline": personal.get("linkedin_headline", "") or "",
            "linkedin_summary": personal.get("linkedin_summary", "") or "",
            "personality_type": personality.get("personality_type", ""),
            "personality_traits": personality.get("traits_json", "") or personality.get("traits", ""),
            "work_style": personality.get("work_style", ""),
            "overall_score": computed.get("overall_score", 0),
            "best_test_score": computed.get("best_test_score", 0),
            "avg_test_score": computed.get("avg_test_score", 0),
            "total_assessments": (
                computed.get("total_tests", 0)
                + computed.get("total_quizzes", 0)
                + computed.get("total_case_studies", 0)
            ),
            "total_case_studies": computed.get("total_case_studies", 0),
            "completed_courses": computed.get("completed_courses", 0),
            "total_courses": computed.get("total_courses", 0),
            "training_hours": computed.get("total_hours", 0),
            "improvement_pct": computed.get("improvement_pct", 0),
            "consistency": computed.get("consistency_score", 0),
        }

    # ═══════════════════════════════════════════════════════════
    #  INTELLIGENCE LAYER
    # ═══════════════════════════════════════════════════════════

    def _variety_seed(self, name: str, ctx: Dict) -> int:
        fingerprint = (
            f"{name}|{ctx.get('edu_str','')}|{len(ctx.get('work_strs',[]))}|"
            f"{ctx.get('best_case_score',0)}|{ctx.get('overall_score',0)}|"
            f"{ctx.get('personality_type','')}|{ctx.get('hobbies','')}"
        )
        return int(hashlib.md5(fingerprint.encode()).hexdigest()[:8], 16)

    def _detect_archetype(self, ctx: Dict) -> str:
        """Classify this student so the prompt loads the right examples."""
        if ctx["current_designation"] and ctx["current_employer"]:
            return "working_professional"
        if ctx["work_strs"]:
            return "experienced_fresher"
        if ctx["completed_courses"] == 0 and ctx["best_test_score"] == 0:
            return "new_student"
        if ctx["completed_courses"] > 0 or ctx["best_test_score"] > 0:
            return "active_learner"
        return "new_student"

    def _determine_lead_angle(self, ctx: Dict) -> str:
        """Pick the strongest available signal to lead with.
        Not random — data-driven. The seed varies phrasing WITHIN
        the chosen angle, but the angle itself is always the strongest."""

        # Tier 1: Currently employed
        if ctx["current_designation"] and ctx["current_employer"]:
            return "current_role"

        # Tier 2: Has work experience (even if not current)
        if ctx["work_strs"]:
            return "work_experience"

        # Tier 3: Has meaningful scores (case study or assessment >= 60%)
        if ctx["best_case_score"] >= 60 or ctx["best_test_score"] >= 60:
            return "scores"

        # Tier 4: Has completed courses
        if ctx["completed_courses"] > 0:
            return "courses"

        # Tier 5: Has education
        if ctx["edu_str"]:
            return "education"

        # Tier 6: Has career goals
        if ctx["career_goals"] or ctx["preferred_role"]:
            return "goals"

        return "education"

    def _find_differentiators(self, ctx: Dict) -> List[str]:
        diffs = []
        edu_field = (ctx.get("edu_field") or "").lower()
        domain = (ctx.get("domain") or "").lower()

        if edu_field and domain:
            edu_tech = any(k in edu_field for k in ["computer", "software", "it", "cse"])
            domain_bfsi = any(k in domain for k in ["banking", "finance", "bfsi", "fintech"])
            edu_bfsi = any(k in edu_field for k in ["commerce", "finance", "banking", "bba", "mba"])
            domain_tech = any(k in domain for k in ["software", "engineering", "development"])
            if edu_tech and domain_bfsi:
                diffs.append("tech-to-bfsi-crossover")
            elif edu_bfsi and domain_tech:
                diffs.append("commerce-to-tech-crossover")
            elif not edu_tech and not edu_bfsi:
                diffs.append("non-traditional-background")

        if len(ctx.get("work_strs", [])) >= 2:
            combined = " ".join(ctx["work_strs"]).lower()
            if any(k in combined for k in ["founding", "promoted", "lead"]):
                diffs.append("rapid-career-growth")
            diffs.append("multi-role-experience")

        if ctx.get("best_case_score", 0) >= 80:
            diffs.append("high-case-study-performer")
        if ctx.get("best_test_score", 0) >= 85:
            diffs.append("high-assessment-performer")
        if ctx.get("consistency", 0) >= 75:
            diffs.append("high-consistency")
        if ctx.get("improvement_pct", 0) >= 20:
            diffs.append("strong-improvement-arc")
        if ctx.get("personality_type") and ctx.get("career_goals"):
            diffs.append("personality-goal-alignment")
        hobbies = ctx.get("hobbies", "").lower()
        if hobbies and len(hobbies) > 3:
            diffs.append("has-personal-dimension")
        about = ctx.get("about_me", "")
        if about and len(about) > 50:
            diffs.append("has-self-narrative")
        return diffs

    # ═══════════════════════════════════════════════════════════
    #  AI GENERATION — DYNAMIC PROMPT
    # ═══════════════════════════════════════════════════════════

    async def _ai_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        prompt = self._build_dynamic_prompt(name, ctx)

        async with httpx.AsyncClient(timeout=25.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 600,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            text = resp.json()["content"][0]["text"].strip()

        return self._clean_bullets(text, max_bullets=5)

    def _build_dynamic_prompt(self, name: str, ctx: Dict) -> str:
        """Construct the prompt dynamically based on available data.
        Empty sections are excluded entirely — no wasted tokens."""

        archetype = ctx["_archetype"]
        lead = ctx["_lead_angle"]
        diff_str = ", ".join(ctx["_differentiators"]) or "none detected"
        data_str = self._build_data_block(name, ctx)

        # ── Lead instruction (based on strongest data signal) ──
        lead_instructions = {
            "current_role": f"Lead bullet: state current role and employer. Just the fact — no adjectives. Example: 'Currently working as {{title}} at {{company}}.'",
            "work_experience": f"Lead bullet: state most recent or strongest work experience. Just role and company. Example: 'Worked as {{title}} at {{company}} ({{duration}}).'",
            "scores": f"Lead bullet: state the single best score with the assignment/case-study name. Example: 'Scored 85% on the SVB Collapse case study through Upskillize coursework.'",
            "courses": f"Lead bullet: state completed courses and platform. Example: 'Completed Banking Foundation and Payments & Cards courses on Upskillize.'",
            "education": f"Lead bullet: state degree and institution. Example: 'B.Tech in CSE from NIT Patna (2024) graduate.'",
            "goals": f"Lead bullet: state career direction. Example: 'Targeting Credit Analyst roles in banking.'",
        }

        # ── Archetype-specific examples ──
        examples = self._get_examples(archetype)

        # ── Build prompt sections conditionally ──
        sections = []

        sections.append(f"""Write the Summary section for a candidate profile on Upskillize. Read all the data below. Write 3 to 5 bullet points — honest, specific, grounded in facts.

{lead_instructions.get(lead, lead_instructions['education'])}""")

        # Rules (always included, compact)
        sections.append("""RULES:
- 3 to 5 bullets. Pick 3 if data is thin. 5 only if 5 different things are worth saying.
- Each bullet: one sentence, max 25 words. No two-line bullets.
- Every claim traces to a fact in the data. Nothing invented. Nothing inflated.
- LEAD WITH STRENGTHS: real work experience, qualifications, a standout score, key skills, and the goal.
  Build the summary around the candidate's actual experience and education, not just LMS coursework.
- Never mention: ProfileIQ, TestGen, AiRev, NudgeAI, InterviewIQ, CareerIQ.
- DO NOT report raw tallies: NO "completed X of Y courses", NO "N assessments attempted",
  NO course counts, NO attempt counts. Recruiters don't want a progress log.
- DO NOT state an overall/aggregate score or completion percentage (e.g. "overall score 24%").
  Never surface a weak average, and never use defensive framing like "still in progress" or
  "when fully engaged". If a single score is strong, name that one score; otherwise omit scores.
- DO NOT describe Upskillize as an employer. NEVER write "interning at Upskillize" or imply the
  platform is a workplace — Upskillize is where they learn, not where they work.
- Course scores are course scores. Skills = "trained in" or "skills include"; only "working with" if
  currently employed using them. Completing courses is completing courses — no "deep specialisation".
- If someone is an intern (real internship at a real company), say intern. If fresher, don't pretend
  they're experienced. The summary must survive the interview — nothing exaggerated.""")

        # Banned phrases
        banned_sample = ", ".join(f'"{p}"' for p in BANNED_PHRASES[:12])
        sections.append(f"""BANNED PHRASES (never use any of these or similar):
{banned_sample}, and all similar marketing/filler language.""")

        # Examples (archetype-specific)
        sections.append(f"""EXAMPLES of honest summaries for similar candidates:

{examples}""")

        # Differentiators (only if detected)
        if ctx["_differentiators"]:
            sections.append(f"WHAT MAKES THIS CANDIDATE DIFFERENT from others with similar credentials:\n{diff_str}")

        # Data
        sections.append(f"CANDIDATE DATA:\n{data_str}")

        # Variety instruction (use seed to vary phrasing)
        seed_mod = ctx["_seed"] % 3
        variety_hints = [
            "Start each bullet with a different word. No two bullets should open the same way.",
            "Vary sentence structure: mix short declarative bullets with slightly longer ones that connect two facts.",
            "At least one bullet should connect two facts from different parts of the data into a single insight.",
        ]
        sections.append(f"VARIETY: {variety_hints[seed_mod]}")

        sections.append("""OUTPUT:
• (bullet 1)
• (bullet 2)
• (bullet 3)
(3 to 5 total. Nothing else — no preamble, no headings, no closing.)""")

        return "\n\n".join(sections)

    def _get_examples(self, archetype: str) -> str:
        """Return 2 example summaries matched to the student's archetype.
        These are GOOD examples — honest, specific, no inflation."""

        if archetype == "working_professional":
            return """Example A (working professional with strong data):
• Currently working as Software Developer at TCS, writing Java microservices for a banking client.
• Completed Banking Foundation and Payments & Cards courses on Upskillize (2 of 4 enrolled).
• Scored 82% on the Yes Bank governance failure case study — applied risk assessment frameworks.
• Integrity-type psychometric profile: principled, self-directed, independent in team settings.
• Targeting senior developer roles in BFSI within 2-3 years.

Example B (working professional, fewer scores):
• Operations Analyst at HDFC Bank, 14 months in retail branch operations.
• Finished Banking Foundation course on Upskillize; currently enrolled in Payments & Cards.
• Best assessment score: 71%. No case studies submitted yet.
• Career direction: moving from operations into credit analysis."""

        elif archetype == "experienced_fresher":
            return """Example A (fresher with internship):
• Interned at Wipro for 3 months building REST APIs in Python and Django.
• B.Tech in CSE from JNTU (2024) graduate, trained in Python, React, and SQL.
• Completed Banking Foundation on Upskillize; scored 68% on the NPCI case study.
• Adaptability-type psychometric profile — flexible, collaborative in team settings.

Example B (fresher with multiple short roles):
• Worked as Web Development Intern at Aagaz Training Center (3 months), then Operations Support at Startek (5 months).
• B.Tech in CSE from VIT (2024), trained in JavaScript, React, and Node.js.
• Course scores: 74% best assessment, 65% on UPI Fraud Detection case study.
• Career direction: Software Developer in a bank."""

        elif archetype == "new_student":
            return """Example A (very sparse — just enrolled):
• B.Com from Mumbai University (2024) graduate.
• Currently enrolled in Banking Foundation on Upskillize.
• Targeting: Banking career.

Example B (slightly more data but still early):
• BBA from Christ University (2024), trained in Excel and basic accounting.
• Enrolled in Banking Foundation and Payments & Cards on Upskillize. No scores yet.
• Career direction: Branch banking or operations roles."""

        else:  # active_learner
            return """Example A (active learner, no job, has scores):
• B.Tech in ECE from GITAM (2024), trained in Python, C++, and MATLAB.
• Completed 2 courses on Upskillize: Banking Foundation, Payments & Cards.
• Scored 76% on SVB case study; best assessment score 80%.
• Assessment scores improved by 18% over the course of training.
• Targeting: FinTech analyst or payments operations roles.

Example B (active learner with personality data):
• B.Com in Accounting from Osmania University (2024).
• Finished Banking Foundation course on Upskillize (1 of 3 enrolled).
• Course scores: 65% best assessment. No case studies above 60%.
• Execution-type psychometric profile — builder, action-oriented, focused.
• Also interested in cricket and stock market analysis."""

    def _build_data_block(self, name: str, ctx: Dict) -> str:
        lines = [f"Name: {name}"]
        if ctx["edu_str"]:
            lines.append(f"Education: {ctx['edu_str']}")
        if ctx["work_strs"]:
            lines.append(f"Work: {' | '.join(ctx['work_strs'])}")
        if ctx["current_designation"] or ctx["current_employer"]:
            cur = f"Current role: {ctx['current_designation']}" if ctx["current_designation"] else "Current: "
            if ctx["current_employer"]:
                cur += f" at {ctx['current_employer']}"
            if ctx["work_years"]:
                cur += f" ({ctx['work_years']}y)"
            lines.append(cur)
        if ctx["top_skills"]:
            lines.append(f"Skills: {', '.join(ctx['top_skills'])}")
        if ctx["top_courses"]:
            lines.append(f"Courses: {', '.join(ctx['top_courses'])}")
        if ctx["best_case"]:
            lines.append(f"Best case study: {ctx['best_case']}")
        if ctx["cert_names"]:
            lines.append(f"Certifications: {', '.join(ctx['cert_names'])}")
        if ctx["overall_score"] > 0:
            lines.append(f"Overall score: {ctx['overall_score']}%")
        # v11.1: expose the single best score as a strength signal, but NOT
        # aggregate/overall score, course tallies, attempt counts, or
        # "consistency %" — those read as a progress log or a weak average
        # and must never appear in the summary (see prompt RULES).
        if ctx["best_test_score"] >= 60:
            lines.append(f"Strongest assessment score: {ctx['best_test_score']}%")
        if ctx["personality_type"] and ctx["personality_type"] != "Getting Started":
            lines.append(f"Personality: {ctx['personality_type']}")
        if ctx["personality_traits"]:
            lines.append(f"Traits: {ctx['personality_traits']}")
        if ctx["work_style"]:
            lines.append(f"Work style: {ctx['work_style']}")
        if ctx["linkedin_headline"]:
            lines.append(f"LinkedIn: {ctx['linkedin_headline']}")
        if ctx["career_goals"]:
            lines.append(f"Goal: {ctx['career_goals']}")
        if ctx["preferred_role"]:
            lines.append(f"Target role: {ctx['preferred_role']}")
        if ctx["hobbies"]:
            lines.append(f"Hobbies: {ctx['hobbies']}")
        if ctx["about_me"]:
            lines.append(f"About me: {ctx['about_me'][:200]}")
        if ctx["domain"]:
            lines.append(f"Domain: {ctx['domain']}")
        return "\n".join(lines)

    # ═══════════════════════════════════════════════════════════
    #  TEMPLATE FALLBACK
    # ═══════════════════════════════════════════════════════════

    def _template_summary(self, name: str, first_name: str, ctx: Dict) -> str:
        seed = ctx["_seed"]
        lead = ctx["_lead_angle"]
        bullets = []

        # Bullet 1: Lead (based on strongest signal)
        b1 = self._lead_bullet(ctx, seed, lead)
        if b1:
            bullets.append(b1)

        # Bullet 2: Courses / training (if available and not already in lead)
        if lead != "courses":
            b2 = self._courses_bullet(ctx, seed)
            if b2:
                bullets.append(b2)

        # Bullet 3: Scores (if available and not already in lead)
        if lead != "scores":
            b3 = self._scores_bullet(ctx, seed)
            if b3:
                bullets.append(b3)

        # Bullet 4: Personality OR hobbies (whichever has data)
        b4 = self._personality_bullet(ctx, seed)
        if b4:
            bullets.append(b4)

        # Bullet 5: Trajectory / goal (if available)
        b5 = self._trajectory_bullet(ctx, seed)
        if b5:
            bullets.append(b5)

        return "\n".join(bullets[:5])

    def _pick(self, variants: List[str], seed: int) -> str:
        return variants[seed % len(variants)]

    def _lead_bullet(self, ctx: Dict, seed: int, lead: str) -> str:
        if lead == "current_role":
            cd, ce = ctx["current_designation"], ctx["current_employer"]
            return self._pick([
                f"• Currently working as {cd} at {ce}.",
                f"• {cd} at {ce}.",
                f"• Active {cd} at {ce}.",
            ], seed)

        if lead == "work_experience":
            w = ctx["work_strs"][0]
            return self._pick([
                f"• Most recent role: {w}.",
                f"• Worked as {w}.",
                f"• Professional experience: {w}.",
            ], seed + 1)

        if lead == "scores":
            if ctx["best_case_score"] >= 60:
                return self._pick([
                    f"• Scored {ctx['best_case_score']:.0f}% on the {ctx['best_case_title']} case study on Upskillize.",
                    f"• Best case study score: {ctx['best_case_score']:.0f}% on {ctx['best_case_title']}.",
                ], seed + 2)
            if ctx["best_test_score"] >= 60:
                return f"• Best assessment score: {ctx['best_test_score']}% on Upskillize coursework."

        if lead == "courses":
            return self._courses_bullet(ctx, seed)

        if lead == "goals":
            goal = ctx["preferred_role"] or ctx["career_goals"]
            return f"• Targeting: {goal[:100]}."

        # education (default)
        if ctx["edu_str"]:
            if ctx["top_skills"]:
                sk = ", ".join(ctx["top_skills"][:3])
                return self._pick([
                    f"• {ctx['edu_str']} graduate, trained in {sk}.",
                    f"• {ctx['edu_str']} graduate with training in {sk}.",
                    f"• {ctx['edu_str']} — skills include {sk}.",
                ], seed + 3)
            return f"• {ctx['edu_str']} graduate."

        return f"• Early-career candidate in {ctx['domain']}."

    def _courses_bullet(self, ctx: Dict, seed: int) -> str:
        # v11.1: no "X of Y" tallies — name the coursework as a skill signal.
        courses = ctx["top_courses"]
        if not courses:
            return ""
        cl = ", ".join(courses[:2])
        if ctx["completed_courses"] > 0:
            return self._pick([
                f"• Trained in {cl} through Upskillize coursework.",
                f"• Completed {cl} on Upskillize.",
                f"• Upskillize coursework in {cl}.",
            ], seed + 4)
        return f"• Building {cl} skills through Upskillize."

    def _scores_bullet(self, ctx: Dict, seed: int) -> str:
        parts = []
        if ctx["best_case_score"] >= 60:
            parts.append(f"{ctx['best_case_score']:.0f}% on the {ctx['best_case_title']} case study")
        if ctx["best_test_score"] >= 60:
            parts.append(f"{ctx['best_test_score']}% best assessment score")
        if not parts:
            if ctx["cert_names"]:
                return f"• Holds {ctx['cert_names'][0]} certification."
            return ""
        joined = "; ".join(parts[:2])
        return self._pick([
            f"• Course scores: {joined}.",
            f"• Scores to date: {joined}.",
            f"• Best scores: {joined}.",
        ], seed + 5)

    def _personality_bullet(self, ctx: Dict, seed: int) -> str:
        ptype = ctx["personality_type"]
        if ptype and ptype not in ("Getting Started", ""):
            traits = ctx["personality_traits"]
            ws = ctx["work_style"]
            base = f"• {ptype}-type psychometric profile"
            if traits and ws:
                article = "an" if ws.lower()[0] in "aeiou" else "a"
                return self._pick([
                    f"{base} — {traits.lower()}, {ws.lower()} in team settings.",
                    f"{base}: {traits.lower()} with {article} {ws.lower()} approach.",
                    f"{base} indicating {traits.lower()} and {ws.lower()} work preferences.",
                ], seed + 6)
            if traits:
                return self._pick([
                    f"{base} — {traits.lower()}.",
                    f"{base}: {traits.lower()}.",
                ], seed + 6)
            return f"{base}."

        if ctx["hobbies"]:
            return self._pick([
                f"• Interests: {ctx['hobbies'].lower()}.",
                f"• Also interested in {ctx['hobbies'].lower()}.",
            ], seed + 7)
        return ""

    def _trajectory_bullet(self, ctx: Dict, seed: int) -> str:
        goal = ctx["preferred_role"] or ctx["career_goals"]
        if goal:
            g = goal[:100]
            return self._pick([
                f"• Targeting: {g}.",
                f"• Career direction: {g}.",
                f"• Goal: {g.lower()}.",
            ], seed + 8)
        # v11.1: dropped the improvement% / consistency% fallbacks — no weak
        # aggregate percentages in the summary.
        return ""

    # ═══════════════════════════════════════════════════════════
    #  UTILITIES
    # ═══════════════════════════════════════════════════════════

    @staticmethod
    def _clean_bullets(text: str, max_bullets: int = 5) -> str:
        lines = text.split("\n")
        clean = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("- "):
                line = "• " + line[2:]
            elif line.startswith("* "):
                line = "• " + line[2:]
            elif not line.startswith("•"):
                line = "• " + line
            # Strip agent names
            for agent in ("ProfileIQ", "TestGen", "AiRev", "NudgeAI", "InterviewIQ", "CareerIQ"):
                line = line.replace(agent, "AI agent")
            # Strip banned phrases (safety net)
            line_lower = line.lower()
            for phrase in BANNED_PHRASES:
                if phrase in line_lower:
                    # Replace the phrase with empty, clean up double spaces
                    import re
                    line = re.sub(re.escape(phrase), "", line, flags=re.IGNORECASE).strip()
                    line = re.sub(r"  +", " ", line)
                    # Fix dangling punctuation
                    line = line.replace(" ,", ",").replace(" .", ".").replace(",,", ",")
            clean.append(line)
        return "\n".join(clean[:max_bullets])

    @staticmethod
    def _derive_domain(course_names: list, education=None, work_experience=None, personal=None) -> str:
        if education is None:
            education = []
        if work_experience is None:
            work_experience = []
        if personal is None:
            personal = {}

        parts = list(course_names)
        for edu in education:
            parts.extend([edu.get("degree", ""), edu.get("field_of_study", ""), edu.get("institution", "")])
        for w in work_experience:
            parts.extend([w.get("title", ""), w.get("company", ""), w.get("description", "")])
        parts.extend([
            personal.get("career_goals", "") or "",
            personal.get("preferred_role", "") or "",
            personal.get("current_designation", "") or "",
            personal.get("linkedin_headline", "") or "",
            personal.get("key_skills", "") or "",
        ])

        text = " ".join(parts).lower()
        if not text.strip():
            return "Financial Services"

        domain_map = [
            (["business analy", "business intelligence"], "Business Analysis & Analytics"),
            (["ux", "user experience", "user interface", "ui design"], "UX/UI Design & Digital Product"),
            (["data analy", "data scien", "power bi", "tableau"], "Data Analytics & Business Intelligence"),
            (["web develop", "full stack", "frontend", "backend"], "Software Development & Engineering"),
            (["digital market", "marketing", "seo"], "Digital Marketing & Strategy"),
            (["fintech", "digital bank"], "FinTech & Digital Banking"),
            (["operations executive", "branch operations", "core banking"], "Banking Operations & Financial Services"),
            (["e-commerce", "ecommerce"], "E-Commerce & Digital Business"),
            (["payment", "card", "upi"], "Payment Systems & Digital Transactions"),
            (["banking", "b.com", "bcom", "commerce"], "Banking & Financial Services"),
            (["insurance"], "Insurance & Risk"),
            (["risk", "compliance"], "Risk & Compliance"),
            (["finance"], "Finance & Financial Services"),
            (["python", "java", "programming"], "Software Development"),
            (["design"], "Design & Creative Technology"),
        ]

        for keywords, domain in domain_map:
            if any(kw in text for kw in keywords):
                return domain

        return "Financial Services"