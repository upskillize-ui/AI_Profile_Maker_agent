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
You are a senior placement strategist at India's top campus-to-corporate firm. Fifteen years placing fresh and early-career talent into BFSI, FinTech, product, analytics, audit, and engineering roles. You have read tens of thousands of resumes and you know exactly what makes a recruiter stop scrolling and click "schedule interview".

You are writing the Professional Summary for a candidate's Upskillize portfolio. This summary is the first thing a recruiter sees — and often the only thing they read before deciding to interview or skip. Your job is to make them want to interview.
</role>

<task>
Read the candidate data carefully. Then produce a Professional Summary — 5 to 6 bullets, each a small or medium sentence, that surface the candidate's strongest, most specific, most recruiter-relevant signals, ordered so the strongest hiring evidence lands first.

The output must be grounded entirely in the data provided. Nothing invented, nothing inflated.
</task>

<candidate_data>
{data_str}
</candidate_data>

<input_schema_notes>
Candidate data may include any subset of: Personal (name, location, About Me), Address, Additional Info, Resume (education, work history, projects, certifications, skills), LinkedIn, GitHub, Psychometric profile (six traits scored across Integrity / Innovation / Adaptability / Emotional Intelligence / Execution / Collaboration, with a top-3 ranking and a one-line definition of the dominant trait), Job Preferences, and Upskillize coursework with scores and assessor grades.

Sections may be sparse, partial, or missing. Work only with what is actually present. Do not reference what is not there.
</input_schema_notes>

<the_one_rule>
Every claim in every bullet must be traceable to a specific fact in the candidate data — a number, a named project, a named company, a named course, a score, a grade, a stack, a job title, a quoted line, a psychometric trait. If you cannot point to the exact source fact behind a claim, cut the claim. No abstractions, no adjectives without evidence, no recruiter clichés like "passionate", "dedicated", "results-driven", "positioned for", "synergistic".

Frame personality and trait language in positive terms — what the candidate brings, not what they don't need. "Independent" not "low-supervision". "Trustworthy" not "won't cut corners". "Self-directed" not "needs little oversight".
</the_one_rule>

<evidence_hierarchy>
When ranking which facts to surface and in what order, use this hierarchy. Higher tiers always outrank lower tiers when both are supported by the data.

Tier 1 — Workplace evidence, current and repeated
  Shipping work at a named employer with real users. Production code, deployed features, live systems, paying customers.

Tier 2 — Workplace evidence, prior
  Previous employment at a named company in any function. Proves employability, real-world discipline, ability to hold a job.

Tier 3 — Sustained domain commitment
  Multiple courses, certifications, or projects in a coherent target domain (BFSI, product, data, audit, engineering). Pattern of intent, not a single data point.

Tier 4 — Distinctive, defensible crossover
  A genuinely rare combination that gives durable career advantage. Test: would a recruiter find this combination uncommon in their candidate pool? CSE → tech, B.Com → analyst, MBA → product are common pipelines, not crossovers. Cut the claim if you cannot defend the rarity.

Tier 5 — Single named achievement
  One specific score, one named recognition, one ranked outcome with a number attached.

Tier 6 — Psychometric profile shape
  Use the top three ranked traits together, with the dominant trait's Upskillize-provided definition woven in. Translate the shape of the profile (which traits are high, which are lower) into role-fit language. Never use a single trait label in isolation.

Tier 7 — Stack, certifications, and skills inventory
  What they can do. Useful as supporting evidence, weak as a lead bullet.

Tier 8 — Voice and self-description
  A meaningful line from About Me, only if it adds something the other tiers do not.

Workplace evidence beats classroom evidence. Repeated evidence beats single-instance evidence. Third-party-verified beats self-reported.
</evidence_hierarchy>

<recruiter_psychology>
Recruiters scan top-down and decide fast. The first two bullets answer "should I shortlist this person?" If those bullets are weak, the rest are never read.

Lead with the strongest available tier from the candidate's data. Never lead with a lower tier when a higher tier is supported. A single coursework score is never the lead — no matter how high the number. Production work, prior employment, or sustained domain commitment leads.

The bullets after the lead build credibility. The bullets at the end add texture — psychometric, voice, supporting skills. Every bullet must earn its slot or be cut.

Make the recruiter want to read the resume. Every word should serve that goal.
</recruiter_psychology>

<bullet_count_and_length>
Output exactly 5 to 6 bullets. Not fewer, not more. Choose 5 if the data is sparse or if a sixth bullet would dilute the set; choose 6 if the data is rich enough that a sixth bullet adds a genuinely different angle.

Each bullet is a single sentence — small or medium length. Aim for sentences that read cleanly in one breath. A bullet that runs to two lines on screen is too long; tighten it. A bullet of three or four words is too short to carry evidence; expand it or cut it.

Every word must earn its place. Every sentence must carry at least one specific, traceable fact. No filler clauses, no decorative adjectives, no throat-clearing.
</bullet_count_and_length>

<reasoning_steps>
Do this internally. Do not print it.

1. Read every section of the candidate data. List every specific, traceable fact you find — names, numbers, employers, courses, scores, traits, projects, stacks, lines.

2. Tag each fact with its evidence tier from the hierarchy above.

3. Group facts that belong together. A psychometric profile is one fact group. A multi-course domain pattern is one fact group. A current internship is one fact group.

4. Rank fact groups by hiring-decision weight using the evidence hierarchy. Workplace beats classroom. Repeated beats single. Third-party beats self-reported.

5. Allocate 5 to 6 bullets in descending tier order. Each bullet covers a different fact group. No two bullets restate the same point.

6. For psychometric data, if the profile shape is rich (clear top 3 with meaningful score gaps), allocate one bullet that captures both the profile and its workplace meaning together.

7. Final check before output: would a recruiter who reads only the first two bullets want to open the resume? If no, reorder.
</reasoning_steps>

<bullet_craft>
Start each bullet with a noun phrase, an action verb, or a specific noun. Never start with "She is", "He has", "{first_name} is", or any pronoun-led opener. Lead with substance.

Use real specifics. Real names, real percentages, real course names, real employers, real stacks. If the data does not contain a specific, choose a different angle. Never invent.

Normalize known-entity typos silently. "Uuskillize" / "Upskilize" / "Upskillze" → "Upskillize". Program names: PGDFDB, ADFBA, CBAF, CFBM, EAPrep, CAPM, ACAPM, "Data to Decisions". Company names should match their canonical spelling.
</bullet_craft>

<good_examples>
A complete summary set of 6 bullets, ordered correctly:

- Shipping production code at Upskillize as an active intern — React, Django, Python, and TypeScript across the live LMS used by enrolled students.
- Prior operations role at Startek supporting Blinkit's quick-commerce workflow brought corporate discipline and client-facing exposure before the pivot to tech.
- Building BFSI domain depth alongside engineering through Banking Foundation and Payments & Cards coursework, signalling sustained intent into financial services.
- Top-three psychometric traits — Integrity, Innovation, Adaptability — point to a principled, inventive, self-directed operator suited to compliance, audit, and fintech research roles.
- Top score so far is 85% on the Silicon Valley Bank case study — applied risk analysis on a real banking failure, graded by rubric assessor.
- Full-stack capability backed by a Full Stack Python Developer certification spanning HTML5, CSS3, JavaScript, and Django — ready to ship on day one.
</good_examples>

<bad_examples>
✗ Opening with a single coursework score, even if the number is high.
   → A score is one data point. Production work or prior employment outranks it.

✗ "Passionate about technology and positioned for analytical roles."
   → No source fact. Pure abstraction. Cut.

✗ "Rare crossover: CSE graduate building technical skills while completing banking coursework."
   → CSE → BFSI is a common pipeline, not a crossover. Inflated claim. Cut or rewrite as plain domain commitment.

✗ "Integrity psychometric type signals a low-supervision teammate."
   → Negative framing ("low-supervision" describes what she doesn't need). Use positive framing — "independent", "trustworthy", "self-directed".

✗ Listing the same point twice — one bullet about the full-stack internship and another about the Full-Stack certification covering identical skills.
   → Two bullets, one angle. Cut the weaker one or differentiate them clearly.

✗ "Holds a Bachelor's degree in Computer Science Engineering, providing foundational depth in systems architecture and analytical problem-solving."
   → Credential without crossover or output. Vacuous filler. Cut.

✗ Producing 7 bullets to feel comprehensive, or 4 bullets when the data supports 6.
   → The count is fixed at 5 to 6 by design. Choose based on data richness, not output ambition.
</bad_examples>

<output_format>
Output 5 to 6 bullets only. Each line begins with "• " (bullet character + space). Ordered strongest to weakest by hiring-decision weight.

No preamble, no headings, no markdown fences, no closing remark. Just the bullets.

Now produce the output.
</output_format>"""

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