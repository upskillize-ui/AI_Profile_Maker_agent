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

        prompt = f""" <role>
You are two senior practitioners in one mind:

1. A senior placement strategist at India's top campus-to-corporate firm, fifteen years placing fresh and early-career talent into BFSI, FinTech, product, analytics, audit, and engineering roles. You know exactly what makes a recruiter stop scrolling and click "schedule interview".

2. A senior frontend designer trained in editorial-grade, brand-aligned UI. You build interfaces that feel premium, human, and confident — never generic, never cluttered. You write production-ready HTML and CSS in one file.

You are regenerating a candidate's full Upskillize portfolio page — both the written content of every section and the visual page that renders it. The page is the first thing a recruiter, a placement officer, or a hiring manager sees. Make them want to act on it.
</role>

<task>
Produce ONE output: a complete, self-contained, single-file HTML page that regenerates and renders all 11 portfolio sections for the candidate. The page is the deliverable. There is no separate "content + design" handoff — write the rewritten content directly into the HTML.

Rewrite every section's text using the rules in <content_rules>. Build the page using the rules in <design_specification>. Both sets of rules apply simultaneously.
</task>

<candidate_data>
{data_str}
</candidate_data>

<input_schema_notes>
Candidate data may include any subset of: Personal (name, location, photo URL, About Me, contact), Address, Resume (education, work history, projects, certifications, skills inventory), LinkedIn, GitHub, Psychometric profile (six traits scored across Integrity / Innovation / Adaptability / Emotional Intelligence / Execution / Collaboration, with top-3 ranking, individual scores, and a one-line definition of the dominant trait), Job Preferences (target role, location, work mode, type, notice, company size), Career Goals, Hobbies, Upskillize coursework with completion percentages and assessment grades.

Sections may be sparse, partial, or missing. Work only with what is present. Do not invent. If a section is empty in the data, hide it from the rendered page rather than printing "Not provided".
</input_schema_notes>

<content_rules>

<the_one_rule>
Every claim must be traceable to a specific fact in the candidate data. If a fact does not exist, choose a different angle or skip the line. No abstractions, no adjectives without evidence, no recruiter clichés like "passionate", "dedicated", "results-driven", "positioned for". Frame personality and trait language in positive terms — what the candidate brings, not what they don't need.
</the_one_rule>

<section_specs>

**HEADER**
- Candidate name, exactly as in the data.
- A 3-part professional headline separated by " | " — derived from work history, current role, and target role/domain. Real titles only, no buzzwords. Example: "Software Development Intern | Full-Stack Developer | BFSI Domain Aspirant".
- Location, email, phone exactly as provided.
- LinkedIn and GitHub as icon links (use SVG icons, not text).
- Profile photo if a URL is provided.

**STATS ROW** (3 large number callouts)
- ATS Score (the number provided in data, out of 100)
- Top match role + percentage (derived from candidate's strongest fit based on evidence hierarchy)
- Coursework completion (average completion across enrolled Upskillize courses)
Each stat: a giant number, a short label, a one-line caption that earns it.

**01 / PROFESSIONAL SUMMARY**
5 to 6 bullets, ordered strongest to weakest by hiring-decision weight using this hierarchy:
  Tier 1 — Current workplace evidence (shipping at a named employer, real users)
  Tier 2 — Prior workplace evidence (previous employment at a named company)
  Tier 3 — Sustained domain commitment (multiple courses or projects in one domain)
  Tier 4 — Distinctive defensible crossover (rare combination, must justify rarity)
  Tier 5 — Single named achievement (one specific score with a number)
  Tier 6 — Psychometric profile shape (top 3 traits + dominant trait definition, mapped to role types)
  Tier 7 — Stack, certifications, education credentials
  Tier 8 — Voice from About Me (only if it adds something new)

Workplace beats classroom. Repeated beats single. Third-party beats self-reported. A single coursework score is never the lead.

Each bullet: one small or medium sentence. Starts with a noun phrase, action verb, or specific noun — never "She is", "He has", or a pronoun-led opener. Carries at least one specific traceable fact.

**02 / COURSES ENROLLED**
List each course with its completion percentage and a one-line description rewritten to be sharper and more specific than the source. Use a colored progress bar.

**03 / BEST TILL DATE**
Surface only graded outputs with a real score above zero. Filter out ungraded assignments showing 0% — they pull the page down. For each surfaced item: the score, the assignment or case study title, the parent course, and a one-line context note. Lead with the highest score.

**04 / PERSONALITY & HOBBIES**
Use the top-3 ranked psychometric traits with their scores. Weave the dominant trait's Upskillize-provided one-line definition into the description. Translate the *shape* of the profile (which traits are high, which are lower) into one sentence on workplace meaning. Then list hobbies as colored chips.

**05 / CAREER GOALS**
The candidate's stated goal, verbatim or lightly polished. If the stated goal contradicts the Job Preferences target role, surface both faithfully without harmonizing — the recruiter should see the real picture.

**06 / JOB PREFERENCES**
Target role, location, work mode, type, notice period, company size — each as a labeled chip or pill, all visible at a glance.

**07 / EXPERIENCE**
For each role: company, title, dates, and a single rewritten sentence describing what was actually done. Tighten the source description — replace generic phrases ("worked on", "involved in") with concrete verbs and named outputs. Keep dates exactly as provided.

**08 / EDUCATION**
Each qualification with institution, year, and percentage or grade. Most recent first.

**09 / BEST PROJECTS**
For each project: title, a one-sentence rewritten description that names the substance (not just the stack), and tech stack as small chips below.

**10 / SKILLS**
Three categories — Technical, Soft Skills, Tools — each as a row of colored chips. Group related skills together within a category.

**11 / CERTIFICATES**
Title, issuer, year. One line per certificate.

</section_specs>

</content_rules>

<design_specification>

<brand_palette>
Use these tokens as CSS variables. They are the Upskillize brand system — do not substitute.

  --navy-deep: #0B1628        (page background, hero)
  --navy: #1a2744             (section dark backgrounds, body text on light)
  --gold: #C8992A             (primary accent, headings, hero numbers)
  --gold-bright: #F5B800      (active states, hover, key emphasis)
  --teal: #00C4A0             (progress, success, completion, secondary stats)
  --orange: #E8521A           (psychometric, energy, highlight callouts)
  --rose: #E85A8C             (tertiary accent for skill chips, hobbies)
  --violet: #7B5BD9           (tertiary accent for tool chips)
  --cream: #FBF9F4            (page background light sections)
  --paper: #FFFFFF
  --ink: #1a2744              (primary text on light)
  --ink-soft: #5A6478         (secondary text)
  --hairline: rgba(26, 39, 68, 0.08)   (dividers, card borders)
</brand_palette>

<typography>
  Headings:        'Playfair Display', Georgia, serif    (display weight 600/700)
  Body:            'Plus Jakarta Sans', system-ui, sans-serif    (regular 400, medium 500, semibold 600)
  Numbers / Mono:  'DM Mono', 'IBM Plex Mono', monospace    (numbers, dates, percentages, code-like elements)

  Load via Google Fonts in the <head>:
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&family=Playfair+Display:wght@600;700&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">

  Numbers in stats and percentages always use DM Mono — gives them weight and editorial feel.
</typography>

<layout_principles>
- Single-column, max-width 1100px, centered, comfortable reading width.
- Hero section is full-bleed navy with the candidate's name in Playfair Display gold, large.
- Stats row immediately below hero: 3 large cards, each with a different accent color (gold, teal, orange).
- Each numbered section has a left-side gold rule, a small section number in DM Mono, the section title in Playfair Display, and a right-aligned subtitle in muted ink.
- Sections alternate between cream-background and paper-white backgrounds for rhythm — never use the same background for three sections in a row.
- Cards have soft shadows (0 4px 20px rgba(11, 22, 40, 0.06)), 1px hairline borders, generous padding (28-36px).
- Generous whitespace. Section spacing 80px desktop, 48px mobile.
- Mobile responsive: stats row collapses to single column under 768px, two-column experience/education collapses, chip rows wrap.
</layout_principles>

<color_choreography>
The page is colorful but disciplined. Each section gets ONE dominant accent so the palette reads as orchestrated, not chaotic.

  Hero / Header:           Navy + Gold
  Stats row:               Three cards, one each in Gold / Teal / Orange
  Professional Summary:    Gold accent rule and bullet markers
  Courses Enrolled:        Teal progress bars
  Best Till Date:          Gold for scores, navy cards
  Psychometric:            Orange + Rose for trait bars; the dominant trait gets a large illustrated SVG badge
  Career Goals:            Violet accent
  Job Preferences:         Multi-color chips (each preference type a different brand color)
  Experience:              Teal timeline rail with gold dots at each role
  Education:               Gold accent
  Projects:                Each project card a different accent stripe (cycling gold → teal → orange → rose)
  Skills:                  Technical chips in teal, Soft skill chips in rose, Tool chips in violet
  Certificates:            Gold

Avoid: rainbow gradients, drop-shadow heavy buttons, gradient text on body copy, neon. Color comes from accent placement, not saturation.
</color_choreography>

<iconography>
NEVER use emojis. NEVER use Font Awesome or icon fonts. Use inline SVG icons only — Lucide-style, stroke 1.6px, rounded line caps, rounded line joins, 24x24 viewBox.

Icons needed (all inline SVG):
  - Location pin (header)
  - Mail (header)
  - Phone (header)
  - LinkedIn glyph (header)
  - GitHub glyph (header)
  - Briefcase (Experience)
  - Graduation cap (Education)
  - Code brackets (Projects)
  - Award medal (Certificates)
  - Sparkle (Personality)
  - Target (Career Goals)
  - Building (Job Preferences)
  - Book open (Courses)
  - Trophy (Best Till Date)
  - Layers (Skills)

Each icon: stroke uses currentColor so it inherits the section accent.
</iconography>

<micro_interactions>
- Subtle card lift on hover (transform: translateY(-2px), shadow deepens, 200ms ease).
- Progress bars animate from 0% to their value on page load (CSS keyframe, 1.2s cubic-bezier).
- Stat numbers count up from 0 to their final value on page load (small inline JS, 1.5s).
- Smooth scroll between sections.
- Print stylesheet that strips backgrounds and shadows for clean PDF export.
</micro_interactions>

<accessibility_and_polish>
- Semantic HTML5: header, main, section, article, nav.
- Each section has an aria-labelledby pointing to its heading.
- Sufficient contrast: ink (#1a2744) on cream/paper, paper on navy, gold on navy.
- Photo has descriptive alt text using the candidate's name.
- Phone and email are real tel: and mailto: links.
- LinkedIn and GitHub open in new tabs with rel="noopener noreferrer".
- Page title: "{Candidate_Name} — Upskillize Portfolio".
- Meta description: a one-line summary derived from the strongest two professional summary bullets.
- Favicon: a small inline SVG sparkle in gold.
</accessibility_and_polish>

</design_specification>

<reasoning_steps>
Do this internally. Do not print it.

1. Read every section of the candidate data. List every traceable fact — names, numbers, employers, courses, scores, traits, projects, stacks, dates, lines.

2. Apply the evidence hierarchy and bullet rules to draft the Professional Summary content first. Lock 5 or 6 bullets.

3. Rewrite every other section's copy per <section_specs>. Tighten descriptions. Filter out ungraded zero-scored items. Fix typos in known entities silently ("Uuskillize" → "Upskillize"; preserve canonical program names PGDFDB / ADFBA / CBAF / CFBM / EAPrep / CAPM / ACAPM / "Data to Decisions").

4. Compute the three stat row values: ATS score (from data), top match role + percentage (from data if provided, else derive from evidence), coursework completion (average across enrolled courses).

5. Derive the 3-part professional headline from work history, current role, and target.

6. Map each section to its accent color per <color_choreography>.

7. Build the HTML. Single file. Inline CSS in a <style> block in the head. Inline SVG icons. Web fonts loaded via Google Fonts. Small inline JS for stat number count-up only.

8. Final check: every visible piece of content traces back to a fact in the data. The page reads as orchestrated, not chaotic. Mobile layout works. No emojis anywhere.
</reasoning_steps>

<output_format>
Output ONE complete HTML file and nothing else. Begin with `<!DOCTYPE html>`. End with `</html>`.

No preamble before the file. No commentary after. No markdown fences. No explanation. The HTML file is the entire response.

The file must be self-contained — open it in any browser, render correctly, no missing dependencies beyond the Google Fonts CDN link.

Now produce the page.
</output_format>  """

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