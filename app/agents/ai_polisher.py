"""
AI Profile Polisher — v3
═══════════════════════════
v3 changes:
  • Upgraded model: Haiku → Sonnet 4.6 (sharper headlines, better synthesis)
  • Higher token budget (1500 → 2500) for richer polishing
  • Headline guardrails: ONLY real job titles allowed
      — no skills (Web Developer, Backend Developer)
      — no domains (Banking & Payments, FinTech)
      — drawn from a curated whitelist
  • Fact-purity preserved (no invented metrics — kept from v2)
  • Tighter project/experience polishing — impact-led, not feature-led

Cost: ~$0.045 per profile (one Sonnet call).
Fallback: rule-based polish if API key missing.
"""

import os
import json
import logging
import re
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAS_API = bool(ANTHROPIC_API_KEY.strip())

# Model — Sonnet for synthesis quality.
MODEL_PRIMARY  = "claude-sonnet-4-6"
MODEL_FALLBACK = "claude-haiku-4-5-20251001"  # if primary fails (e.g. rate-limit)

# ═══════════════════════════════════════════════
# Whitelist — the ONLY job titles allowed in the headline.
# Mirror this with role_matcher.ROLE_DATABASE keys.
# ═══════════════════════════════════════════════
ALLOWED_HEADLINE_ROLES = [
    # Banking & Finance
    "Credit Analyst", "Business Analyst - BFSI", "Risk Operations Associate",
    "Compliance Officer", "Relationship Manager", "Operations Executive - Banking",
    "Financial Analyst", "Investment Analyst", "Wealth Management Associate",
    "Underwriting Associate", "Insurance Analyst",
    # FinTech
    "Digital Payment Specialist", "FinTech Product Analyst", "Digital Banking Associate",
    "Payment Operations Analyst",
    # Data & Tech
    "Data Analyst", "Business Intelligence Analyst", "Technology Analyst",
    "AI Product Analyst",
    # Product / Strategy
    "Product Analyst", "Business Strategy Analyst", "Family Business Consultant",
    "Banking Professional", "Financial Services Analyst",
    # Technology / Engineering — a learner's real profession or qualification
    # (from experience/education) may legitimately be a tech role. v13: the
    # headline is TWO roles — one BFSI/course-based + one profession-based —
    # so these must be allowed rather than banned.
    "Software Developer", "Software Engineer", "Full Stack Developer",
    "Backend Developer", "Frontend Developer", "Web Developer",
    "Technology Analyst", "Systems Engineer", "Application Developer",
    "Data Engineer", "QA Engineer",
]

# Banned phrases — if any appear in the polished headline, reject it and fall
# back to rule-based generation. v13: real job titles like "Software
# Developer" are NO LONGER banned (a student's actual profession/qualification
# may be a tech role). We still reject bare technology/skill tokens and
# malformed domain fragments that are not job titles.
BANNED_HEADLINE_TERMS = [
    "banking & payments", "fintech & ", "& payments",
    "html", "css", "react developer", "node developer", "python developer",
    "java developer",
]


def _title_case_fallback(title: str) -> str:
    if not title:
        return ""
    acronyms = {"lms", "api", "crm", "cms", "erp", "ui", "ux", "ai", "ml",
                "db", "sql", "jwt", "html", "css", "js", "aws", "gcp", "ci",
                "cd", "rest", "crud", "iot", "saas", "sdk", "cli", "http",
                "kyc", "aml", "upi", "neft", "rtgs", "imps", "rbi", "sebi",
                "irda", "irdai", "npa", "dpdpa", "regtech", "insurtech"}
    title = title.strip().strip("_-.")
    parts = re.split(r'[_\-]+', title)
    expanded = []
    for part in parts:
        expanded.extend(re.sub(r'([a-z])([A-Z])', r'\1 \2', part).split())
    result = []
    for w in expanded:
        if w.lower() in acronyms:
            result.append(w.upper())
        else:
            result.append(w.capitalize())
    return " ".join(result)


def _clean_description_fallback(desc: str) -> str:
    if not desc:
        return ""
    boilerplate = [
        "this repository contains", "this repo contains",
        "this is a simple", "this is a basic", "this is a",
        "a simple", "just a", "my first",
    ]
    cleaned = desc.strip()
    lower = cleaned.lower()
    for bp in boilerplate:
        if lower.startswith(bp):
            cleaned = cleaned[len(bp):].strip().lstrip("my the a an ")
            if cleaned:
                cleaned = cleaned[0].upper() + cleaned[1:]
            break
    return cleaned


def _title_case_hobby(raw: str) -> str:
    """'watching movie.' → 'Watching Movies', 'reading' → 'Reading'.
    Deterministic cleanup used for the fallback path and to normalise the
    chip label. Strips trailing punctuation, title-cases, and lightly
    pluralises a few common singular hobby nouns so labels read naturally."""
    if not raw or not isinstance(raw, str):
        return ""
    t = raw.strip().strip(".;,/|-_ ").strip()
    if not t:
        return ""
    # light, safe pluralisation for common hobby nouns entered singular
    plural = {
        "movie": "movies", "book": "books", "game": "games", "song": "songs",
        "novel": "novels", "puzzle": "puzzles", "sport": "sports",
    }
    words = t.split()
    words = [plural.get(w.lower(), w) for w in words]
    small = {"and", "of", "the", "to", "a", "an", "in", "on", "with"}
    out = []
    for i, w in enumerate(words):
        lw = w.lower()
        out.append(lw if (lw in small and i > 0) else (w[:1].upper() + w[1:].lower()))
    return " ".join(out)


def _split_hobbies(raw) -> List[str]:
    """Split an LMS hobbies free-text field into individual hobby tokens."""
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw if str(x).strip()]
    return [x.strip() for x in re.split(r"[,;/|\n]", str(raw)) if x.strip()]


def _fallback_cert_line(name: str, issuer: str) -> str:
    """Deterministic one-line certification description (first person, short)."""
    if not name:
        return ""
    if issuer:
        return f"Completed through {issuer.strip()} — validated, job-relevant training."
    return "Industry-recognised, validated training I completed."


def _fallback_achv_line(title: str, tag: str) -> str:
    """Deterministic one-line achievement description (first person, short)."""
    if not title:
        return ""
    if tag:
        return f"A {tag.strip().lower()} that reflects my consistent, assessed performance."
    return "A validated result that reflects my consistent, assessed performance."


def _variety_seed(name: str, user_id) -> str:
    """Short deterministic token unique per student, injected into the prompt
    so two learners with the SAME hobby still get differently-phrased lines.
    The model is told to use it only to vary tone/angle, never as content."""
    import hashlib
    basis = f"{name}|{user_id}"
    return hashlib.md5(basis.encode("utf-8")).hexdigest()[:8]


# Ordered angle bank — first person, short. The fallback (no-API) path rotates
# through these by a per-student offset so hobby lines differ across students.
_HOBBY_ANGLES = [
    "Keeps me curious and self-directed.",
    "Sharpens the focus I bring to detailed work.",
    "Builds the discipline I apply to problem-solving.",
    "Feeds the big-picture thinking I use to frame problems.",
    "Builds the persistence I rely on to finish hard work.",
]


def _fallback_beyond_work(name: str, hobbies: List[str], career_goal: str,
                          personality_type: str, personality_summary: str,
                          seed: str) -> Dict[str, Any]:
    """Deterministic Beyond Work descriptions when the API is unavailable.
    First person, short, point-wise. A per-student offset (from the seed)
    rotates the angle so students get different phrasings for the same hobby."""
    try:
        offset = int(seed, 16) % len(_HOBBY_ANGLES)
    except (ValueError, TypeError):
        offset = 0

    hobby_cards = []
    for i, h in enumerate(hobbies):
        clean = _title_case_hobby(h)
        if not clean:
            continue
        # line is the descriptive part only — the template prefixes the name.
        hobby_cards.append({"name": clean, "line": _HOBBY_ANGLES[(offset + i) % len(_HOBBY_ANGLES)]})

    goal_line = ""
    if career_goal and len(career_goal.strip()) > 4:
        goal_line = f"Working toward {career_goal.strip().rstrip('.')}, and building the depth to get there."

    persona_line = ""
    if personality_type:
        if personality_summary and len(personality_summary.strip()) > 8:
            persona_line = personality_summary.strip()
        else:
            art = "an" if personality_type.strip()[:1].lower() in "aeiou" else "a"
            persona_line = (f"My {personality_type.strip()} profile shapes how I approach "
                            f"ownership, collaboration, and judgement.")

    return {"hobby_cards": hobby_cards, "career_goal_line": goal_line,
            "personality_line": persona_line}


def _validate_headline(headline: str) -> bool:
    """Return True if headline is a clean role-only headline."""
    if not headline:
        return False
    h = headline.lower()
    # Reject if any banned term present
    for term in BANNED_HEADLINE_TERMS:
        if term in h:
            return False
    # Require pipe-separator structure (job titles separated by " | ")
    if "|" not in headline:
        # single-role headlines are okay if from whitelist
        return any(r.lower() in h for r in ALLOWED_HEADLINE_ROLES)
    # Multi-role headline: at least one segment must match whitelist
    segments = [s.strip() for s in headline.split("|")]
    matches = sum(1 for seg in segments
                  if any(r.lower() in seg.lower() for r in ALLOWED_HEADLINE_ROLES))
    return matches >= max(1, len(segments) - 1)


class AIPolisher:

    def __init__(self):
        self.has_api = HAS_API
        self._client = None
        if HAS_API:
            try:
                import httpx
                self._client = httpx.Client(timeout=45.0)
            except ImportError:
                self.has_api = False

    def _call_claude(self, system_prompt: str, user_prompt: str,
                     model: str = MODEL_PRIMARY) -> Optional[str]:
        if not self.has_api or not self._client:
            return None
        try:
            resp = self._client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 2500,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["content"][0]["text"]
        except Exception as e:
            logger.warning(f"AI Polisher [{model}] failed: {e}")
            # Try fallback model once
            if model == MODEL_PRIMARY:
                logger.info(f"Falling back to {MODEL_FALLBACK}")
                return self._call_claude(system_prompt, user_prompt, model=MODEL_FALLBACK)
            return None

    def polish_all(self, student_data: Dict[str, Any], merged_data: Dict[str, Any]) -> Dict[str, Any]:
        personal = student_data.get("personal", {}) or {}
        projects = merged_data.get("projects", []) or []
        work_exp = merged_data.get("work_experience", []) or []
        education = merged_data.get("education", []) or []
        skills = merged_data.get("all_skills", {}) or {}
        tech_skills = skills.get("technical_skills", [])
        courses = student_data.get("courses", []) or []

        name = (personal.get("full_name") or "Student").strip()
        designation = personal.get("current_designation", "") or ""
        bio = personal.get("about_me", "") or personal.get("bio", "") or ""
        edu_summary = ", ".join(
            f"{e.get('degree', '')} {e.get('field_of_study', '')} from {e.get('institution', '')}"
            for e in education if e.get("degree")
        )

        raw_projects = []
        for p in projects[:5]:
            raw_projects.append({
                "title": p.get("name") or p.get("title") or "",
                "desc": (p.get("description") or "")[:200],
                "tech": p.get("technologies") or p.get("languages") or "",
            })

        raw_experience = []
        for w in work_exp[:4]:
            raw_experience.append({
                "role": w.get("title") or w.get("role") or "",
                "company": w.get("company") or "",
                "duration": w.get("duration") or "",
                "desc": (w.get("description") or "")[:200],
            })

        raw_skills = [s.get("name", "") for s in tech_skills[:15] if s.get("name")]
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]

        # ── v13: Beyond Work / Certifications / Achievements inputs ──
        personality = merged_data.get("personality", {}) or {}
        personality_type = personality.get("personality_type") or ""
        personality_summary = personality.get("summary") or personality.get("traits") or ""
        if isinstance(personality_summary, (list, tuple)):
            personality_summary = ", ".join(str(x) for x in personality_summary if x)
        raw_hobbies = _split_hobbies(personal.get("hobbies", ""))
        career_goal = (personal.get("career_goals") or personal.get("preferred_role") or "").strip()
        seed = _variety_seed(name, personal.get("user_id") or personal.get("id") or "")

        certs_in = merged_data.get("certifications", []) or student_data.get("certifications", []) or []
        raw_certs = [{
            "name": (c.get("certificate_name") or c.get("name") or "").strip(),
            "issuer": (c.get("course_name") or c.get("issuer") or "").strip(),
        } for c in certs_in if (c.get("certificate_name") or c.get("name"))][:10]

        achv_in = merged_data.get("achievement_cards", []) or student_data.get("achievement_cards", []) or []
        raw_achv = [{
            "title": (a.get("title") or "").strip(),
            "tag": (a.get("tag") or "").strip(),
            "score": a.get("score", ""),
        } for a in achv_in if a.get("title")][:8]

        # Deterministic fallback for Beyond Work — always computed so the card
        # NEVER shows the false "complete the psychometric test" lock when a
        # type exists, even if the API path is unavailable or fails.
        fb_beyond = _fallback_beyond_work(
            name, raw_hobbies, career_goal,
            personality_type, personality_summary, seed,
        )

        if self.has_api and (raw_projects or raw_experience or raw_skills
                             or raw_hobbies or career_goal or personality_type
                             or raw_certs or raw_achv):
            result = self._ai_polish(
                name=name, designation=designation, bio=bio,
                edu_summary=edu_summary, raw_projects=raw_projects,
                raw_experience=raw_experience, raw_skills=raw_skills,
                course_names=course_names, raw_hobbies=raw_hobbies,
                career_goal=career_goal, personality_type=personality_type,
                personality_summary=personality_summary, raw_certs=raw_certs,
                raw_achv=raw_achv, seed=seed,
            )
            if result:
                # Validate headline — drop it if it contains banned terms,
                # so the orchestrator can fall back to rule-based.
                if result.get("polished_headline") and not _validate_headline(result["polished_headline"]):
                    logger.warning(
                        f"AI headline rejected (banned/invalid): "
                        f"{result['polished_headline']!r}"
                    )
                    result["polished_headline"] = ""
                # Backfill any missing Beyond Work fields from the fallback so
                # the section is never left blank.
                bw = result.get("beyond_work") or {}
                if not bw.get("hobby_cards"):     bw["hobby_cards"] = fb_beyond["hobby_cards"]
                if not bw.get("career_goal_line"): bw["career_goal_line"] = fb_beyond["career_goal_line"]
                if not bw.get("personality_line"): bw["personality_line"] = fb_beyond["personality_line"]
                result["beyond_work"] = bw
                return result

        rb = self._rule_based_polish(
            raw_projects=raw_projects,
            raw_experience=raw_experience,
            raw_skills=raw_skills,
        )
        # Attach deterministic descriptions to the rule-based path too.
        rb["beyond_work"] = fb_beyond
        rb["certifications"] = [
            {"name": c["name"], "line": _fallback_cert_line(c["name"], c["issuer"])}
            for c in raw_certs
        ]
        rb["achievements"] = [
            {"title": a["title"], "line": _fallback_achv_line(a["title"], a["tag"])}
            for a in raw_achv
        ]
        return rb

    def _ai_polish(self, name, designation, bio, edu_summary,
                   raw_projects, raw_experience, raw_skills, course_names,
                   raw_hobbies=None, career_goal="", personality_type="",
                   personality_summary="", raw_certs=None, raw_achv=None,
                   seed="") -> Optional[Dict]:
        raw_hobbies = raw_hobbies or []
        raw_certs = raw_certs or []
        raw_achv = raw_achv or []

        # ══════════════════════════════════════════════════════
        # System prompt — the rules.
        # ══════════════════════════════════════════════════════
        system = f"""You are a senior career editor at a top placement firm
that places candidates into BFSI, FinTech, and AI roles. You receive raw
student data and produce SHORT, PRECISE, RECRUITER-READY content.

ABSOLUTE RULES — violation = entire response rejected:

1. NEVER invent facts, metrics, numbers, user counts, percentages,
   or claims not present in the input. No "served 500+ users",
   no "reduced time by 40%", no "across 12 modules". If you don't
   see the number in the input, you do NOT write the number.

2. The headline field is EXACTLY TWO job titles separated by " | ".
   - Title 1: the candidate's strongest BFSI/FinTech/course-based role.
   - Title 2: a role reflecting their REAL profession, experience, or
     academic qualification — this MAY be a technology/engineering role
     (e.g. "Software Developer", "Full Stack Developer", "Data Analyst")
     if their education or work experience supports it.
   Choose from, or closely match, these titles:
   {', '.join(ALLOWED_HEADLINE_ROLES)}
   The headline is JOB TITLES ONLY — never bare skills or technology
   names (no "React", "Python", "HTML"), never a domain phrase.

3. Skills go into "skills_grouped", NOT into the headline.

4. Use crisp action verbs: Built, Engineered, Implemented, Designed,
   Analyzed, Modeled. No "responsible for", no "involved in",
   no "helped with".

5. Project/experience descriptions are 1 sentence each, max 22 words.
   Lead with what was built/analyzed. End with the domain or stack.

6. BEYOND WORK descriptions (hobbies, career goal, personality) and the
   certification / achievement lines are written in the FIRST PERSON, as the
   CANDIDATE speaking about themselves. Use "I", "my", "me". NEVER use the
   candidate's name and NEVER third person ("she", "her", "he", "they",
   "Ranjana"). It must read as if the candidate wrote it themselves.
   Keep every line SHORT and point-wise — ONE crisp sentence, max ~18 words,
   no filler, no "a habit of", no throat-clearing. Connect the item to a
   genuine professional strength. Personalize to THIS candidate (courses,
   goal, personality). Two candidates with the same hobby must get DIFFERENT
   lines; use VARIETY_SEED only to vary tone/angle, never as literal content.
   Do NOT invent achievements or numbers; interpretation is fine, fabricated
   facts are not.
   Examples (voice + length):
     hobby "Reading" → "Sharpens my ability to absorb complex material fast."
     personality "Integrity" → "My integrity-driven approach means teams can
       trust me with sensitive financial data."

7. Output MUST be valid JSON, no markdown fences, no explanation.
"""

        user_prompt = f"""Polish this candidate's data. Return JSON only.

NAME: {name}
DESIGNATION: {designation or 'Not specified'}
EDUCATION: {edu_summary or 'Not specified'}
BIO: {bio or 'Not provided'}
COURSES (LMS-recorded): {', '.join(course_names[:6]) or 'None'}
CAREER GOAL: {career_goal or 'Not specified'}
PERSONALITY TYPE (from psychometric test): {personality_type or 'Not taken'}
PERSONALITY NOTES: {personality_summary or 'None'}
HOBBIES (raw): {', '.join(raw_hobbies) if raw_hobbies else 'None'}
VARIETY_SEED: {seed or 'none'}

PROJECTS (raw):
{json.dumps(raw_projects, indent=2) if raw_projects else '[]'}

EXPERIENCE (raw):
{json.dumps(raw_experience, indent=2) if raw_experience else '[]'}

SKILLS (raw): {', '.join(raw_skills) if raw_skills else 'None'}

CERTIFICATIONS (raw):
{json.dumps(raw_certs, indent=2) if raw_certs else '[]'}

ACHIEVEMENTS (raw):
{json.dumps(raw_achv, indent=2) if raw_achv else '[]'}

Return this exact JSON shape:
{{
  "headline": "BFSI/Course Role | Profession-or-Qualification Role  (EXACTLY two titles, ' | ' separated)",
  "projects": [
    {{"title": "Clean Professional Title", "description": "One sentence, ≤22 words, action-verb led, only facts from input"}}
  ],
  "experience": [
    {{"role": "role", "company": "company", "description": "One sentence, ≤22 words, action-verb led"}}
  ],
  "skills_grouped": {{
    "Languages": ["Python", "JavaScript"], "Frameworks": ["Django", "React"],
    "Databases": ["MySQL", "MongoDB"], "Tools": ["Git", "Docker"]
  }},
  "bio_enhanced": "If a bio was provided, 2-sentence version with same facts. Else empty string.",
  "beyond_work": {{
    "personality_line": "FIRST PERSON ('I'/'my'), ONE short sentence interpreting my personality type. Empty string if no type.",
    "career_goal_line": "FIRST PERSON, ONE short sentence on my career goal with grounded ambition. Empty if no goal.",
    "hobby_cards": [
      {{"name": "Clean Title-Cased Hobby", "line": "FIRST PERSON, ONE short sentence (max ~18 words) linking the hobby to a strength — do NOT repeat the hobby name at the start"}}
    ]
  }},
  "certifications": [
    {{"name": "Certificate Name (unchanged)", "line": "FIRST PERSON, ONE short sentence on what this certification gives me professionally"}}
  ],
  "achievements": [
    {{"title": "Achievement Title (unchanged)", "line": "FIRST PERSON, ONE short sentence on why this achievement matters"}}
  ]
}}"""

        raw_response = self._call_claude(system, user_prompt)
        if not raw_response:
            return None

        try:
            cleaned = raw_response.strip()
            if cleaned.startswith("```"):
                cleaned = re.sub(r'^```\w*\n?', '', cleaned)
                cleaned = re.sub(r'\n?```$', '', cleaned)
            parsed = json.loads(cleaned)
            logger.info(
                f"AI Polisher [Sonnet]: polished {len(parsed.get('projects', []))} projects, "
                f"{len(parsed.get('experience', []))} experience entries, "
                f"{len(parsed.get('beyond_work', {}).get('hobby_cards', []))} hobbies"
            )
            return {
                "polished_projects": parsed.get("projects", []),
                "polished_experience": parsed.get("experience", []),
                "skills_grouped": parsed.get("skills_grouped", {}),
                "polished_headline": parsed.get("headline", ""),
                "polished_bio": parsed.get("bio_enhanced", ""),
                "beyond_work": parsed.get("beyond_work", {}) or {},
                "certifications": parsed.get("certifications", []) or [],
                "achievements": parsed.get("achievements", []) or [],
                "ai_polished": True,
            }
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"AI Polisher: failed to parse response: {e}")
            return None

    def _rule_based_polish(self, raw_projects, raw_experience, raw_skills) -> Dict:
        polished_projects = []
        for p in raw_projects:
            polished_projects.append({
                "title": _title_case_fallback(p.get("title", "")),
                "description": _clean_description_fallback(p.get("desc", "")),
            })

        polished_experience = []
        for e in raw_experience:
            polished_experience.append({
                "role": e.get("role", ""),
                "company": e.get("company", ""),
                "description": _clean_description_fallback(e.get("desc", "")),
            })

        # Categorize skills naively
        languages, frameworks, databases, tools = [], [], [], []
        lang_kw = {"python", "javascript", "java", "c++", "c#", "ruby", "go",
                   "typescript", "php", "swift", "kotlin", "rust", "dart"}
        fw_kw = {"react", "vue", "angular", "django", "flask", "fastapi",
                 "express", "spring", "rails", "next", "nuxt", "svelte",
                 "tailwind", "bootstrap"}
        db_kw = {"mysql", "postgres", "postgresql", "mongodb", "sqlite",
                 "redis", "oracle", "dynamodb", "firestore", "aiven"}
        for s in raw_skills:
            sl = s.lower().strip()
            if sl in lang_kw:        languages.append(s)
            elif sl in fw_kw:        frameworks.append(s)
            elif sl in db_kw:        databases.append(s)
            else:                    tools.append(s)

        return {
            "polished_projects": polished_projects,
            "polished_experience": polished_experience,
            "skills_grouped": {
                "Languages": languages,
                "Frameworks": frameworks,
                "Databases": databases,
                "Tools": tools,
            },
            "polished_headline": "",     # let orchestrator fall back to rule-based headline
            "polished_bio": "",
            "ai_polished": False,
        }