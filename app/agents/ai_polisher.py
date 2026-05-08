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
]

# Banned phrases — if any appear in the polished headline, reject it
# and fall back to rule-based generation in the orchestrator.
BANNED_HEADLINE_TERMS = [
    "web developer", "backend developer", "frontend developer", "full stack",
    "full-stack", "software engineer",
    "banking & payments", "fintech & ", "& payments",
    "developer ", " developer", "engineer ", " engineer",
    "html", "css", "react developer", "node developer", "python developer",
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

        if self.has_api and (raw_projects or raw_experience or raw_skills):
            result = self._ai_polish(
                name=name, designation=designation, bio=bio,
                edu_summary=edu_summary, raw_projects=raw_projects,
                raw_experience=raw_experience, raw_skills=raw_skills,
                course_names=course_names,
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
                return result

        return self._rule_based_polish(
            raw_projects=raw_projects,
            raw_experience=raw_experience,
            raw_skills=raw_skills,
        )

    def _ai_polish(self, name, designation, bio, edu_summary,
                   raw_projects, raw_experience, raw_skills, course_names) -> Optional[Dict]:

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

2. The headline field is for JOB TITLES ONLY, not skills, technologies,
   or domains. Allowed job titles (pick 2–3, separated by " | "):
   {', '.join(ALLOWED_HEADLINE_ROLES)}

   Forbidden in the headline: "Web Developer", "Backend Developer",
   "Software Engineer", "Banking & Payments", "FinTech Developer",
   any technology name (React, Python, Node), any domain word.

3. Skills go into "skills_grouped", NOT into the headline.

4. Use crisp action verbs: Built, Engineered, Implemented, Designed,
   Analyzed, Modeled. No "responsible for", no "involved in",
   no "helped with".

5. Project/experience descriptions are 1 sentence each, max 22 words.
   Lead with what was built/analyzed. End with the domain or stack.
   Example good: "Engineered a Razorpay payment integration with
   server-side signature verification — Node.js, Express, MySQL."
   Example bad: "Worked on payment integration project for the LMS
   platform using various technologies and frameworks."

6. Output MUST be valid JSON, no markdown fences, no explanation.
"""

        user_prompt = f"""Polish this candidate's data. Return JSON only.

NAME: {name}
DESIGNATION: {designation or 'Not specified'}
EDUCATION: {edu_summary or 'Not specified'}
BIO: {bio or 'Not provided'}
COURSES (LMS-verified): {', '.join(course_names[:6]) or 'None'}

PROJECTS (raw):
{json.dumps(raw_projects, indent=2) if raw_projects else '[]'}

EXPERIENCE (raw):
{json.dumps(raw_experience, indent=2) if raw_experience else '[]'}

SKILLS (raw): {', '.join(raw_skills) if raw_skills else 'None'}

Return this exact JSON shape:
{{
  "headline": "Job Title 1 | Job Title 2 | Job Title 3 — pick from the allowed list above, fitted to the candidate's courses + skills",
  "projects": [
    {{"title": "Clean Professional Title", "description": "One-sentence description, ≤22 words, action-verb led, only facts from input"}}
  ],
  "experience": [
    {{"role": "role", "company": "company", "description": "One-sentence description, ≤22 words, action-verb led"}}
  ],
  "skills_grouped": {{
    "Languages": ["Python", "JavaScript"],
    "Frameworks": ["Django", "React"],
    "Databases": ["MySQL", "MongoDB"],
    "Tools": ["Git", "Docker"]
  }},
  "bio_enhanced": "If a bio was provided, return a 2-sentence version with the same facts. Else return empty string."
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
                f"{len(parsed.get('experience', []))} experience entries"
            )
            return {
                "polished_projects": parsed.get("projects", []),
                "polished_experience": parsed.get("experience", []),
                "skills_grouped": parsed.get("skills_grouped", {}),
                "polished_headline": parsed.get("headline", ""),
                "polished_bio": parsed.get("bio_enhanced", ""),
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