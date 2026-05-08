"""
Course Intelligence — Dynamic course analysis
═══════════════════════════════════════════════════════════════
Replaces the hardcoded COURSE_KEYWORD_MAP and ROLE_DATABASE in
role_matcher.py / achievement_engine.py.

Given a student's enrolled courses, derives — at generation time:
  • skills the courses teach
  • job roles the courses prepare students for
  • primary domain category

Means: any new course Upskillize launches is supported instantly.
No code change. No new role added to a Python dict.
The agent reads the course from the LMS, asks Sonnet to interpret
it, and feeds the result into the existing matching pipeline.

Performance: in-memory cache by course_id, so within one process
the same course is only analyzed once.

Cost: ~1 Sonnet call per profile generation, batched across all
of that student's enrolled courses (~$0.015 per profile).

Fallback chain:
  1. Sonnet  →  rich, accurate
  2. Rule-based keyword extraction from course name  →  good
  3. Generic seed defaults  →  safe baseline
"""

import os
import json
import logging
import re
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAS_API = bool(ANTHROPIC_API_KEY.strip())
MODEL_PRIMARY  = "claude-sonnet-4-6"
MODEL_FALLBACK = "claude-haiku-4-5-20251001"


# Generic baseline when AI is unavailable AND name has no recognizable keywords.
SEED_FALLBACK = {
    "skills": ["professional development", "industry knowledge"],
    "roles": ["Business Analyst", "Operations Analyst"],
    "domain": "General",
}


# Rule-based fallback patterns — used ONLY when AI is unavailable.
# Generic enough to cover most domains; not a hardcoded course list.
_NAME_PATTERNS = [
    # Pattern, derived skills, derived roles, domain
    ("credit",       ["credit risk", "credit analysis", "loan processing", "financial statements"],  ["Credit Analyst", "Lending Operations Associate"],         "BFSI"),
    ("invest",       ["investment analysis", "portfolio management", "wealth management"],           ["Investment Analyst", "Wealth Management Associate"],     "BFSI"),
    ("wealth",       ["wealth management", "financial planning", "advisory"],                        ["Wealth Management Associate", "Financial Advisor"],      "BFSI"),
    ("fintech",      ["fintech", "digital payments", "payment systems"],                             ["FinTech Product Analyst", "Digital Payment Specialist"], "FinTech"),
    ("payment",      ["digital payments", "UPI", "payment gateway", "merchant acquiring"],           ["Digital Payment Specialist", "Payment Operations Analyst"], "FinTech"),
    ("regtech",      ["regulatory compliance", "compliance", "risk management"],                     ["Compliance Officer", "Risk Operations Associate"],       "Compliance"),
    ("compliance",   ["regulatory compliance", "KYC", "AML", "audit"],                               ["Compliance Officer", "Risk Operations Associate"],       "Compliance"),
    ("risk",         ["risk management", "credit risk", "operational risk"],                         ["Risk Operations Associate", "Credit Analyst"],           "Risk"),
    ("insur",        ["insurance", "underwriting", "claims processing"],                             ["Insurance Analyst", "Underwriting Associate"],           "Insurance"),
    ("bank",         ["banking operations", "banking", "core banking", "KYC"],                       ["Operations Executive - Banking", "Banking Professional"], "BFSI"),
    ("data",         ["data analytics", "SQL", "data visualization", "reporting"],                   ["Data Analyst", "Business Intelligence Analyst"],         "Data"),
    ("analytics",    ["data analytics", "statistics", "reporting"],                                  ["Data Analyst", "Business Intelligence Analyst"],         "Data"),
    ("genai",        ["generative AI", "prompt engineering", "LLMs"],                                ["AI Product Analyst", "AI Engineer"],                     "AI/ML"),
    ("ml",           ["machine learning", "data analytics"],                                         ["ML Engineer", "Data Analyst"],                           "AI/ML"),
    ("ai",           ["artificial intelligence", "machine learning"],                                ["AI Product Analyst", "AI Engineer"],                     "AI/ML"),
    ("product",      ["product management", "user research", "roadmapping"],                         ["Product Analyst", "Associate Product Manager"],          "Product"),
    ("family business", ["family business", "succession planning", "governance"],                    ["Family Business Consultant", "Business Strategy Analyst"], "Family Business"),
    ("financ",       ["financial analysis", "financial modeling", "accounting"],                     ["Financial Analyst", "Business Analyst - BFSI"],          "BFSI"),
    ("market",       ["marketing", "brand management", "go-to-market"],                              ["Marketing Analyst", "Brand Associate"],                  "Marketing"),
    ("digital",      ["digital transformation", "digital strategy"],                                 ["Digital Strategy Analyst", "Technology Analyst"],        "Tech"),
    ("python",       ["python", "scripting", "data analytics"],                                      ["Data Analyst", "Technology Analyst"],                    "Tech"),
    ("ea ",          ["US tax", "IRS forms", "tax compliance"],                                      ["Enrolled Agent", "Tax Associate"],                       "BFSI"),
    ("ea-",          ["US tax", "IRS forms", "tax compliance"],                                      ["Enrolled Agent", "Tax Associate"],                       "BFSI"),
    ("eaprep",       ["US tax", "IRS forms", "tax compliance"],                                      ["Enrolled Agent", "Tax Associate"],                       "BFSI"),
]


class CourseIntelligence:
    """Stateless analyzer with per-process course-id cache."""

    def __init__(self):
        self.has_api = HAS_API
        self._client = None
        self._cache: Dict[Any, Dict] = {}   # course_id (or name) → derived metadata
        if HAS_API:
            try:
                import httpx
                self._client = httpx.Client(timeout=30.0)
            except ImportError:
                self.has_api = False
                logger.warning("CourseIntel: httpx not available, AI mode disabled")

    # ─────────────────────────────────────────────────────────────
    # Public entry point
    # ─────────────────────────────────────────────────────────────
    def analyze(self, courses: List[Dict]) -> Dict[str, Any]:
        """Analyze enrolled courses; aggregate to skills + roles + domain.

        Returns:
          {
            "skills":  [...],         # up to 20 unique, lowercase
            "roles":   [...],          # up to 6 unique job titles
            "domain":  "BFSI",        # most common domain across enrolled courses
            "per_course": {course_key: {skills, roles, domain}}
          }
        """
        if not courses:
            return {"skills": [], "roles": [], "domain": "Professional", "per_course": {}}

        # Split into cached / uncached
        uncached: List[Dict] = []
        per_course: Dict[Any, Dict] = {}
        for c in courses:
            key = c.get("course_id") or c.get("id") or (c.get("course_name") or c.get("name"))
            if key and key in self._cache:
                per_course[key] = self._cache[key]
            else:
                uncached.append(c)

        # Analyze uncached batch
        if uncached:
            if self.has_api:
                ai_results = self._ai_analyze_batch(uncached)
            else:
                ai_results = [self._rule_based(c) for c in uncached]

            for c, result in zip(uncached, ai_results):
                key = c.get("course_id") or c.get("id") or (c.get("course_name") or c.get("name"))
                if key is not None:
                    self._cache[key] = result
                    per_course[key] = result

        # Aggregate across all courses
        all_skills: List[str] = []
        all_roles:  List[str] = []
        domains:    List[str] = []
        for r in per_course.values():
            all_skills.extend(r.get("skills") or [])
            all_roles.extend(r.get("roles") or [])
            if r.get("domain"):
                domains.append(r["domain"])

        # Dedupe (case-insensitive, preserve first occurrence order)
        unique_skills = self._dedupe_lower(all_skills)[:20]
        unique_roles  = self._dedupe_preserve(all_roles)[:6]
        domain_pick   = max(set(domains), key=domains.count) if domains else "Professional"

        return {
            "skills":  unique_skills,
            "roles":   unique_roles,
            "domain":  domain_pick,
            "per_course": per_course,
        }

    # ─────────────────────────────────────────────────────────────
    # AI path
    # ─────────────────────────────────────────────────────────────
    def _ai_analyze_batch(self, courses: List[Dict]) -> List[Dict]:
        """One Sonnet call to analyze ALL uncached courses."""
        if not self._client:
            return [self._rule_based(c) for c in courses]

        course_summaries = []
        for c in courses:
            course_summaries.append({
                "name": (c.get("course_name") or c.get("name") or "Unknown course").strip(),
                "description": ((c.get("description") or c.get("course_description") or "")[:300]).strip(),
            })

        system = (
            "You analyze educational courses and derive what skills they teach "
            "and what real-world job roles they prepare students for. "
            "You output ONLY valid JSON — no markdown, no preamble, no explanation. "
            "You never invent metrics, percentages, or facts beyond what the course "
            "name and description imply."
        )

        user_prompt = f"""For each course below, output a JSON object with three fields:
  • skills: 5–10 lowercase keywords describing what the course teaches.
            Use industry-standard terms ("credit risk", "regulatory compliance",
            "machine learning", "digital payments"). Avoid generic words like
            "knowledge", "skills", "concepts".
  • roles:  2–4 real-world job titles this course prepares students for, in
            proper case ("Credit Analyst", "Risk Operations Associate"). Do NOT
            include skills or domains in this field — job titles only.
  • domain: ONE primary domain. Pick from this list ONLY:
            BFSI, FinTech, Data, AI/ML, Tech, Product, Risk, Compliance,
            Insurance, Family Business, Marketing, Operations, General.

COURSES TO ANALYZE:
{json.dumps(course_summaries, indent=2)}

OUTPUT — a JSON array, one object per course, in the SAME ORDER as the input.
No markdown fences, no preamble. Just the JSON array.

Example for two courses:
[
  {{"skills": ["credit risk", "loan processing", "banking operations", "regulatory compliance", "financial statements"], "roles": ["Credit Analyst", "Lending Operations Associate"], "domain": "BFSI"}},
  {{"skills": ["digital payments", "UPI", "payment gateway", "fintech", "merchant acquiring"], "roles": ["Payment Operations Analyst", "FinTech Product Analyst"], "domain": "FinTech"}}
]

Now produce the JSON array for the {len(course_summaries)} course(s) above:"""

        # Try primary model, fall back once to Haiku
        for model in (MODEL_PRIMARY, MODEL_FALLBACK):
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
                        "max_tokens": 1500,
                        "system": system,
                        "messages": [{"role": "user", "content": user_prompt}],
                    },
                )
                resp.raise_for_status()
                text_response = resp.json()["content"][0]["text"].strip()

                # Strip markdown fences if present
                if text_response.startswith("```"):
                    text_response = re.sub(r'^```\w*\n?', '', text_response)
                    text_response = re.sub(r'\n?```$', '', text_response)

                parsed = json.loads(text_response)
                if not isinstance(parsed, list) or len(parsed) != len(courses):
                    logger.warning(
                        f"CourseIntel: AI returned wrong shape "
                        f"(got {type(parsed).__name__} of len "
                        f"{len(parsed) if isinstance(parsed, list) else 0}, "
                        f"expected list of {len(courses)})"
                    )
                    return [self._rule_based(c) for c in courses]

                # Normalize each result
                cleaned: List[Dict] = []
                for item in parsed:
                    if not isinstance(item, dict):
                        cleaned.append(SEED_FALLBACK.copy())
                        continue
                    skills = [s.strip().lower() for s in (item.get("skills") or []) if isinstance(s, str) and s.strip()]
                    roles  = [r.strip()         for r in (item.get("roles")  or []) if isinstance(r, str) and r.strip()]
                    domain = str(item.get("domain") or "General").strip()
                    cleaned.append({
                        "skills": skills[:10],
                        "roles":  roles[:4],
                        "domain": domain,
                    })

                logger.info(f"CourseIntel: analyzed {len(cleaned)} courses via {model}")
                return cleaned

            except Exception as e:
                logger.warning(f"CourseIntel [{model}] failed: {e}")
                continue

        # Both AI models failed — rule-based fallback
        return [self._rule_based(c) for c in courses]

    # ─────────────────────────────────────────────────────────────
    # Rule-based fallback
    # ─────────────────────────────────────────────────────────────
    def _rule_based(self, course: Dict) -> Dict:
        """No-AI fallback: pattern-match on the course name itself."""
        name = (course.get("course_name") or course.get("name") or "").lower()
        if not name:
            return SEED_FALLBACK.copy()

        skills: List[str] = []
        roles:  List[str] = []
        domain: str = "General"

        for pattern, ks, rs, dom in _NAME_PATTERNS:
            if pattern in name:
                skills.extend(ks)
                roles.extend(rs)
                domain = dom
                # Don't break — accumulate across multiple matches (e.g. "AI in FinTech")
        # Dedupe within this single course
        skills = self._dedupe_lower(skills)
        roles  = self._dedupe_preserve(roles)

        if not skills and not roles:
            return SEED_FALLBACK.copy()

        return {
            "skills": skills[:10],
            "roles":  roles[:4],
            "domain": domain,
        }

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────
    @staticmethod
    def _dedupe_lower(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for item in items:
            if not isinstance(item, str):
                continue
            k = item.strip().lower()
            if k and k not in seen:
                seen.add(k)
                out.append(k)
        return out

    @staticmethod
    def _dedupe_preserve(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for item in items:
            if not isinstance(item, str):
                continue
            k = item.strip().lower()
            if k and k not in seen:
                seen.add(k)
                out.append(item.strip())
        return out