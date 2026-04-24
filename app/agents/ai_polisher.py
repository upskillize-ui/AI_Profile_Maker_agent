"""
AI Profile Polisher — v2
════════════════════════
Single Claude Haiku call that transforms raw student data into
recruiter-impressive, professionally articulated content.

v2 FIX: Removed "Add quantifiable impact where reasonable" instruction
that caused the AI to invent fake metrics like "serving 500+ users",
"reduced processing time by 40%", etc.

What it polishes:
  1. Project titles & descriptions (GitHub ugliness → professional)
  2. Work experience descriptions (generic → articulated)
  3. Skills grouping & prioritization
  4. Headline optimization
  5. Education formatting

Cost: ~$0.003 per profile (one Haiku call, ~400 input + ~600 output tokens)
Fallback: If no API key, returns data unchanged (zero cost)
"""

import os
import json
import logging
import re
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAS_API = bool(ANTHROPIC_API_KEY.strip())


def _title_case_fallback(title: str) -> str:
    if not title:
        return ""
    acronyms = {"lms", "api", "crm", "cms", "erp", "ui", "ux", "ai", "ml",
                "db", "sql", "jwt", "html", "css", "js", "aws", "gcp", "ci",
                "cd", "rest", "crud", "iot", "saas", "sdk", "cli", "http"}
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


class AIPolisher:

    def __init__(self):
        self.has_api = HAS_API
        if HAS_API:
            try:
                import httpx
                self._client = httpx.Client(timeout=30.0)
            except ImportError:
                self._client = None
                self.has_api = False

    def _call_haiku(self, system_prompt: str, user_prompt: str) -> Optional[str]:
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
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 1500,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["content"][0]["text"]
        except Exception as e:
            logger.warning(f"AI Polisher API call failed: {e}")
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
                return result

        return self._rule_based_polish(
            raw_projects=raw_projects,
            raw_experience=raw_experience,
            raw_skills=raw_skills,
        )

    def _ai_polish(self, name, designation, bio, edu_summary,
                   raw_projects, raw_experience, raw_skills, course_names) -> Optional[Dict]:

        # ══════════════════════════════════════════════════════
        # v2 FIX: Removed "Add quantifiable impact" instruction
        # that caused fake metrics like "serving 500+ users"
        # ══════════════════════════════════════════════════════
        system = """You are a professional profile writer for a career platform.
You receive raw student/professional data and REWRITE it to be clean,
professional, and recruiter-ready.

STRICT RULES:
- NEVER invent facts, metrics, numbers, user counts, percentages, or any data not in the input.
- NEVER add fake quantifiable claims like "serving 500+ users", "reduced time by 40%", "processing 1000+ records".
- If no description is provided, write a SHORT generic one based only on the title and tech stack.
- Use action verbs: Built, Developed, Implemented, Designed, Engineered
- Clean ugly code-style names: "Lms_portal" → "Learning Management Portal"
- Make descriptions concise (1-2 sentences each) using ONLY information from the input.
- Group skills logically: Languages, Frameworks, Databases, Tools
- Respond ONLY with valid JSON, no markdown fences, no explanation."""

        user_prompt = f"""Polish this student's profile data. Return JSON only.

NAME: {name}
DESIGNATION: {designation or 'Not specified'}
EDUCATION: {edu_summary or 'Not specified'}
BIO: {bio or 'Not provided'}
COURSES: {', '.join(course_names[:5]) or 'None'}

PROJECTS (raw):
{json.dumps(raw_projects, indent=2) if raw_projects else '[]'}

EXPERIENCE (raw):
{json.dumps(raw_experience, indent=2) if raw_experience else '[]'}

SKILLS (raw): {', '.join(raw_skills) if raw_skills else 'None'}

Return this exact JSON structure:
{{
  "projects": [
    {{"title": "Cleaned Professional Title", "description": "Clean 1-2 sentence description using ONLY facts from the input"}}
  ],
  "experience": [
    {{"role": "role", "company": "company", "description": "Professional description using ONLY facts from the input"}}
  ],
  "skills_grouped": {{
    "Languages": ["Python", "JavaScript"],
    "Frameworks": ["Django", "React"],
    "Databases": ["MySQL", "MongoDB"],
    "Tools": ["Git", "Docker", "VS Code"]
  }},
  "headline": "2-3 role headline based on courses and skills, e.g.: Credit Analyst | Banking Operations | Risk Assessment",
  "bio_enhanced": "2-3 sentence professional bio if original bio was provided, else empty string"
}}"""

        raw_response = self._call_haiku(system, user_prompt)
        if not raw_response:
            return None

        try:
            cleaned = raw_response.strip()
            if cleaned.startswith("```"):
                cleaned = re.sub(r'^```\w*\n?', '', cleaned)
                cleaned = re.sub(r'\n?```$', '', cleaned)
            parsed = json.loads(cleaned)
            logger.info("AI Polisher: successfully enhanced profile data")
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
        for w in raw_experience:
            polished_experience.append({
                "role": w.get("role", ""),
                "company": w.get("company", ""),
                "description": w.get("desc", ""),
            })

        lang_keywords = {"python", "java", "javascript", "typescript", "c++", "c#",
                         "ruby", "go", "rust", "php", "swift", "kotlin", "r", "scala",
                         "html", "css", "sql", "dart", "perl"}
        framework_keywords = {"react", "django", "flask", "express", "angular", "vue",
                              "spring", "laravel", "rails", "fastapi", "nextjs", "next.js",
                              "node.js", "nodejs", "flutter", "bootstrap", "tailwind",
                              "tensorflow", "pytorch", "pandas", "numpy"}
        db_keywords = {"mysql", "postgresql", "mongodb", "redis", "sqlite", "firebase",
                       "dynamodb", "oracle", "sql server", "cassandra", "elasticsearch"}
        tool_keywords = {"git", "github", "docker", "kubernetes", "aws", "gcp", "azure",
                         "linux", "jenkins", "ci/cd", "jira", "figma", "postman",
                         "vs code", "vscode", "android studio", "heroku", "netlify",
                         "render", "vercel", "nginx"}

        languages, frameworks, databases, tools, other = [], [], [], [], []
        for skill in raw_skills:
            s_lower = skill.lower().strip()
            if s_lower in lang_keywords: languages.append(skill)
            elif s_lower in framework_keywords: frameworks.append(skill)
            elif s_lower in db_keywords: databases.append(skill)
            elif s_lower in tool_keywords: tools.append(skill)
            else: other.append(skill)

        skills_grouped = {}
        if languages: skills_grouped["Languages"] = languages
        if frameworks: skills_grouped["Frameworks"] = frameworks
        if databases: skills_grouped["Databases"] = databases
        if tools: skills_grouped["Tools"] = tools
        if other: skills_grouped["Other"] = other

        return {
            "polished_projects": polished_projects,
            "polished_experience": polished_experience,
            "skills_grouped": skills_grouped,
            "polished_headline": "",
            "polished_bio": "",
            "ai_polished": False,
        }