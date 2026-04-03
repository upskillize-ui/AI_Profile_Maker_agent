"""
Resume Parser v4
════════════════
Extracts structured data from student's uploaded resume PDF.
Uses ONE Claude Haiku API call (~$0.003) for intelligent extraction.
Falls back to keyword-based extraction if no API key.
"""

import os
import re
import logging
import httpx
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ResumeParser:

    def __init__(self):
        self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    async def parse(self, resume_text: str) -> Dict[str, Any]:
        """Parse resume text into structured data."""
        if not resume_text or len(resume_text.strip()) < 50:
            return self._empty_result()

        # Try AI parsing first (more accurate)
        if self.api_key:
            try:
                return await self._ai_parse(resume_text)
            except Exception as e:
                logger.warning(f"AI resume parse failed, using fallback: {e}")

        # Fallback: keyword-based extraction
        return self._keyword_parse(resume_text)

    async def _ai_parse(self, text: str) -> Dict[str, Any]:
        """Parse resume using ONE Claude Haiku call."""
        prompt = f"""Extract structured data from this resume. Return ONLY valid JSON, no other text.

RESUME TEXT:
{text[:3000]}

Return this exact JSON structure (fill from resume, empty string/array if not found):
{{
  "name": "",
  "email": "",
  "phone": "",
  "location": "",
  "linkedin_url": "",
  "github_url": "",
  "portfolio_url": "",
  "headline": "",
  "summary": "",
  "technical_skills": ["Python", "React", ...],
  "tools": ["Git", "AWS", ...],
  "soft_skills": ["Problem Solving", ...],
  "languages": ["English", "Hindi", ...],
  "education": [
    {{"degree": "B.Tech CSE", "institution": "XYZ College", "year": "2024", "percentage": "71%"}}
  ],
  "work_experience": [
    {{"title": "Software Developer", "company": "ABC Corp", "duration": "Feb 2026 - Present", "description": "Built web apps..."}}
  ],
  "projects": [
    {{"title": "Book Library App", "description": "Responsive app using React", "technologies": ["React", "CSS"]}}
  ],
  "certifications": [
    {{"name": "Full Stack Python", "issuer": "Naresh IT", "year": "2025"}}
  ]
}}"""

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
                    "max_tokens": 1500,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            data = response.json()
            raw = data["content"][0]["text"].strip()

            # Clean JSON (remove markdown fences if present)
            raw = re.sub(r'^```json\s*', '', raw)
            raw = re.sub(r'\s*```$', '', raw)

            import json
            parsed = json.loads(raw)
            parsed["_source"] = "ai_parsed"
            return parsed

    def _keyword_parse(self, text: str) -> Dict[str, Any]:
        """Fallback: extract data using keywords and patterns."""
        text_lower = text.lower()
        result = self._empty_result()
        result["_source"] = "keyword_parsed"

        # Extract email
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
        if emails:
            result["email"] = emails[0]

        # Extract phone
        phones = re.findall(r'[\+]?[\d\s\-\(\)]{10,15}', text)
        if phones:
            result["phone"] = phones[0].strip()

        # Extract LinkedIn
        linkedin = re.findall(r'(?:https?://)?(?:www\.)?linkedin\.com/in/[\w\-]+/?', text)
        if linkedin:
            result["linkedin_url"] = linkedin[0] if linkedin[0].startswith('http') else f"https://{linkedin[0]}"

        # Extract GitHub
        github = re.findall(r'(?:https?://)?(?:www\.)?github\.com/[\w\-]+/?', text)
        if github:
            result["github_url"] = github[0] if github[0].startswith('http') else f"https://{github[0]}"

        # Extract technical skills by keyword matching
        tech_keywords = {
            "python", "django", "react", "javascript", "typescript", "html", "css",
            "sql", "mysql", "mongodb", "postgresql", "node.js", "express",
            "java", "c++", "c#", "php", "ruby", "swift", "kotlin",
            "aws", "azure", "gcp", "docker", "kubernetes", "git",
            "tensorflow", "pytorch", "numpy", "pandas", "flask", "fastapi",
            "angular", "vue", "next.js", "tailwind", "bootstrap",
            "redis", "elasticsearch", "graphql", "rest api",
            "machine learning", "deep learning", "data science", "ai",
            "power bi", "tableau", "excel", "figma",
        }
        found_skills = []
        for skill in tech_keywords:
            if skill in text_lower:
                found_skills.append(skill.title() if len(skill) > 3 else skill.upper())
        result["technical_skills"] = found_skills

        # Extract tools
        tool_keywords = {"git", "github", "figma", "aws", "vs code", "docker", "jira", "postman"}
        result["tools"] = [t.title() for t in tool_keywords if t in text_lower]

        # Extract soft skills
        soft_keywords = {
            "problem solving": "Problem Solving",
            "communication": "Communication",
            "team": "Team Collaboration",
            "leadership": "Leadership",
            "analytical": "Analytical Thinking",
            "adaptability": "Adaptability",
            "innovative": "Innovation",
        }
        result["soft_skills"] = [v for k, v in soft_keywords.items() if k in text_lower]

        # Extract education (basic pattern)
        edu_patterns = [
            r'(B\.?Tech|B\.?E|BCA|MCA|MBA|M\.?Tech|B\.?Com|M\.?Com|B\.?Sc|M\.?Sc)[\s,]*(.+?)[\s]*[\|·]?\s*(\d{2,3}\s*%)?',
        ]
        for pat in edu_patterns:
            matches = re.findall(pat, text, re.IGNORECASE)
            for m in matches:
                result["education"].append({
                    "degree": m[0].strip(),
                    "institution": m[1].strip()[:100] if len(m) > 1 else "",
                    "percentage": m[2].strip() if len(m) > 2 else "",
                })

        return result

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "name": "", "email": "", "phone": "", "location": "",
            "linkedin_url": "", "github_url": "", "portfolio_url": "",
            "headline": "", "summary": "",
            "technical_skills": [], "tools": [], "soft_skills": [], "languages": [],
            "education": [], "work_experience": [], "projects": [], "certifications": [],
            "_source": "empty",
        }
