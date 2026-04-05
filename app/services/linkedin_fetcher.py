"""
LinkedIn Data Fetcher v1
════════════════════════
Attempts to fetch LinkedIn public profile data.
LinkedIn blocks most scraping, so this uses multiple strategies:
  1. Try to get basic info from LinkedIn public profile page
  2. Parse any user-uploaded LinkedIn PDF export
  3. Fall back gracefully to LMS profile fields

Priority: LinkedIn PDF > LinkedIn scrape > LMS profile fields
"""

import re
import logging
import httpx
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class LinkedInFetcher:

    async def fetch(self, linkedin_url: str) -> Dict[str, Any]:
        """Fetch LinkedIn profile data. Best-effort — never crashes."""
        if not linkedin_url:
            return self._empty_result()

        username = self._extract_username(linkedin_url)
        if not username:
            return self._empty_result()

        # Strategy 1: Try fetching public LinkedIn page
        try:
            profile_data = await self._fetch_public_profile(linkedin_url, username)
            if profile_data and profile_data.get("_source") != "empty":
                return profile_data
        except Exception as e:
            logger.info(f"LinkedIn public fetch failed (expected): {e}")

        # LinkedIn blocked — return what we can (just the URL and username)
        return {
            "username": username,
            "profile_url": f"https://linkedin.com/in/{username}",
            "headline": "",
            "summary": "",
            "skills": [],
            "experience": [],
            "education": [],
            "certifications": [],
            "_source": "linkedin_url_only",
        }

    async def _fetch_public_profile(self, url: str, username: str) -> Dict[str, Any]:
        """Try to extract data from LinkedIn's public profile page."""
        clean_url = f"https://www.linkedin.com/in/{username}"

        async with httpx.AsyncClient(
            timeout=10.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
        ) as client:
            resp = await client.get(clean_url)

            if resp.status_code != 200:
                logger.info(f"LinkedIn returned {resp.status_code} for {username}")
                return self._empty_result()

            html = resp.text

            # Extract from meta tags and structured data (available on public profiles)
            result = {
                "username": username,
                "profile_url": clean_url,
                "headline": self._extract_meta(html, "og:title") or "",
                "summary": self._extract_meta(html, "og:description") or "",
                "skills": [],
                "experience": [],
                "education": [],
                "certifications": [],
                "_source": "linkedin_public",
            }

            # Clean headline (remove " | LinkedIn" suffix)
            if result["headline"]:
                result["headline"] = re.sub(r'\s*[\|–-]\s*LinkedIn\s*$', '', result["headline"]).strip()

            # Try to extract structured data from JSON-LD if present
            json_ld = self._extract_json_ld(html)
            if json_ld:
                if json_ld.get("jobTitle"):
                    result["headline"] = json_ld["jobTitle"]
                if json_ld.get("description"):
                    result["summary"] = json_ld["description"]

            # Only return if we got something useful
            if result["headline"] or result["summary"]:
                return result

            return self._empty_result()

    def _extract_meta(self, html: str, property_name: str) -> str:
        """Extract content from meta tags."""
        patterns = [
            rf'<meta\s+property="{property_name}"\s+content="([^"]*)"',
            rf'<meta\s+content="([^"]*)"\s+property="{property_name}"',
            rf'<meta\s+name="{property_name}"\s+content="([^"]*)"',
        ]
        for pat in patterns:
            match = re.search(pat, html, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return ""

    def _extract_json_ld(self, html: str) -> Optional[Dict]:
        """Try to extract JSON-LD structured data."""
        import json
        pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(pattern, html, re.DOTALL)
        for match in matches:
            try:
                data = json.loads(match.strip())
                if isinstance(data, dict) and data.get("@type") == "Person":
                    return data
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "Person":
                            return item
            except (json.JSONDecodeError, TypeError):
                continue
        return None

    async def parse_linkedin_pdf(self, pdf_text: str) -> Dict[str, Any]:
        """Parse a LinkedIn PDF export into structured data.
        Users can download their LinkedIn profile as PDF — this extracts it."""
        if not pdf_text or len(pdf_text.strip()) < 50:
            return self._empty_result()

        result = {
            "username": "",
            "profile_url": "",
            "headline": "",
            "summary": "",
            "skills": [],
            "experience": [],
            "education": [],
            "certifications": [],
            "_source": "linkedin_pdf",
        }

        lines = pdf_text.split("\n")
        lines = [l.strip() for l in lines if l.strip()]

        # Name is usually first line, headline is second
        if len(lines) >= 2:
            result["headline"] = lines[1] if len(lines[1]) < 200 else ""

        # Extract sections
        current_section = ""
        section_lines = []

        for line in lines:
            line_lower = line.lower().strip()
            if line_lower in ("experience", "work experience"):
                if current_section == "experience":
                    result["experience"] = self._parse_experience_lines(section_lines)
                current_section = "experience"
                section_lines = []
            elif line_lower in ("education",):
                if current_section == "experience":
                    result["experience"] = self._parse_experience_lines(section_lines)
                current_section = "education"
                section_lines = []
            elif line_lower in ("skills", "skills & endorsements"):
                if current_section == "education":
                    result["education"] = self._parse_education_lines(section_lines)
                current_section = "skills"
                section_lines = []
            elif line_lower in ("certifications", "licenses & certifications"):
                if current_section == "skills":
                    result["skills"] = [l for l in section_lines if len(l) < 60]
                current_section = "certifications"
                section_lines = []
            elif line_lower == "summary" or line_lower == "about":
                current_section = "summary"
                section_lines = []
            else:
                section_lines.append(line)

        # Process last section
        if current_section == "experience":
            result["experience"] = self._parse_experience_lines(section_lines)
        elif current_section == "education":
            result["education"] = self._parse_education_lines(section_lines)
        elif current_section == "skills":
            result["skills"] = [l for l in section_lines if len(l) < 60]
        elif current_section == "summary":
            result["summary"] = " ".join(section_lines)[:500]

        # Extract LinkedIn URL from the PDF
        urls = re.findall(r'linkedin\.com/in/([\w\-]+)', pdf_text)
        if urls:
            result["username"] = urls[0]
            result["profile_url"] = f"https://linkedin.com/in/{urls[0]}"

        return result

    def _parse_experience_lines(self, lines: list) -> list:
        """Parse experience section lines into structured data."""
        experiences = []
        current = {}

        for line in lines:
            # Detect date patterns (e.g., "Jan 2023 - Present", "2022 - 2024")
            date_match = re.search(
                r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4}|'
                r'\d{4})\s*[-–]\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4}|'
                r'\d{4}|Present)',
                line, re.IGNORECASE
            )
            if date_match and not current.get("duration"):
                current["duration"] = date_match.group(0)
            elif not current.get("title") and len(line) < 100:
                current["title"] = line
            elif not current.get("company") and len(line) < 100:
                current["company"] = line
            elif current.get("title"):
                current.setdefault("description", "")
                current["description"] += line + " "

            # If we have enough for an entry, save it
            if current.get("title") and current.get("duration"):
                experiences.append({
                    "title": current.get("title", ""),
                    "company": current.get("company", ""),
                    "duration": current.get("duration", ""),
                    "description": current.get("description", "").strip()[:300],
                })
                current = {}

        # Don't forget the last one
        if current.get("title"):
            experiences.append({
                "title": current.get("title", ""),
                "company": current.get("company", ""),
                "duration": current.get("duration", ""),
                "description": current.get("description", "").strip()[:300],
            })

        return experiences[:5]

    def _parse_education_lines(self, lines: list) -> list:
        """Parse education section lines into structured data."""
        education = []
        current = {}

        for line in lines:
            year_match = re.search(r'(\d{4})\s*[-–]\s*(\d{4})', line)
            degree_match = re.search(
                r'(B\.?Tech|B\.?E|BCA|MCA|MBA|M\.?Tech|B\.?Com|M\.?Com|B\.?Sc|M\.?Sc|Ph\.?D|'
                r'Bachelor|Master|Associate|Diploma)',
                line, re.IGNORECASE,
            )

            if degree_match and not current.get("degree"):
                current["degree"] = line
            elif year_match:
                current["year"] = year_match.group(0)
            elif not current.get("institution") and len(line) < 150:
                current["institution"] = line

            if current.get("degree") and (current.get("year") or current.get("institution")):
                education.append({
                    "degree": current.get("degree", ""),
                    "institution": current.get("institution", ""),
                    "year": current.get("year", ""),
                })
                current = {}

        if current.get("degree"):
            education.append({
                "degree": current.get("degree", ""),
                "institution": current.get("institution", ""),
                "year": current.get("year", ""),
            })

        return education[:4]

    def _extract_username(self, url: str) -> Optional[str]:
        """Extract LinkedIn username from URL."""
        if not url:
            return None
        url = url.strip().rstrip("/")
        match = re.search(r'linkedin\.com/in/([\w\-]+)', url)
        if match:
            return match.group(1)
        return None

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "username": "",
            "profile_url": "",
            "headline": "",
            "summary": "",
            "skills": [],
            "experience": [],
            "education": [],
            "certifications": [],
            "_source": "empty",
        }
