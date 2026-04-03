"""
GitHub Data Fetcher v4
══════════════════════
Fetches real data from GitHub public API. 100% FREE.
No auth needed for public profiles.
Rate limit: 60 requests/hour per IP (plenty for profile generation).
"""

import re
import logging
import httpx
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"


class GitHubFetcher:

    async def fetch(self, github_url: str) -> Dict[str, Any]:
        """Fetch GitHub profile data from a URL or username."""
        if not github_url:
            return self._empty_result()

        username = self._extract_username(github_url)
        if not username:
            return self._empty_result()

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Fetch profile
                profile = await self._fetch_profile(client, username)
                if not profile:
                    return self._empty_result()

                # Fetch repos (top 10 by stars)
                repos = await self._fetch_repos(client, username)

                # Fetch languages across repos
                languages = await self._fetch_languages(client, username, repos[:5])

                return {
                    "username": username,
                    "name": profile.get("name", ""),
                    "bio": profile.get("bio", ""),
                    "avatar_url": profile.get("avatar_url", ""),
                    "profile_url": f"https://github.com/{username}",
                    "public_repos": profile.get("public_repos", 0),
                    "followers": profile.get("followers", 0),
                    "following": profile.get("following", 0),
                    "created_at": profile.get("created_at", ""),
                    "top_repos": [
                        {
                            "name": r.get("name", ""),
                            "description": r.get("description", ""),
                            "language": r.get("language", ""),
                            "stars": r.get("stargazers_count", 0),
                            "forks": r.get("forks_count", 0),
                            "url": r.get("html_url", ""),
                            "updated_at": r.get("updated_at", ""),
                        }
                        for r in repos[:5]
                    ],
                    "languages": languages,
                    "technical_skills": self._derive_skills(languages, repos),
                    "_source": "github_api",
                }

        except Exception as e:
            logger.warning(f"GitHub fetch failed for {username}: {e}")
            return self._empty_result()

    async def _fetch_profile(self, client: httpx.AsyncClient, username: str) -> Optional[Dict]:
        resp = await client.get(f"{GITHUB_API}/users/{username}")
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"GitHub profile fetch failed: {resp.status_code}")
        return None

    async def _fetch_repos(self, client: httpx.AsyncClient, username: str) -> List[Dict]:
        resp = await client.get(
            f"{GITHUB_API}/users/{username}/repos",
            params={"sort": "stars", "direction": "desc", "per_page": 10}
        )
        if resp.status_code == 200:
            return resp.json()
        return []

    async def _fetch_languages(self, client: httpx.AsyncClient, username: str, repos: List[Dict]) -> Dict[str, int]:
        """Aggregate languages across top repos."""
        lang_totals = {}
        for repo in repos:
            repo_name = repo.get("name", "")
            if not repo_name:
                continue
            try:
                resp = await client.get(f"{GITHUB_API}/repos/{username}/{repo_name}/languages")
                if resp.status_code == 200:
                    for lang, bytes_count in resp.json().items():
                        lang_totals[lang] = lang_totals.get(lang, 0) + bytes_count
            except Exception:
                continue

        # Convert to percentages
        total = sum(lang_totals.values()) or 1
        return {lang: round(count / total * 100, 1) for lang, count in
                sorted(lang_totals.items(), key=lambda x: -x[1])}

    def _derive_skills(self, languages: Dict, repos: List[Dict]) -> List[Dict]:
        """Derive technical skills from GitHub activity."""
        skills = []

        # From languages
        lang_skill_map = {
            "Python": ["Python Programming", "Backend Development"],
            "JavaScript": ["JavaScript", "Web Development"],
            "TypeScript": ["TypeScript", "Frontend Development"],
            "HTML": ["HTML5", "Web Development"],
            "CSS": ["CSS3", "UI Design"],
            "Java": ["Java Programming"],
            "C++": ["C++ Programming"],
            "SQL": ["SQL", "Database Management"],
            "Shell": ["Shell Scripting", "DevOps"],
            "Dockerfile": ["Docker", "Containerization"],
        }

        for lang, pct in languages.items():
            if lang in lang_skill_map:
                confidence = min(90, int(pct * 0.8) + 30)
                for skill_name in lang_skill_map[lang]:
                    skills.append({
                        "name": skill_name,
                        "score": confidence,
                        "evidence": f"GitHub: {pct}% of code in {lang}",
                        "source": "github",
                    })

        # From repo descriptions (detect frameworks)
        all_text = " ".join((r.get("description") or "") + " " + (r.get("name") or "") for r in repos).lower()
        framework_map = {
            "react": ("React.js", 70), "django": ("Django", 70), "flask": ("Flask", 65),
            "fastapi": ("FastAPI", 70), "express": ("Express.js", 65), "node": ("Node.js", 65),
            "vue": ("Vue.js", 65), "angular": ("Angular", 65), "nextjs": ("Next.js", 70),
            "tensorflow": ("TensorFlow", 60), "pytorch": ("PyTorch", 60),
            "docker": ("Docker", 60), "kubernetes": ("Kubernetes", 55),
        }
        for keyword, (name, score) in framework_map.items():
            if keyword in all_text:
                skills.append({"name": name, "score": score, "evidence": f"GitHub: found in repos", "source": "github"})

        return skills

    def _extract_username(self, url: str) -> Optional[str]:
        """Extract GitHub username from URL or plain username."""
        if not url:
            return None
        url = url.strip().rstrip("/")

        # Full URL
        match = re.search(r'github\.com/([A-Za-z0-9_-]+)', url)
        if match:
            return match.group(1)

        # Plain username (no slashes, no dots)
        if re.match(r'^[A-Za-z0-9_-]+$', url):
            return url

        return None

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "username": "", "name": "", "bio": "", "avatar_url": "",
            "profile_url": "", "public_repos": 0, "followers": 0,
            "top_repos": [], "languages": {}, "technical_skills": [],
            "_source": "empty",
        }
