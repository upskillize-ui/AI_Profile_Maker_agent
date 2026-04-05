"""
Data Merger v4
══════════════
Merges data from all sources into one unified student profile.
Priority: Resume > GitHub > LMS (for skills/experience)
Priority: LMS > Resume (for verified scores/assessments)
Deduplicates skills and takes highest confidence score.
"""

import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class DataMerger:

    def merge(self, lms_data: Dict, resume_data: Dict = None, github_data: Dict = None) -> Dict[str, Any]:
        """Merge all sources into unified student profile."""
        if not resume_data:
            resume_data = {}
        if not github_data:
            github_data = {}

        merged = dict(lms_data)  # Start with LMS as base

        # ── Enrich personal info from resume ──
        personal = merged.get("personal", {})
        if resume_data.get("linkedin_url") and not personal.get("linkedin_url"):
            personal["linkedin_url"] = resume_data["linkedin_url"]
        if resume_data.get("github_url") and not personal.get("github_url"):
            personal["github_url"] = resume_data["github_url"]
        if resume_data.get("portfolio_url") and not personal.get("portfolio_url"):
            personal["portfolio_url"] = resume_data["portfolio_url"]
        if resume_data.get("phone") and not personal.get("phone"):
            personal["phone"] = resume_data["phone"]
        if resume_data.get("location") and not personal.get("location"):
            personal["location"] = resume_data["location"]
        if resume_data.get("headline"):
            personal["resume_headline"] = resume_data["headline"]
        if resume_data.get("summary"):
            personal["resume_summary"] = resume_data["summary"]

        # GitHub avatar as fallback photo
        if github_data.get("avatar_url") and not personal.get("photo_url"):
            personal["photo_url"] = github_data["avatar_url"]
        if github_data.get("profile_url"):
            personal["github_url"] = github_data["profile_url"]

        merged["personal"] = personal

        # ── Education (from resume — LMS doesn't have this) ──
        merged["education"] = resume_data.get("education", [])

        # ── Work Experience (from resume — LMS doesn't have this) ──
        merged["work_experience"] = resume_data.get("work_experience", [])

        # ── Projects (merge LMS + resume + GitHub) ──
        projects = list(merged.get("projects", []))

        # Add resume projects
        for p in resume_data.get("projects", []):
            projects.append({
                "title": p.get("title", ""),
                "description": p.get("description", ""),
                "technologies_used": p.get("technologies", []),
                "source": "resume",
            })

        # Add GitHub repos as projects
        for repo in github_data.get("top_repos", [])[:3]:
            if repo.get("name"):
                projects.append({
                    "title": repo["name"],
                    "description": repo.get("description", ""),
                    "technologies_used": [repo["language"]] if repo.get("language") else [],
                    "github_url": repo.get("url", ""),
                    "stars": repo.get("stars", 0),
                    "source": "github",
                })

        merged["projects"] = self._dedupe_projects(projects)

        # ── Certifications (merge LMS + resume) ──
        certs = list(merged.get("certifications", []))
        for c in resume_data.get("certifications", []):
            certs.append({
                "certificate_name": c.get("name", ""),
                "course_name": c.get("issuer", ""),
                "issued_at": c.get("year", ""),
                "source": "resume",
            })
        merged["certifications"] = self._dedupe_certs(certs)

        # ── All skills from all sources ──
        merged["all_skills"] = self._merge_skills(
            lms_skills=merged.get("computed", {}),
            resume_skills=resume_data,
            github_skills=github_data.get("technical_skills", []),
        )

        # ── GitHub profile data ──
        merged["github_profile"] = {
            "username": github_data.get("username", ""),
            "public_repos": github_data.get("public_repos", 0),
            "followers": github_data.get("followers", 0),
            "languages": github_data.get("languages", {}),
            "top_repos": github_data.get("top_repos", [])[:5],
        } if github_data.get("username") else {}

        # ── Languages spoken ──
        merged["spoken_languages"] = resume_data.get("languages", [])

        # ── Data sources tracking ──
        sources = ["lms"]
        if resume_data.get("_source") and resume_data["_source"] != "empty":
            sources.append("resume")
        if github_data.get("_source") and github_data["_source"] != "empty":
            sources.append("github")
        merged["data_sources"] = sources

        return merged

    def _merge_skills(self, lms_skills: Dict, resume_skills: Dict, github_skills: List) -> Dict[str, List]:
        """Merge skills from all sources, deduplicate, keep highest score."""
        all_technical = {}
        all_tools = {}
        all_soft = {}

        # From resume (highest priority for skill names)
        for skill in resume_skills.get("technical_skills", []):
            name = skill if isinstance(skill, str) else skill.get("name", "")
            if name:
                key = name.lower()
                all_technical[key] = {
                    "name": name,
                    "score": 65,
                    "evidence": "Listed on resume",
                    "source": "resume",
                }

        for tool in resume_skills.get("tools", []):
            name = tool if isinstance(tool, str) else tool.get("name", "")
            if name:
                all_tools[name.lower()] = {
                    "name": name,
                    "score": 60,
                    "evidence": "Listed on resume",
                    "source": "resume",
                }

        for skill in resume_skills.get("soft_skills", []):
            name = skill if isinstance(skill, str) else skill.get("name", "")
            if name:
                all_soft[name.lower()] = {
                    "name": name,
                    "score": 65,
                    "evidence": "Listed on resume",
                    "source": "resume",
                }

        # From GitHub (real evidence)
        for skill in github_skills:
            if isinstance(skill, dict):
                name = skill.get("name", "")
                key = name.lower()
                score = skill.get("score", 60)
                if key in all_technical:
                    # Boost score — skill confirmed by both resume AND GitHub
                    all_technical[key]["score"] = min(95, max(all_technical[key]["score"], score) + 10)
                    all_technical[key]["evidence"] += " + Verified on GitHub"
                else:
                    all_technical[key] = {
                        "name": name,
                        "score": score,
                        "evidence": skill.get("evidence", "Found on GitHub"),
                        "source": "github",
                    }

        # From LMS (verified by assessments)
        top_subjects = lms_skills.get("top_subjects", [])
        for subj_name, subj_score in top_subjects[:6]:
            key = subj_name.lower()
            if key in all_technical:
                all_technical[key]["score"] = min(95, max(all_technical[key]["score"], int(subj_score)) + 5)
                all_technical[key]["evidence"] += f" + LMS test avg: {subj_score}%"
            else:
                all_technical[key] = {
                    "name": subj_name,
                    "score": int(subj_score),
                    "evidence": f"LMS test average: {subj_score}%",
                    "source": "lms",
                }

        return {
            "technical_skills": sorted(all_technical.values(), key=lambda x: x["score"], reverse=True),
            "tools": sorted(all_tools.values(), key=lambda x: x["score"], reverse=True),
            "soft_skills": sorted(all_soft.values(), key=lambda x: x["score"], reverse=True),
        }

    def _dedupe_projects(self, projects: List[Dict]) -> List[Dict]:
        """Deduplicate projects by title similarity."""
        seen = set()
        unique = []
        for p in projects:
            title_key = (p.get("title") or "").lower().strip()
            if title_key and title_key not in seen:
                seen.add(title_key)
                unique.append(p)
        return unique[:8]

    def _dedupe_certs(self, certs: List[Dict]) -> List[Dict]:
        """Deduplicate certifications."""
        seen = set()
        unique = []
        for c in certs:
            name_key = (c.get("certificate_name") or c.get("name", "")).lower().strip()
            if name_key and name_key not in seen:
                seen.add(name_key)
                unique.append(c)
        return unique
