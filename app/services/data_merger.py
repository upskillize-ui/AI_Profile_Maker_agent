"""
Data Merger v5
══════════════
Merges data from ALL sources into one unified student profile.
Priority for skills: Resume > LinkedIn > GitHub > LMS
Priority for scores: LMS (verified) > everything else
Priority for education/work: Resume > LinkedIn > LMS profile fields

NEW in v5:
- Merges LinkedIn data (headline, summary, experience, education, skills)
- Uses LMS profile fields as structured education/work fallback
- Never leaves education or work_experience empty if data exists anywhere
"""

import logging
import re
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Legal suffixes stripped when normalizing company names for matching.
_LEGAL_SUFFIXES = {
    "pvt", "private", "ltd", "limited", "llp", "inc", "incorporated",
    "corp", "corporation", "co", "company", "plc", "gmbh", "sa", "llc",
}

# Bare status words that must never win over a fuller title.
_BARE_STATUS_TITLES = {"intern", "fresher", "trainee", "student", "employee"}

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Far-future sentinel for "Present"/"Current" end dates (year*12 + month form).
_PRESENT = 10 ** 9


class DataMerger:

    def merge(
        self,
        lms_data: Dict,
        resume_data: Dict = None,
        github_data: Dict = None,
        linkedin_data: Dict = None,
    ) -> Dict[str, Any]:
        """Merge all sources into unified student profile."""
        if not resume_data:
            resume_data = {}
        if not github_data:
            github_data = {}
        if not linkedin_data:
            linkedin_data = {}

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

        # ── Enrich from LinkedIn ──
        if linkedin_data.get("headline") and not personal.get("resume_headline"):
            personal["linkedin_headline"] = linkedin_data["headline"]
        if linkedin_data.get("summary"):
            personal["linkedin_summary"] = linkedin_data["summary"]
        if linkedin_data.get("profile_url") and not personal.get("linkedin_url"):
            personal["linkedin_url"] = linkedin_data["profile_url"]

        # GitHub avatar as fallback photo
        if github_data.get("avatar_url") and not personal.get("photo_url"):
            personal["photo_url"] = github_data["avatar_url"]
        if github_data.get("profile_url"):
            personal["github_url"] = github_data["profile_url"]

        merged["personal"] = personal

        # ══════════════════════════════════════════════════
        # EDUCATION — cascading: Resume > LinkedIn > LMS fields
        # ══════════════════════════════════════════════════
        education = resume_data.get("education", [])

        # If resume had no education, try LinkedIn
        if not education and linkedin_data.get("education"):
            education = []
            for edu in linkedin_data["education"]:
                education.append({
                    "degree": edu.get("degree", ""),
                    "institution": edu.get("institution", ""),
                    "year": edu.get("year", ""),
                    "field_of_study": edu.get("field_of_study", ""),
                    "percentage": "",
                    "source": "linkedin",
                })

        # If still no education, use LMS profile fields
        if not education:
            education = lms_data.get("lms_education", [])

        merged["education"] = education

        # ══════════════════════════════════════════════════
        # WORK EXPERIENCE — true merge: Resume + LinkedIn + LMS fields
        # Same position across sources (matched by normalized company +
        # overlapping duration) is collapsed into ONE entry keeping the
        # most recent / fullest information. Positions present in only
        # one source always survive.
        # ══════════════════════════════════════════════════
        work_experience = self._merge_work_experience(
            resume_list=resume_data.get("work_experience", []),
            linkedin_list=linkedin_data.get("experience", []),
            lms_list=lms_data.get("lms_work_experience", []),
        )
        merged["work_experience"] = work_experience

        # ── Derived current designation — from the merged current role ──
        # (LMS current_designation field becomes fallback only.)
        self._derive_current_designation(personal, work_experience)

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

        # ── Certifications (merge LMS + resume + LinkedIn) ──
        certs = list(merged.get("certifications", []))
        for c in resume_data.get("certifications", []):
            certs.append({
                "certificate_name": c.get("name", ""),
                "course_name": c.get("issuer", ""),
                "issued_at": c.get("year", ""),
                "source": "resume",
            })
        for c in linkedin_data.get("certifications", []):
            certs.append({
                "certificate_name": c.get("name", "") or c.get("certificate_name", ""),
                "course_name": c.get("issuer", "") or c.get("course_name", ""),
                "issued_at": c.get("year", "") or c.get("issued_at", ""),
                "source": "linkedin",
            })
        merged["certifications"] = self._dedupe_certs(certs)

        # ── All skills from all sources ──
        merged["all_skills"] = self._merge_skills(
            lms_skills=merged.get("computed", {}),
            resume_skills=resume_data,
            github_skills=github_data.get("technical_skills", []),
            linkedin_skills=linkedin_data.get("skills", []),
            personal=personal,
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
        if linkedin_data.get("_source") and linkedin_data["_source"] not in ("empty", "linkedin_url_only"):
            sources.append("linkedin")
        # Also track if LMS profile fields contributed
        if lms_data.get("lms_education") or lms_data.get("lms_work_experience"):
            if "lms_profile" not in sources:
                sources.append("lms_profile")
        merged["data_sources"] = sources

        return merged

    # ══════════════════════════════════════════════════════════════════
    # WORK EXPERIENCE MERGE HELPERS
    # ══════════════════════════════════════════════════════════════════

    @staticmethod
    def _norm_company(name: str) -> str:
        """Normalize company name for matching: casefold, strip punctuation
        and trailing legal suffixes (Pvt Ltd, Inc, LLP, ...)."""
        s = re.sub(r"[^\w\s]", " ", (name or "").casefold())
        tokens = [t for t in s.split() if t]
        while tokens and tokens[-1] in _LEGAL_SUFFIXES:
            tokens.pop()
        return " ".join(tokens)

    @staticmethod
    def _is_current_duration(duration: str) -> bool:
        d = (duration or "").casefold()
        return "present" in d or "current" in d or "till date" in d or "ongoing" in d

    @classmethod
    def _duration_bounds(cls, duration: str) -> Tuple[Optional[int], Optional[int]]:
        """Parse duration text into (start, end) as year*12+month ints.
        'Present'/'Current' end → far-future sentinel. Unparseable → None."""
        d = (duration or "").casefold()
        points = []
        for mon, yr in re.findall(r"(?:([a-z]{3,9})[.\s]*)?((?:19|20)\d{2})", d):
            month = _MONTHS.get(mon[:3], 6) if mon else 6
            points.append(int(yr) * 12 + month)
        start = min(points) if points else None
        if cls._is_current_duration(duration):
            end = _PRESENT
        else:
            end = max(points) if points else None
        return start, end

    @classmethod
    def _durations_match(cls, d1: str, d2: str) -> bool:
        """True when two duration texts are equal or their date ranges overlap."""
        n1 = re.sub(r"[^\w]", "", (d1 or "").casefold())
        n2 = re.sub(r"[^\w]", "", (d2 or "").casefold())
        if n1 and n1 == n2:
            return True
        if cls._is_current_duration(d1) and cls._is_current_duration(d2):
            return True
        s1, e1 = cls._duration_bounds(d1)
        s2, e2 = cls._duration_bounds(d2)
        if None in (s1, e1, s2, e2):
            # No comparable dates on one side — only exact-text equality counts.
            return False
        return s1 <= e2 and s2 <= e1

    @staticmethod
    def _title_rank(title: str) -> tuple:
        """Rank a title: bare status words lowest, senior titles higher,
        longer/fuller titles win ties."""
        t = (title or "").strip()
        if not t:
            return (0, 0, 0, 0)
        words = t.casefold().split()
        bare_status = 0 if (len(words) == 1 and words[0] in _BARE_STATUS_TITLES) else 1
        tl = t.casefold()
        if any(k in tl for k in ("senior", "lead", "head", "manager", "principal", "director")):
            seniority = 3
        elif any(k in tl for k in ("intern", "trainee", "apprentice")):
            seniority = 1
        else:
            seniority = 2
        return (1, bare_status, seniority, len(t))

    @staticmethod
    def _normalize_work_entry(raw: Dict, source: str) -> Optional[Dict]:
        """Normalize any source's work entry to a common shape."""
        if not isinstance(raw, dict):
            return None
        title = (raw.get("title") or raw.get("role") or raw.get("designation") or "").strip()
        company = (raw.get("company") or raw.get("employer") or raw.get("organization") or "").strip()
        if not title and not company:
            return None
        entry = {
            "title": title,
            "company": company,
            "duration": (raw.get("duration") or raw.get("dates") or "").strip(),
            "description": (raw.get("description") or "").strip(),
            "source": source,
            "sources": [source],
        }
        emp_type = (raw.get("employment_type") or raw.get("type") or "").strip()
        if emp_type:
            entry["employment_type"] = emp_type
        return entry

    @classmethod
    def _same_position(cls, a: Dict, b: Dict) -> bool:
        """Same position = same normalized company + overlapping/equal duration.
        When both companies are missing, fall back to same title + duration."""
        ca, cb = cls._norm_company(a.get("company", "")), cls._norm_company(b.get("company", ""))
        if ca and cb:
            if ca != cb:
                return False
        elif ca or cb:
            return False
        else:
            # Neither has a company — require matching titles too.
            if (a.get("title") or "").casefold().strip() != (b.get("title") or "").casefold().strip():
                return False
        return cls._durations_match(a.get("duration", ""), b.get("duration", ""))

    @classmethod
    def _merge_entry_into(cls, base: Dict, other: Dict) -> None:
        """Fold `other` (same position, different source) into `base`,
        keeping the most recent / fullest information."""
        is_current = cls._is_current_duration(base.get("duration", "")) or \
                     cls._is_current_duration(other.get("duration", ""))

        # Title: for a CURRENT role conflict LinkedIn wins over the resume
        # snapshot (resume = stale, LinkedIn = live); otherwise keep the
        # fuller / more senior title.
        if other.get("title"):
            other_wins = cls._title_rank(other["title"]) > cls._title_rank(base.get("title", ""))
            if is_current and other["source"] == "linkedin":
                other_wins = True
            elif is_current and base.get("source") == "linkedin" and base.get("title"):
                other_wins = False
            if other_wins:
                base["title"] = other["title"]
                if other.get("company"):
                    base["company"] = other["company"]
                if other.get("employment_type"):
                    base["employment_type"] = other["employment_type"]
                base["source"] = other["source"]

        # Description: keep the longer one.
        if len(other.get("description", "")) > len(base.get("description", "")):
            base["description"] = other["description"]

        # Duration: prefer the one with parseable dates, else the longer text.
        bs, be = cls._duration_bounds(base.get("duration", ""))
        os_, oe = cls._duration_bounds(other.get("duration", ""))
        if (bs is None or be is None) and (os_ is not None and oe is not None):
            base["duration"] = other["duration"]
        elif len(other.get("duration", "")) > len(base.get("duration", "")) and os_ is not None:
            base["duration"] = other["duration"]

        if not base.get("employment_type") and other.get("employment_type"):
            base["employment_type"] = other["employment_type"]
        if other["sources"][0] not in base["sources"]:
            base["sources"].append(other["sources"][0])

    def _merge_work_experience(
        self,
        resume_list: List[Dict],
        linkedin_list: List[Dict],
        lms_list: List[Dict],
    ) -> List[Dict]:
        """True cross-source merge of work experience. Never drops a position
        present in only one source. Result ordered current/most-recent first."""
        merged: List[Dict] = []
        for source, entries in (("resume", resume_list or []),
                                ("linkedin", linkedin_list or []),
                                ("lms", lms_list or [])):
            for raw in entries:
                entry = self._normalize_work_entry(raw, source)
                if not entry:
                    continue
                for existing in merged:
                    if self._same_position(existing, entry):
                        self._merge_entry_into(existing, entry)
                        break
                else:
                    merged.append(entry)

        def sort_key(e):
            start, end = self._duration_bounds(e.get("duration", ""))
            current = 1 if self._is_current_duration(e.get("duration", "")) else 0
            return (current, end if end is not None else -1, start if start is not None else -1)

        merged.sort(key=sort_key, reverse=True)
        return merged

    def _derive_current_designation(self, personal: Dict, work_experience: List[Dict]) -> None:
        """Overwrite personal.current_designation / current_employer with the
        merged current (or most recent) role's full title and company. The LMS
        designation field is only a fallback — but a bare status word never
        replaces a fuller existing designation."""
        if not work_experience:
            return
        current = next(
            (w for w in work_experience if self._is_current_duration(w.get("duration", ""))),
            work_experience[0],  # list is sorted most-recent first
        )
        title = (current.get("title") or "").strip()
        if title:
            existing = (personal.get("current_designation") or "").strip()
            title_is_bare = title.casefold() in _BARE_STATUS_TITLES
            existing_is_fuller = existing and existing.casefold() not in _BARE_STATUS_TITLES \
                and len(existing) > len(title)
            if not (title_is_bare and existing_is_fuller):
                personal["current_designation"] = title
        if (current.get("company") or "").strip():
            personal["current_employer"] = current["company"].strip()
        if current.get("employment_type"):
            personal["employment_type"] = current["employment_type"]

    def _merge_skills(
        self,
        lms_skills: Dict,
        resume_skills: Dict,
        github_skills: list,
        linkedin_skills: list = None,
        personal: Dict = None,
    ) -> Dict[str, List]:
        """Merge skills from all sources, deduplicate, keep highest score."""
        if linkedin_skills is None:
            linkedin_skills = []
        if personal is None:
            personal = {}

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

        # From LinkedIn skills
        for skill in linkedin_skills:
            name = skill if isinstance(skill, str) else skill.get("name", "")
            if name:
                key = name.lower()
                if key in all_technical:
                    # Boost — confirmed on both resume and LinkedIn
                    all_technical[key]["score"] = min(90, all_technical[key]["score"] + 10)
                    all_technical[key]["evidence"] += " + LinkedIn"
                else:
                    all_technical[key] = {
                        "name": name,
                        "score": 60,
                        "evidence": "Listed on LinkedIn",
                        "source": "linkedin",
                    }

        # From LMS profile key_skills field
        lms_key_skills = personal.get("key_skills", "") or personal.get("skills", "") or ""
        if lms_key_skills:
            skill_list = [s.strip() for s in lms_key_skills.replace(";", ",").split(",") if s.strip()]
            for name in skill_list:
                key = name.lower()
                if key in all_technical:
                    all_technical[key]["score"] = min(90, all_technical[key]["score"] + 5)
                    all_technical[key]["evidence"] += " + LMS profile"
                else:
                    all_technical[key] = {
                        "name": name,
                        "score": 55,
                        "evidence": "Listed on LMS profile",
                        "source": "lms_profile",
                    }

        # From GitHub (real evidence)
        for skill in github_skills:
            if isinstance(skill, dict):
                name = skill.get("name", "")
                key = name.lower()
                score = skill.get("score", 60)
                if key in all_technical:
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
        seen = set()
        unique = []
        for p in projects:
            title_key = (p.get("title") or "").lower().strip()
            if title_key and title_key not in seen:
                seen.add(title_key)
                unique.append(p)
        return unique[:8]

    def _dedupe_certs(self, certs: List[Dict]) -> List[Dict]:
        seen = set()
        unique = []
        for c in certs:
            name_key = (c.get("certificate_name") or c.get("name", "")).lower().strip()
            if name_key and name_key not in seen:
                seen.add(name_key)
                unique.append(c)
        return unique
