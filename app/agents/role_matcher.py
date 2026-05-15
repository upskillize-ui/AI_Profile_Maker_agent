"""
Role Matcher & ATS Calculator v6 — Dynamic + Legacy Fallback
═════════════════════════════════════════════════════════════
FIXES from v5:
  • Removed duplicate match_roles dead code after _match_static_legacy return
  • Moved class docstring to proper position (was floating after __init__)
  • Kept ROLE_DATABASE + COURSE_KEYWORD_MAP as legacy fallback only
"""
from app.agents.course_intelligence import CourseIntelligence
import logging
from typing import Dict, List, Any, Set

logger = logging.getLogger(__name__)

ROLE_DATABASE = {
    "Credit Analyst": {
        "category": "Banking",
        "keywords": {"credit risk", "credit analysis", "financial analysis", "credit scoring",
                     "NPA", "loan processing", "risk assessment", "financial statements",
                     "credit appraisal", "banking", "excel", "financial modeling",
                     "regulatory compliance", "Basel", "portfolio analysis"},
        "education": ["B.Com", "BBA", "MBA", "M.Com", "CA", "Finance"],
        "min_score": 30,
    },
    "Business Analyst - BFSI": {
        "category": "Banking",
        "keywords": {"business analysis", "data analytics", "financial services",
                     "banking operations", "requirements gathering", "process improvement",
                     "SQL", "excel", "stakeholder management", "documentation",
                     "KYC", "AML", "digital banking", "payment systems"},
        "education": ["BBA", "MBA", "B.Com", "B.Tech", "MCA"],
        "min_score": 30,
    },
    "Risk Operations Associate": {
        "category": "Risk",
        "keywords": {"risk management", "operational risk", "credit risk", "compliance",
                     "KYC", "AML", "regulatory", "audit", "Basel", "financial services",
                     "risk assessment", "control testing", "banking"},
        "education": ["B.Com", "MBA", "CA", "Finance", "Law"],
        "min_score": 30,
    },
    "Digital Payment Specialist": {
        "category": "FinTech",
        "keywords": {"digital payments", "UPI", "payment gateway", "fintech",
                     "mobile banking", "NEFT", "RTGS", "IMPS", "card processing",
                     "payment systems", "API", "digital transactions", "merchant acquiring",
                     "digital wallet", "QR payment"},
        "education": ["B.Tech", "BCA", "MCA", "B.Com", "MBA"],
        "min_score": 30,
    },
    "Compliance Officer": {
        "category": "Risk",
        "keywords": {"compliance", "KYC", "AML", "regulatory", "RBI guidelines",
                     "audit", "risk management", "banking regulations", "financial services",
                     "anti-money laundering", "PMLA", "FEMA", "governance"},
        "education": ["Law", "CA", "CS", "MBA", "B.Com"],
        "min_score": 30,
    },
    "Relationship Manager": {
        "category": "Banking",
        "keywords": {"relationship management", "customer service", "banking products",
                     "financial products", "wealth management", "cross-selling",
                     "client management", "communication", "banking", "financial advisory",
                     "sales", "portfolio management"},
        "education": ["MBA", "BBA", "B.Com", "Any Graduate"],
        "min_score": 25,
    },
    "Operations Executive - Banking": {
        "category": "Banking",
        "keywords": {"banking operations", "core banking", "transaction processing",
                     "customer service", "account management", "branch operations",
                     "NEFT", "RTGS", "deposit", "loan processing", "KYC"},
        "education": ["B.Com", "BBA", "Any Graduate"],
        "min_score": 25,
    },
    "FinTech Product Analyst": {
        "category": "FinTech",
        "keywords": {"fintech", "product analysis", "digital banking", "payment technology",
                     "API integration", "user analytics", "data analytics", "agile",
                     "product management", "UPI", "neo banking", "lending platform"},
        "education": ["B.Tech", "MBA", "BCA", "MCA"],
        "min_score": 30,
    },
    "Insurance Analyst": {
        "category": "Insurance",
        "keywords": {"insurance", "underwriting", "claims processing", "risk assessment",
                     "actuarial", "policy analysis", "insurtech", "premium calculation",
                     "reinsurance", "financial services", "compliance"},
        "education": ["B.Com", "MBA", "B.Sc Actuarial", "CA"],
        "min_score": 30,
    },
    "Treasury Analyst": {
        "category": "Banking",
        "keywords": {"treasury management", "forex", "money market", "fixed income",
                     "liquidity management", "ALM", "financial markets", "derivatives",
                     "bond", "investment", "financial modeling", "excel"},
        "education": ["MBA Finance", "CA", "CFA", "B.Com"],
        "min_score": 35,
    },
    "Full Stack Developer": {
        "category": "Technology",
        "keywords": {"full stack", "python", "javascript", "react", "node.js",
                     "SQL", "API", "REST", "docker", "git", "agile",
                     "web development", "database", "cloud", "CI/CD"},
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Data Analyst": {
        "category": "Technology",
        "keywords": {"python", "SQL", "excel", "data analytics", "pandas", "numpy",
                     "power bi", "tableau", "statistics", "data visualization",
                     "machine learning", "reporting", "ETL", "database"},
        "education": ["B.Tech", "B.Sc", "MCA", "Statistics", "Mathematics"],
        "min_score": 30,
    },
}

COURSE_KEYWORD_MAP = {
    "banking": ["banking operations", "financial products", "banking", "core banking",
                "KYC", "account management", "deposit", "branch operations",
                "customer service", "banking regulations", "financial services"],
    "fintech": ["fintech", "digital banking", "payment technology", "digital payments",
                "neo banking", "API integration", "digital transformation"],
    "payment": ["payment systems", "UPI", "NEFT", "RTGS", "IMPS", "payment gateway",
                "digital transactions", "card processing", "merchant acquiring"],
    "risk": ["risk management", "credit risk", "operational risk", "risk assessment",
             "Basel", "control testing", "risk modeling"],
    "compliance": ["compliance", "KYC", "AML", "regulatory", "audit",
                   "anti-money laundering", "PMLA", "governance"],
    "credit": ["credit analysis", "credit risk", "credit scoring", "NPA",
               "loan processing", "credit appraisal", "financial statements"],
    "insurance": ["insurance", "underwriting", "claims processing", "insurtech",
                  "policy analysis", "premium calculation"],
    "investment": ["investment analysis", "portfolio management", "wealth management",
                   "financial advisory", "mutual funds"],
    "finance": ["financial analysis", "financial services", "financial modeling",
                "financial statements", "corporate finance"],
    "data": ["data analytics", "data visualization", "reporting", "statistics", "SQL", "excel"],
    "python": ["python", "data analytics", "machine learning", "automation"],
    "ai": ["machine learning", "artificial intelligence", "deep learning"],
    "digital": ["digital transformation", "digital banking", "mobile banking", "digital payments"],
    "cbaf": ["credit analysis", "banking", "financial analysis", "credit risk",
             "banking operations", "financial services", "KYC", "compliance"],
    "adfba": ["banking", "financial services", "digital banking", "fintech",
              "banking operations", "financial products"],
    "cfbm": ["family business", "management", "business strategy", "financial planning",
             "governance", "succession planning"],
    "pgdfba": ["banking", "financial analysis", "digital banking", "fintech",
               "financial services", "compliance", "risk management"],
}


class RoleMatcher:
    """Course-first role matching. LMS data drives roles, background is supplementary."""

    _EDU_SYNONYMS = {
        "btech": ["b.tech", "btech", "bachelor of technology", "be ", "b.e."],
        "bcom":  ["b.com", "bcom", "bachelor of commerce"],
        "bba":   ["bba", "bachelor of business"],
        "bca":   ["bca", "bachelor of computer applications"],
        "mca":   ["mca", "master of computer applications"],
        "mba":   ["mba", "master of business", "pgdm"],
        "bsc":   ["b.sc", "bsc", "bachelor of science"],
        "msc":   ["m.sc", "msc", "master of science"],
        "ca":    ["chartered accountant", " ca "],
        "law":   ["llb", "law", "ll.b"],
        "any graduate": ["bachelor", "graduate", "degree"],
    }

    def __init__(self):
        self.course_intel = CourseIntelligence()

    def _normalize_edu_token(self, token: str) -> str:
        t = token.lower().replace(".", "").replace(" ", "")
        if "tech" in t or t == "be": return "btech"
        if "com" in t and "computer" not in t: return "bcom"
        if "bba" in t: return "bba"
        if "bca" in t: return "bca"
        if "mca" in t: return "mca"
        if "mba" in t or "pgdm" in t: return "mba"
        if "bsc" in t: return "bsc"
        if "msc" in t: return "msc"
        if t == "ca": return "ca"
        if "law" in t or "llb" in t: return "law"
        if "graduate" in t or "any" in t: return "any graduate"
        return t

    def _extract_lms_keywords(self, d: Dict) -> Set[str]:
        keywords = set()
        for course in d.get("courses", []):
            name = (course.get("course_name") or "").lower()
            for key, mapped_keywords in COURSE_KEYWORD_MAP.items():
                if key in name:
                    keywords.update(kw.lower() for kw in mapped_keywords)
            for word in name.split():
                if len(word) > 3:
                    keywords.add(word)
        for cs in d.get("case_studies", []):
            concepts = cs.get("key_concepts", [])
            if isinstance(concepts, list):
                keywords.update(c.lower() for c in concepts if isinstance(c, str))
            topic = (cs.get("topic") or "").lower()
            if topic:
                keywords.update(word for word in topic.split() if len(word) > 3)
        for test in d.get("test_scores", []):
            subject = (test.get("subject") or "").lower()
            keywords.update(word for word in subject.split() if len(word) > 3)
        for quiz in d.get("quiz_scores", []):
            title = (quiz.get("quiz_title") or "").lower()
            keywords.update(word for word in title.split() if len(word) > 3)
        computed = d.get("computed", {})
        if computed.get("total_quizzes", 0) + computed.get("total_case_studies", 0) >= 5:
            keywords.update(["analytical thinking", "problem solving"])
        if computed.get("total_case_studies", 0) >= 2:
            keywords.update(["critical thinking", "research", "report writing"])
        keywords.discard("")
        return keywords

    def _extract_background_keywords(self, d: Dict) -> Set[str]:
        keywords = set()
        all_skills = d.get("all_skills", {})
        for skill in all_skills.get("technical_skills", []):
            name = skill.get("name", "").lower() if isinstance(skill, dict) else str(skill).lower()
            if name and len(name) > 2: keywords.add(name)
        for tool in all_skills.get("tools", []):
            name = tool.get("name", "").lower() if isinstance(tool, dict) else str(tool).lower()
            if name and len(name) > 2: keywords.add(name)
        for skill in all_skills.get("soft_skills", []):
            name = skill.get("name", "").lower() if isinstance(skill, dict) else str(skill).lower()
            if name and len(name) > 2: keywords.add(name)
        github = d.get("github_profile", {})
        for lang in github.get("languages", {}).keys():
            keywords.add(lang.lower())
        for exp in d.get("work_experience", []):
            title = (exp.get("title") or "").lower()
            keywords.update(word for word in title.split() if len(word) > 3)
            desc = (exp.get("description") or "").lower()
            keywords.update(word for word in desc.split() if len(word) > 4)
        for edu in d.get("education", []):
            field = (edu.get("field_of_study") or "").lower()
            keywords.update(word for word in field.split() if len(word) > 3)
        personal = d.get("personal", {})
        pref_role = (personal.get("preferred_role") or "").lower()
        if pref_role:
            keywords.update(word for word in pref_role.split() if len(word) > 3)
            keywords.add(pref_role.strip())
        keywords.discard("")
        return keywords

    def _lms_keyword_score(self, lms_keywords: Set[str], role_keywords: set) -> int:
        # v12.6: case-insensitive intersection — role catalogue uses 'SQL'/'API'/'CI/CD',
        # extracted keywords are lowercased, so the raw set-intersection misses matches.
        rk_lower = {k.lower() for k in role_keywords}
        matched = {k.lower() for k in lms_keywords} & rk_lower
        if not matched: return 0
        return round(len(matched) / len(rk_lower) * 50)

    def _background_keyword_score(self, bg_keywords: Set[str], role_keywords: set) -> int:
        rk_lower = {k.lower() for k in role_keywords}
        matched = {k.lower() for k in bg_keywords} & rk_lower
        if not matched: return 0
        return round(len(matched) / len(rk_lower) * 15)

    def _education_fit_score(self, role_education: list, student_education: list) -> int:
        if not student_education: return 0
        if not role_education: return 8
        student_text = " ".join(
            f"{edu.get('degree', '')} {edu.get('field_of_study', '')}"
            for edu in student_education
        ).lower()
        if not student_text.strip(): return 0
        for role_edu in role_education:
            canonical = self._normalize_edu_token(role_edu)
            if canonical == "any graduate":
                if student_text.strip(): return 8
                continue
            synonyms = self._EDU_SYNONYMS.get(canonical, [canonical])
            for syn in synonyms:
                if syn.strip() in student_text: return 15
        return 0

    def _work_fit_score(self, role_keywords: set, student_work: list) -> int:
        if not student_work: return 0
        work_text = " ".join(
            f"{w.get('title', '')} {w.get('company', '')} {w.get('description', '')}"
            for w in student_work
        ).lower()
        if not work_text.strip(): return 0
        matches = sum(1 for kw in role_keywords if kw.lower() in work_text)
        if matches >= 4: return 10
        if matches >= 2: return 7
        if matches >= 1: return 4
        return 0

    def _completion_score(self, computed: dict) -> int:
        completed = computed.get("completed_courses", 0) or 0
        assessments = (computed.get("total_tests", 0) or 0) + (computed.get("total_quizzes", 0) or 0)
        score = 0
        if completed >= 3: score += 6
        elif completed >= 1: score += 4
        elif (computed.get("total_courses", 0) or 0) >= 1: score += 2
        if assessments >= 5: score += 4
        elif assessments >= 2: score += 2
        return min(10, score)

    # ─── Main matching ───

    def match_roles(self, student_data: Dict[str, Any]) -> List[Dict]:
        """v12.6: BLEND dynamic (course-derived) + static (background-derived) roles
        with category-diversity enforcement so background-fit roles (e.g. Tech roles
        for a B.Tech CSE student) always surface alongside course-derived BFSI roles.
        """
        courses = student_data.get("courses", []) or []
        dynamic_matches = []
        if courses:
            try:
                intel = self.course_intel.analyze(courses)
                if intel and intel.get("roles"):
                    dynamic_matches = self._match_dynamic(student_data, intel)
            except Exception as e:
                logger.warning(f"CourseIntel match failed: {e}")

        static_matches = self._match_static_legacy(student_data)

        # Dedupe across both paths — keep the higher score per role_title.
        seen = {}
        for m in dynamic_matches + static_matches:
            title = m["role_title"]
            if title not in seen or m["match_percentage"] > seen[title]["match_percentage"]:
                seen[title] = m
        merged = sorted(seen.values(), key=lambda x: x["match_percentage"], reverse=True)

        if not merged:
            return []

        # Bucket roles into "primary category" groups so we can enforce diversity.
        # The bucket key is the role's category field which the static catalogue
        # populates as Banking / Risk / FinTech / Insurance / Technology / etc.
        def bucket(m):
            cat = (m.get("category") or "Other").lower()
            if cat in ("technology", "tech"): return "tech"
            if cat in ("banking", "bfsi", "fintech", "risk", "insurance"): return "bfsi"
            return "other"

        # If we have both 'tech' AND 'bfsi' roles in the merged pool, ensure the
        # top 5 includes at least one tech AND at least one bfsi — even if their
        # scores would otherwise have been pushed down.
        tech_roles = [m for m in merged if bucket(m) == "tech"]
        bfsi_roles = [m for m in merged if bucket(m) == "bfsi"]

        top = []
        used = set()
        if tech_roles and bfsi_roles:
            # Take top BFSI (course-driven main path)
            for m in bfsi_roles[:3]:
                if m["role_title"] not in used:
                    top.append(m); used.add(m["role_title"])
            # Guarantee at least 2 tech roles
            for m in tech_roles[:2]:
                if m["role_title"] not in used:
                    top.append(m); used.add(m["role_title"])
            # Fill remainder by score
            for m in merged:
                if len(top) >= 5: break
                if m["role_title"] not in used:
                    top.append(m); used.add(m["role_title"])
            top.sort(key=lambda x: x["match_percentage"], reverse=True)
            return top[:5]

        # Single-category pool — return top 5 by score
        return merged[:5]

    def _match_dynamic(self, student_data: Dict[str, Any], intel: Dict) -> List[Dict]:
        derived_roles = intel.get("roles", [])
        derived_skills = set(s.lower() for s in intel.get("skills", []))
        domain = intel.get("domain", "Professional")
        bg_keywords = self._extract_background_keywords(student_data)
        lms_keywords = self._extract_lms_keywords(student_data)
        all_keywords = bg_keywords | lms_keywords
        education = student_data.get("education", [])
        work_experience = student_data.get("work_experience", [])
        computed = student_data.get("computed", {})
        completion_pts = self._completion_score(computed)

        matches = []
        for role_title in derived_roles:
            lms_score = 50
            overlap = len(derived_skills & {k.lower() for k in bg_keywords})
            bg_score = min(15, int(15 * (overlap / max(1, len(derived_skills)))))
            edu_score = 12 if education else 0
            work_text = " ".join(
                f"{(w.get('title') or '')} {(w.get('description') or '')}"
                for w in work_experience
            ).lower()
            work_hits = sum(1 for s in derived_skills if s in work_text)
            work_score = min(10, work_hits * 3)
            comp_score = completion_pts
            total = max(0, min(95, lms_score + bg_score + edu_score + work_score + comp_score))
            matched_kw = sorted(derived_skills & {k.lower() for k in all_keywords})
            missing_kw = sorted(derived_skills - {k.lower() for k in all_keywords})[:5]
            matches.append({
                "role_title": role_title, "category": domain,
                "match_percentage": total,
                "matching_keywords": matched_kw[:6], "missing_keywords": missing_kw,
                "total_matched": len(matched_kw), "total_required": len(derived_skills),
                "lms_driven": True,
                "score_breakdown": {"lms_courses": lms_score, "background": bg_score,
                                    "education": edu_score, "experience": work_score,
                                    "completion": comp_score},
                "recommendation": f"Sharpen {missing_kw[0]}" if missing_kw else "Strong fit on current data",
            })
        matches.sort(key=lambda x: x["match_percentage"], reverse=True)
        return matches[:5]

    def _match_static_legacy(self, student_data: Dict[str, Any]) -> List[Dict]:
        """Original ROLE_DATABASE scan — safety net when AI unavailable."""
        lms_keywords = self._extract_lms_keywords(student_data)
        bg_keywords = self._extract_background_keywords(student_data)
        all_keywords = lms_keywords | bg_keywords
        computed = student_data.get("computed", {})
        education = student_data.get("education", [])
        work_experience = student_data.get("work_experience", [])
        has_lms_courses = len(student_data.get("courses", [])) > 0
        completion_pts = self._completion_score(computed)

        matches = []
        for role_name, role_info in ROLE_DATABASE.items():
            role_keywords = role_info["keywords"]
            lms_score = self._lms_keyword_score(lms_keywords, role_keywords)
            bg_score = self._background_keyword_score(bg_keywords, role_keywords)
            edu_score = self._education_fit_score(role_info.get("education", []), education)
            work_score = self._work_fit_score(role_keywords, work_experience)
            comp_score = completion_pts
            total = lms_score + bg_score + edu_score + work_score + comp_score
            if lms_score == 0 and bg_score == 0 and edu_score == 0 and work_score == 0:
                continue
            if not has_lms_courses:
                total = min(50, total)
            final_match = max(0, min(99, total))
            # v12.6: when education AND background BOTH match strongly, the student
            # really is a fit even if LMS courses aren't in this domain. Lower the
            # threshold by 10 so tech roles surface for a B.Tech CSE + Python student
            # doing BFSI courses (and vice versa).
            effective_min = role_info.get("min_score", 25)
            if edu_score >= 12 and bg_score >= 6:
                effective_min = max(20, effective_min - 10)
            if final_match >= effective_min:
                matched_kw = all_keywords & role_keywords
                missing_kw = role_keywords - all_keywords
                matches.append({
                    "role_title": role_name, "category": role_info["category"],
                    "match_percentage": final_match,
                    "matching_keywords": sorted(list(matched_kw)),
                    "missing_keywords": sorted(list(missing_kw))[:5],
                    "total_matched": len(matched_kw), "total_required": len(role_keywords),
                    "lms_driven": lms_score > 0,
                    "score_breakdown": {"lms_courses": lms_score, "background": bg_score,
                                        "education": edu_score, "experience": work_score,
                                        "completion": comp_score},
                    "recommendation": self._get_recommendation(missing_kw, role_name),
                })
        matches.sort(key=lambda x: (x["lms_driven"], x["match_percentage"]), reverse=True)
        return matches[:5]

    def calculate_ats_score(self, student_data: Dict[str, Any]) -> Dict:
        lms_keywords = self._extract_lms_keywords(student_data)
        bg_keywords = self._extract_background_keywords(student_data)
        all_keywords = lms_keywords | bg_keywords
        computed = student_data.get("computed", {})
        matches = self.match_roles(student_data)
        best_match = matches[0]["match_percentage"] if matches else 0
        lms_kw_score = min(20, len(lms_keywords) * 2)
        bg_kw_score = min(10, len(bg_keywords))
        evidence_score = 0
        if computed.get("total_tests", 0) > 0: evidence_score += 10
        if computed.get("total_case_studies", 0) > 0: evidence_score += 10
        if computed.get("completed_courses", 0) > 0: evidence_score += 10
        perf_score = min(15, int(computed.get("overall_score", 0) * 0.15))
        data_sources = student_data.get("data_sources", [])
        source_bonus = 0
        if "resume" in data_sources: source_bonus += 4
        if "linkedin" in data_sources: source_bonus += 2
        if "github" in data_sources: source_bonus += 2
        personal = student_data.get("personal", {})
        pref_bonus = 0
        if personal.get("preferred_role"): pref_bonus += 3
        if personal.get("preferred_location"): pref_bonus += 2
        ats_total = min(98, lms_kw_score + bg_kw_score + evidence_score +
                        perf_score + int(best_match * 0.25) + source_bonus + pref_bonus)
        return {
            "total_score": ats_total,
            "keyword_count": len(all_keywords),
            "lms_keyword_count": len(lms_keywords),
            "keyword_score": lms_kw_score + bg_kw_score,
            "evidence_score": evidence_score,
            "performance_score": perf_score,
            "best_role_match": matches[0]["role_title"] if matches else "General",
            "best_role_match_pct": best_match,
            "keywords_list": sorted(list(all_keywords)),
            "improvement_tips": self._get_ats_tips(student_data, all_keywords, lms_keywords),
        }

    def _get_recommendation(self, missing: set, role: str) -> str:
        if not missing: return "Strong match — profile well-aligned with this role"
        return f"To strengthen your match, build skills in: {', '.join(sorted(list(missing))[:3])}"

    def _get_ats_tips(self, d: Dict, all_kw: set, lms_kw: set) -> List[str]:
        tips = []
        computed = d.get("computed", {})
        if len(lms_kw) == 0:
            tips.append("Enroll in courses to unlock stronger role matches")
        if computed.get("completed_courses", 0) == 0 and computed.get("total_courses", 0) > 0:
            tips.append("Complete your enrolled courses to boost profile strength")
        if computed.get("total_case_studies", 0) == 0:
            tips.append("Submit case studies to demonstrate analytical skills")
        if computed.get("total_tests", 0) < 3:
            tips.append("Take more assessments to build skill evidence")
        return tips[:4]