"""
Role Matcher & ATS Calculator v6 — Dynamic + Legacy Fallback
═════════════════════════════════════════════════════════════
FIXES from v5:
  • Removed duplicate match_roles dead code after _match_static_legacy return
  • Moved class docstring to proper position (was floating after __init__)
  • Kept ROLE_DATABASE + COURSE_KEYWORD_MAP as legacy fallback only

v13:
  • ROLE_DATABASE expanded with 10 Technology/Engineering/Product roles
  • Education/work fit weighting raised (see per-method comments: edu 15→20,
    work 10→16) so real experience + matching degree surfaces the profession
  • calculate_ats_score replaced by deterministic ATS Readiness (55/25/20),
    prestige/location-blind by construction, with locked state + bands
"""
from app.agents.course_intelligence import CourseIntelligence
import logging
import re
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
    # ── v13: Technology / Engineering / Product expansion ──
    # Real hiring-market roles so students with genuine tech backgrounds
    # (degree + work experience + skills) surface matching professions even
    # when their LMS coursework is in another domain.
    # NOTE: "Full Stack Developer" and "Data Analyst" already exist above.
    "Software Engineer": {
        "category": "Technology",
        "keywords": {"python", "java", "c++", "data structures", "algorithms", "git",
                     "SQL", "oop", "software development", "debugging", "agile",
                     "REST", "API", "unit testing", "problem solving"},
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS", "M.Tech"],
        "min_score": 35,
    },
    "Backend Developer": {
        "category": "Technology",
        "keywords": {"python", "java", "node", "django", "spring", "REST", "API",
                     "SQL", "database", "microservices", "git", "docker", "redis",
                     "authentication", "caching", "flask"},
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Frontend Developer": {
        "category": "Technology",
        "keywords": {"javascript", "typescript", "react", "angular", "vue", "html",
                     "css", "responsive design", "ui development", "git", "REST",
                     "API", "webpack", "redux", "accessibility", "tailwind"},
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Data Engineer": {
        "category": "Technology",
        "keywords": {"python", "SQL", "ETL", "data pipeline", "spark", "airflow",
                     "kafka", "data warehouse", "aws", "hadoop", "database",
                     "big data", "snowflake", "data modeling", "git"},
        "education": ["B.Tech", "MCA", "B.Sc CS", "M.Tech"],
        "min_score": 35,
    },
    "DevOps Engineer": {
        "category": "Technology",
        "keywords": {"linux", "docker", "kubernetes", "CI/CD", "aws", "azure",
                     "terraform", "ansible", "jenkins", "git", "monitoring",
                     "scripting", "bash", "cloud", "automation"},
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "QA Engineer": {
        "category": "Technology",
        "keywords": {"manual testing", "automation testing", "selenium", "test cases",
                     "bug tracking", "jira", "api testing", "regression testing",
                     "SQL", "python", "quality assurance", "agile", "postman",
                     "unit testing"},
        "education": ["B.Tech", "BCA", "MCA", "B.Sc"],
        "min_score": 30,
    },
    "Mobile App Developer": {
        "category": "Technology",
        "keywords": {"android", "ios", "kotlin", "swift", "flutter", "react native",
                     "java", "dart", "mobile development", "REST", "API", "firebase",
                     "git", "play store", "ui development"},
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Product Analyst": {
        "category": "Technology",
        "keywords": {"product analytics", "SQL", "a/b testing", "user analytics",
                     "data visualization", "excel", "product metrics", "funnel analysis",
                     "dashboards", "stakeholder management", "agile", "user research",
                     "reporting"},
        "education": ["B.Tech", "MBA", "BBA", "B.Sc"],
        "min_score": 30,
    },
    "Business Analyst": {
        "category": "Technology",
        "keywords": {"business analysis", "requirements gathering", "SQL", "excel",
                     "process improvement", "stakeholder management", "documentation",
                     "data analytics", "agile", "user stories", "gap analysis",
                     "reporting", "power bi"},
        "education": ["BBA", "MBA", "B.Com", "B.Tech", "MCA"],
        "min_score": 30,
    },
    "UI/UX Designer": {
        "category": "Technology",
        "keywords": {"figma", "wireframing", "prototyping", "user research",
                     "usability testing", "design systems", "adobe xd", "sketch",
                     "interaction design", "visual design", "html", "css",
                     "accessibility", "user personas"},
        "education": ["B.Des", "B.Tech", "BCA", "Any Graduate"],
        "min_score": 30,
    },
}

# ═══════════════════════════════════════════════════════════════════════
# ATS Readiness keyword weighting classes (v13)
#   • TOOL_KEYWORDS  → weight 2 (tools matter, but are learnable fast)
#   • SOFT_KEYWORDS  → weight 1 (valuable, but ATS engines discount them)
#   • everything else (hard skills) → weight 3
# Membership check is casefolded, so 'Git'/'GIT'/'git' all classify alike.
# ═══════════════════════════════════════════════════════════════════════
TOOL_KEYWORDS = {
    "git", "github", "gitlab", "bitbucket", "aws", "azure", "gcp", "docker",
    "kubernetes", "jenkins", "terraform", "ansible", "excel", "power bi",
    "tableau", "figma", "sketch", "adobe xd", "jira", "confluence", "postman",
    "selenium", "vscode", "firebase", "airflow", "kafka", "spark", "snowflake",
    "hadoop", "redis", "webpack", "canva", "notion", "slack", "trello",
}

SOFT_KEYWORDS = {
    "communication", "teamwork", "leadership", "problem solving",
    "critical thinking", "time management", "adaptability", "collaboration",
    "presentation", "stakeholder management", "analytical thinking",
    "creativity", "decision making", "negotiation", "attention to detail",
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
        # v13 weighting: a degree that directly matches the role's education list
        # now contributes 20 pts (was 15 in v12.6); generic-graduate / no-requirement
        # fallbacks contribute 10 (was 8). Combined with the _work_fit_score raise,
        # a student with a real matching degree + matching work experience gains up
        # to +11 pts on the static path, enough to surface their profession in the
        # top matches even when LMS courses sit in another domain — without touching
        # the lms_driven sort preference.
        if not student_education: return 0
        if not role_education: return 10          # was 8 (v12.6)
        student_text = " ".join(
            f"{edu.get('degree', '')} {edu.get('field_of_study', '')}"
            for edu in student_education
        ).lower()
        if not student_text.strip(): return 0
        for role_edu in role_education:
            canonical = self._normalize_edu_token(role_edu)
            if canonical == "any graduate":
                if student_text.strip(): return 10   # was 8 (v12.6)
                continue
            synonyms = self._EDU_SYNONYMS.get(canonical, [canonical])
            for syn in synonyms:
                if syn.strip() in student_text: return 20   # was 15 (v12.6)
        return 0

    def _work_fit_score(self, role_keywords: set, student_work: list) -> int:
        # v13 weighting: real work experience that hits the role's keywords is
        # now worth up to 16 pts (was capped at 10 in v12.6). Tiers old→new:
        # ≥4 kw hits 10→16, ≥2 hits 7→11, ≥1 hit 4→6. Moderate raise so
        # experience counts, while LMS course evidence (max 50) stays dominant.
        if not student_work: return 0
        work_text = " ".join(
            f"{w.get('title', '')} {w.get('company', '')} {w.get('description', '')}"
            for w in student_work
        ).lower()
        if not work_text.strip(): return 0
        matches = sum(1 for kw in role_keywords if kw.lower() in work_text)
        if matches >= 4: return 16    # was 10 (v12.6)
        if matches >= 2: return 11    # was 7  (v12.6)
        if matches >= 1: return 6     # was 4  (v12.6)
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

    # ═══════════════════════════════════════════════════════════════════
    # ATS Readiness (v13) — deterministic 0-100, formula 55/25/20
    #
    # PRESTIGE / LOCATION-BLIND BY CONSTRUCTION:
    # Institution names, college tier, and city are NEVER read by any
    # component of this score. The keyword corpus is built ONLY from
    # skills, work titles/descriptions, project titles/descriptions,
    # course names, and key_skills — education contributes only a binary
    # "degree with year present" completeness check that never inspects
    # the institution string, and no location field is touched anywhere.
    # Two students with identical activity but different colleges or
    # cities therefore receive EXACTLY the same score. This is deliberate:
    # ATS Readiness measures evidence a hiring system can parse, not
    # pedigree — a SEC Sasaram student with the same work earns the same
    # number as an IIT Bombay student.
    # ═══════════════════════════════════════════════════════════════════

    _ROLE_TOKEN_SYNONYMS = {"developer": "engineer", "programmer": "engineer",
                            "dev": "engineer", "engg": "engineer"}

    @staticmethod
    def _keyword_weight(kw: str) -> int:
        k = kw.casefold()
        if k in TOOL_KEYWORDS: return 2
        if k in SOFT_KEYWORDS: return 1
        return 3  # hard skill

    @staticmethod
    def _kw_in_text(kw: str, text: str) -> bool:
        """Boundary-aware match so 'java' never matches 'javascript'.
        Tolerates a simple plural ('api' matches 'REST APIs')."""
        pat = (r"(?<![a-z0-9])" + re.escape(kw.casefold()) +
               r"(?:es|s)?(?![a-z0-9])")
        return bool(re.search(pat, text))

    def _build_ats_corpus(self, d: Dict) -> tuple:
        """Return (corpus_text, skills_list). Corpus = all_skills + work
        titles/descriptions + project titles/descriptions + course names
        + key_skills. NO institution, NO city — see blindness note above."""
        skills = []
        all_skills = d.get("all_skills", {}) or {}
        for section in ("technical_skills", "tools", "soft_skills"):
            for s in all_skills.get(section, []) or []:
                name = s.get("name", "") if isinstance(s, dict) else str(s)
                if name and name.strip(): skills.append(name.strip())
        key_skills = (d.get("personal", {}) or {}).get("key_skills") or ""
        for s in str(key_skills).replace(";", ",").split(","):
            if s.strip(): skills.append(s.strip())
        # de-dupe skills, order-preserving then sorted for determinism
        skills = sorted({s.casefold(): s for s in skills}.values(), key=str.casefold)

        parts = list(skills)
        for w in d.get("work_experience", []) or []:
            parts.append(w.get("title") or "")
            parts.append(w.get("description") or "")
        for p in d.get("projects", []) or []:
            parts.append(p.get("title") or p.get("name") or "")
            parts.append(p.get("description") or "")
        for c in d.get("courses", []) or []:
            parts.append(c.get("course_name") or c.get("name") or "")
        text = " \n ".join(str(x).casefold() for x in parts if x)
        return text, skills

    def _resolve_target_role(self, preferred: str):
        """Fuzzy-map personal.preferred_role to the closest ROLE_DATABASE key
        via casefold substring / normalized token overlap. None if no fit."""
        p = (preferred or "").casefold().strip()
        if not p: return None

        def norm_tokens(s: str) -> set:
            toks = re.findall(r"[a-z0-9+#.]+", s.casefold())
            return {self._ROLE_TOKEN_SYNONYMS.get(t, t) for t in toks}

        p_tokens = norm_tokens(p)
        best_role, best_score = None, 0.0
        for role in sorted(ROLE_DATABASE):     # sorted → deterministic ties
            r = role.casefold()
            if p == r: return role
            score = 0.9 if (p in r or r in p) else 0.0
            r_tokens = norm_tokens(r)
            if p_tokens and r_tokens:
                overlap = len(p_tokens & r_tokens) / len(p_tokens | r_tokens)
                score = max(score, overlap)
            if score > best_score:
                best_role, best_score = role, score
        return best_role if best_score >= 0.34 else None

    def calculate_ats_score(self, student_data: Dict[str, Any]) -> Dict:
        """ATS Readiness: keyword match (55) + completeness (25) + verified
        evidence (20, additive-only). Deterministic, pure stdlib."""
        d = student_data
        personal = d.get("personal", {}) or {}
        education = d.get("education", []) or []
        work = d.get("work_experience", []) or []
        projects = d.get("projects", []) or []
        certifications = d.get("certifications", []) or []

        corpus_text, skills_list = self._build_ats_corpus(d)

        # Assessment/test scores (test_scores is a legacy alias of assessments
        # upstream — collect from all, de-duped by object identity is unneeded
        # since we only take max / count best).
        numeric_scores = []
        for key in ("test_scores", "assessments", "quiz_scores"):
            for t in d.get(key, []) or []:
                sc = t.get("score")
                if isinstance(sc, (int, float)) and sc is not None:
                    numeric_scores.append(float(sc))

        # ── LOCKED STATE ── no skills AND no assessment/test scores
        if not skills_list and not numeric_scores:
            return {
                "locked": True, "total_score": 0, "score": 0, "band": "Locked",
                "message": ("ATS Readiness unlocks after you add your skills "
                            "and complete your first assessment."),
                "target_role": "General",
                "components": {
                    "keyword": {"score": 0.0, "max": 55, "matched": [], "missing": []},
                    "completeness": {"score": 0, "max": 25, "checks": {
                        "contact_complete": False, "education_with_year": False,
                        "work_fully_dated": False, "skills_8_plus": False,
                        "quantified_achievement": False, "target_role_set": False}},
                    "evidence": {"score": 0, "max": 20, "details": {}},
                },
                "tips": ["Add your skills to your profile",
                         "Complete your first assessment to unlock ATS Readiness"],
                "keywords_list": [],
                "best_role_match": "General", "best_role_match_pct": 0,
            }

        matches = self.match_roles(d)

        # ── TARGET ROLE ── preferred_role fuzzy → top match_roles → "General"
        target_role = self._resolve_target_role(personal.get("preferred_role", ""))
        if not target_role and matches:
            top_title = matches[0]["role_title"]
            target_role = top_title if top_title in ROLE_DATABASE \
                else self._resolve_target_role(top_title)
        if not target_role:
            target_role = "General"

        # ── KEYWORD MATCH (55) ── weighted coverage of target-role keywords
        role_keywords = sorted(ROLE_DATABASE.get(target_role, {}).get("keywords", set()),
                               key=str.casefold)
        matched, missing = [], []
        matched_weight = total_weight = 0
        for kw in role_keywords:
            w = self._keyword_weight(kw)
            total_weight += w
            if self._kw_in_text(kw, corpus_text):
                matched.append(kw); matched_weight += w
            else:
                missing.append(kw)
        kw_score = round(matched_weight / total_weight * 55, 1) if total_weight else 0.0

        # ── COMPLETENESS (25) ── binary checks
        descriptions = [(w.get("description") or "") for w in work] + \
                       [(p.get("description") or "") for p in projects]
        checks = {
            "contact_complete": bool(str(personal.get("email") or "").strip())
                                and bool(str(personal.get("phone") or "").strip()),
            "education_with_year": any(
                (str(e.get("degree") or "").strip() or str(e.get("field_of_study") or "").strip())
                and str(e.get("year") or "").strip() for e in education),
            "work_fully_dated": any(
                str(w.get("title") or "").strip() and str(w.get("company") or "").strip()
                and str(w.get("duration") or "").strip() for w in work),
            "skills_8_plus": len(skills_list) >= 8,
            "quantified_achievement": any(ch.isdigit() for desc in descriptions for ch in str(desc)),
            "target_role_set": bool(str(personal.get("preferred_role") or "").strip()),
        }
        check_points = {"contact_complete": 4, "education_with_year": 4,
                        "work_fully_dated": 5, "skills_8_plus": 4,
                        "quantified_achievement": 4, "target_role_set": 4}
        comp_score = sum(pts for name, pts in check_points.items() if checks[name])

        # ── VERIFIED EVIDENCE (20, ADDITIVE ONLY) ──
        # Weak data earns 0 — it never subtracts. A 40% test simply adds
        # nothing; it cannot pull the score below what the profile earns.
        best_score = max(numeric_scores) if numeric_scores else None
        assess_pts = 0
        if best_score is not None and best_score >= 60:
            assess_pts = int(round(4 + (min(best_score, 100.0) - 60.0) / 40.0 * 4))
        graded_cs = sum(1 for cs in d.get("case_studies", []) or []
                        if isinstance(cs.get("score"), (int, float)) and cs.get("score") > 0)
        cs_pts = min(6, graded_cs * 2)
        cert_pts = min(6, len(certifications) * 2)
        ev_score = assess_pts + cs_pts + cert_pts
        ev_details = {
            "best_assessment_score": best_score, "assessment_points": assess_pts,
            "graded_case_studies": graded_cs, "case_study_points": cs_pts,
            "certification_count": len(certifications), "certification_points": cert_pts,
        }

        total = int(round(kw_score + comp_score + ev_score))
        total = max(0, min(100, total))
        band = "Strong" if total >= 80 else ("Ready" if total >= 60 else "Developing")

        # ── TIPS ── top missing keywords (heaviest first) + failed checks
        tips = []
        for kw in sorted(missing, key=lambda k: (-self._keyword_weight(k), k.casefold()))[:2]:
            tips.append(f"Add or demonstrate '{kw}' in your skills or projects — "
                        f"it's a core keyword for {target_role} roles")
        check_tips = {
            "contact_complete": "Add both an email and a phone number so recruiters can reach you",
            "education_with_year": "Add your degree with its completion year",
            "work_fully_dated": "Add a work entry with title, company, and duration",
            "skills_8_plus": "List at least 8 skills to strengthen keyword coverage",
            "quantified_achievement": "Add a number to an achievement (e.g. 'improved X by 30%')",
            "target_role_set": "Set your preferred role so we can target the right keywords",
        }
        for name in check_points:                       # fixed order → deterministic
            if len(tips) >= 4: break
            if not checks[name]:
                tips.append(check_tips[name])
        tips = tips[:4]

        return {
            "total_score": total,
            "score": total,                              # legacy alias
            "band": band,
            "locked": False,
            "target_role": target_role,
            "components": {
                "keyword": {"score": kw_score, "max": 55,
                            "matched": sorted(matched, key=str.casefold),
                            "missing": sorted(missing, key=str.casefold)},
                "completeness": {"score": comp_score, "max": 25, "checks": checks},
                "evidence": {"score": ev_score, "max": 20, "details": ev_details},
            },
            "tips": tips,
            "keywords_list": sorted(matched, key=str.casefold),   # legacy
            "best_role_match": matches[0]["role_title"] if matches else "General",
            "best_role_match_pct": matches[0]["match_percentage"] if matches else 0,
        }

    def _get_recommendation(self, missing: set, role: str) -> str:
        if not missing: return "Strong match — profile well-aligned with this role"
        return f"To strengthen your match, build skills in: {', '.join(sorted(list(missing))[:3])}"