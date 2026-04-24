"""
Role Matcher & ATS Calculator v5 — Course-First, Dynamic
═════════════════════════════════════════════════════════
KEY CHANGE: LMS enrolled/completed courses drive role matching.
Resume/education/GitHub are SUPPLEMENTARY, not primary.

Scoring weights (total 100):
  • 50 pts — LMS course keyword overlap (enrolled/completed courses)
  • 15 pts — education fit
  • 15 pts — resume/skills keyword overlap
  • 10 pts — work experience fit
  • 10 pts — course completion + assessment volume

A student with zero LMS courses can still match roles via
resume/education, but at a capped 50% max — ensuring LMS
engagement is always the primary signal.
"""

import logging
from typing import Dict, List, Any, Set

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# ROLE DATABASE — keywords from actual JDs
# ═══════════════════════════════════════════════

ROLE_DATABASE = {
    # ── Banking & Finance ──
    "Credit Analyst": {
        "category": "Banking",
        "keywords": {
            "credit risk", "credit analysis", "financial analysis", "credit scoring",
            "NPA", "loan processing", "risk assessment", "financial statements",
            "credit appraisal", "banking", "excel", "financial modeling",
            "regulatory compliance", "Basel", "portfolio analysis",
        },
        "education": ["B.Com", "BBA", "MBA", "M.Com", "CA", "Finance"],
        "min_score": 30,
    },
    "Business Analyst - BFSI": {
        "category": "Banking",
        "keywords": {
            "business analysis", "data analytics", "financial services",
            "banking operations", "requirements gathering", "process improvement",
            "SQL", "excel", "stakeholder management", "documentation",
            "KYC", "AML", "digital banking", "payment systems",
        },
        "education": ["BBA", "MBA", "B.Com", "B.Tech", "MCA"],
        "min_score": 30,
    },
    "Risk Operations Associate": {
        "category": "Risk",
        "keywords": {
            "risk management", "operational risk", "credit risk", "compliance",
            "KYC", "AML", "regulatory", "audit", "Basel", "financial services",
            "risk assessment", "control testing", "banking",
        },
        "education": ["B.Com", "MBA", "CA", "Finance", "Law"],
        "min_score": 30,
    },
    "Digital Payment Specialist": {
        "category": "FinTech",
        "keywords": {
            "digital payments", "UPI", "payment gateway", "fintech",
            "mobile banking", "NEFT", "RTGS", "IMPS", "card processing",
            "payment systems", "API", "digital transactions", "merchant acquiring",
            "digital wallet", "QR payment",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Com", "MBA"],
        "min_score": 30,
    },
    "Compliance Officer": {
        "category": "Risk",
        "keywords": {
            "compliance", "KYC", "AML", "regulatory", "RBI guidelines",
            "audit", "risk management", "banking regulations", "financial services",
            "anti-money laundering", "PMLA", "FEMA", "governance",
        },
        "education": ["Law", "CA", "CS", "MBA", "B.Com"],
        "min_score": 30,
    },
    "Relationship Manager": {
        "category": "Banking",
        "keywords": {
            "relationship management", "customer service", "banking products",
            "financial products", "wealth management", "cross-selling",
            "client management", "communication", "banking", "financial advisory",
            "sales", "portfolio management",
        },
        "education": ["MBA", "BBA", "B.Com", "Any Graduate"],
        "min_score": 25,
    },
    "Operations Executive - Banking": {
        "category": "Banking",
        "keywords": {
            "banking operations", "core banking", "transaction processing",
            "customer service", "account management", "branch operations",
            "NEFT", "RTGS", "deposit", "loan processing", "KYC",
        },
        "education": ["B.Com", "BBA", "Any Graduate"],
        "min_score": 25,
    },
    "FinTech Product Analyst": {
        "category": "FinTech",
        "keywords": {
            "fintech", "product analysis", "digital banking", "payment technology",
            "API integration", "user analytics", "data analytics", "agile",
            "product management", "UPI", "neo banking", "lending platform",
        },
        "education": ["B.Tech", "MBA", "BCA", "MCA"],
        "min_score": 30,
    },
    "Insurance Analyst": {
        "category": "Insurance",
        "keywords": {
            "insurance", "underwriting", "claims processing", "risk assessment",
            "actuarial", "policy analysis", "insurtech", "premium calculation",
            "reinsurance", "financial services", "compliance",
        },
        "education": ["B.Com", "MBA", "B.Sc Actuarial", "CA"],
        "min_score": 30,
    },
    "Treasury Analyst": {
        "category": "Banking",
        "keywords": {
            "treasury management", "forex", "money market", "fixed income",
            "liquidity management", "ALM", "financial markets", "derivatives",
            "bond", "investment", "financial modeling", "excel",
        },
        "education": ["MBA Finance", "CA", "CFA", "B.Com"],
        "min_score": 35,
    },
    # ── Technology ──
    "Full Stack Developer - BFSI": {
        "category": "Technology",
        "keywords": {
            "full stack", "python", "javascript", "react", "node.js",
            "SQL", "API", "REST", "docker", "git", "agile",
            "web development", "database", "cloud", "CI/CD",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Python Developer": {
        "category": "Technology",
        "keywords": {
            "python", "django", "flask", "fastapi", "SQL", "database",
            "API", "REST", "git", "docker", "cloud", "agile",
            "backend", "web development", "testing", "linux",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Frontend Developer": {
        "category": "Technology",
        "keywords": {
            "javascript", "react", "html", "css", "typescript",
            "responsive design", "web development", "figma", "git",
            "API", "REST", "node.js", "tailwind", "bootstrap",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Data Analyst - Financial Services": {
        "category": "Technology",
        "keywords": {
            "data analytics", "SQL", "python", "excel", "power BI", "tableau",
            "data visualization", "statistics", "financial data", "reporting",
            "data modeling", "ETL", "machine learning", "R",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc", "Statistics"],
        "min_score": 35,
    },
    "Data Analyst": {
        "category": "Technology",
        "keywords": {
            "python", "SQL", "excel", "data analytics", "pandas", "numpy",
            "power bi", "tableau", "statistics", "data visualization",
            "machine learning", "reporting", "ETL", "database",
        },
        "education": ["B.Tech", "B.Sc", "MCA", "Statistics", "Mathematics"],
        "min_score": 30,
    },
    "Software Engineer": {
        "category": "Technology",
        "keywords": {
            "python", "javascript", "java", "SQL", "git", "docker",
            "agile", "API", "REST", "database", "cloud", "CI/CD",
            "testing", "linux", "web development", "data structures",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 35,
    },
    "Junior DevOps Engineer": {
        "category": "Technology",
        "keywords": {
            "docker", "linux", "git", "CI/CD", "cloud", "aws", "azure",
            "kubernetes", "python", "shell", "automation", "monitoring",
            "nginx", "jenkins",
        },
        "education": ["B.Tech", "BCA", "MCA"],
        "min_score": 35,
    },
    "QA / Test Engineer": {
        "category": "Technology",
        "keywords": {
            "testing", "python", "selenium", "automation", "API",
            "SQL", "git", "agile", "test cases", "bug tracking",
            "quality assurance", "regression",
        },
        "education": ["B.Tech", "BCA", "MCA"],
        "min_score": 30,
    },
}


# ═══════════════════════════════════════════════
# COURSE → KEYWORD MAPPING (Upskillize courses)
# ═══════════════════════════════════════════════

COURSE_KEYWORD_MAP = {
    "banking": ["banking operations", "financial products", "banking", "core banking",
                "KYC", "account management", "deposit", "branch operations",
                "customer service", "banking regulations", "financial services"],
    "fintech": ["fintech", "digital banking", "payment technology", "digital payments",
                "neo banking", "API integration", "digital transformation"],
    "payment": ["payment systems", "UPI", "NEFT", "RTGS", "IMPS", "payment gateway",
                "digital transactions", "card processing", "merchant acquiring", "digital wallet"],
    "risk": ["risk management", "credit risk", "operational risk", "risk assessment",
             "Basel", "control testing", "risk modeling"],
    "compliance": ["compliance", "KYC", "AML", "regulatory", "audit", "anti-money laundering",
                   "PMLA", "governance", "RBI guidelines"],
    "credit": ["credit analysis", "credit risk", "credit scoring", "NPA",
               "loan processing", "credit appraisal", "financial statements"],
    "lending": ["loan processing", "digital lending", "credit assessment",
                "lending platform", "P2P lending", "BNPL"],
    "insurance": ["insurance", "underwriting", "claims processing", "insurtech",
                  "policy analysis", "premium calculation", "reinsurance"],
    "investment": ["investment analysis", "portfolio management", "wealth management",
                   "financial advisory", "mutual funds", "equity analysis"],
    "finance": ["financial analysis", "financial services", "financial modeling",
                "financial statements", "corporate finance", "financial data"],
    "data": ["data analytics", "data visualization", "reporting", "data modeling",
             "statistics", "SQL", "excel", "power BI"],
    "python": ["python", "data analytics", "machine learning", "automation", "scripting"],
    "ai": ["machine learning", "artificial intelligence", "data analytics", "deep learning"],
    "blockchain": ["blockchain", "distributed ledger", "smart contracts", "cryptocurrency"],
    "digital": ["digital transformation", "digital banking", "mobile banking",
                "digital payments", "online banking"],
    "management": ["project management", "stakeholder management", "agile",
                   "process improvement", "team management"],
    "excel": ["excel", "financial modeling", "data analysis", "spreadsheet modeling"],
    "sql": ["SQL", "database", "data querying", "reporting"],
    "cloud": ["cloud computing", "AWS", "azure", "cloud infrastructure"],
    # Upskillize programme names
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
    """Course-first role matching. LMS data drives roles, background is supplementary.

    v5 — Weighted scoring:
      • 50 pts — LMS course keyword overlap (enrolled/completed courses)
      • 15 pts — education fit
      • 15 pts — resume/skills/background keyword overlap
      • 10 pts — work experience fit
      • 10 pts — course completion + assessment volume

    Students with zero LMS courses are capped at 50% max match,
    ensuring course engagement is always the primary signal.
    """

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

    # ─── Keyword extraction: SEPARATE LMS vs background ───

    def _extract_lms_keywords(self, d: Dict) -> Set[str]:
        """Extract keywords ONLY from LMS activity: courses, assessments, case studies."""
        keywords = set()

        # From enrolled/completed courses (highest priority)
        for course in d.get("courses", []):
            name = (course.get("course_name") or "").lower()
            for key, mapped_keywords in COURSE_KEYWORD_MAP.items():
                if key in name:
                    keywords.update(kw.lower() for kw in mapped_keywords)
            # Also add individual words from course name
            for word in name.split():
                if len(word) > 3:
                    keywords.add(word)

        # From case study topics/concepts
        for cs in d.get("case_studies", []):
            concepts = cs.get("key_concepts", [])
            if isinstance(concepts, list):
                keywords.update(c.lower() for c in concepts if isinstance(c, str))
            topic = (cs.get("topic") or "").lower()
            if topic:
                for word in topic.split():
                    if len(word) > 3:
                        keywords.add(word)

        # From test/quiz subjects
        for test in d.get("test_scores", []):
            subject = (test.get("subject") or "").lower()
            keywords.update(word for word in subject.split() if len(word) > 3)

        for quiz in d.get("quiz_scores", []):
            title = (quiz.get("quiz_title") or "").lower()
            keywords.update(word for word in title.split() if len(word) > 3)

        # Universal soft skills from LMS activity
        computed = d.get("computed", {})
        if computed.get("total_quizzes", 0) + computed.get("total_case_studies", 0) >= 5:
            keywords.update(["analytical thinking", "problem solving"])
        if computed.get("total_case_studies", 0) >= 2:
            keywords.update(["critical thinking", "research", "report writing"])

        keywords.discard("")
        return keywords

    def _extract_background_keywords(self, d: Dict) -> Set[str]:
        """Extract keywords from NON-LMS sources: resume, skills, GitHub, education, work."""
        keywords = set()

        # From resume/merged skills
        all_skills = d.get("all_skills", {})
        for skill in all_skills.get("technical_skills", []):
            name = skill.get("name", "").lower() if isinstance(skill, dict) else str(skill).lower()
            if name and len(name) > 2:
                keywords.add(name)
        for tool in all_skills.get("tools", []):
            name = tool.get("name", "").lower() if isinstance(tool, dict) else str(tool).lower()
            if name and len(name) > 2:
                keywords.add(name)
        for skill in all_skills.get("soft_skills", []):
            name = skill.get("name", "").lower() if isinstance(skill, dict) else str(skill).lower()
            if name and len(name) > 2:
                keywords.add(name)

        # From GitHub languages
        github = d.get("github_profile", {})
        for lang in github.get("languages", {}).keys():
            keywords.add(lang.lower())

        # From work experience
        for exp in d.get("work_experience", []):
            title = (exp.get("title") or "").lower()
            keywords.update(word for word in title.split() if len(word) > 3)
            desc = (exp.get("description") or "").lower()
            keywords.update(word for word in desc.split() if len(word) > 4)

        # From education
        for edu in d.get("education", []):
            field = (edu.get("field_of_study") or "").lower()
            keywords.update(word for word in field.split() if len(word) > 3)

        # From career goals / preferences
        personal = d.get("personal", {})
        pref_role = (personal.get("preferred_role") or "").lower()
        if pref_role:
            keywords.update(word for word in pref_role.split() if len(word) > 3)
            keywords.add(pref_role.strip())

        keywords.discard("")
        return keywords

    # ─── Scoring components ───

    def _lms_keyword_score(self, lms_keywords: Set[str], role_keywords: set) -> int:
        """0-50 points from LMS course keyword overlap."""
        matched = lms_keywords & role_keywords
        if not matched:
            return 0
        overlap_pct = len(matched) / len(role_keywords)
        return round(overlap_pct * 50)

    def _background_keyword_score(self, bg_keywords: Set[str], role_keywords: set) -> int:
        """0-15 points from background (resume/GitHub/education) keyword overlap."""
        matched = bg_keywords & role_keywords
        if not matched:
            return 0
        overlap_pct = len(matched) / len(role_keywords)
        return round(overlap_pct * 15)

    def _education_fit_score(self, role_education: list, student_education: list) -> int:
        """0-15 points based on degree match."""
        if not student_education:
            return 0
        if not role_education:
            return 8

        student_text = " ".join(
            f"{edu.get('degree', '')} {edu.get('field_of_study', '')}"
            for edu in student_education
        ).lower()

        if not student_text.strip():
            return 0

        for role_edu in role_education:
            canonical = self._normalize_edu_token(role_edu)
            if canonical == "any graduate":
                if student_text.strip():
                    return 8
                continue
            synonyms = self._EDU_SYNONYMS.get(canonical, [canonical])
            for syn in synonyms:
                if syn.strip() in student_text:
                    return 15
        return 0

    def _work_fit_score(self, role_keywords: set, student_work: list) -> int:
        """0-10 points from work history keyword overlap."""
        if not student_work:
            return 0
        work_text = " ".join(
            f"{w.get('title', '')} {w.get('company', '')} {w.get('description', '')}"
            for w in student_work
        ).lower()
        if not work_text.strip():
            return 0
        matches = sum(1 for kw in role_keywords if kw.lower() in work_text)
        if matches >= 4: return 10
        if matches >= 2: return 7
        if matches >= 1: return 4
        return 0

    def _completion_score(self, computed: dict) -> int:
        """0-10 points from course completion + assessment volume."""
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
        """Find top matching roles — LMS courses are PRIMARY signal."""
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

            # ── LMS score (50 pts max) ──
            lms_score = self._lms_keyword_score(lms_keywords, role_keywords)

            # ── Background score (15 pts max) ──
            bg_score = self._background_keyword_score(bg_keywords, role_keywords)

            # ── Education fit (15 pts max) ──
            edu_score = self._education_fit_score(role_info.get("education", []), education)

            # ── Work experience (10 pts max) ──
            work_score = self._work_fit_score(role_keywords, work_experience)

            # ── Completion volume (10 pts max) ──
            comp_score = completion_pts

            # Total
            total = lms_score + bg_score + edu_score + work_score + comp_score

            # ── Filter: must have SOME signal ──
            if lms_score == 0 and bg_score == 0 and edu_score == 0 and work_score == 0:
                continue

            # ── Cap: if NO LMS courses, max 50% match ──
            if not has_lms_courses:
                total = min(50, total)

            final_match = max(0, min(99, total))

            if final_match >= role_info.get("min_score", 25):
                matched_kw = all_keywords & role_keywords
                missing_kw = role_keywords - all_keywords
                matches.append({
                    "role_title": role_name,
                    "category": role_info["category"],
                    "match_percentage": final_match,
                    "matching_keywords": sorted(list(matched_kw)),
                    "missing_keywords": sorted(list(missing_kw))[:5],
                    "total_matched": len(matched_kw),
                    "total_required": len(role_keywords),
                    "lms_driven": lms_score > 0,
                    "score_breakdown": {
                        "lms_courses": lms_score,
                        "background": bg_score,
                        "education": edu_score,
                        "experience": work_score,
                        "completion": comp_score,
                    },
                    "recommendation": self._get_recommendation(missing_kw, role_name),
                })

        # Sort: LMS-driven roles FIRST, then by match percentage
        matches.sort(key=lambda x: (x["lms_driven"], x["match_percentage"]), reverse=True)
        return matches[:5]

    def calculate_ats_score(self, student_data: Dict[str, Any]) -> Dict:
        """ATS score — LMS activity weighted higher."""
        lms_keywords = self._extract_lms_keywords(student_data)
        bg_keywords = self._extract_background_keywords(student_data)
        all_keywords = lms_keywords | bg_keywords
        computed = student_data.get("computed", {})

        matches = self.match_roles(student_data)
        best_match = matches[0]["match_percentage"] if matches else 0

        # LMS keyword density (higher weight)
        lms_kw_score = min(20, len(lms_keywords) * 2)
        # Background keyword density (lower weight)
        bg_kw_score = min(10, len(bg_keywords))

        # Evidence from assessments
        evidence_score = 0
        if computed.get("total_tests", 0) > 0: evidence_score += 10
        if computed.get("total_case_studies", 0) > 0: evidence_score += 10
        if computed.get("completed_courses", 0) > 0: evidence_score += 10

        # Performance
        perf_score = min(15, int(computed.get("overall_score", 0) * 0.15))

        # External data bonus
        data_sources = student_data.get("data_sources", [])
        source_bonus = 0
        if "resume" in data_sources: source_bonus += 4
        if "linkedin" in data_sources: source_bonus += 2
        if "github" in data_sources: source_bonus += 2

        # Preferences bonus
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
        if not missing:
            return "Strong match — profile well-aligned with this role"
        missing_list = sorted(list(missing))[:3]
        return f"To strengthen your match, build skills in: {', '.join(missing_list)}"

    def _get_ats_tips(self, d: Dict, all_kw: set, lms_kw: set) -> List[str]:
        tips = []
        computed = d.get("computed", {})

        if len(lms_kw) == 0:
            tips.append("Enroll in courses to unlock stronger role matches and ATS keywords")
        if computed.get("completed_courses", 0) == 0 and computed.get("total_courses", 0) > 0:
            tips.append("Complete your enrolled courses to boost your profile strength")
        if computed.get("total_case_studies", 0) == 0:
            tips.append("Submit case studies to demonstrate applied analytical skills")
        if computed.get("total_tests", 0) < 3:
            tips.append("Take more assessments to build credible skill evidence")
        if len(all_kw) < 10:
            tips.append("Enroll in more specialized courses to expand your keyword footprint")

        return tips[:4]