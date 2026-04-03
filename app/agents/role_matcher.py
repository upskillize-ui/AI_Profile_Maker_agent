"""
Role Matcher & ATS Calculator v4
═════════════════════════════════
100% rule-based. Matches students to real job roles using
keyword intersection from their actual course data, skills,
and achievements. Calculates ATS compatibility scores.
"""

import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# REAL JOB ROLE DATABASE — keywords from actual JDs
# ═══════════════════════════════════════════════

ROLE_DATABASE = {
    "Credit Analyst": {
        "category": "Banking",
        "keywords": {
            "credit risk", "credit analysis", "financial analysis", "credit scoring",
            "NPA", "loan processing", "risk assessment", "financial statements",
            "credit appraisal", "banking", "excel", "financial modeling",
            "regulatory compliance", "Basel", "portfolio analysis",
        },
        "education": ["B.Com", "BBA", "MBA", "M.Com", "CA", "Finance"],
        "min_score": 40,
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
        "min_score": 35,
    },
    "Risk Operations Associate": {
        "category": "Risk",
        "keywords": {
            "risk management", "operational risk", "credit risk", "compliance",
            "KYC", "AML", "regulatory", "audit", "Basel", "financial services",
            "risk assessment", "control testing", "banking",
        },
        "education": ["B.Com", "MBA", "CA", "Finance", "Law"],
        "min_score": 35,
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
        "min_score": 35,
    },
    "Compliance Officer": {
        "category": "Risk",
        "keywords": {
            "compliance", "KYC", "AML", "regulatory", "RBI guidelines",
            "audit", "risk management", "banking regulations", "financial services",
            "anti-money laundering", "PMLA", "FEMA", "governance",
        },
        "education": ["Law", "CA", "CS", "MBA", "B.Com"],
        "min_score": 40,
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
        "min_score": 30,
    },
    "Data Analyst - Financial Services": {
        "category": "Technology",
        "keywords": {
            "data analytics", "SQL", "python", "excel", "power BI", "tableau",
            "data visualization", "statistics", "financial data", "reporting",
            "data modeling", "ETL", "machine learning", "R",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc", "Statistics"],
        "min_score": 40,
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
        "min_score": 40,
    },
    "Insurance Analyst": {
        "category": "Insurance",
        "keywords": {
            "insurance", "underwriting", "claims processing", "risk assessment",
            "actuarial", "policy analysis", "insurtech", "premium calculation",
            "reinsurance", "financial services", "compliance",
        },
        "education": ["B.Com", "MBA", "B.Sc Actuarial", "CA"],
        "min_score": 35,
    },
    "Treasury Analyst": {
        "category": "Banking",
        "keywords": {
            "treasury management", "forex", "money market", "fixed income",
            "liquidity management", "ALM", "financial markets", "derivatives",
            "bond", "investment", "financial modeling", "excel",
        },
        "education": ["MBA Finance", "CA", "CFA", "B.Com"],
        "min_score": 45,
    },
    "Full Stack Developer - BFSI": {
        "category": "Technology",
        "keywords": {
            "full stack", "python", "javascript", "react", "node.js",
            "SQL", "API", "REST", "docker", "git", "agile",
            "web development", "database", "cloud", "CI/CD",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 45,
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
    "Software Engineer": {
        "category": "Technology",
        "keywords": {
            "python", "javascript", "java", "SQL", "git", "docker",
            "agile", "API", "REST", "database", "cloud", "CI/CD",
            "testing", "linux", "web development", "data structures",
        },
        "education": ["B.Tech", "BCA", "MCA", "B.Sc CS"],
        "min_score": 40,
    },
    "Data Analyst": {
        "category": "Technology",
        "keywords": {
            "python", "SQL", "excel", "data analytics", "pandas", "numpy",
            "power bi", "tableau", "statistics", "data visualization",
            "machine learning", "reporting", "ETL", "database",
        },
        "education": ["B.Tech", "B.Sc", "MCA", "Statistics", "Mathematics"],
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
        "min_score": 40,
    },
    "QA / Test Engineer": {
        "category": "Technology",
        "keywords": {
            "testing", "python", "selenium", "automation", "API",
            "SQL", "git", "agile", "test cases", "bug tracking",
            "quality assurance", "regression",
        },
        "education": ["B.Tech", "BCA", "MCA"],
        "min_score": 35,
    },
}

# ═══════════════════════════════════════════════
# COURSE → KEYWORD MAPPING (your LMS courses)
# ═══════════════════════════════════════════════

COURSE_KEYWORD_MAP = {
    "banking": ["banking operations", "financial products", "banking", "core banking",
                "KYC", "account management", "deposit", "branch operations",
                "customer service", "banking regulations"],
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
}


class RoleMatcher:
    """Matches students to real job roles based on their actual data."""

    def match_roles(self, student_data: Dict[str, Any]) -> List[Dict]:
        """Find top matching roles for a student."""
        student_keywords = self._extract_student_keywords(student_data)
        computed = student_data.get("computed", {})
        overall_score = computed.get("overall_score", 0)

        matches = []
        for role_name, role_info in ROLE_DATABASE.items():
            role_keywords = role_info["keywords"]

            # Calculate keyword overlap
            matched = student_keywords & role_keywords
            if not matched:
                continue

            match_pct = round(len(matched) / len(role_keywords) * 100)
            missing = role_keywords - student_keywords

            # Boost for strong performance
            score_bonus = 0
            if overall_score >= 70:
                score_bonus = 10
            elif overall_score >= 50:
                score_bonus = 5

            final_match = min(99, match_pct + score_bonus)

            if final_match >= role_info["min_score"]:
                matches.append({
                    "role_title": role_name,
                    "category": role_info["category"],
                    "match_percentage": final_match,
                    "matching_keywords": sorted(list(matched)),
                    "missing_keywords": sorted(list(missing))[:5],
                    "total_matched": len(matched),
                    "total_required": len(role_keywords),
                    "recommendation": self._get_recommendation(missing, role_name),
                })

        # Sort by match percentage
        matches.sort(key=lambda x: x["match_percentage"], reverse=True)
        return matches[:5]

    def calculate_ats_score(self, student_data: Dict[str, Any]) -> Dict:
        """Calculate overall ATS compatibility score."""
        student_keywords = self._extract_student_keywords(student_data)
        computed = student_data.get("computed", {})

        # ATS score is based on best role match + keyword density
        matches = self.match_roles(student_data)
        best_match = matches[0]["match_percentage"] if matches else 0

        # Keyword density score
        total_unique_keywords = len(student_keywords)
        keyword_score = min(30, total_unique_keywords * 2)

        # Skills evidence score (from actual assessments)
        evidence_score = 0
        if computed.get("total_tests", 0) > 0:
            evidence_score += 10
        if computed.get("total_case_studies", 0) > 0:
            evidence_score += 10
        if computed.get("completed_courses", 0) > 0:
            evidence_score += 10

        # Performance score
        perf_score = min(20, int(computed.get("overall_score", 0) * 0.2))

        ats_total = min(98, keyword_score + evidence_score + perf_score + int(best_match * 0.3))

        return {
            "total_score": ats_total,
            "keyword_count": total_unique_keywords,
            "keyword_score": keyword_score,
            "evidence_score": evidence_score,
            "performance_score": perf_score,
            "best_role_match": matches[0]["role_title"] if matches else "General",
            "best_role_match_pct": best_match,
            "keywords_list": sorted(list(student_keywords)),
            "improvement_tips": self._get_ats_tips(student_data, student_keywords),
        }

    def _extract_student_keywords(self, d: Dict) -> set:
        """Extract ALL keywords from student's real data."""
        keywords = set()

        # From courses
        for course in d.get("courses", []):
            name = (course.get("course_name") or "").lower()
            for key, mapped_keywords in COURSE_KEYWORD_MAP.items():
                if key in name:
                    keywords.update(kw.lower() for kw in mapped_keywords)

        # From case study topics/concepts
        for cs in d.get("case_studies", []):
            concepts = cs.get("key_concepts", [])
            if isinstance(concepts, list):
                keywords.update(c.lower() for c in concepts if isinstance(c, str))
            topic = (cs.get("topic") or "").lower()
            if topic:
                keywords.update(topic.split())

        # From test subjects
        for test in d.get("test_scores", []):
            subject = (test.get("subject") or "").lower()
            keywords.update(word for word in subject.split() if len(word) > 3)

        # From quiz titles
        for quiz in d.get("quiz_scores", []):
            title = (quiz.get("quiz_title") or "").lower()
            keywords.update(word for word in title.split() if len(word) > 3)

        # From resume skills (highest priority — student's own claim)
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

        # From work experience keywords
        for exp in d.get("work_experience", []):
            title = (exp.get("title") or "").lower()
            keywords.update(word for word in title.split() if len(word) > 3)

        # From education
        for edu in d.get("education", []):
            degree = (edu.get("degree") or "").lower()
            keywords.update(word for word in degree.split() if len(word) > 3)

        # Universal soft skills (derived from activity)
        computed = d.get("computed", {})
        if computed.get("total_quizzes", 0) + computed.get("total_case_studies", 0) >= 5:
            keywords.update(["analytical thinking", "problem solving", "self-directed learning"])
        if computed.get("total_case_studies", 0) >= 2:
            keywords.update(["critical thinking", "research", "report writing"])
        if computed.get("forum_threads", 0) + computed.get("forum_replies", 0) >= 3:
            keywords.update(["communication", "team collaboration", "knowledge sharing"])

        # Clean up
        keywords.discard("")
        return keywords

    def _get_recommendation(self, missing: set, role: str) -> str:
        """Suggest next steps based on what's missing."""
        if not missing:
            return "Strong match — profile well-aligned with this role"

        missing_list = sorted(list(missing))[:3]
        return f"To strengthen your match, build skills in: {', '.join(missing_list)}"

    def _get_ats_tips(self, d: Dict, current_keywords: set) -> List[str]:
        """Generate actionable tips to improve ATS score."""
        tips = []
        computed = d.get("computed", {})

        if computed.get("completed_courses", 0) == 0:
            tips.append("Complete at least one course to demonstrate commitment")

        if computed.get("total_case_studies", 0) == 0:
            tips.append("Submit case studies to show applied analytical skills")

        if computed.get("total_tests", 0) < 3:
            tips.append("Take more assessments to build credible skill evidence")

        if len(current_keywords) < 10:
            tips.append("Enroll in more specialized courses to expand your keyword footprint")

        if "python" not in current_keywords and "sql" not in current_keywords:
            tips.append("Adding technical skills (Python, SQL, Excel) significantly boosts ATS scores")

        forum = d.get("forum_activity", {})
        if (forum.get("threads_created", 0) or 0) + (forum.get("replies_given", 0) or 0) == 0:
            tips.append("Participate in forums to demonstrate communication and collaboration skills")

        return tips[:4]
