"""
Skills Agent v3 — 100% Rule-Based
No AI/LLM dependencies. Skills derived from course data using keyword mapping.
"""

import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

SKILL_MAP = {
    "banking": ["Banking Operations", "Financial Products", "Banking Fundamentals"],
    "finance": ["Financial Analysis", "Financial Services", "Corporate Finance"],
    "fintech": ["FinTech Solutions", "Digital Banking", "Payment Technologies"],
    "payment": ["Payment Systems", "Transaction Processing", "UPI & Digital Payments"],
    "digital": ["Digital Transformation", "Digital Banking"],
    "blockchain": ["Blockchain Technology", "Distributed Ledger"],
    "ai": ["Artificial Intelligence", "Machine Learning Basics"],
    "data": ["Data Analytics", "Data-Driven Decision Making"],
    "risk": ["Risk Management", "Risk Assessment", "Credit Risk"],
    "compliance": ["Regulatory Compliance", "KYC/AML"],
    "lending": ["Digital Lending", "Loan Processing", "Credit Assessment"],
    "insurance": ["InsurTech", "Insurance Products"],
    "investment": ["Investment Analysis", "Portfolio Basics"],
    "credit": ["Credit Analysis", "Credit Scoring"],
    "upi": ["UPI & Payments", "Digital Payment Infrastructure"],
    "card": ["Card Processing", "Card Networks"],
    "wealth": ["Wealth Management", "Financial Planning"],
    "management": ["Project Management", "Business Management"],
}

ATS_KEYWORD_MAP = {
    "banking": "Banking", "finance": "Financial Services", "fintech": "FinTech",
    "payment": "Payments", "digital": "Digital Transformation", "blockchain": "Blockchain",
    "ai": "Artificial Intelligence", "data": "Data Analytics", "risk": "Risk Management",
    "compliance": "Regulatory Compliance", "lending": "Digital Lending",
    "insurance": "InsurTech", "investment": "Investment Analysis", "credit": "Credit Analysis",
    "upi": "UPI & Payments",
}


class SkillsAgent:

    async def generate(self, student_data: dict) -> dict:
        computed = student_data.get("computed", {})
        courses = student_data.get("courses", [])

        if not courses:
            return {"technical_skills": [], "tools": [], "soft_skills": [], "domain_knowledge": [], "ats_keywords": []}

        technical_skills = self._derive_technical_skills(courses, computed)
        domain_knowledge = self._derive_domain_knowledge(courses)
        soft_skills = self._derive_soft_skills(computed, student_data)
        ats_keywords = self._extract_keywords(courses)

        return {
            "technical_skills": technical_skills[:10],
            "tools": [],
            "soft_skills": soft_skills,
            "domain_knowledge": domain_knowledge[:6],
            "ats_keywords": ats_keywords,
        }

    def _derive_technical_skills(self, courses, computed):
        """Derive technical skills ONLY if real evidence exists.
        No more 40% placeholder skills with no scores backing them up."""
        skills = {}
        total_quizzes = computed.get("total_quizzes", 0)
        total_cases = computed.get("total_case_studies", 0)
        total_assignments = computed.get("total_assignments", 0)
        avg_test_score = computed.get("avg_test_score", 0)

        # Only generate skills if there's REAL activity backing them
        has_real_activity = (total_quizzes + total_cases + total_assignments) > 0 or avg_test_score > 0

        if has_real_activity:
            for course in courses:
                cname = (course.get("course_name") or "").lower()
                progress = course.get("progress_percentage", 0) or 0
                completed = course.get("completed_at") is not None

                # Only derive skill if course has actual progress or completion
                if progress < 10 and not completed:
                    continue

                for keyword, skill_names in SKILL_MAP.items():
                    if keyword in cname:
                        for skill_name in skill_names:
                            if skill_name not in skills:
                                base = progress * 0.4
                                test_bonus = avg_test_score * 0.3
                                activity = min(30, total_quizzes * 2 + total_cases * 5 + total_assignments * 3)
                                completion = 15 if completed else 0
                                final_score = max(50, min(95, int(base + test_bonus + activity + completion)))

                                skills[skill_name] = {
                                    "name": skill_name,
                                    "score": final_score,
                                    "evidence": f"Course: {course.get('course_name', '')}, {progress}% complete",
                                }

        # Top subjects from real test scores ALWAYS valid
        for subj_name, subj_score in computed.get("top_subjects", [])[:4]:
            if subj_name not in skills and subj_score > 0:
                skills[subj_name] = {"name": subj_name, "score": int(subj_score), "evidence": f"Test avg: {subj_score}%"}

        return sorted(skills.values(), key=lambda x: x["score"], reverse=True)

    def _derive_domain_knowledge(self, courses):
        """Only show domain knowledge for courses with REAL progress (not 0%)."""
        domain, seen = [], set()
        for course in courses:
            cname = course.get("course_name", "")
            progress = course.get("progress_percentage", 0) or 0
            # Skip courses with no real progress — don't fabricate 40% scores
            if progress < 10:
                continue
            if cname and cname not in seen:
                seen.add(cname)
                score = max(40, int(progress * 0.8))
                domain.append({"name": cname, "score": score, "evidence": f"{progress}% complete"})
        return domain

    def _derive_soft_skills(self, computed, student_data):
        soft = []
        total_quizzes = computed.get("total_quizzes", 0)
        total_cases = computed.get("total_case_studies", 0)
        total_assignments = computed.get("total_assignments", 0)
        total_activity = total_quizzes + total_cases + total_assignments

        if total_activity == 0:
            return []

        if total_activity >= 3:
            soft.append({"name": "Self-Directed Learning", "score": min(90, 50 + total_activity * 3), "evidence": f"{total_activity} assessments completed"})

        consistency = computed.get("consistency_score", 0)
        if consistency > 60:
            soft.append({"name": "Consistency", "score": int(consistency), "evidence": f"Score consistency: {consistency}%"})

        if total_cases > 0:
            case_score = computed.get("avg_case_study_score", 50)
            soft.append({"name": "Problem Solving", "score": min(90, int(case_score) + total_cases * 5), "evidence": f"{total_cases} case studies"})

        improvement = computed.get("improvement_pct", 0)
        if improvement > 5:
            soft.append({"name": "Growth Mindset", "score": min(90, 50 + int(improvement)), "evidence": f"{improvement}% improvement"})

        return soft

    def _extract_keywords(self, courses):
        keywords = set()
        for course in courses:
            name = (course.get("course_name") or "").lower()
            for key, value in ATS_KEYWORD_MAP.items():
                if key in name:
                    keywords.add(value)
        return list(keywords) if keywords else []
