"""
Summary Agent v4 — Hybrid (AI + Fallback)
══════════════════════════════════════════
Uses ONE Claude Haiku API call for the professional summary.
NEVER mentions course names, platform name, or LMS.
Speaks only about SKILLS, DOMAINS, and ACHIEVEMENTS.
"""

import os
import hashlib
import logging
import httpx
from typing import Dict, Any

logger = logging.getLogger(__name__)


class SummaryAgent:

    def __init__(self):
        self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.has_api = bool(self.api_key)

    async def generate(self, student_data: Dict[str, Any]) -> str:
        personal = student_data.get("personal", {})
        computed = student_data.get("computed", {})
        courses = student_data.get("courses", [])
        name = (personal.get("full_name") or "Student").strip()
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]

        if not course_names:
            domain = "Financial Services"
        else:
            domain = self._derive_domain(course_names)

        if not course_names:
            return f"{name} is an aspiring {domain} professional, building foundational skills through structured assessment programs and hands-on training."

        if self.has_api:
            try:
                return await self._ai_summary(name, student_data, domain, computed)
            except Exception as e:
                logger.warning(f"AI summary failed, using fallback: {e}")

        return self._template_summary(name, domain, computed)

    async def _ai_summary(self, name: str, student_data: Dict, domain: str, computed: Dict) -> str:
        personality = student_data.get("personality", {})
        case_studies = student_data.get("case_studies", [])
        best_case = max(case_studies, key=lambda x: float(x.get("score", 0) or 0)).get("title", "") if case_studies else ""

        data_str = f"""Candidate: {name}
Domain: {domain}
Overall Score: {computed.get('overall_score', 0)}%
Certifications Earned: {computed.get('completed_courses', 0)}
Best Assessment Score: {computed.get('best_test_score', 0)}%
Avg Assessment Score: {computed.get('avg_test_score', 0)}%
Case Analyses Completed: {computed.get('total_case_studies', 0)} (Avg: {computed.get('avg_case_study_score', 0)}%)
Assessments Taken: {computed.get('total_quizzes', 0)} (Avg: {computed.get('avg_quiz_score', 0)}%)
Training Hours: {computed.get('total_hours', 0)}
Performance Growth: {computed.get('improvement_pct', 0)}%
Top Case Analysis: {best_case}
Personality: {personality.get('personality_type', '')}
Traits: {personality.get('traits_json', '')}"""

        prompt = f"""Write a 4-5 sentence professional summary for a candidate profile. Recruiters and HR managers will read this.

STRICT RULES:
1. Use ONLY the data below — never invent achievements
2. Start with the candidate's name and a strong opening
3. Include 2-3 specific numbers (scores, counts, hours)
4. Use industry keywords naturally: {domain}, analytics, risk management, compliance
5. End with a career-readiness statement
6. NEVER mention any platform, course title, school, or training provider name
7. Talk ONLY about skills, domains, competencies, and results
8. Sound like a LinkedIn summary written by a career counselor
9. Never say "dedicated learner" or "passionate about"
10. Write in third person

DATA:
{data_str}"""

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
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            data = response.json()
            return data["content"][0]["text"].strip()

    def _template_summary(self, name: str, domain: str, computed: Dict) -> str:
        """Template summaries — NO course names, NO platform names."""
        score = computed.get("overall_score", 0)
        total_quizzes = computed.get("total_quizzes", 0)
        total_cases = computed.get("total_case_studies", 0)
        total_tests = computed.get("total_tests", 0)
        best_test = computed.get("best_test_score", 0)
        completed = computed.get("completed_courses", 0)
        improvement = computed.get("improvement_pct", 0)
        hours = computed.get("total_hours", 0)
        name_hash = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)
        total_assessments = total_quizzes + total_cases + total_tests

        if score >= 70:
            templates = [
                f"{name} is a high-performing {domain} professional, achieving {score}% overall across {total_assessments} rigorous assessments. "
                f"With a peak score of {best_test}% and {completed} certification{'s' if completed != 1 else ''} earned, {name} demonstrates strong analytical capabilities and verified domain expertise. "
                f"Ready for immediate contribution in roles requiring {domain} competency.",

                f"With a commanding {score}% performance in {domain}, {name} stands out as a results-driven professional. "
                f"Having completed {total_cases} case analyses and {total_quizzes} assessments, {name} consistently demonstrates practical problem-solving ability and domain mastery. "
                f"Positioned for high-impact roles with assessment-backed credentials.",

                f"{name} brings proven expertise in {domain}, developed through {round(hours, 1)} hours of focused professional training. "
                f"Achieving {score}% overall with {total_assessments} assessments, {name} combines theoretical knowledge with practical application. "
                f"A {improvement}% performance improvement trajectory reflects the growth mindset that employers value.",
            ]
        elif score >= 35:
            templates = [
                f"{name} is building strong expertise in {domain} through structured professional development. "
                f"With {total_assessments} assessments completed at {score}% overall, {name} shows consistent engagement and growing competency. "
                f"Well-positioned for junior roles in {domain} with a proven upward trajectory.",

                f"Currently developing {domain} capabilities, {name} has completed {total_assessments} assessments including {total_cases} case analyses. "
                f"At {score}% overall performance, {name} is building the practical skills that {domain} employers require. "
                f"An emerging professional ready for growth-oriented opportunities.",

                f"{name} brings dedication to {domain}, having invested {round(hours, 1)} hours in professional skill development. "
                f"With {total_assessments} verified assessments completed, {name} is building credible industry expertise. "
                f"Positioned for entry-level {domain} opportunities with strong growth potential.",
            ]
        else:
            quiz_text = f" Completed {total_quizzes} assessment{'s' if total_quizzes != 1 else ''}." if total_quizzes > 0 else ""
            case_text = f" Submitted {total_cases} case {'analyses' if total_cases != 1 else 'analysis'}." if total_cases > 0 else ""
            templates = [
                f"{name} is building foundational expertise in {domain} through structured professional training.{quiz_text}{case_text} "
                f"Developing core competencies and positioned for growth in the {domain} sector.",

                f"{name} is at the early stages of a {domain} career, developing industry-relevant skills through structured assessment programs.{quiz_text} "
                f"Building the foundational expertise that {domain} employers value.",

                f"{name} has begun professional development in {domain}, building industry-relevant skills through hands-on training.{case_text} "
                f"Positioned for entry-level opportunities with strong growth potential.",
            ]

        return templates[name_hash % len(templates)]

    def _derive_domain(self, course_names: list) -> str:
        text = " ".join(course_names).lower()
        if "fintech" in text: return "FinTech & Digital Banking"
        elif "payment" in text or "card" in text or "upi" in text: return "Payment Systems & Digital Transactions"
        elif "banking" in text: return "Banking & Financial Services"
        elif "insurance" in text: return "Insurance & Risk"
        elif "risk" in text or "compliance" in text: return "Risk & Compliance"
        elif "data" in text or "analytics" in text: return "Data Analytics"
        elif "finance" in text: return "Finance & Financial Services"
        return "Financial Services"
