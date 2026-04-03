"""
Summary Agent v4 — Hybrid (AI + Fallback)
══════════════════════════════════════════
Uses ONE Claude Haiku API call for the professional summary.
Cost: ~$0.003 per profile.
Falls back to rule-based templates if API is unavailable.
ALL data in the prompt is real — nothing invented.
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
        """Generate professional summary — AI if available, template fallback."""
        personal = student_data.get("personal", {})
        computed = student_data.get("computed", {})
        courses = student_data.get("courses", [])
        name = (personal.get("full_name") or "Student").strip()

        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]

        if not course_names:
            return f"{name} is registered on Upskillize and ready to begin their professional development journey in financial services."

        # Try AI first
        if self.has_api:
            try:
                return await self._ai_summary(name, student_data, course_names, computed)
            except Exception as e:
                logger.warning(f"AI summary failed, using fallback: {e}")

        # Fallback to templates
        return self._template_summary(name, course_names, computed, student_data)

    async def _ai_summary(self, name: str, student_data: Dict, course_names: list, computed: Dict) -> str:
        """Generate summary using Claude Haiku — ONE API call."""
        personality = student_data.get("personality", {})
        case_studies = student_data.get("case_studies", [])
        best_case = max(case_studies, key=lambda x: float(x.get("score", 0) or 0)).get("title", "") if case_studies else ""

        # Build compact data string — only real numbers
        data_str = f"""Student: {name}
Courses: {', '.join(course_names[:4])}
Overall Score: {computed.get('overall_score', 0)}%
Courses Completed: {computed.get('completed_courses', 0)}/{computed.get('total_courses', 0)}
Best Test: {computed.get('best_test_score', 0)}%
Avg Test: {computed.get('avg_test_score', 0)}%
Case Studies: {computed.get('total_case_studies', 0)} (Avg: {computed.get('avg_case_study_score', 0)}%)
Quizzes: {computed.get('total_quizzes', 0)} (Avg: {computed.get('avg_quiz_score', 0)}%)
Assignments: {computed.get('total_assignments', 0)}
Learning Hours: {computed.get('total_hours', 0)}
Improvement: {computed.get('improvement_pct', 0)}%
Best Case Study: {best_case}
Personality: {personality.get('personality_type', '')}
Traits: {personality.get('traits_json', '')}"""

        prompt = f"""Write a 4-5 sentence professional summary for this student's profile page. This will be read by recruiters and placement cells.

STRICT RULES:
1. Use ONLY the data provided — never invent courses, scores, or achievements
2. Start with the student's name and a strong opening
3. Include 2-3 specific numbers from the data (scores, counts)
4. Use industry keywords naturally: banking, financial services, analytics, risk management
5. End with a career-readiness statement
6. Sound like a career counselor wrote it, not a template
7. If scores are low, focus on learning commitment and potential
8. Each summary must be unique — vary structure and vocabulary
9. Never use phrases like "dedicated learner" or "passionate about learning"
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

    def _template_summary(self, name: str, course_names: list, computed: Dict, student_data: Dict) -> str:
        """Rich template-based fallback — still unique per student."""
        score = computed.get("overall_score", 0)
        total_quizzes = computed.get("total_quizzes", 0)
        total_cases = computed.get("total_case_studies", 0)
        total_tests = computed.get("total_tests", 0)
        best_test = computed.get("best_test_score", 0)
        completed = computed.get("completed_courses", 0)
        improvement = computed.get("improvement_pct", 0)
        avg_quiz = computed.get("avg_quiz_score", 0)
        hours = computed.get("total_hours", 0)

        course_text = ", ".join(course_names[:3])
        domain = self._derive_domain(course_names)
        name_hash = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)

        # Assessment count
        total_assessments = total_quizzes + total_cases + total_tests

        if score >= 70:
            templates = [
                f"{name} has emerged as a high-performing learner in {domain}, achieving an overall score of {score}% through rigorous coursework in {course_text}. "
                f"With {total_assessments} assessments completed and a best score of {best_test}%, {name} demonstrates strong analytical capabilities and domain expertise. "
                f"Having completed {completed} course{'s' if completed != 1 else ''}, {name} brings verified, assessment-backed competency ready for professional application in the {domain} sector.",

                f"With a commanding {score}% overall performance in {course_text}, {name} stands out as a results-driven professional in {domain}. "
                f"{name} has tackled {total_cases} case studies and {total_quizzes} quizzes, consistently demonstrating practical problem-solving ability. "
                f"Backed by {completed} completed course{'s' if completed != 1 else ''} and a peak score of {best_test}%, {name} is positioned for immediate contribution in entry-level {domain} roles.",

                f"{name} brings proven expertise in {domain}, built through structured training in {course_text} on Upskillize. "
                f"Achieving {score}% overall with {total_assessments} assessments and {round(hours, 1)} hours of focused learning, {name} combines theoretical knowledge with practical application. "
                f"With demonstrated improvement of {improvement}% over the learning journey, {name} shows the growth mindset that employers value.",
            ]
        elif score >= 35:
            templates = [
                f"{name} is actively building expertise in {domain} through professional coursework in {course_text} on Upskillize. "
                f"With {total_assessments} assessments completed and a current score of {score}%, {name} demonstrates consistent engagement and growing competency. "
                + (f"A {improvement}% improvement trend highlights {name}'s capacity for rapid skill development. " if improvement > 5 else "")
                + f"Well-positioned for junior roles in {domain} with strong foundational knowledge.",

                f"Currently developing professional capabilities in {domain}, {name} is progressing through {course_text} with {total_assessments} assessments under their belt. "
                f"With {total_quizzes} quizzes and {total_cases} case studies completed, {name} is building the practical skills that {domain} employers require. "
                + (f"Maintaining a {avg_quiz}% quiz average demonstrates solid knowledge retention. " if avg_quiz > 50 else "")
                + f"An emerging professional ready for growth-oriented opportunities.",

                f"{name} brings dedication and structured learning in {domain} through enrollment in {course_text}. "
                f"Having invested {round(hours, 1)} learning hours and completed {total_assessments} assessments, {name} is building credible, assessment-verified expertise. "
                f"Positioned for entry-level opportunities in {domain} with an upward performance trajectory.",
            ]
        else:
            quiz_text = f" Has completed {total_quizzes} quiz{'zes' if total_quizzes != 1 else ''}." if total_quizzes > 0 else ""
            case_text = f" Submitted {total_cases} case {'studies' if total_cases != 1 else 'study'}." if total_cases > 0 else ""
            templates = [
                f"{name} is building foundational knowledge in {domain} through enrollment in {course_text} on Upskillize.{quiz_text}{case_text} "
                f"Developing core competencies through structured coursework and positioned for growth in the {domain} sector.",

                f"Currently enrolled in {course_text}, {name} is at the early stages of their {domain} career journey.{quiz_text} "
                f"Engaged with Upskillize's structured curriculum and building the foundational skills that {domain} employers value.",

                f"{name} has begun their professional development in {domain} with {course_text} on Upskillize.{case_text} "
                f"Building industry-relevant skills through hands-on coursework and assessments, with strong growth potential.",
            ]

        return templates[name_hash % len(templates)]

    def _derive_domain(self, course_names: list) -> str:
        text = " ".join(course_names).lower()
        if "fintech" in text:
            return "FinTech & Digital Banking"
        elif "banking" in text:
            return "Banking & Financial Services"
        elif "insurance" in text:
            return "Insurance & Risk"
        elif "payment" in text or "upi" in text:
            return "Payment Systems"
        elif "risk" in text or "compliance" in text:
            return "Risk & Compliance"
        elif "data" in text or "analytics" in text:
            return "Data Analytics"
        elif "finance" in text:
            return "Finance & Financial Services"
        return "Financial Services"
