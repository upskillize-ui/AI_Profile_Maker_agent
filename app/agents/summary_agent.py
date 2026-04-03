"""
Summary Agent — generates unique professional summaries per student.
"""

import os
import logging
import hashlib
from typing import Dict, Any

logger = logging.getLogger(__name__)

try:
    from anthropic import Anthropic
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    HAS_ANTHROPIC = True
except Exception:
    HAS_ANTHROPIC = False
    client = None


class SummaryAgent:

    async def generate(self, student_data: Dict[str, Any]) -> str:
        personal = student_data.get("personal", {})
        computed = student_data.get("computed", {})
        courses = student_data.get("courses", [])
        quiz_scores = student_data.get("quiz_scores", [])
        name = (personal.get("full_name") or "Student").strip()

        if not HAS_ANTHROPIC or not client:
            return self._fallback_summary(name, computed, courses, quiz_scores)

        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]

        name_hash = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)

        styles = [
            "Write in a confident, achievement-focused tone emphasizing career readiness.",
            "Write in a warm, growth-oriented tone highlighting learning journey and potential.",
            "Write in a professional, corporate tone suitable for banking/finance recruiters.",
            "Write in an aspirational tone focusing on future career trajectory and ambition.",
            "Write in a results-driven tone emphasizing completed milestones and capabilities.",
        ]
        style = styles[name_hash % len(styles)]

        openers = [
            f"{name} is a motivated",
            f"With a focused approach to professional development, {name} is a",
            f"As an emerging talent in financial services, {name} is a",
            f"Driven by a passion for the banking industry, {name} is a",
            f"{name} brings dedication and curiosity as a",
            f"Currently building expertise through structured learning, {name} is a",
        ]
        opener = openers[name_hash % len(openers)]

        prompt = f"""Write a compelling 4-5 sentence professional summary for a student profile.

Student: {name}
Enrolled courses: {', '.join(course_names) if course_names else 'BFSI & FinTech programs'}
Overall score: {computed.get('overall_score', 0)}%
Tests taken: {computed.get('total_tests', 0)}
Case studies: {computed.get('total_case_studies', 0)}
Quizzes completed: {computed.get('total_quizzes', 0)}
Courses enrolled: {computed.get('total_courses', 0)}
Completed courses: {computed.get('completed_courses', 0)}
Quiz average: {computed.get('avg_quiz_score', 0)}%

CRITICAL RULES:
1. Start the summary with: "{opener}"
2. {style}
3. NEVER use generic phrases like "dedicated learner" or "committed to learning" - be SPECIFIC
4. Mention SPECIFIC course names ({', '.join(course_names[:3]) if course_names else 'banking programs'})
5. If quiz scores exist, mention assessment performance
6. Include 2-3 industry keywords from: payment systems, banking operations, financial compliance, risk management, digital banking, card processing
7. Each student summary MUST be unique - vary sentence structure and vocabulary
8. If scores are low or zero, focus on POTENTIAL and course completion
9. End with a forward-looking career readiness statement
10. Write exactly 4-5 sentences"""

        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.warning(f"Summary generation failed: {e}")
            return self._fallback_summary(name, computed, courses, quiz_scores)

    def _fallback_summary(self, name: str, computed: dict, courses: list, quiz_scores: list = None) -> str:
        course_names = [c.get("course_name", "") for c in courses[:3] if c.get("course_name")]
        score = computed.get("overall_score", 0)
        total_quizzes = computed.get("total_quizzes", 0)
        total_cases = computed.get("total_case_studies", 0)
        best_test = computed.get("best_test_score", 0)

        if not course_names:
            return f"{name} is registered on Upskillize and ready to begin their professional learning journey."

        course_text = ", ".join(course_names)

        if score > 70:
            return f"{name} has demonstrated strong aptitude across {course_text}, achieving an overall score of {score}%. A high-performing professional ready for industry challenges."
        elif score > 30:
            return f"{name} is actively building expertise in {course_text} through Upskillize with {total_quizzes} quizzes completed. Demonstrates consistent engagement and growing competency."
        else:
            quiz_text = f" Has completed {total_quizzes} quizzes." if total_quizzes > 0 else ""
            return f"{name} is currently enrolled in {course_text} on Upskillize.{quiz_text} Building foundational knowledge and positioned for growth in financial services."
