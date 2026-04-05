"""
Summary Agent v5 — Hybrid (AI + Fallback)
══════════════════════════════════════════
Uses ONE Claude Haiku API call for the professional summary.
NEVER mentions course names, platform name, or LMS.
Speaks about SKILLS, DOMAINS, ACHIEVEMENTS, EDUCATION, and EXPERIENCE.

NEW in v5:
- Uses education and work experience data in summary
- Uses LinkedIn headline/summary for context
- Domain derived from education + courses + skills (not just courses)
- Emergency summary uses actual background data
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
        education = student_data.get("education", [])
        work_experience = student_data.get("work_experience", [])
        name = (personal.get("full_name") or "Student").strip()
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]

        # Derive domain from ALL available sources (not just courses)
        domain = self._derive_domain(course_names, education, work_experience, personal)

        # Build education string
        edu_str = ""
        if education:
            e = education[0]
            degree = e.get("degree", "")
            inst = e.get("institution", "")
            field = e.get("field_of_study", "")
            if degree and inst:
                edu_str = f"{degree} from {inst}"
                if field:
                    edu_str += f" ({field})"
            elif degree:
                edu_str = degree

        # Build work string
        work_str = ""
        if work_experience:
            w = work_experience[0]
            title = w.get("title", "")
            company = w.get("company", "")
            duration = w.get("duration", "")
            if title and company:
                work_str = f"{title} at {company}"
                if duration:
                    work_str += f" ({duration})"
            elif title:
                work_str = title

        # If we have education or work data, we can make a real profile even without courses
        has_background = bool(edu_str or work_str)

        if not course_names and not has_background:
            return f"{name} is an aspiring {domain} professional, building foundational skills through structured assessment programs and hands-on training."

        if self.has_api:
            try:
                return await self._ai_summary(name, student_data, domain, computed, edu_str, work_str)
            except Exception as e:
                logger.warning(f"AI summary failed, using fallback: {e}")

        return self._template_summary(name, domain, computed, edu_str, work_str)

    async def _ai_summary(self, name: str, student_data: Dict, domain: str, computed: Dict, edu_str: str, work_str: str) -> str:
        personality = student_data.get("personality", {})
        case_studies = student_data.get("case_studies", [])
        best_case = max(case_studies, key=lambda x: float(x.get("score", 0) or 0)).get("title", "") if case_studies else ""
        personal = student_data.get("personal", {})

        # LinkedIn context
        linkedin_headline = personal.get("linkedin_headline", "") or ""
        linkedin_summary = personal.get("linkedin_summary", "") or ""

        data_str = f"""Candidate: {name}
Domain: {domain}
Education: {edu_str or 'Not specified'}
Current/Past Role: {work_str or 'Not specified'}
LinkedIn Headline: {linkedin_headline}
LinkedIn Summary: {linkedin_summary[:200]}
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
Traits: {personality.get('traits_json', '')}
Career Goals: {personal.get('career_goals', '')}
Preferred Role: {personal.get('preferred_role', '')}"""

        prompt = f"""Write a 4-5 sentence professional summary for a candidate profile. Recruiters and HR managers will read this.

STRICT RULES:
1. Use ONLY the data below — never invent achievements
2. Start with the candidate's name, their education background, and a strong opening
3. If they have work experience, mention it prominently
4. Include 2-3 specific facts (education, scores, experience, skills)
5. Use industry keywords naturally: {domain}
6. End with a career-readiness statement
7. NEVER mention any platform, course title, school name, or training provider name other than their actual college/university
8. Talk about skills, domains, competencies, education, experience, and results
9. Sound like a LinkedIn summary written by a career counselor
10. Never say "dedicated learner" or "passionate about"
11. Write in third person
12. If assessment scores are 0, focus on education and experience instead — do NOT mention 0 scores
13. If they have a LinkedIn headline, draw from it for professional positioning

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

    def _template_summary(self, name: str, domain: str, computed: Dict, edu_str: str = "", work_str: str = "") -> str:
        """Template summaries — uses education and work background."""
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

        # Build intro parts
        edu_intro = f", a {edu_str} graduate" if edu_str else ""
        work_intro = f" with experience as {work_str}" if work_str else ""

        if score >= 70:
            templates = [
                f"{name}{edu_intro}{work_intro}, is a high-performing {domain} professional achieving {score}% overall across {total_assessments} rigorous assessments. "
                f"With a peak score of {best_test}% and {completed} certification{'s' if completed != 1 else ''} earned, {name} demonstrates strong analytical capabilities and verified domain expertise. "
                f"Ready for immediate contribution in roles requiring {domain} competency.",

                f"With a commanding {score}% performance in {domain}, {name}{edu_intro}{work_intro} stands out as a results-driven professional. "
                f"Having completed {total_cases} case analyses and {total_quizzes} assessments, {name} consistently demonstrates practical problem-solving ability and domain mastery. "
                f"Positioned for high-impact roles with assessment-backed credentials.",

                f"{name}{edu_intro}{work_intro} brings proven expertise in {domain}, developed through {round(hours, 1)} hours of focused professional training. "
                f"Achieving {score}% overall with {total_assessments} assessments, {name} combines theoretical knowledge with practical application. "
                f"A {improvement}% performance improvement trajectory reflects the growth mindset that employers value.",
            ]
        elif score >= 35 or total_assessments > 0:
            templates = [
                f"{name}{edu_intro}{work_intro} is building strong expertise in {domain} through structured professional development. "
                f"With {total_assessments} assessments completed at {score}% overall, {name} shows consistent engagement and growing competency. "
                f"Well-positioned for junior roles in {domain} with a proven upward trajectory.",

                f"Currently developing {domain} capabilities, {name}{edu_intro}{work_intro} has completed {total_assessments} assessments including {total_cases} case analyses. "
                f"At {score}% overall performance, {name} is building the practical skills that {domain} employers require. "
                f"An emerging professional ready for growth-oriented opportunities.",

                f"{name}{edu_intro}{work_intro} brings dedication to {domain}, having invested {round(hours, 1)} hours in professional skill development. "
                f"With {total_assessments} verified assessments completed, {name} is building credible industry expertise. "
                f"Positioned for entry-level {domain} opportunities with strong growth potential.",
            ]
        elif edu_str or work_str:
            # NEW: Has education/work background but no Upskillize assessments yet
            templates = [
                f"{name}{edu_intro}{work_intro} is an emerging {domain} professional combining academic foundations with hands-on skill development. "
                f"Currently expanding expertise through structured professional training and industry-relevant coursework. "
                f"Positioned for roles in {domain} with a strong educational foundation and proactive approach to professional growth.",

                f"{name}{edu_intro}{work_intro} brings a solid academic foundation to the {domain} sector. "
                f"Actively developing industry-relevant skills through structured professional training and certification programs. "
                f"A motivated professional ready to apply theoretical knowledge to real-world {domain} challenges.",

                f"With a background in {edu_str or domain}{work_intro}, {name} is building expertise in {domain} through focused professional development. "
                f"Combining academic knowledge with practical skill-building, {name} is positioned for emerging opportunities in the {domain} industry. "
                f"A growth-oriented professional with strong foundational capabilities.",
            ]
        else:
            templates = [
                f"{name} is building foundational expertise in {domain} through structured professional training. "
                f"Developing core competencies and positioned for growth in the {domain} sector.",
            ]

        return templates[name_hash % len(templates)]

    def _derive_domain(self, course_names: list, education: list = None, work_experience: list = None, personal: dict = None) -> str:
        """Derive professional domain from ALL available sources."""
        if education is None:
            education = []
        if work_experience is None:
            work_experience = []
        if personal is None:
            personal = {}

        # Combine all text sources for keyword matching
        all_text_parts = list(course_names)

        # Add education fields
        for edu in education:
            all_text_parts.append(edu.get("degree", ""))
            all_text_parts.append(edu.get("field_of_study", ""))
            all_text_parts.append(edu.get("institution", ""))

        # Add work experience
        for work in work_experience:
            all_text_parts.append(work.get("title", ""))
            all_text_parts.append(work.get("company", ""))
            all_text_parts.append(work.get("description", ""))

        # Add personal fields
        all_text_parts.append(personal.get("career_goals", "") or "")
        all_text_parts.append(personal.get("preferred_role", "") or "")
        all_text_parts.append(personal.get("current_designation", "") or "")
        all_text_parts.append(personal.get("linkedin_headline", "") or "")
        all_text_parts.append(personal.get("key_skills", "") or "")

        text = " ".join(all_text_parts).lower()

        if not text.strip():
            return "Financial Services"

        # More comprehensive domain detection
        if "business analy" in text or "business intelligence" in text:
            return "Business Analysis & Analytics"
        elif "ux" in text or "ui" in text or "user experience" in text or "user interface" in text:
            return "UX/UI Design & Digital Product"
        elif "data analy" in text or "data scien" in text or "power bi" in text or "tableau" in text:
            return "Data Analytics & Business Intelligence"
        elif "web develop" in text or "full stack" in text or "frontend" in text or "backend" in text:
            return "Software Development & Engineering"
        elif "digital market" in text or "marketing" in text or "seo" in text:
            return "Digital Marketing & Strategy"
        elif "fintech" in text or "digital bank" in text:
            return "FinTech & Digital Banking"
        elif "e-commerce" in text or "ecommerce" in text or "e commerce" in text:
            return "E-Commerce & Digital Business"
        elif "payment" in text or "card" in text or "upi" in text:
            return "Payment Systems & Digital Transactions"
        elif "banking" in text or "b.com" in text or "bcom" in text or "commerce" in text:
            return "Banking & Financial Services"
        elif "insurance" in text:
            return "Insurance & Risk"
        elif "risk" in text or "compliance" in text:
            return "Risk & Compliance"
        elif "finance" in text:
            return "Finance & Financial Services"
        elif "python" in text or "java" in text or "programming" in text:
            return "Software Development"
        elif "design" in text:
            return "Design & Creative Technology"
        return "Financial Services"
