"""
Skills Agent — REVISED
═══════════════════════
Changes:
  - Removed fake fallback data ("PGCDF - FinTech, Banking & AI", "Digital Lending, Blockchain, Credit Risk")
  - When no data exists, returns empty skills — not invented ones
  - AI prompt strictly uses only real course/score data
  - Skills only generated for courses student actually enrolled in
"""

from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from app.config import get_settings
import json
import logging

logger = logging.getLogger(__name__)


class SkillsAgent:

    def __init__(self):
        s = get_settings()
        self.llm = ChatAnthropic(
            model=s.AI_MODEL, api_key=s.ANTHROPIC_API_KEY,
            max_tokens=2048, temperature=0.3,
        )
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a skills analyst for Upskillize, an EdTech platform.
Given a student's ACTUAL course enrollments, test scores, and case study results,
map their achievements to industry-standard skill names.

STRICT RULES:
- ONLY generate skills that are directly supported by the student's ACTUAL data.
- If the student has NO courses, NO tests, NO case studies — return empty arrays.
- Do NOT invent skills the student hasn't demonstrated.
- Do NOT assume "FinTech" or "Blockchain" skills unless the student's courses cover those topics.
- If a student is enrolled in "Banking Foundation" only, skills should be banking-related only.
- Calculate confidence scores (0-100) based on evidence strength.
- Only include skills with confidence >= 40 (lower threshold for early-stage learners).

Return ONLY valid JSON:
{{
  "technical_skills": [{{"name": "Banking Fundamentals", "score": 65, "evidence": "Enrolled in Banking Foundation"}}],
  "tools": [],
  "soft_skills": [{{"name": "Self-Directed Learning", "score": 70, "evidence": "Active enrollment"}}],
  "domain_knowledge": [{{"name": "Banking Operations", "score": 55, "evidence": "Course: Banking Foundation"}}],
  "ats_keywords": ["Banking", "Financial Services"]
}}"""),
            ("human", """Student data (use ONLY this — do not invent):
ENROLLED COURSES: {courses}
TEST SCORES BY SUBJECT: {subject_scores}
CASE STUDY TOPICS: {case_topics}
TOTAL QUIZZES COMPLETED: {quizzes}
TOTAL ASSIGNMENTS: {assignments}
PERSONALITY TRAITS: {traits}

Generate skills mapping based on ONLY the above data:"""),
        ])
        self.chain = self.prompt | self.llm | JsonOutputParser()

    async def generate(self, student_data: dict) -> dict:
        computed = student_data.get("computed", {})
        personality = student_data.get("personality", {})

        # ── FIXED: Use actual course names, not fake fallbacks ──
        courses = ", ".join(
            c.get("course_name", "") for c in student_data.get("courses", []) if c.get("course_name")
        )
        if not courses:
            courses = "No courses enrolled yet"

        case_topics = ", ".join(
            c.get("topic") or c.get("title", "")
            for c in student_data.get("case_studies", [])[:10]
            if c.get("topic") or c.get("title")
        )
        if not case_topics:
            case_topics = "No case studies submitted yet"

        total_quizzes = computed.get("total_quizzes", 0)

        try:
            result = await self.chain.ainvoke({
                "courses": courses,
                "subject_scores": json.dumps(computed.get("subject_averages", {})),
                "case_topics": case_topics,
                "quizzes": f"{total_quizzes} completed",
                "assignments": f"{computed.get('total_assignments', 0)} completed",
                "traits": personality.get("traits_json", ""),
            })
            if "ats_keywords" not in result:
                result["ats_keywords"] = self._extract_keywords(student_data)
            return result
        except Exception as e:
            logger.error(f"Skills generation failed: {e}")
            return self._fallback(student_data, computed)

    def _extract_keywords(self, student_data: dict) -> list:
        """Extract ATS keywords from ACTUAL course names only."""
        keywords = set()
        for course in student_data.get("courses", []):
            name = (course.get("course_name") or "").lower()
            # Map course name words to industry keywords
            keyword_map = {
                "banking": "Banking",
                "finance": "Financial Services",
                "fintech": "FinTech",
                "payment": "Payments",
                "digital": "Digital Transformation",
                "blockchain": "Blockchain",
                "ai": "Artificial Intelligence",
                "data": "Data Analytics",
                "risk": "Risk Management",
                "compliance": "Regulatory Compliance",
                "lending": "Digital Lending",
                "insurance": "InsurTech",
                "investment": "Investment Analysis",
                "credit": "Credit Analysis",
                "upi": "UPI & Payments",
            }
            for key, value in keyword_map.items():
                if key in name:
                    keywords.add(value)

        # If no courses, return empty — don't invent
        return list(keywords) if keywords else []

    def _fallback(self, student_data: dict, computed: dict) -> dict:
        """Fallback: derive skills from ACTUAL data only. No invented skills."""
        top = computed.get("top_subjects", [])
        courses = student_data.get("courses", [])

        # Technical skills from actual test subjects
        technical = [
            {"name": s[0], "score": int(s[1]), "evidence": f"Test avg: {s[1]}%"}
            for s in top[:4]
        ]

        # Domain knowledge from actual enrolled courses
        domain = []
        for course in courses[:4]:
            cname = course.get("course_name", "")
            if cname:
                progress = course.get("progress_percentage", 0) or 0
                domain.append({
                    "name": cname,
                    "score": max(40, int(progress * 0.8)) if progress > 0 else 40,
                    "evidence": f"Enrolled, {progress}% complete" if progress > 0 else "Currently enrolled",
                })

        # Soft skills — only if there's actual activity
        soft = []
        if computed.get("total_quizzes", 0) > 0 or computed.get("total_case_studies", 0) > 0:
            soft.append({
                "name": "Self-Directed Learning",
                "score": 65,
                "evidence": "Active assessment participation",
            })

        return {
            "technical_skills": technical,
            "tools": [],
            "soft_skills": soft,
            "domain_knowledge": domain,
            "ats_keywords": self._extract_keywords(student_data),
        }
