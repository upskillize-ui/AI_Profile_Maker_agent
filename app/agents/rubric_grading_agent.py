"""
Rubric Grading Agent — AI grader for case studies and assignments.
"""

from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from app.config import get_settings
from typing import Dict, List, Any
import time
import logging

logger = logging.getLogger(__name__)


class RubricGradingAgent:

    def __init__(self):
        s = get_settings()
        self.llm = ChatAnthropic(
            model=s.AI_MODEL, api_key=s.ANTHROPIC_API_KEY,
            max_tokens=4096, temperature=0.25,
        )

    def _build_rubric_text(self, dimensions: List[Dict]) -> str:
        lines = []
        for i, dim in enumerate(dimensions, 1):
            guide = "\n".join(
                f"      {k} points: {v}" for k, v in dim.get("scoring_guide", {}).items()
            )
            lines.append(
                f"    DIMENSION {i}: {dim['name']} (0-{dim['max_points']} pts)\n"
                f"    Description: {dim['description']}\n"
                f"    Scoring Guide:\n{guide}"
            )
        return "\n\n".join(lines)

    async def grade_case_study(
        self,
        student_submission: str,
        case_study_title: str,
        case_study_brief: str,
        dimensions: List[Dict],
        custom_system_prompt: str = None,
    ) -> Dict[str, Any]:

        system = custom_system_prompt or (
            "You are an expert academic evaluator for a FinTech & Banking "
            "education platform (Upskillize). You grade student case study "
            "submissions with absolute fairness.\n\n"
            "PRINCIPLES:\n"
            "- Grade based on evidence in the submission ONLY\n"
            "- Cite actual sentences from the submission\n"
            "- Every criticism includes a specific improvement suggestion\n"
            "- Reserve 18+ for truly exceptional work\n"
            "- 15-17 = solid professional quality"
        )

        prompt = ChatPromptTemplate.from_messages([
            ("system", system),
            ("human", """CASE STUDY: {case_title}
BRIEF: {case_brief}

RUBRIC:\n{rubric}

SUBMISSION:\n---\n{submission}\n---

Grade ALL {dim_count} dimensions ({dim_list}). Return ONLY valid JSON:
{{
    "dimensions": [
        {{"name":"<name>","score":<int>,"max_score":<int>,"feedback":"...","suggestion":"...","evidence":["..."]}}
    ],
    "overall_feedback": "3-4 sentences",
    "strengths": ["..."],
    "improvement_areas": ["..."],
    "top_competencies": ["..."],
    "confidence": <float 0-100>
}}"""),
        ])

        chain = prompt | self.llm | JsonOutputParser()
        start = time.time()

        result = await chain.ainvoke({
            "case_title": case_study_title,
            "case_brief": case_study_brief,
            "rubric": self._build_rubric_text(dimensions),
            "submission": student_submission,
            "dim_count": len(dimensions),
            "dim_list": ", ".join(d["name"] for d in dimensions),
        })

        result["grading_time_ms"] = int((time.time() - start) * 1000)
        result["total_score"] = sum(d["score"] for d in result.get("dimensions", []))
        result["max_score"] = sum(d["max_score"] for d in result.get("dimensions", []))
        result["percentage"] = (
            round(result["total_score"] / result["max_score"] * 100, 2)
            if result["max_score"] > 0 else 0
        )
        return result

    async def grade_assignment(self, **kwargs) -> Dict[str, Any]:
        return await self.grade_case_study(
            student_submission=kwargs["student_submission"],
            case_study_title=kwargs["assignment_title"],
            case_study_brief=kwargs["assignment_instructions"],
            dimensions=kwargs["dimensions"],
            custom_system_prompt=(
                "You are an academic evaluator grading module assignments "
                "for a FinTech education platform. Focus on conceptual "
                "accuracy and application quality."
            ),
        )
