"""
Pydantic Schemas
════════════════
Request/response validation for all API endpoints.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


# ─── PROFILE ────────────────────────────────────────────

class ProfileGenerateRequest(BaseModel):
    student_id: Optional[int] = None
    force_regenerate: bool = False


class VisibilityToggleRequest(BaseModel):
    visibility: str = Field(..., pattern="^(public|private)$")


class ProfileResponse(BaseModel):
    id: int
    slug: str
    status: str
    visibility: str
    student_name: Optional[str] = None
    summary: Optional[str] = None
    skills: Optional[Dict[str, Any]] = None
    performance: Optional[Dict[str, Any]] = None
    views: int = 0
    public_url: Optional[str] = None


class ProfilePublicResponse(BaseModel):
    html: str
    visibility: str = "public"


# ─── RUBRIC ─────────────────────────────────────────────

class GradeCaseStudyRequest(BaseModel):
    case_study_id: int
    submission_text: str
    case_title: str
    case_brief: str
    course_id: Optional[int] = None


class GradeAssignmentRequest(BaseModel):
    assignment_id: int
    submission_text: str
    assignment_title: str
    assignment_instructions: str
    course_id: Optional[int] = None


class DimensionScoreResponse(BaseModel):
    name: str
    score: float
    max_score: int
    percentage: float
    feedback: str
    suggestion: Optional[str] = None


class RubricResultResponse(BaseModel):
    id: int
    evaluation_type: str
    total_score: float
    max_score: int
    percentage: float
    grade: str
    grade_label: str
    overall_feedback: str
    strengths: Optional[list] = []
    improvement_areas: Optional[list] = []
    top_competencies: Optional[list] = []
    confidence: float = 0
    dimensions: List[DimensionScoreResponse] = []


class RubricDimensionCreate(BaseModel):
    name: str
    description: str
    max_points: int
    scoring_guide: dict
    skill_tags: Optional[list] = []


class RubricTemplateCreate(BaseModel):
    name: str
    description: str
    evaluation_type: str
    course_id: Optional[int] = None
    total_points: int = 100
    passing_score: int = 50
    grade_scale: list
    dimensions: List[RubricDimensionCreate]
