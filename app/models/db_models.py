"""
Database Models
═══════════════
SQLAlchemy ORM models for the AI Profile Builder.
These create NEW tables — they don't touch your existing LMS tables.
"""

from sqlalchemy import (
    Column, Integer, String, Text, JSON, Boolean,
    DateTime, Float, ForeignKey, Enum as SQLEnum, create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
import enum

Base = declarative_base()


# ─── ENUMS ───────────────────────────────────────────────

class VisibilityMode(str, enum.Enum):
    PUBLIC = "public"
    PRIVATE = "private"


class ProfileStatus(str, enum.Enum):
    PENDING = "pending"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


# ─── AI PROFILE ─────────────────────────────────────────

class StudentProfile(Base):
    __tablename__ = "ai_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, unique=True, nullable=False, index=True)
    slug = Column(String(100), unique=True, index=True)

    # AI-Generated Content
    professional_summary = Column(Text)
    skills_data = Column(JSON)
    performance_data = Column(JSON)
    journey_data = Column(JSON)
    personality_data = Column(JSON)
    case_studies_data = Column(JSON)
    testgen_data = Column(JSON)
    projects_data = Column(JSON)
    certifications_data = Column(JSON)
    ats_keywords = Column(JSON)

    # Student info snapshot (cached at generation time)
    student_name = Column(String(200))
    student_email = Column(String(200))
    student_photo_url = Column(String(500))
    student_headline = Column(String(300))
    program_name = Column(String(300))
    cohort_name = Column(String(100))

    # Rendered HTML
    rendered_html = Column(Text)

    # Visibility
    visibility = Column(SQLEnum(VisibilityMode), default=VisibilityMode.PUBLIC)
    status = Column(SQLEnum(ProfileStatus), default=ProfileStatus.PENDING)

    # Metadata
    generated_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    generation_time_seconds = Column(Float)
    ai_model_used = Column(String(50))

    # Analytics
    total_views = Column(Integer, default=0)
    recruiter_views = Column(Integer, default=0)
    pdf_downloads = Column(Integer, default=0)
    linkedin_shares = Column(Integer, default=0)


class ProfileViewLog(Base):
    __tablename__ = "ai_profile_views"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey("ai_profiles.id"))
    viewer_type = Column(String(20))
    viewer_id = Column(Integer, nullable=True)
    ip_address = Column(String(45))
    user_agent = Column(String(500))
    referrer = Column(String(500))
    viewed_at = Column(DateTime, server_default=func.now())


# ─── RUBRIC FRAMEWORK ───────────────────────────────────

class RubricTemplate(Base):
    __tablename__ = "rubric_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text)
    evaluation_type = Column(String(20), nullable=False)
    course_id = Column(Integer, nullable=True)
    module_id = Column(Integer, nullable=True)
    total_points = Column(Integer, default=100)
    passing_score = Column(Integer, default=50)
    grade_scale = Column(JSON, nullable=False)
    ai_system_prompt = Column(Text, nullable=True)
    ai_temperature = Column(Float, default=0.3)
    is_active = Column(Boolean, default=True)
    version = Column(Integer, default=1)
    created_by = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    dimensions = relationship(
        "RubricDimension", back_populates="rubric", cascade="all, delete-orphan"
    )


class RubricDimension(Base):
    __tablename__ = "rubric_dimensions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rubric_id = Column(Integer, ForeignKey("rubric_templates.id", ondelete="CASCADE"))
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=False)
    max_points = Column(Integer, nullable=False)
    weight = Column(Float, default=1.0)
    sort_order = Column(Integer, default=0)
    scoring_guide = Column(JSON, nullable=False)
    skill_tags = Column(JSON, nullable=True)
    is_active = Column(Boolean, default=True)

    rubric = relationship("RubricTemplate", back_populates="dimensions")


class RubricResult(Base):
    __tablename__ = "rubric_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, nullable=False, index=True)
    rubric_id = Column(Integer, ForeignKey("rubric_templates.id"))
    evaluation_type = Column(String(20), nullable=False)
    submission_id = Column(Integer, nullable=False)
    total_score = Column(Float, nullable=False)
    max_score = Column(Integer, nullable=False)
    percentage = Column(Float, nullable=False)
    grade = Column(String(5), nullable=False)
    grade_label = Column(String(50), nullable=False)
    overall_feedback = Column(Text, nullable=False)
    strengths = Column(JSON)
    improvement_areas = Column(JSON)
    top_competencies = Column(JSON)
    confidence_score = Column(Float)
    graded_by = Column(String(10), default="ai")
    ai_model_used = Column(String(50))
    grading_time_ms = Column(Integer)
    is_reviewed = Column(Boolean, default=False)
    reviewed_by = Column(Integer, nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    dimension_scores = relationship(
        "RubricDimensionScore", back_populates="result", cascade="all, delete-orphan"
    )


class RubricDimensionScore(Base):
    __tablename__ = "rubric_dimension_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    result_id = Column(Integer, ForeignKey("rubric_results.id", ondelete="CASCADE"))
    dimension_id = Column(Integer, ForeignKey("rubric_dimensions.id"), nullable=True)
    score = Column(Float, nullable=False)
    max_score = Column(Integer, nullable=False)
    percentage = Column(Float, nullable=False)
    feedback = Column(Text, nullable=False)
    suggestion = Column(Text, nullable=True)
    evidence_quotes = Column(JSON, nullable=True)

    result = relationship("RubricResult", back_populates="dimension_scores")
    dimension = relationship("RubricDimension")


# ─── DATABASE HELPERS ────────────────────────────────────

def get_engine(database_url: str):
    return create_engine(
        database_url, pool_pre_ping=True, pool_recycle=3600, pool_size=10
    )


def get_session_maker(engine):
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)
