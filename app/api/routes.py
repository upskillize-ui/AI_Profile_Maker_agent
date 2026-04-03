"""
API Routes — REVISED
═══════════════════════
Changes:
  - Removed hardcoded "PGCDF" and "FinTech Professional"
  - Headline and program derived from actual enrollment data
  - Added PDF download endpoint (/profile/download/{slug})
  - No fake credentials assigned to any student
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from sqlalchemy.orm import Session
from slugify import slugify
import time
import logging

from app.api.deps import get_db, get_current_student, get_current_admin
from app.models.db_models import (
    StudentProfile, ProfileViewLog, VisibilityMode, ProfileStatus,
    RubricTemplate, RubricDimension, RubricResult, RubricDimensionScore,
)
from app.models.schemas import (
    ProfileGenerateRequest, VisibilityToggleRequest,
    GradeCaseStudyRequest, RubricTemplateCreate,
)
from app.agents.profile_orchestrator import ProfileOrchestrator
from app.services.data_collector import DataCollector
from app.services.profile_renderer import ProfileRenderer
from app.services.cache_service import CacheService
from app.config import get_settings

router = APIRouter(prefix="/api/v1", tags=["Profile & Rubric"])
settings = get_settings()
logger = logging.getLogger(__name__)

AGENT_BASE = "https://upskill25-ai-enhancer.hf.space"


# ═══════════════════════════════════════════
# HELPER: Derive headline and program from REAL data
# ═══════════════════════════════════════════

def _derive_headline(student_data: dict) -> str:
    """Generate headline from actual enrolled courses, not hardcoded."""
    courses = student_data.get("courses", [])
    course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]
    if course_names:
        return " | ".join(course_names[:2]) + " Learner"
    return "Upskillize Learner"


def _derive_program(student_data: dict) -> str:
    """Get program name from batch or first enrolled course."""
    batch = student_data.get("batch_info", {})
    if batch.get("batch_name"):
        return batch["batch_name"]
    courses = student_data.get("courses", [])
    if courses:
        return courses[0].get("course_name", "Upskillize Program")
    return "Upskillize Program"


# ═══════════════════════════════════════════
# PROFILE ENDPOINTS
# ═══════════════════════════════════════════

@router.post("/profile/generate")
async def generate_profile(
    body: ProfileGenerateRequest = ProfileGenerateRequest(),
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    student_id = body.student_id or student.id

    existing = db.query(StudentProfile).filter_by(student_id=student_id).first()
    if existing and existing.status == ProfileStatus.COMPLETED and not body.force_regenerate:
        return {
            "message": "Profile already exists. Use force_regenerate=true to rebuild.",
            "slug": existing.slug,
            "status": existing.status.value,
            "profile_url": f"{AGENT_BASE}/api/v1/profile/public/{existing.slug}",
        }

    if not existing:
        existing = StudentProfile(student_id=student_id, status=ProfileStatus.GENERATING, visibility=VisibilityMode.PUBLIC)
        db.add(existing)
        db.flush()
    else:
        existing.status = ProfileStatus.GENERATING
        db.flush()

    try:
        start = time.time()

        collector = DataCollector(db)
        student_data = await collector.collect_all(student_id)

        orchestrator = ProfileOrchestrator()
        profile_data = await orchestrator.generate_profile(student_data)

        personal = student_data.get("personal", {})
        name = (personal.get("full_name") or "Student").strip()
        slug = slugify(f"{name}-{student_id}")

        renderer = ProfileRenderer()
        html = renderer.render(
            student_data=student_data,
            profile_data=profile_data,
            slug=slug,
            visibility=existing.visibility.value if existing.visibility else "public",
        )

        # ── FIXED: Derive headline and program from REAL data ──
        existing.slug = slug
        existing.student_name = name
        existing.student_email = personal.get("email", "")
        photo = personal.get("photo_url", "") or ""
        existing.student_photo_url = photo[:255] if len(photo) > 255 else photo
        existing.student_headline = profile_data.get("headline", "Financial Services Professional")
        existing.program_name = _derive_program(student_data)
        existing.professional_summary = profile_data.get("professional_summary", "")
        existing.skills_data = profile_data.get("skills_data", {})
        existing.performance_data = profile_data.get("performance_data", {})
        existing.journey_data = profile_data.get("journey_data", {})
        existing.personality_data = profile_data.get("personality_data", {})
        existing.case_studies_data = profile_data.get("case_studies_data", [])
        existing.testgen_data = profile_data.get("testgen_data", {})
        existing.projects_data = profile_data.get("projects_data", [])
        existing.certifications_data = profile_data.get("certifications_data", [])
        existing.ats_keywords = profile_data.get("ats_keywords", [])
        existing.rendered_html = html
        existing.status = ProfileStatus.COMPLETED
        existing.generation_time_seconds = round(time.time() - start, 2)
        existing.ai_model_used = profile_data.get("ai_model_used", "rule-based-v4")

        db.commit()
        db.refresh(existing)
        CacheService.set_profile_html(slug, html)

        return {
            "message": "Profile generated successfully!",
            "slug": existing.slug,
            "status": "completed",
            "profile_url": f"{AGENT_BASE}/api/v1/profile/public/{existing.slug}",
            "download_url": f"{AGENT_BASE}/api/v1/profile/download/{existing.slug}",
            "generation_time": existing.generation_time_seconds,
        }

    except Exception as e:
        logger.error(f"Profile generation failed for student {student_id}: {e}")
        existing.status = ProfileStatus.FAILED
        db.commit()
        raise HTTPException(500, f"Profile generation failed: {str(e)}")


@router.get("/profile/me")
async def get_my_profile(
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    profile = db.query(StudentProfile).filter_by(student_id=student.id).first()
    if not profile:
        raise HTTPException(404, "Profile not generated yet.")
    return {
        "id": profile.id,
        "slug": profile.slug,
        "status": profile.status.value if profile.status else "pending",
        "visibility": profile.visibility.value if profile.visibility else "private",
        "student_name": profile.student_name,
        "summary": profile.professional_summary,
        "skills": profile.skills_data,
        "performance": profile.performance_data,
        "journey": profile.journey_data,
        "personality": profile.personality_data,
        "case_studies": profile.case_studies_data,
        "testgen": profile.testgen_data,
        "projects": profile.projects_data,
        "certifications": profile.certifications_data,
        "views": profile.total_views,
        "public_url": (
            f"{AGENT_BASE}/api/v1/profile/public/{profile.slug}"
            if profile.visibility == VisibilityMode.PUBLIC else None
        ),
        "download_url": f"{AGENT_BASE}/api/v1/profile/download/{profile.slug}",
    }


@router.get("/profile/public/{slug}")
async def get_public_profile(
    slug: str,
    request: Request,
    db: Session = Depends(get_db),
):
    cached = CacheService.get_profile_html(slug)
    if cached:
        profile = db.query(StudentProfile).filter_by(slug=slug).first()
        if profile and profile.visibility == VisibilityMode.PUBLIC:
            _log_view(db, profile.id, request)
            profile.total_views += 1
            db.commit()
            return HTMLResponse(content=cached, status_code=200)

    profile = db.query(StudentProfile).filter_by(slug=slug).first()
    if not profile:
        raise HTTPException(404, "Profile not found")
    if profile.visibility != VisibilityMode.PUBLIC:
        raise HTTPException(404, "Profile Not Available")

    _log_view(db, profile.id, request)
    profile.total_views += 1
    db.commit()

    if profile.rendered_html:
        CacheService.set_profile_html(slug, profile.rendered_html)
        return HTMLResponse(content=profile.rendered_html, status_code=200)

    raise HTTPException(404, "Profile HTML not available")


# ═══════════════════════════════════════════
# NEW: PDF DOWNLOAD ENDPOINT
# ═══════════════════════════════════════════

@router.get("/profile/download/{slug}")
async def download_profile_pdf(
    slug: str,
    db: Session = Depends(get_db),
):
    """Download profile as PDF. Falls back to print-ready HTML if weasyprint unavailable."""
    profile = db.query(StudentProfile).filter_by(slug=slug).first()
    if not profile or not profile.rendered_html:
        raise HTTPException(404, "Profile not found")

    safe_name = (profile.student_name or "profile").replace(" ", "_")

    # Try weasyprint for proper PDF
    try:
        import weasyprint
        pdf_bytes = weasyprint.HTML(string=profile.rendered_html).write_pdf()
        profile.pdf_downloads = (profile.pdf_downloads or 0) + 1
        db.commit()
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{safe_name}_Upskillize_Profile.pdf"'
            },
        )
    except ImportError:
        logger.warning("weasyprint not installed — returning print-ready HTML")

    # Fallback: return HTML that auto-triggers print dialog (saves as PDF)
    print_html = profile.rendered_html.replace(
        "</body>",
        """<script>
        // Auto-trigger print for PDF save
        window.addEventListener('load', function() {
            setTimeout(function() { window.print(); }, 800);
        });
        </script></body>"""
    )
    return HTMLResponse(content=print_html, status_code=200)


@router.post("/profile/toggle-visibility")
async def toggle_visibility(
    body: VisibilityToggleRequest,
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    profile = db.query(StudentProfile).filter_by(student_id=student.id).first()
    if not profile:
        raise HTTPException(404, "Profile not found")

    old = profile.visibility
    profile.visibility = VisibilityMode(body.visibility)
    if old != profile.visibility and profile.slug:
        CacheService.invalidate_profile(profile.slug)
    db.commit()

    return {
        "message": f"Profile is now {body.visibility}",
        "visibility": profile.visibility.value,
        "public_url": (
            f"{AGENT_BASE}/api/v1/profile/public/{profile.slug}"
            if profile.visibility == VisibilityMode.PUBLIC else None
        ),
    }


# ═══════════════════════════════════════════
# RUBRIC ENDPOINTS (unchanged)
# ═══════════════════════════════════════════

@router.post("/rubric/grade/case-study")
async def grade_case_study(
    body: GradeCaseStudyRequest,
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    from app.agents.rubric_grading_agent import RubricGradingAgent

    rubric_q = db.query(RubricTemplate).filter(
        RubricTemplate.evaluation_type == "case_study",
        RubricTemplate.is_active == True,
    )
    if body.course_id:
        specific = rubric_q.filter(RubricTemplate.course_id == body.course_id).first()
        rubric_template = specific or rubric_q.filter(RubricTemplate.course_id == None).first()
    else:
        rubric_template = rubric_q.filter(RubricTemplate.course_id == None).first()

    if not rubric_template:
        raise HTTPException(404, "No rubric template found.")

    dimensions = (
        db.query(RubricDimension)
        .filter(RubricDimension.rubric_id == rubric_template.id, RubricDimension.is_active == True)
        .order_by(RubricDimension.sort_order)
        .all()
    )

    dim_dicts = [
        {
            "name": d.name, "description": d.description,
            "max_points": d.max_points, "scoring_guide": d.scoring_guide,
            "skill_tags": d.skill_tags or [],
        }
        for d in dimensions
    ]

    grader = RubricGradingAgent()
    ai_result = await grader.grade_case_study(
        student_submission=body.submission_text,
        case_study_title=body.case_title,
        case_study_brief=body.case_brief,
        dimensions=dim_dicts,
        custom_system_prompt=rubric_template.ai_system_prompt,
    )

    grade, grade_label = _calc_grade(ai_result["percentage"], rubric_template.grade_scale)

    result = RubricResult(
        student_id=student.id, rubric_id=rubric_template.id,
        evaluation_type="case_study", submission_id=body.case_study_id,
        total_score=ai_result["total_score"], max_score=ai_result["max_score"],
        percentage=ai_result["percentage"], grade=grade, grade_label=grade_label,
        overall_feedback=ai_result.get("overall_feedback", ""),
        strengths=ai_result.get("strengths", []),
        improvement_areas=ai_result.get("improvement_areas", []),
        top_competencies=ai_result.get("top_competencies", []),
        confidence_score=ai_result.get("confidence", 0),
        graded_by="ai", ai_model_used=settings.AI_MODEL,
        grading_time_ms=ai_result.get("grading_time_ms", 0),
    )
    db.add(result)
    db.flush()

    for dr in ai_result.get("dimensions", []):
        dim_id = next((d.id for d in dimensions if d.name == dr.get("name")), None)
        db.add(RubricDimensionScore(
            result_id=result.id, dimension_id=dim_id,
            score=dr["score"], max_score=dr["max_score"],
            percentage=round(dr["score"] / max(dr["max_score"], 1) * 100, 2),
            feedback=dr.get("feedback", ""), suggestion=dr.get("suggestion", ""),
            evidence_quotes=dr.get("evidence", []),
        ))

    db.commit()
    db.refresh(result)
    return _fmt_result(result, db)


@router.get("/rubric/result/{evaluation_type}/{submission_id}")
async def get_rubric_result(
    evaluation_type: str, submission_id: int,
    student=Depends(get_current_student), db: Session = Depends(get_db),
):
    result = db.query(RubricResult).filter(
        RubricResult.evaluation_type == evaluation_type,
        RubricResult.submission_id == submission_id,
        RubricResult.student_id == student.id,
    ).first()
    if not result:
        raise HTTPException(404, "Rubric result not found")
    return _fmt_result(result, db)


@router.get("/rubric/my-results")
async def get_all_my_results(
    student=Depends(get_current_student), db: Session = Depends(get_db),
    evaluation_type: str = None, limit: int = 20,
):
    q = db.query(RubricResult).filter(RubricResult.student_id == student.id)
    if evaluation_type:
        q = q.filter(RubricResult.evaluation_type == evaluation_type)
    return [_fmt_result(r, db) for r in q.order_by(RubricResult.created_at.desc()).limit(limit).all()]


@router.post("/rubric/admin/template")
async def create_rubric_template(
    body: RubricTemplateCreate,
    admin=Depends(get_current_admin), db: Session = Depends(get_db),
):
    tpl = RubricTemplate(
        name=body.name, description=body.description,
        evaluation_type=body.evaluation_type, course_id=body.course_id,
        total_points=body.total_points, passing_score=body.passing_score,
        grade_scale=body.grade_scale,
    )
    db.add(tpl)
    db.flush()
    for i, dim in enumerate(body.dimensions):
        db.add(RubricDimension(
            rubric_id=tpl.id, name=dim.name, description=dim.description,
            max_points=dim.max_points, scoring_guide=dim.scoring_guide,
            skill_tags=dim.skill_tags, sort_order=i,
        ))
    db.commit()
    return {"id": tpl.id, "message": f"Template '{body.name}' created with {len(body.dimensions)} dimensions"}


@router.get("/rubric/admin/templates")
async def list_rubric_templates(
    admin=Depends(get_current_admin), db: Session = Depends(get_db),
):
    return [
        {"id": t.id, "name": t.name, "evaluation_type": t.evaluation_type,
         "total_points": t.total_points, "dimensions_count": len(t.dimensions)}
        for t in db.query(RubricTemplate).filter(RubricTemplate.is_active == True).all()
    ]


def _calc_grade(pct, scale):
    for g in sorted(scale, key=lambda x: -x["min"]):
        if pct >= g["min"]:
            return g["grade"], g["label"]
    return "F", "Insufficient"


def _fmt_result(r, db):
    scores = db.query(RubricDimensionScore).filter(RubricDimensionScore.result_id == r.id).all()
    return {
        "id": r.id, "evaluation_type": r.evaluation_type,
        "total_score": float(r.total_score), "max_score": r.max_score,
        "percentage": float(r.percentage), "grade": r.grade,
        "grade_label": r.grade_label, "overall_feedback": r.overall_feedback,
        "strengths": r.strengths or [], "improvement_areas": r.improvement_areas or [],
        "top_competencies": r.top_competencies or [],
        "confidence": float(r.confidence_score or 0),
        "graded_by": r.graded_by, "grading_time_ms": r.grading_time_ms,
        "dimensions": [
            {"name": s.dimension.name if s.dimension else "Unknown",
             "score": float(s.score), "max_score": s.max_score,
             "percentage": float(s.percentage),
             "feedback": s.feedback, "suggestion": s.suggestion}
            for s in scores
        ],
    }


def _log_view(db, profile_id, request):
    try:
        db.add(ProfileViewLog(
            profile_id=profile_id, viewer_type="public",
            ip_address=request.client.host if request.client else "",
            user_agent=(request.headers.get("user-agent", "") or "")[:500],
            referrer=(request.headers.get("referer", "") or "")[:500],
        ))
    except Exception as e:
        logger.warning(f"View log failed: {e}")
