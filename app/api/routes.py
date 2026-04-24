"""
API Routes — v3
═══════════════════════════
KEY FIX: force_regenerate=True now does full generate with POST-GENERATION
data-loss guard. Compares new profile richness vs DB. If 30%+ fewer items,
ABORTs and keeps old profile (only refreshes template HTML).
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


def _count_db_richness(profile: StudentProfile) -> int:
    """Count data items from existing DB columns — for data-loss detection."""
    count = 0
    if profile.professional_summary: count += 1
    skills = profile.skills_data or {}
    if isinstance(skills, dict):
        count += len(skills.get("technical_skills", []))
        count += len(skills.get("tools", []))
        count += len(skills.get("soft_skills", []))
    count += len(profile.projects_data or [])
    count += len(profile.certifications_data or [])
    count += len(profile.case_studies_data or [])
    if (profile.personality_data or {}).get("personality_type"): count += 1
    perf = profile.performance_data or {}
    if perf.get("overall_score", 0) > 0: count += 1
    if perf.get("total_tests", 0) > 0: count += 1
    if perf.get("completed_courses", 0) > 0: count += 1
    return count


def _count_profile_data_richness(pd: dict) -> int:
    """Count data items in newly generated profile_data dict."""
    count = 0
    if pd.get("professional_summary"): count += 1
    skills = pd.get("skills_data", {})
    if isinstance(skills, dict):
        count += len(skills.get("technical_skills", []))
        count += len(skills.get("tools", []))
        count += len(skills.get("soft_skills", []))
    count += len(pd.get("projects_data", []))
    count += len(pd.get("certifications_data", []))
    count += len(pd.get("case_studies_data", []))
    count += len(pd.get("education_data", []))
    count += len(pd.get("work_experience", []))
    count += len(pd.get("role_matches", []))
    count += len(pd.get("top_achievements", []))
    if pd.get("personality_data", {}).get("personality_type"): count += 1
    perf = pd.get("performance_data", {})
    if perf.get("overall_score", 0) > 0: count += 1
    if perf.get("total_tests", 0) > 0: count += 1
    if perf.get("completed_courses", 0) > 0: count += 1
    return count


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

    # ── Case 1: profile exists, no force_regenerate → return as-is ──
    if existing and existing.status == ProfileStatus.COMPLETED and not body.force_regenerate:
        return {
            "message": "Profile already exists. Use force_regenerate=true to rebuild.",
            "slug": existing.slug,
            "status": existing.status.value,
            "profile_url": f"{AGENT_BASE}/api/v1/profile/public/{existing.slug}",
            "download_url": f"{AGENT_BASE}/api/v1/profile/download/{existing.slug}",
        }

    # ── Case 2: profile exists + force_regenerate → full regen with safety ──
    if existing and existing.status == ProfileStatus.COMPLETED and body.force_regenerate:
        return await _safe_regenerate(student_id, existing, db)

    # ── Case 3: no profile yet (or previous failed) → full generation ──
    if not existing:
        existing = StudentProfile(
            student_id=student_id,
            status=ProfileStatus.GENERATING,
            visibility=VisibilityMode.PUBLIC,
        )
        db.add(existing)
        db.flush()
    else:
        existing.status = ProfileStatus.GENERATING
        db.flush()

    return await _full_generate(student_id, existing, db)


async def _full_generate(student_id: int, existing: StudentProfile, db: Session) -> dict:
    """Full profile generation — runs all agents from scratch."""
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

        existing.slug = slug
        existing.student_name = name
        existing.student_email = personal.get("email", "")
        photo = personal.get("photo_url", "") or ""
        existing.student_photo_url = photo[:255] if len(photo) > 255 else photo
        existing.student_headline = profile_data.get("headline", "Professional")
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
        existing.ai_model_used = profile_data.get("ai_model_used", "rule-based-v6")

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
            "updated_sections": "all",
        }

    except Exception as e:
        logger.error(f"Profile generation failed for student {student_id}: {e}")
        existing.status = ProfileStatus.FAILED
        db.commit()
        raise HTTPException(500, f"Profile generation failed: {str(e)}")


async def _safe_regenerate(student_id: int, existing: StudentProfile, db: Session) -> dict:
    """Full regeneration with DATA-LOSS SAFETY GUARD.

    Runs a complete fresh generation, then compares the new profile's
    data richness against the existing DB profile. If the new profile
    has 30%+ fewer data items, ABORTS and keeps the old profile,
    only refreshing the HTML template.
    """
    try:
        start = time.time()

        # ── Measure old richness BEFORE regeneration ──
        old_richness = _count_db_richness(existing)
        logger.info(f"Safe regen student {student_id}: old richness = {old_richness}")

        collector = DataCollector(db)
        student_data = await collector.collect_all(student_id)

        orchestrator = ProfileOrchestrator()
        profile_data = await orchestrator.generate_profile(student_data)

        # ── Measure new richness ──
        new_richness = _count_profile_data_richness(profile_data)
        logger.info(f"Safe regen student {student_id}: new richness = {new_richness}")

        personal = student_data.get("personal", {})
        name = (personal.get("full_name") or "Student").strip()
        slug = slugify(f"{name}-{student_id}")

        # ── DATA-LOSS CHECK ──
        if old_richness >= 5 and new_richness < old_richness * 0.7:
            logger.error(
                f"DATA LOSS DETECTED student {student_id}: "
                f"old={old_richness}, new={new_richness}. Keeping old profile."
            )

            # Re-render HTML with old DB data + fresh LMS passthroughs
            old_profile_data = {
                "professional_summary":  existing.professional_summary or "",
                "skills_data":           existing.skills_data or {},
                "performance_data":      existing.performance_data or {},
                "journey_data":          existing.journey_data or {},
                "personality_data":      existing.personality_data or {},
                "case_studies_data":     existing.case_studies_data or [],
                "testgen_data":          existing.testgen_data or {},
                "projects_data":         existing.projects_data or [],
                "certifications_data":   existing.certifications_data or [],
                "ats_keywords":          existing.ats_keywords or [],
                "headline":              existing.student_headline or "Professional",
                "education_data":        profile_data.get("education_data", []),
                "work_experience":       profile_data.get("work_experience", []),
                "role_matches":          profile_data.get("role_matches", []),
                "ats_data":              profile_data.get("ats_data", {}),
                "top_achievements":      profile_data.get("top_achievements", []),
                "case_study_highlights": profile_data.get("case_study_highlights", []),
                "test_highlights":       profile_data.get("test_highlights", []),
                "data_sources":          profile_data.get("data_sources", ["lms"]),
            }

            renderer = ProfileRenderer()
            html = renderer.render(
                student_data=student_data,
                profile_data=old_profile_data,
                slug=slug,
                visibility=existing.visibility.value if existing.visibility else "public",
            )

            existing.slug = slug
            existing.rendered_html = html
            existing.generation_time_seconds = round(time.time() - start, 2)
            db.commit()
            CacheService.invalidate_profile(slug)
            CacheService.set_profile_html(slug, html)

            return {
                "message": "Profile refreshed with latest template (data preserved).",
                "slug": slug,
                "status": "completed",
                "profile_url": f"{AGENT_BASE}/api/v1/profile/public/{slug}",
                "download_url": f"{AGENT_BASE}/api/v1/profile/download/{slug}",
                "updated_sections": [],
                "was_no_op": True,
                "regen_time": round(time.time() - start, 2),
            }

        # ── No data loss — save new profile ──
        renderer = ProfileRenderer()
        html = renderer.render(
            student_data=student_data,
            profile_data=profile_data,
            slug=slug,
            visibility=existing.visibility.value if existing.visibility else "public",
        )

        existing.slug = slug
        existing.student_name = name
        existing.student_email = personal.get("email", "")
        photo = personal.get("photo_url", "") or ""
        existing.student_photo_url = photo[:255] if len(photo) > 255 else photo
        existing.student_headline = profile_data.get("headline", "Professional")
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
        existing.ai_model_used = profile_data.get("ai_model_used", "rule-based-v6")

        db.commit()
        db.refresh(existing)
        CacheService.invalidate_profile(slug)
        CacheService.set_profile_html(slug, html)

        return {
            "message": "Profile regenerated successfully!",
            "slug": existing.slug,
            "status": "completed",
            "profile_url": f"{AGENT_BASE}/api/v1/profile/public/{existing.slug}",
            "download_url": f"{AGENT_BASE}/api/v1/profile/download/{existing.slug}",
            "generation_time": existing.generation_time_seconds,
            "updated_sections": "all",
        }

    except Exception as e:
        logger.error(f"Safe regeneration failed for student {student_id}: {e}")
        raise HTTPException(500, f"Regeneration failed: {str(e)}")


# ═══════════════════════════════════════════
# REMAINING ENDPOINTS (unchanged)
# ═══════════════════════════════════════════

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
        "updated_at": str(profile.updated_at) if hasattr(profile, 'updated_at') else None,
    }


@router.get("/profile/debug/me")
async def debug_my_profile_data(
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """Diagnostic: shows what data the agent sees for the current student."""
    from app.services.data_collector import DataCollector
    from app.services.resume_parser import ResumeParser
    from app.services.github_fetcher import GitHubFetcher
    from app.services.linkedin_fetcher import LinkedInFetcher
    from app.agents.profile_orchestrator import ProfileOrchestrator

    collector = DataCollector(db)
    student_data = await collector.collect_all(student.id)
    personal = student_data.get("personal", {})

    diag = {
        "student_id": student.id,
        "student_name": personal.get("full_name", ""),
        "lms_profile_fields": {
            "current_designation":   personal.get("current_designation", ""),
            "current_employer":      personal.get("current_employer", ""),
            "work_experience_years": personal.get("work_experience_years", ""),
            "education_level":       personal.get("education_level", ""),
            "institution":           personal.get("institution", ""),
            "field_of_study":        personal.get("field_of_study", ""),
            "graduation_year":       personal.get("graduation_year", ""),
            "key_skills":            personal.get("key_skills", ""),
            "career_goals":          personal.get("career_goals", ""),
            "preferred_role":        personal.get("preferred_role", ""),
            "about_me":              personal.get("about_me", ""),
        },
        "external_urls": {
            "resume_url":     personal.get("resume_url", ""),
            "linkedin_url":   personal.get("linkedin_url", ""),
            "github_url":     personal.get("github_url", ""),
            "portfolio_url":  personal.get("portfolio_url", ""),
        },
        "lms_activity": {
            "courses_enrolled":  len(student_data.get("courses", [])),
            "test_scores":       len(student_data.get("test_scores", [])),
            "case_studies":      len(student_data.get("case_studies", [])),
            "assignments":       len(student_data.get("assignments", [])),
            "quizzes":           len(student_data.get("quiz_scores", [])),
            "certifications":    len(student_data.get("certifications", [])),
            "projects":          len(student_data.get("projects", [])),
        },
        "computed": student_data.get("computed", {}),
        "personality_from_psychometric": student_data.get("personality", {}),
    }

    # Try resume
    if personal.get("resume_url"):
        try:
            orch = ProfileOrchestrator()
            resume_text = await orch._download_resume(personal["resume_url"])
            if resume_text:
                parser = ResumeParser()
                parsed = await parser.parse(resume_text)
                diag["resume"] = {"status": "success", "text_length": len(resume_text), "skills": len(parsed.get("technical_skills", []))}
            else:
                diag["resume"] = {"status": "download_failed"}
        except Exception as e:
            diag["resume"] = {"status": "error", "error": str(e)}
    else:
        diag["resume"] = {"status": "no_url"}

    # Try GitHub
    if personal.get("github_url"):
        try:
            gh = GitHubFetcher()
            gh_data = await gh.fetch(personal["github_url"])
            diag["github"] = {"status": "success" if gh_data.get("username") else "failed", "repos": gh_data.get("public_repos", 0)}
        except Exception as e:
            diag["github"] = {"status": "error", "error": str(e)}
    else:
        diag["github"] = {"status": "no_url"}

    # Try LinkedIn
    if personal.get("linkedin_url"):
        try:
            li = LinkedInFetcher()
            li_data = await li.fetch(personal["linkedin_url"])
            diag["linkedin"] = {"status": "success" if li_data.get("_source") not in ("empty", "linkedin_url_only") else "blocked"}
        except Exception as e:
            diag["linkedin"] = {"status": "error", "error": str(e)}
    else:
        diag["linkedin"] = {"status": "no_url"}

    missing = []
    if not personal.get("resume_url"): missing.append("Resume not uploaded")
    if not personal.get("linkedin_url"): missing.append("LinkedIn URL missing")
    if not personal.get("github_url"): missing.append("GitHub URL missing")
    if not personal.get("key_skills"): missing.append("Skills not filled")
    diag["missing"] = missing

    return diag


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


@router.get("/profile/download/{slug}")
async def download_profile_pdf(
    slug: str,
    db: Session = Depends(get_db),
):
    profile = db.query(StudentProfile).filter_by(slug=slug).first()
    if not profile or not profile.rendered_html:
        raise HTTPException(404, "Profile not found")

    safe_name = (profile.student_name or "profile").replace(" ", "_")

    try:
        import weasyprint
        pdf_bytes = weasyprint.HTML(string=profile.rendered_html).write_pdf()
        profile.pdf_downloads = (profile.pdf_downloads or 0) + 1
        db.commit()
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{safe_name}_Upskillize_Profile.pdf"'},
        )
    except ImportError:
        logger.warning("weasyprint not installed — returning print-ready HTML")

    print_html = profile.rendered_html.replace(
        "</body>",
        '<script>window.addEventListener("load",function(){setTimeout(function(){window.print()},800)});</script></body>'
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
# RUBRIC ENDPOINTS (unchanged from your existing code)
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
        {"name": d.name, "description": d.description, "max_points": d.max_points,
         "scoring_guide": d.scoring_guide, "skill_tags": d.skill_tags or []}
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