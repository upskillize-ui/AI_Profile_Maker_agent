"""
ProfileIQ API Routes — v5.0 (SECURITY REBUILD)
═══════════════════════════════════════════════════════════════════════════
Full rewrite of the profile visibility model, security posture, and error
handling. Deploy this on top of migration_visibility_share.sql.

WHAT CHANGED FROM v4
────────────────────
Security fixes:
  • IDOR closed on POST /profile/generate (body.student_id ignored for
    student callers; always uses authenticated student.id)
  • DELETED GET /profile/public/{slug} — the enumerable-slug leak
  • DELETED GET /profile/download/{slug} — the unauth PDF endpoint
  • DELETED weasyprint import and pdf_downloads counter references
  • Unified 404 error message across all "not found" cases so attackers
    cannot distinguish "exists but hidden" from "does not exist"

New visibility model — two independent flags:
  • visible_to_corporates (bool, default TRUE on generation)
      → gates GET /profile/corporate/{student_id}
  • share_token (str, default NULL on generation)
      → gates GET /profile/share/{token}
      → created via POST /profile/share/create
      → revoked via POST /profile/share/revoke

New endpoints:
  • POST /profile/share/create             — mint or return share token
  • POST /profile/share/revoke             — invalidate share token
  • GET  /profile/share/{token}            — public HTML by token
  • POST /profile/toggle-corporate         — flip corporate visibility
  • GET  /profile/corporate/{student_id}   — corporate view (auth required)

Owner-always-sees-own-profile:
  • GET /profile/me returns the profile regardless of visibility state.
    Owner is the owner; visibility is about who ELSE can see it.

Configurable domain:
  • AGENT_BASE reads from settings.PUBLIC_DOMAIN (with hf.space fallback)
  • Set PUBLIC_DOMAIN=https://upskillize.com in HF Space env when the
    domain rewrite lands. No code change needed to switch.

Preserved from v4 (unchanged, still working):
  • _safe_regenerate with data-loss guard (30% richness threshold)
  • Debug endpoint /profile/debug/me
  • Rubric grading endpoints
  • Cache invalidation on visibility changes
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from slugify import slugify
import secrets
import time
import logging

from app.api.deps import (
    get_db,
    get_current_student,
    get_current_admin,
    get_current_corporate,   # NEW — see deps.py note at bottom of this file
)
from app.models.db_models import (
    StudentProfile, ProfileViewLog, ProfileStatus,
    RubricTemplate, RubricDimension, RubricResult, RubricDimensionScore,
)
from app.models.schemas import (
    ProfileGenerateRequest,
    CorporateToggleRequest,
    GradeCaseStudyRequest,
    RubricTemplateCreate,
)
from app.agents.profile_orchestrator import ProfileOrchestrator
from app.services.data_collector import DataCollector
from app.services.profile_renderer import ProfileRenderer
from app.services.cache_service import CacheService
from app.config import get_settings

router  = APIRouter(prefix="/api/v1", tags=["Profile & Rubric"])
settings = get_settings()
logger   = logging.getLogger(__name__)

# ─── Configurable public domain ──────────────────────────────────────────
# Reads from settings.PUBLIC_DOMAIN if set; falls back to the HF Space URL.
# When you set up upskillize.com/profile/*, change this env var — no code
# change needed. Every share URL and corporate URL automatically updates.
AGENT_BASE = getattr(settings, "PUBLIC_DOMAIN", None) \
             or "https://upskill25-ai-enhancer.hf.space"

# Unified error text — do not vary this. Different messages leak information
# about which paths hit rows vs miss rows.
_ERR_NOT_FOUND = "Profile not found"


# =========================================================================
# HELPERS
# =========================================================================

def _derive_headline(student_data: dict) -> str:
    """Generate headline from actual enrolled courses."""
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
    """Data-item count from existing DB columns — for regeneration safety."""
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
    if perf.get("total_tests", 0)   > 0: count += 1
    if perf.get("completed_courses", 0) > 0: count += 1
    return count


def _count_profile_data_richness(pd: dict) -> int:
    """Same shape as _count_db_richness, for a freshly generated dict."""
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
    if perf.get("total_tests", 0)   > 0: count += 1
    if perf.get("completed_courses", 0) > 0: count += 1
    return count


def _log_view(db, profile_id, request, viewer_type="public"):
    """Non-blocking analytics write — never fail the render on log errors."""
    try:
        db.add(ProfileViewLog(
            profile_id  = profile_id,
            viewer_type = viewer_type,
            ip_address  = request.client.host if request.client else "",
            user_agent  = (request.headers.get("user-agent", "") or "")[:500],
            referrer    = (request.headers.get("referer",    "") or "")[:500],
        ))
    except Exception as e:
        logger.warning(f"View log failed: {e}")


def _share_url(token: str) -> str:
    """Build the shareable URL from a token."""
    return f"{AGENT_BASE}/api/v1/profile/share/{token}"


def _corporate_url(student_id: int) -> str:
    """Build the corporate-facing URL for a student profile."""
    return f"{AGENT_BASE}/api/v1/profile/corporate/{student_id}"


def _mint_share_token() -> str:
    """32-character URL-safe token. ~10^57 space, unguessable."""
    return secrets.token_urlsafe(24)   # 24 bytes → 32 base64url chars


def _profile_response(profile: StudentProfile) -> dict:
    """Standard shape returned to the student after generate/toggle/create."""
    return {
        "id":                     profile.id,
        "slug":                   profile.slug,
        "student_name":           profile.student_name or "",
        "status":                 profile.status.value if profile.status else "pending",
        "visible_to_corporates":  bool(profile.visible_to_corporates),
        "has_share_link":         profile.share_token is not None,
        "share_url":              _share_url(profile.share_token) if profile.share_token else None,
        "corporate_url":          _corporate_url(profile.student_id) if profile.visible_to_corporates else None,
        "views":                  profile.total_views or 0,
        "updated_at":             str(profile.updated_at) if hasattr(profile, "updated_at") else None,
    }


# =========================================================================
# PROFILE — GENERATION
# =========================================================================

@router.post("/profile/generate")
async def generate_profile(
    body: ProfileGenerateRequest = ProfileGenerateRequest(),
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """
    Generate or regenerate the authenticated student's own profile.

    v5 SECURITY: body.student_id is IGNORED for student callers. The
    profile is always generated for the authenticated student.
    Previously this allowed body.student_id to override, which was an
    IDOR — a student could regenerate another student's profile.
    """
    if student is None:
        raise HTTPException(401, "Authentication required. Please log in and try again.")

    # ── IDOR guard ────────────────────────────────────────────────────
    # If body.student_id was provided and doesn't match, log it as a
    # potential attack signal. Do NOT honor the requested id.
    if body.student_id is not None and body.student_id != student.id:
        logger.warning(
            "IDOR attempt blocked: authenticated student %s tried to "
            "generate for student %s",
            student.id, body.student_id,
        )
    student_id = student.id

    existing = db.query(StudentProfile).filter_by(student_id=student_id).first()

    # Case 1: already exists, no regen requested → return current state
    if existing and existing.status == ProfileStatus.COMPLETED and not body.force_regenerate:
        return {
            "message":  "Profile already exists. Use force_regenerate=true to rebuild.",
            **_profile_response(existing),
        }

    # Case 2: exists + regen → full generation with data-loss safety
    if existing and existing.status == ProfileStatus.COMPLETED and body.force_regenerate:
        return await _safe_regenerate(student_id, existing, db)

    # Case 3: no profile yet → create with locked defaults
    if not existing:
        existing = StudentProfile(
            student_id            = student_id,
            status                = ProfileStatus.GENERATING,
            # ─── Locked v5 defaults ────────────────────────────────
            # Product decision: Corporate ON, Visibility PRIVATE.
            # Corporates find the student immediately (placements are the
            # point of using ProfileIQ). Share URL waits for one deliberate
            # click by the student ("Copy Link" mints the token).
            visible_to_corporates = True,
            share_token           = None,
            # ──────────────────────────────────────────────────────────
        )
        db.add(existing)
        db.flush()
    else:
        # Retry after failure — reset status but preserve visibility choices
        existing.status = ProfileStatus.GENERATING
        db.flush()

    return await _full_generate(student_id, existing, db)


async def _full_generate(student_id: int, existing: StudentProfile, db: Session) -> dict:
    """First-time generation — runs the full orchestrator pipeline."""
    try:
        start = time.time()

        collector    = DataCollector(db)
        student_data = await collector.collect_all(student_id)

        orchestrator = ProfileOrchestrator()
        profile_data = await orchestrator.generate_profile(student_data)

        personal = student_data.get("personal", {})
        name     = (personal.get("full_name") or "Student").strip()
        slug     = f"{slugify(name)}-{student_id}-{secrets.token_hex(4)}"

        renderer = ProfileRenderer()
        html = renderer.render(
            student_data = student_data,
            profile_data = profile_data,
            slug         = slug,
            visibility   = "private",   # template hint for footer badge
        )

        # ── Persist everything ────────────────────────────────────────
        existing.slug                    = slug
        existing.student_name            = name
        existing.student_email           = personal.get("email", "")
        photo                            = personal.get("photo_url", "") or ""
        existing.student_photo_url       = photo[:255]
        existing.student_headline        = profile_data.get("headline", "Professional")
        existing.program_name            = _derive_program(student_data)
        existing.professional_summary    = profile_data.get("professional_summary", "")
        existing.skills_data             = profile_data.get("skills_data", {})
        existing.performance_data        = profile_data.get("performance_data", {})
        existing.journey_data            = profile_data.get("journey_data", {})
        existing.personality_data        = profile_data.get("personality_data", {})
        existing.case_studies_data       = profile_data.get("case_studies_data", [])
        existing.testgen_data            = profile_data.get("testgen_data", {})
        existing.projects_data           = profile_data.get("projects_data", [])
        existing.certifications_data     = profile_data.get("certifications_data", [])
        existing.ats_keywords            = profile_data.get("ats_keywords", [])
        existing.rendered_html           = html
        existing.status                  = ProfileStatus.COMPLETED
        existing.generation_time_seconds = round(time.time() - start, 2)
        existing.ai_model_used           = profile_data.get("ai_model_used", "rule-based-v6")

        db.commit()
        db.refresh(existing)
        # NOTE: we do NOT cache the HTML here because visibility can change
        # right after generation. Cache is populated on first read.

        return {
            "message":  (
                "Profile generated successfully. Visible to corporate recruiters. "
                "Click 'Copy Link' to create a shareable URL for friends and family."
            ),
            **_profile_response(existing),
            "generation_time":  existing.generation_time_seconds,
            "updated_sections": "all",
        }

    except Exception as e:
        logger.exception(f"Profile generation failed for student {student_id}")
        existing.status = ProfileStatus.FAILED
        db.commit()
        raise HTTPException(500, "Profile generation failed. Please try again.")


async def _safe_regenerate(student_id: int, existing: StudentProfile, db: Session) -> dict:
    """
    Regeneration with DATA-LOSS SAFETY GUARD.
    Preserves visibility choices (visible_to_corporates + share_token) —
    a student who published their share URL last month keeps that URL.
    """
    try:
        start = time.time()

        old_richness = _count_db_richness(existing)
        logger.info(f"Safe regen student {student_id}: old richness = {old_richness}")

        collector    = DataCollector(db)
        student_data = await collector.collect_all(student_id)

        orchestrator = ProfileOrchestrator()
        profile_data = await orchestrator.generate_profile(student_data)

        new_richness = _count_profile_data_richness(profile_data)
        logger.info(f"Safe regen student {student_id}: new richness = {new_richness}")

        personal = student_data.get("personal", {})
        name     = (personal.get("full_name") or "Student").strip()
        # Preserve existing slug so old owner-side bookmarks keep working
        slug     = existing.slug or f"{slugify(name)}-{student_id}-{secrets.token_hex(4)}"

        # ── Data-loss check ───────────────────────────────────────────
        if old_richness >= 5 and new_richness < old_richness * 0.7:
            logger.error(
                f"DATA LOSS DETECTED student {student_id}: "
                f"old={old_richness}, new={new_richness}. Keeping old profile."
            )
            # Rebuild HTML from old DB data but with fresh LMS passthroughs
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
            html = ProfileRenderer().render(
                student_data = student_data,
                profile_data = old_profile_data,
                slug         = slug,
                visibility   = "private",
            )
            existing.rendered_html            = html
            existing.generation_time_seconds  = round(time.time() - start, 2)
            db.commit()
            if existing.share_token:
                CacheService.invalidate_profile(existing.share_token)

            return {
                "message":  "Profile refreshed with latest template (data preserved).",
                **_profile_response(existing),
                "was_no_op": True,
                "regen_time": existing.generation_time_seconds,
            }

        # ── Normal regeneration path ──────────────────────────────────
        renderer = ProfileRenderer()
        html = renderer.render(
            student_data = student_data,
            profile_data = profile_data,
            slug         = slug,
            visibility   = "private",
        )

        existing.slug                    = slug
        existing.student_name            = name
        existing.student_email           = personal.get("email", "")
        photo                            = personal.get("photo_url", "") or ""
        existing.student_photo_url       = photo[:255]
        existing.student_headline        = profile_data.get("headline", "Professional")
        existing.program_name            = _derive_program(student_data)
        existing.professional_summary    = profile_data.get("professional_summary", "")
        existing.skills_data             = profile_data.get("skills_data", {})
        existing.performance_data        = profile_data.get("performance_data", {})
        existing.journey_data            = profile_data.get("journey_data", {})
        existing.personality_data        = profile_data.get("personality_data", {})
        existing.case_studies_data       = profile_data.get("case_studies_data", [])
        existing.testgen_data            = profile_data.get("testgen_data", {})
        existing.projects_data           = profile_data.get("projects_data", [])
        existing.certifications_data     = profile_data.get("certifications_data", [])
        existing.ats_keywords            = profile_data.get("ats_keywords", [])
        existing.rendered_html           = html
        existing.status                  = ProfileStatus.COMPLETED
        existing.generation_time_seconds = round(time.time() - start, 2)
        existing.ai_model_used           = profile_data.get("ai_model_used", "rule-based-v6")

        db.commit()
        db.refresh(existing)
        if existing.share_token:
            CacheService.invalidate_profile(existing.share_token)

        return {
            "message":  "Profile regenerated successfully.",
            **_profile_response(existing),
            "generation_time":  existing.generation_time_seconds,
            "updated_sections": "all",
        }

    except Exception as e:
        logger.exception(f"Safe regeneration failed for student {student_id}")
        raise HTTPException(500, "Regeneration failed. Please try again.")


# =========================================================================
# PROFILE — OWNER READ
# =========================================================================

@router.get("/profile/me")
async def get_my_profile(
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """
    v5 FIX: owner always sees own profile, regardless of visibility state.
    Previously this endpoint respected the visibility flag and returned
    "Profile Not Available" when private — wrong. The owner IS the owner.
    """
    if student is None:
        raise HTTPException(401, "Authentication required. Please log in and try again.")

    profile = db.query(StudentProfile).filter_by(student_id=student.id).first()
    if not profile:
        return {
            "status":  "not_generated",
            "message": "No profile generated yet. Click 'Generate My AI Profile' to create one.",
        }

    return {
        "id":                    profile.id,
        "slug":                  profile.slug,
        "status":                profile.status.value if profile.status else "pending",
        "visible_to_corporates": bool(profile.visible_to_corporates),
        "has_share_link":        profile.share_token is not None,
        "share_url":             _share_url(profile.share_token) if profile.share_token else None,
        "corporate_url":         _corporate_url(profile.student_id) if profile.visible_to_corporates else None,

        # Content (owner sees everything)
        "student_name":   profile.student_name,
        "summary":        profile.professional_summary,
        "skills":         profile.skills_data,
        "performance":    profile.performance_data,
        "journey":        profile.journey_data,
        "personality":    profile.personality_data,
        "case_studies":   profile.case_studies_data,
        "testgen":        profile.testgen_data,
        "projects":       profile.projects_data,
        "certifications": profile.certifications_data,
        "views":          profile.total_views or 0,
        "updated_at":     str(profile.updated_at) if hasattr(profile, "updated_at") else None,

        # HTML for embedded iframe in the LMS
        "rendered_html":  profile.rendered_html,
    }


# =========================================================================
# PROFILE — SHARE TOKEN (public, holder-of-URL access)
# =========================================================================

@router.post("/profile/share/create")
async def create_share_link(
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """
    Mint a share token if one doesn't exist. Idempotent: returns the
    existing token if there already is one.
    """
    if student is None:
        raise HTTPException(401, "Authentication required. Please log in and try again.")

    profile = db.query(StudentProfile).filter_by(student_id=student.id).first()
    if not profile:
        raise HTTPException(404, _ERR_NOT_FOUND)

    if profile.share_token is None:
        profile.share_token = _mint_share_token()
        db.commit()

    return {
        "message":   "Share link ready. Anyone with this URL can view your profile.",
        "share_url": _share_url(profile.share_token),
    }


@router.post("/profile/share/revoke")
async def revoke_share_link(
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """Invalidate the share token. All existing share URLs stop working."""
    if student is None:
        raise HTTPException(401, "Authentication required. Please log in and try again.")

    profile = db.query(StudentProfile).filter_by(student_id=student.id).first()
    if not profile:
        raise HTTPException(404, _ERR_NOT_FOUND)

    old_token = profile.share_token
    profile.share_token = None
    db.commit()

    if old_token:
        CacheService.invalidate_profile(old_token)

    return {
        "message":   "Share link revoked. Previous URLs will no longer work.",
        "share_url": None,
    }


@router.get("/profile/share/{token}")
async def get_profile_by_share_token(
    token: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Public: anyone with the token can view the profile.
    The token itself is the auth mechanism. No login required.

    Rejects any request where the token doesn't exactly match a stored
    non-NULL share_token. Returns unified 404 to prevent enumeration.
    """
    if not token or len(token) < 20:
        raise HTTPException(404, _ERR_NOT_FOUND)

    # Cache lookup — but always re-check the DB for token validity
    cached = CacheService.get_profile_html(token)
    if cached:
        profile = db.query(StudentProfile).filter_by(share_token=token).first()
        if profile:
            _log_view(db, profile.id, request, viewer_type="share")
            profile.total_views = (profile.total_views or 0) + 1
            db.commit()
            return HTMLResponse(content=cached, status_code=200)

    profile = db.query(StudentProfile).filter_by(share_token=token).first()
    if not profile or not profile.rendered_html:
        raise HTTPException(404, _ERR_NOT_FOUND)

    _log_view(db, profile.id, request, viewer_type="share")
    profile.total_views = (profile.total_views or 0) + 1
    db.commit()

    CacheService.set_profile_html(token, profile.rendered_html)
    return HTMLResponse(content=profile.rendered_html, status_code=200)


# =========================================================================
# PROFILE — CORPORATE VIEW (Model A: any authenticated corporate)
# =========================================================================

@router.post("/profile/toggle-corporate")
async def toggle_corporate_visibility(
    body: CorporateToggleRequest,
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """Flip the visible_to_corporates flag on the student's own profile."""
    if student is None:
        raise HTTPException(401, "Authentication required. Please log in and try again.")

    profile = db.query(StudentProfile).filter_by(student_id=student.id).first()
    if not profile:
        raise HTTPException(404, _ERR_NOT_FOUND)

    profile.visible_to_corporates = bool(body.visible)
    db.commit()

    return {
        "message":              (
            "Your profile is now visible to corporate recruiters."
            if profile.visible_to_corporates
            else "Your profile is hidden from corporate recruiters."
        ),
        "visible_to_corporates": bool(profile.visible_to_corporates),
        "corporate_url":         _corporate_url(profile.student_id) if profile.visible_to_corporates else None,
    }


@router.get("/profile/corporate/{student_id}")
async def get_profile_for_corporate(
    student_id: int,
    request: Request,
    corporate=Depends(get_current_corporate),
    db: Session = Depends(get_db),
):
    """
    Corporate view. Any authenticated corporate can view any opted-in
    student (Model A: broad discovery, matches placements-portal UX).

    Gates:
      1. Caller must be authenticated as role=corporate
      2. Target profile must have visible_to_corporates = TRUE
    """
    if corporate is None:
        raise HTTPException(401, "Corporate authentication required.")

    profile = db.query(StudentProfile).filter_by(student_id=student_id).first()
    if not profile or not profile.rendered_html:
        raise HTTPException(404, _ERR_NOT_FOUND)
    if not profile.visible_to_corporates:
        # 404 not 403: don't leak "this student exists but hid from corporates"
        raise HTTPException(404, _ERR_NOT_FOUND)

    _log_view(db, profile.id, request, viewer_type="corporate")
    profile.total_views = (profile.total_views or 0) + 1
    db.commit()

    return HTMLResponse(content=profile.rendered_html, status_code=200)


# =========================================================================
# DEBUG (unchanged from v4 — kept for internal support use)
# =========================================================================

@router.get("/profile/debug/me")
async def debug_my_profile_data(
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """
    Internal diagnostic: shows what the DataCollector sees for the current
    student. Useful when a support ticket says "my profile is empty" and
    we need to figure out whether the LMS has the data or not.
    """
    from app.services.data_collector import DataCollector
    from app.services.resume_parser   import ResumeParser
    from app.services.github_fetcher  import GitHubFetcher
    from app.services.linkedin_fetcher import LinkedInFetcher
    from app.agents.profile_orchestrator import ProfileOrchestrator

    if student is None:
        raise HTTPException(401, "Authentication required. Please log in and try again.")

    collector    = DataCollector(db)
    student_data = await collector.collect_all(student.id)
    personal     = student_data.get("personal", {})

    diag = {
        "student_id":   student.id,
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
        },
        "external_urls": {
            "resume_url":    personal.get("resume_url", ""),
            "linkedin_url":  personal.get("linkedin_url", ""),
            "github_url":    personal.get("github_url", ""),
            "portfolio_url": personal.get("portfolio_url", ""),
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
        "computed":                 student_data.get("computed", {}),
        "personality_from_psycho":  student_data.get("personality", {}),
    }

    # External source probes (best-effort; individual failures don't block)
    if personal.get("resume_url"):
        try:
            resume_text = await ProfileOrchestrator()._download_resume(personal["resume_url"])
            if resume_text:
                parsed = await ResumeParser().parse(resume_text)
                diag["resume"] = {
                    "status":       "success",
                    "text_length":  len(resume_text),
                    "skills_count": len(parsed.get("technical_skills", [])),
                }
            else:
                diag["resume"] = {"status": "download_failed"}
        except Exception as e:
            diag["resume"] = {"status": "error", "error": str(e)}
    else:
        diag["resume"] = {"status": "no_url"}

    if personal.get("github_url"):
        try:
            gh_data = await GitHubFetcher().fetch(personal["github_url"])
            diag["github"] = {
                "status": "success" if gh_data.get("username") else "failed",
                "repos":  gh_data.get("public_repos", 0),
            }
        except Exception as e:
            diag["github"] = {"status": "error", "error": str(e)}
    else:
        diag["github"] = {"status": "no_url"}

    if personal.get("linkedin_url"):
        try:
            li_data = await LinkedInFetcher().fetch(personal["linkedin_url"])
            diag["linkedin"] = {
                "status": "success" if li_data.get("_source") not in ("empty", "linkedin_url_only") else "blocked",
            }
        except Exception as e:
            diag["linkedin"] = {"status": "error", "error": str(e)}
    else:
        diag["linkedin"] = {"status": "no_url"}

    missing = []
    if not personal.get("resume_url"):    missing.append("Resume not uploaded")
    if not personal.get("linkedin_url"):  missing.append("LinkedIn URL missing")
    if not personal.get("github_url"):    missing.append("GitHub URL missing")
    if not personal.get("key_skills"):    missing.append("Skills not filled")
    diag["missing"] = missing

    return diag


# =========================================================================
# RUBRIC ENDPOINTS (unchanged from v4)
# =========================================================================

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
          .filter(RubricDimension.rubric_id == rubric_template.id,
                  RubricDimension.is_active == True)
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


# =========================================================================
# PRIVATE HELPERS (rubric formatting)
# =========================================================================

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