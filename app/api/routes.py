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
from fastapi.responses import HTMLResponse, Response
from sqlalchemy.orm import Session
from slugify import slugify
from collections import defaultdict
import asyncio
import hashlib
import json
import os
import re
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
from app.services import profile_renderer as profile_renderer_module
from app.services.cache_service import CacheService
from app.config import get_settings

router  = APIRouter(prefix="/api/v1", tags=["Profile & Rubric"])

# v5.4.2 — CLEAN SHARE URLS: profiles are shared as {AGENT_BASE}/p/{token}
# instead of the API-looking /api/v1/profile/share/{token}. This prefix-less
# router carries the pretty routes; main.py includes it alongside `router`.
# Old /api/v1/... links keep working forever (both routes hit the same handler).
pretty_router = APIRouter(tags=["Share"])
settings = get_settings()
logger   = logging.getLogger(__name__)

# ─── Configurable public domain ──────────────────────────────────────────
# Reads from settings.PUBLIC_DOMAIN if set; falls back to the HF Space URL.
# When you set up upskillize.com/profile/*, change this env var — no code
# change needed. Every share URL and corporate URL automatically updates.
AGENT_BASE = getattr(settings, "PUBLIC_DOMAIN", None) \
             or "https://upskill25-profile-iq.hf.space"   # Space renamed 17 Jul 2026

# Unified error text — do not vary this. Different messages leak information
# about which paths hit rows vs miss rows.
_ERR_NOT_FOUND = "Profile not found"


# =========================================================================
# OPERATIONAL METRICS — v5.3
# =========================================================================
# NOTE: process-local counters (single uvicorn worker). They reset on
# restart and do NOT aggregate across replicas — good enough for the
# single-Space deployment; move to Redis/StatsD if we ever scale out.
METRICS = defaultdict(int)
METRICS_STARTED_AT = time.time()


def _template_version() -> str:
    """Current renderer template version. getattr keeps the app booting
    even if the renderer module predates the TEMPLATE_VERSION export —
    the sentinel simply never matches a stored marker, so healing and
    the fingerprint no-op both fail safe (toward regeneration)."""
    return str(getattr(profile_renderer_module, "TEMPLATE_VERSION", "0.0.0"))


# =========================================================================
# GENERATION ADMISSION CONTROL — v5.1
# =========================================================================
# Protects the Space from a generation stampede (e.g. a whole cohort
# clicking "Generate" together):
#
#   • MAX_PARALLEL_GENERATIONS  — pipelines actually running at once.
#     Everything above this waits its turn (bounded, in order).
#   • GENERATION_WAITING_ROOM   — total students admitted at once
#     (running + waiting). Student #201 gets a polite 503 "agent is
#     occupied" message IMMEDIATELY — the system never queues unbounded
#     work and never falls over.
#   • GENERATION_WAIT_SECONDS   — max time an admitted request waits for
#     a running slot before it too gets the polite message (kept under
#     typical proxy timeouts so students see our message, not a gateway
#     error).
#   • Duplicate-click guard     — a student whose generation is already
#     in flight gets a friendly "already generating" response instead of
#     a second (paid) pipeline.
#
# All state is process-local (single uvicorn worker) and only touched on
# the event loop — no cross-request locking needed.

MAX_PARALLEL_GENERATIONS = int(os.environ.get("MAX_PARALLEL_GENERATIONS", "8"))
GENERATION_WAITING_ROOM  = int(os.environ.get("GENERATION_WAITING_ROOM", "200"))
GENERATION_WAIT_SECONDS  = float(os.environ.get("GENERATION_WAIT_SECONDS", "55"))

# v5.2 — REGEN OVERWRITE POLICY (product decision 23 Jul 2026):
# Every force_regenerate must update the stored profile with the LATEST
# generated result. The old 70%-richness guard silently kept stale data
# when collection came back thinner — students saw "regenerated" but got
# last month's profile. Protection now applies only to CATASTROPHIC
# results (essentially-empty collection), which would otherwise wipe a
# real profile with nothing. Set REGEN_ALWAYS_OVERWRITE=false to restore
# the old cautious behavior.
REGEN_ALWAYS_OVERWRITE = os.environ.get("REGEN_ALWAYS_OVERWRITE", "true").lower() == "true"

_generation_semaphore = asyncio.Semaphore(MAX_PARALLEL_GENERATIONS)
_admitted_generations = 0          # running + waiting, capped by WAITING_ROOM
_inflight_students: set = set()    # student_ids with a generation in flight

AGENT_BUSY_MESSAGE = (
    "Our Profile Agent is helping many learners right now and is fully "
    "occupied. Please wait a few minutes and try again — your data is safe "
    "and nothing has been lost."
)

ALREADY_GENERATING_MESSAGE = (
    "Your profile is already being generated. Give it a minute, then "
    "refresh — clicking again won't make it faster."
)


def _agent_busy(retry_after: int = 180) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=AGENT_BUSY_MESSAGE,
        headers={"Retry-After": str(retry_after)},
    )


async def _run_with_admission(student_id: int, work):
    """Run `work()` (a zero-arg async callable that performs generation)
    under the admission-control rules above."""
    global _admitted_generations

    # Duplicate click → friendly no-op, zero cost.
    if student_id in _inflight_students:
        METRICS["duplicate_clicks"] += 1
        return {
            "message": ALREADY_GENERATING_MESSAGE,
            "status": "generating",
            "already_in_progress": True,
        }

    # Waiting room full → polite message, immediately.
    if _admitted_generations >= GENERATION_WAITING_ROOM:
        METRICS["busy_rejected_room"] += 1
        logger.warning(
            "Generation waiting room full (%d) — student %s politely deferred",
            GENERATION_WAITING_ROOM, student_id,
        )
        raise _agent_busy(retry_after=180)

    _admitted_generations += 1
    _inflight_students.add(student_id)
    acquired = False
    try:
        try:
            await asyncio.wait_for(
                _generation_semaphore.acquire(),
                timeout=GENERATION_WAIT_SECONDS,
            )
            acquired = True
        except asyncio.TimeoutError:
            METRICS["busy_rejected_timeout"] += 1
            logger.warning(
                "Generation slot wait (%.0fs) timed out — student %s politely deferred",
                GENERATION_WAIT_SECONDS, student_id,
            )
            raise _agent_busy(retry_after=120)
        return await work()
    finally:
        if acquired:
            _generation_semaphore.release()
        _inflight_students.discard(student_id)
        _admitted_generations -= 1


def _collect_all_sync(db: Session, student_id: int):
    """v5.1 — run the DataCollector off the event loop. Its 'async'
    methods contain only sync DB calls, so a private loop inside this
    worker thread is safe, and the main loop stays responsive for share
    views and health checks while collection runs."""
    return asyncio.run(DataCollector(db).collect_all(student_id))


# =========================================================================
# TEMPLATE FRESHNESS + SOURCE FINGERPRINT — v5.3
# =========================================================================
# The renderer stamps every HTML it produces with a marker right after
# <head>:   <!-- piq:tpl=<version> fp=<fingerprint> -->
# We use it two ways:
#   • render-on-read healing: a share/corporate view served from a stale
#     template silently re-renders (NO AI) before serving.
#   • smart regen: if the source data fingerprint hasn't changed AND the
#     template is current, force_regenerate becomes a free no-op.

_MARKER_RE = re.compile(r"<!--\s*piq:tpl=(\S+)\s+fp=(\S+?)\s*-->")


def _parse_marker(html):
    """Extract (template_version, fingerprint) from rendered HTML.
    Tolerates absence (pre-v12.9 HTML) → (None, None)."""
    if not html:
        return (None, None)
    m = _MARKER_RE.search(html)
    if not m:
        return (None, None)
    return (m.group(1), m.group(2))


def _source_fingerprint(student_data: dict) -> str:
    """Stable hash of the inputs that materially change a profile.
    Deliberately a SUBSET of collect_all's payload — volatile fields
    (timestamps, view counts, raw rows) are excluded so the fingerprint
    only moves when the student's actual story moves."""
    personal = student_data.get("personal", {}) or {}
    computed = student_data.get("computed", {}) or {}
    projects = (student_data.get("projects")
                or student_data.get("capstone_projects")
                or student_data.get("capstones")
                or [])
    stable = {
        "personal": {
            "name":           personal.get("full_name", ""),
            "designation":    personal.get("current_designation", ""),
            "employer":       personal.get("current_employer", ""),
            "skills":         personal.get("key_skills", ""),
            "goals":          personal.get("career_goals", ""),
            "preferred_role": personal.get("preferred_role", ""),
        },
        "education":       student_data.get("education", []) or [],
        "work_experience": student_data.get("work_experience", []) or [],
        "project_titles":  sorted(
            str(p.get("title") or p.get("name") or "")
            for p in projects if isinstance(p, dict)
        ),
        "courses": sorted(
            (str(c.get("course_name", "")), str(c.get("progress", "")))
            for c in (student_data.get("courses", []) or [])
            if isinstance(c, dict)
        ),
        "best_scores": {
            "overall_score":        computed.get("overall_score", 0),
            "best_test_score":      computed.get("best_test_score", 0),
            "avg_test_score":       computed.get("avg_test_score", 0),
            "avg_case_study_score": computed.get("avg_case_study_score", 0),
            "avg_quiz_score":       computed.get("avg_quiz_score", 0),
            "completed_courses":    computed.get("completed_courses", 0),
        },
        "certifications": sorted(
            str(c.get("name") or c.get("certificate_name") or c)
            for c in (student_data.get("certifications", []) or [])
        ),
    }
    payload = json.dumps(stable, sort_keys=True, default=str)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _profile_data_from_db(existing: StudentProfile, fresh: dict = None) -> dict:
    """Rebuild the renderer's profile_data dict from stored DB columns.
    `fresh` (optional) supplies LMS passthrough sections computed during a
    live pipeline run (education/work/roles/etc.); absent → empty defaults.
    Shared by: _safe_regenerate's data-loss path, render-on-read healing,
    and the owner ATS view."""
    fresh = fresh or {}
    return {
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
        "education_data":        fresh.get("education_data", []),
        "work_experience":       fresh.get("work_experience", []),
        "role_matches":          fresh.get("role_matches", []),
        "ats_data":              fresh.get("ats_data", {}),
        "top_achievements":      fresh.get("top_achievements", []),
        "case_study_highlights": fresh.get("case_study_highlights", []),
        "test_highlights":       fresh.get("test_highlights", []),
        "data_sources":          fresh.get("data_sources", ["lms"]),
    }


def _og_card_url(token) -> str:
    """OG card image URL for a share token (None-safe — renderer falls
    back when a profile has no share token yet)."""
    return f"{AGENT_BASE}/api/v1/profile/share/{token}/card.png" if token else None


def _ats_from_student_data(student_data: dict) -> dict:
    """Cheap fresh ATS: merge collected sources, then score. Pure CPU on
    dicts — call via asyncio.to_thread together with collection."""
    from app.services.data_merger import DataMerger
    from app.agents.role_matcher import RoleMatcher
    merged = DataMerger().merge(
        lms_data      = student_data,
        resume_data   = student_data.get("resume_data"),
        github_data   = student_data.get("github_data"),
        linkedin_data = student_data.get("linkedin_data"),
    )
    return RoleMatcher().calculate_ats_score(merged)


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
    """Build the shareable URL from a token.
    v5.4.2 — clean /p/{token} form (old /api/v1/profile/share/{token}
    links remain valid; both routes serve the same page)."""
    return f"{AGENT_BASE}/p/{token}"


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

    # ── v5.1 admission pre-checks BEFORE any status mutation ──────────
    # (no awaits between here and _run_with_admission's own checks, so
    # these stay consistent on the single event loop)
    if student_id in _inflight_students:
        METRICS["duplicate_clicks"] += 1
        return {
            "message": ALREADY_GENERATING_MESSAGE,
            "status": "generating",
            "already_in_progress": True,
        }
    if _admitted_generations >= GENERATION_WAITING_ROOM:
        METRICS["busy_rejected_room"] += 1
        raise _agent_busy(retry_after=180)

    # Case 2: exists + regen → full generation with data-loss safety.
    # v5.3 SMART REGEN: collect first (cheap DB reads, off the loop),
    # fingerprint the source data, and if NOTHING changed since the last
    # generation AND the template is current → free no-op, zero AI spend.
    # The check runs inside admission so the duplicate-click guard and
    # concurrency caps still apply to the collection work.
    if existing and existing.status == ProfileStatus.COMPLETED and body.force_regenerate:
        async def _regen_work():
            student_data = await asyncio.to_thread(_collect_all_sync, db, student_id)
            fp = _source_fingerprint(student_data)
            stored_tpl, stored_fp = _parse_marker(existing.rendered_html or "")
            if stored_fp == fp and stored_tpl == _template_version():
                METRICS["regen_no_op_fingerprint"] += 1
                logger.info(
                    "Regen no-op for student %s — fingerprint %s unchanged, "
                    "template current", student_id, fp[:8],
                )
                return {
                    "message": (
                        "Already up to date — nothing changed in your data "
                        "since the last generation."
                    ),
                    "was_no_op": True,
                    **_profile_response(existing),
                }
            # Pass the collected data through — NOT collected twice.
            return await _safe_regenerate(
                student_id, existing, db, student_data=student_data, fp=fp
            )
        return await _run_with_admission(student_id, _regen_work)

    # Case 3: no profile yet → create with locked defaults
    if not existing:
        existing = StudentProfile(
            student_id            = student_id,
            status                = ProfileStatus.GENERATING,
            # ─── Locked v5.3 defaults ──────────────────────────────
            # Product decision REVERSED 23 Jul 2026: profiles start
            # UNPUBLISHED. The student reviews the generated profile,
            # then clicks Publish (toggle-corporate) to make it visible
            # to recruiters. Share URL still waits for one deliberate
            # click by the student ("Copy Link" mints the token).
            visible_to_corporates = False,
            share_token           = None,
            # ──────────────────────────────────────────────────────────
        )
        db.add(existing)
        db.flush()
    else:
        # Retry after failure — reset status but preserve visibility choices
        existing.status = ProfileStatus.GENERATING
        db.flush()

    return await _run_with_admission(
        student_id, lambda: _full_generate(student_id, existing, db)
    )


async def _full_generate(student_id: int, existing: StudentProfile, db: Session) -> dict:
    """First-time generation — runs the full orchestrator pipeline."""
    try:
        start = time.time()
        METRICS["generations_started"] += 1

        # v5.1: collector runs in a worker thread — its sync MySQL queries
        # no longer freeze the event loop for every other request.
        student_data = await asyncio.to_thread(_collect_all_sync, db, student_id)
        fp = _source_fingerprint(student_data)

        orchestrator = ProfileOrchestrator()
        profile_data = await orchestrator.generate_profile(student_data)

        personal = student_data.get("personal", {})
        name     = (personal.get("full_name") or "Student").strip()
        slug     = f"{slugify(name)}-{student_id}-{secrets.token_hex(4)}"

        renderer = ProfileRenderer()
        # v5.1: 89 KB Jinja render off the loop too (pure CPU).
        html = await asyncio.to_thread(
            renderer.render,
            student_data       = student_data,
            profile_data       = profile_data,
            slug               = slug,
            visibility         = "private",   # template hint for footer badge
            show_ats           = False,       # public HTML never carries ATS
            source_fingerprint = fp,
            og_card_url        = _og_card_url(existing.share_token),
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

        METRICS["generations_completed"] += 1
        return {
            "message":  (
                "Profile generated. Review it, then click Publish to make it "
                "visible to recruiters."
            ),
            **_profile_response(existing),
            "generation_time":  existing.generation_time_seconds,
            "updated_sections": "all",
        }

    except Exception as e:
        METRICS["generations_failed"] += 1
        logger.exception(f"Profile generation failed for student {student_id}")
        # v5.1 FIX: roll back FIRST — committing on a failed session raises
        # PendingRollbackError and masks the real error.
        try:
            db.rollback()
            existing.status = ProfileStatus.FAILED
            db.commit()
        except Exception:
            logger.exception(
                "Could not mark profile FAILED for student %s", student_id
            )
        raise HTTPException(500, "Profile generation failed. Please try again.")


async def _safe_regenerate(
    student_id: int,
    existing: StudentProfile,
    db: Session,
    student_data: dict = None,
    fp: str = None,
) -> dict:
    """
    Regeneration with DATA-LOSS SAFETY GUARD.
    Preserves visibility choices (visible_to_corporates + share_token) —
    a student who published their share URL last month keeps that URL.

    v5.3: accepts pre-collected `student_data` (and its fingerprint `fp`)
    from the smart-regen path so collection never runs twice. Both are
    optional — when absent, collects/computes here (legacy call shape).
    """
    try:
        start = time.time()
        METRICS["generations_started"] += 1

        old_richness = _count_db_richness(existing)
        logger.info(f"Safe regen student {student_id}: old richness = {old_richness}")

        # v5.1: collector off the event loop (see _collect_all_sync).
        # v5.3: skipped when the caller already collected.
        if student_data is None:
            student_data = await asyncio.to_thread(_collect_all_sync, db, student_id)
        if fp is None:
            fp = _source_fingerprint(student_data)

        orchestrator = ProfileOrchestrator()
        profile_data = await orchestrator.generate_profile(student_data)

        new_richness = _count_profile_data_richness(profile_data)
        logger.info(f"Safe regen student {student_id}: new richness = {new_richness}")

        personal = student_data.get("personal", {})
        name     = (personal.get("full_name") or "Student").strip()
        # Preserve existing slug so old owner-side bookmarks keep working
        slug     = existing.slug or f"{slugify(name)}-{student_id}-{secrets.token_hex(4)}"

        # ── Data-loss check (v5.2 policy) ─────────────────────────────
        # CATASTROPHIC = collection returned essentially nothing — a full
        # overwrite here would destroy a real profile. Always protected.
        _personal_ok = bool((student_data.get("personal", {}) or {}).get("full_name"))
        catastrophic = (new_richness == 0) or not _personal_ok

        # Non-catastrophic richness drop: overwrite with the latest result
        # (REGEN_ALWAYS_OVERWRITE, default) but log it loudly for review.
        if (not catastrophic) and old_richness >= 5 and new_richness < old_richness * 0.7:
            if REGEN_ALWAYS_OVERWRITE:
                logger.warning(
                    f"Richness dropped on regen for student {student_id}: "
                    f"old={old_richness}, new={new_richness} — overwriting with "
                    f"latest anyway (REGEN_ALWAYS_OVERWRITE=true)."
                )

        if catastrophic or (
            not REGEN_ALWAYS_OVERWRITE
            and old_richness >= 5 and new_richness < old_richness * 0.7
        ):
            logger.error(
                f"DATA LOSS GUARD student {student_id}: "
                f"old={old_richness}, new={new_richness}, "
                f"catastrophic={catastrophic}. Keeping old profile."
            )
            # Rebuild HTML from old DB data but with fresh LMS passthroughs
            # (v5.3: shared helper — same mapping used by render-on-read
            # healing and the owner ATS view)
            old_profile_data = _profile_data_from_db(existing, fresh=profile_data)
            html = await asyncio.to_thread(
                ProfileRenderer().render,
                student_data       = student_data,
                profile_data       = old_profile_data,
                slug               = slug,
                visibility         = "private",
                show_ats           = False,
                source_fingerprint = fp,
                og_card_url        = _og_card_url(existing.share_token),
            )
            existing.rendered_html            = html
            existing.generation_time_seconds  = round(time.time() - start, 2)
            db.commit()
            if existing.share_token:
                CacheService.invalidate_profile(existing.share_token)

            METRICS["generations_completed"] += 1
            return {
                "message":  "Profile refreshed with latest template (data preserved).",
                **_profile_response(existing),
                "was_no_op": True,
                "regen_time": existing.generation_time_seconds,
            }

        # ── Normal regeneration path ──────────────────────────────────
        renderer = ProfileRenderer()
        html = await asyncio.to_thread(
            renderer.render,
            student_data       = student_data,
            profile_data       = profile_data,
            slug               = slug,
            visibility         = "private",
            show_ats           = False,
            source_fingerprint = fp,
            og_card_url        = _og_card_url(existing.share_token),
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

        METRICS["generations_completed"] += 1
        return {
            "message":  "Profile regenerated successfully.",
            **_profile_response(existing),
            "generation_time":  existing.generation_time_seconds,
            "updated_sections": "all",
        }

    except Exception as e:
        METRICS["generations_failed"] += 1
        logger.exception(f"Safe regeneration failed for student {student_id}")
        # v5.1 FIX: roll back so the session is clean and the student's
        # existing COMPLETED profile stays untouched.
        try:
            db.rollback()
        except Exception:
            pass
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

    # v5.3 — OWNER ATS: computed fresh on every owner read (cheap: DB
    # collection + pure-CPU merge/score, both off the event loop; no AI).
    # Never breaks /profile/me — any failure degrades to ats: None.
    ats = None
    try:
        student_data = await asyncio.to_thread(_collect_all_sync, db, student.id)
        ats = await asyncio.to_thread(_ats_from_student_data, student_data)
    except Exception:
        logger.exception("Owner ATS computation failed for student %s", student.id)
        ats = None

    return {
        "ats":  ats,
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


@router.get("/profile/me/view")
async def get_my_profile_view(
    student=Depends(get_current_student),
    db: Session = Depends(get_db),
):
    """
    v5.3 — OWNER-ONLY LIVE VIEW with the ATS score rendered in.
    Rebuilds the page from stored sections + fresh LMS data + a freshly
    computed ATS block, rendered with show_ats=True. The stored/public
    HTML NEVER contains ATS — this is the only place it renders.
    """
    if student is None:
        raise HTTPException(401, "Authentication required. Please log in and try again.")

    profile = db.query(StudentProfile).filter_by(student_id=student.id).first()
    if not profile or profile.status != ProfileStatus.COMPLETED:
        raise HTTPException(404, _ERR_NOT_FOUND)

    student_data = await asyncio.to_thread(_collect_all_sync, db, student.id)

    profile_data = _profile_data_from_db(profile)
    try:
        profile_data["ats_data"] = await asyncio.to_thread(
            _ats_from_student_data, student_data
        )
    except Exception:
        logger.exception("ATS computation failed in /profile/me/view for student %s", student.id)
        profile_data["ats_data"] = {}

    html = await asyncio.to_thread(
        ProfileRenderer().render,
        student_data       = student_data,
        profile_data       = profile_data,
        slug               = profile.slug or f"student-{student.id}",
        visibility         = "private",
        show_ats           = True,          # owner sees the ATS score
        source_fingerprint = _source_fingerprint(student_data),
        og_card_url        = _og_card_url(profile.share_token),
    )
    return HTMLResponse(
        content=html,
        status_code=200,
        headers={"Cache-Control": "private, no-store"},
    )


# =========================================================================
# PROFILE — SHARE TOKEN (public, holder-of-URL access)
# =========================================================================

# v5.4.1 — profiles with a healing task already running (avoid stacking a
# fresh 20-30s collection for every concurrent view of the same stale page).
_healing_in_flight: set = set()


def _heal_in_background(profile_id: int) -> None:
    """
    v5.4.1 FIX (found live, 23 Jul): healing used to run INLINE on the share
    request — a full LMS collection takes 20-30s from the Space to the remote
    DB, impatient clients disconnect, uvicorn cancels the request coroutine,
    and the healed HTML never commits — so EVERY view re-ran the whole
    healing. Now: the stale stored HTML is served IMMEDIATELY and healing
    runs as a fire-and-forget background task with its OWN DB session (the
    request session dies with the request). The next view gets the healed
    version instantly.
    """
    if profile_id in _healing_in_flight:
        return
    _healing_in_flight.add(profile_id)

    async def _job():
        from app.api.deps import SessionLocal
        db = SessionLocal()
        try:
            profile = db.query(StudentProfile).filter_by(id=profile_id).first()
            if profile is None:
                return
            stored_tpl, stored_fp = _parse_marker(profile.rendered_html or "")
            if stored_tpl == _template_version():
                return  # another worker healed it meanwhile
            await _heal_stale_html(db, profile, stored_fp)
        except Exception:
            logger.exception("Background healing failed for profile %s", profile_id)
        finally:
            try:
                db.close()
            except Exception:
                pass
            _healing_in_flight.discard(profile_id)

    try:
        asyncio.get_running_loop().create_task(_job())
    except RuntimeError:
        _healing_in_flight.discard(profile_id)


async def _heal_stale_html(db: Session, profile: StudentProfile, old_fp) -> str:
    """
    v5.3 RENDER-ON-READ HEALING: the stored HTML was produced by an older
    template — re-render it from stored DB sections + fresh LMS data with
    the CURRENT template, WITHOUT any AI call, persist, and return it.
    The stored source fingerprint is preserved (data didn't change — only
    the template did), so smart-regen no-op detection keeps working.

    Never raises: on any failure the caller serves the stored HTML
    unchanged — a share view must never break because healing hiccuped.
    """
    try:
        student_data = await asyncio.to_thread(_collect_all_sync, db, profile.student_id)
        profile_data = _profile_data_from_db(profile)
        html = await asyncio.to_thread(
            ProfileRenderer().render,
            student_data       = student_data,
            profile_data       = profile_data,
            slug               = profile.slug or f"student-{profile.student_id}",
            visibility         = "private",
            show_ats           = False,
            source_fingerprint = old_fp,       # preserve — data unchanged
            og_card_url        = _og_card_url(profile.share_token),
        )
        profile.rendered_html = html
        db.commit()
        if profile.share_token:
            CacheService.invalidate_profile(profile.share_token)
        METRICS["template_rerenders"] += 1
        logger.info(
            "Template rerender: student %s healed to tpl %s on read",
            profile.student_id, _template_version(),
        )
        return html
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        logger.exception(
            "Template rerender failed for student %s — serving stored HTML",
            profile.student_id,
        )
        return profile.rendered_html

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
@pretty_router.get("/p/{token}")
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

    # Cache lookup — but always re-check the DB for token validity.
    # v5.3: a cached page from an older template is NOT served — we fall
    # through to the DB path so render-on-read healing can run.
    cached = CacheService.get_profile_html(token)
    if cached:
        profile = db.query(StudentProfile).filter_by(share_token=token).first()
        if profile:
            cached_tpl, _ = _parse_marker(cached)
            if cached_tpl == _template_version():
                _log_view(db, profile.id, request, viewer_type="share")
                profile.total_views = (profile.total_views or 0) + 1
                db.commit()
                METRICS["share_views"] += 1
                return HTMLResponse(content=cached, status_code=200)

    profile = db.query(StudentProfile).filter_by(share_token=token).first()
    if not profile or not profile.rendered_html:
        raise HTTPException(404, _ERR_NOT_FOUND)

    # v5.4.1 TEMPLATE FRESHNESS: stale stored HTML is served IMMEDIATELY
    # and heals in the background — never blocks or slows a share view.
    # The next view gets the current-template version.
    html = profile.rendered_html
    stored_tpl, _stored_fp = _parse_marker(html)
    if stored_tpl != _template_version():
        _heal_in_background(profile.id)

    _log_view(db, profile.id, request, viewer_type="share")
    profile.total_views = (profile.total_views or 0) + 1
    db.commit()

    CacheService.set_profile_html(token, html)
    METRICS["share_views"] += 1
    return HTMLResponse(content=html, status_code=200)


# =========================================================================
# PROFILE — OG SHARE CARD (public, token-gated PNG) — v5.3
# =========================================================================
# 1200x630 Open Graph card so a shared profile unfurls beautifully on
# WhatsApp/LinkedIn. Same auth model as the share view: the token IS the
# credential; anything else gets the unified 404.

_OG_CARD_CACHE: dict = {}          # (token, str(updated_at)) → PNG bytes
_OG_CARD_CACHE_MAX = 500           # ~500 cards ≈ tens of MB, bounded

_OG_NAVY   = (11, 22, 40)          # #0B1628
_OG_GOLD   = (200, 153, 42)        # #C8992A
_OG_WHITE  = (245, 247, 250)
_OG_MUTED  = (154, 164, 178)


def _og_font(size: int):
    """Serif-bold first (brand), sans-bold second, PIL default last."""
    from PIL import ImageFont
    for name in ("DejaVuSerif-Bold.ttf", "DejaVuSans-Bold.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _compose_og_card(name: str, headline: str, photo_url: str) -> bytes:
    """Pure-CPU + short network compose. Runs in a worker thread ONLY."""
    from PIL import Image, ImageDraw, ImageOps
    from io import BytesIO

    img  = Image.new("RGB", (1200, 630), _OG_NAVY)
    draw = ImageDraw.Draw(img)

    # ── Student photo (best-effort; every failure → initial fallback) ──
    photo = None
    try:
        if (photo_url or "").startswith("http"):
            import httpx
            with httpx.Client(timeout=5, follow_redirects=True) as client:
                resp = client.get(photo_url)
            ctype = resp.headers.get("content-type", "") or ""
            if (resp.status_code == 200
                    and ctype.lower().startswith("image/")
                    and len(resp.content) < 5 * 1024 * 1024):
                photo = Image.open(BytesIO(resp.content)).convert("RGB")
    except Exception:
        photo = None

    px, py, d = 100, 165, 300      # photo circle: top-left + diameter
    if photo is not None:
        try:
            photo = ImageOps.fit(photo, (d, d))
            mask = Image.new("L", (d, d), 0)
            ImageDraw.Draw(mask).ellipse((0, 0, d, d), fill=255)
            img.paste(photo, (px, py), mask)
        except Exception:
            photo = None
    if photo is None:
        draw.ellipse((px, py, px + d, py + d), outline=_OG_GOLD, width=6)
        initial = ((name or "U").strip()[:1] or "U").upper()
        f = _og_font(140)
        bbox = draw.textbbox((0, 0), initial, font=f)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        draw.text(
            (px + d / 2 - tw / 2 - bbox[0], py + d / 2 - th / 2 - bbox[1]),
            initial, font=f, fill=_OG_GOLD,
        )

    # ── Name, subtle gold rule, headline ──────────────────────────────
    display_name = (name or "Upskillize Learner").strip()
    if len(display_name) > 26:
        display_name = display_name[:25] + "…"
    draw.text((470, 236), display_name, font=_og_font(64), fill=_OG_WHITE)

    draw.rectangle((470, 336, 1100, 339), fill=_OG_GOLD)   # the gold rule

    hl = (headline or "").strip()
    if hl:
        if len(hl) > 58:
            hl = hl[:57] + "…"
        draw.text((470, 362), hl, font=_og_font(30), fill=_OG_GOLD)

    # ── Footer, letter-spaced ─────────────────────────────────────────
    footer = " ".join("UPSKILLIZE · OFFICIAL LMS RECORD")
    draw.text((100, 562), footer, font=_og_font(20), fill=_OG_MUTED)

    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@router.get("/profile/share/{token}/card.png")
async def get_share_card(
    token: str,
    db: Session = Depends(get_db),
):
    """Open Graph card for a shared profile. Token-gated like the share
    view. Pillow imported lazily — the app boots (and this returns the
    unified 404) even if Pillow isn't installed."""
    if not token or len(token) < 20:
        raise HTTPException(404, _ERR_NOT_FOUND)

    profile = db.query(StudentProfile).filter_by(share_token=token).first()
    if not profile:
        raise HTTPException(404, _ERR_NOT_FOUND)

    try:
        from PIL import Image, ImageDraw, ImageFont   # noqa: F401 — availability probe
    except ImportError:
        raise HTTPException(404, _ERR_NOT_FOUND)

    key = (token, str(getattr(profile, "updated_at", "") or ""))
    png = _OG_CARD_CACHE.get(key)
    if png is None:
        png = await asyncio.to_thread(
            _compose_og_card,
            profile.student_name or "",
            profile.student_headline or "",
            profile.student_photo_url or "",
        )
        while len(_OG_CARD_CACHE) >= _OG_CARD_CACHE_MAX:   # evict oldest
            _OG_CARD_CACHE.pop(next(iter(_OG_CARD_CACHE)))
        _OG_CARD_CACHE[key] = png

    METRICS["og_cards_served"] += 1
    return Response(
        content=png,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )


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

    # ── v5.4 PUBLISH GATE — ATS Readiness below 60 → ask to improve first ──
    # Product rule (user, 23 Jul): the score is a coach. A student publishing
    # below the 60 mark is asked to improve before going live; they can still
    # publish by explicitly confirming (never hard-blocked). Any failure in
    # the ATS computation publishes normally — an internal error must never
    # stand between a student and recruiters.
    if bool(body.visible) and not profile.visible_to_corporates \
            and not body.confirm_low_score:
        try:
            student_data = await asyncio.to_thread(_collect_all_sync, db, student.id)
            ats = await asyncio.to_thread(_ats_from_student_data, student_data)
            _score = int(ats.get("total_score", 0)) if isinstance(ats, dict) else 0
            _locked = bool(ats.get("locked")) if isinstance(ats, dict) else False
            if _locked or _score < 60:
                METRICS["publish_deferred_low_ats"] += 1
                return {
                    "published": False,
                    "needs_improvement": True,
                    "ats_score": _score,
                    "ats_band": (ats.get("band") if isinstance(ats, dict) else None),
                    "tips": (ats.get("tips") if isinstance(ats, dict) else []) or [],
                    "missing_keywords": ((ats.get("components") or {}).get("keyword") or {}).get("missing", [])[:8]
                                        if isinstance(ats, dict) else [],
                    "message": (
                        "Your ATS Readiness needs a start — add your skills and complete "
                        "your first assessment, then publish."
                        if _locked else
                        f"Your ATS Readiness is {_score} — below the 60 mark recruiters "
                        "respond best to. Strengthen it first using the tips on your "
                        "Readiness & Fit section, then publish. You can also publish "
                        "anyway by confirming."
                    ),
                    "visible_to_corporates": False,
                }
        except Exception:
            logger.exception("Publish gate ATS check failed — publishing without gate")

    profile.visible_to_corporates = bool(body.visible)
    db.commit()

    return {
        # v5.3 — publish language (profiles now start unpublished; this
        # toggle IS the Publish button)
        "message":              (
            "Profile published — visible to recruiters."
            if profile.visible_to_corporates
            else "Profile unpublished — hidden from recruiters."
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

    # v5.4.1 TEMPLATE FRESHNESS: serve stored immediately, heal in
    # background (same fix as share views).
    html = profile.rendered_html
    stored_tpl, _stored_fp = _parse_marker(html)
    if stored_tpl != _template_version():
        _heal_in_background(profile.id)

    _log_view(db, profile.id, request, viewer_type="corporate")
    profile.total_views = (profile.total_views or 0) + 1
    db.commit()

    METRICS["corporate_views"] += 1
    return HTMLResponse(content=html, status_code=200)


# =========================================================================
# ADMIN — OPERATIONAL METRICS — v5.3
# =========================================================================

@router.get("/admin/metrics")
async def get_admin_metrics(
    admin=Depends(get_current_admin),
):
    """Process-local counters (reset on restart, per-worker — see METRICS
    note at top of file). For eyeballing load and no-op savings."""
    return {
        "uptime_seconds": round(time.time() - METRICS_STARTED_AT, 1),
        "counters": dict(METRICS),
        "config": {
            "waiting_room":     GENERATION_WAITING_ROOM,
            "max_parallel":     MAX_PARALLEL_GENERATIONS,
            "wait_seconds":     GENERATION_WAIT_SECONDS,
            "template_version": _template_version(),
        },
    }


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


