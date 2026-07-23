import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from app.api.routes import router
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup checks — best-effort only. Nothing here may block or
    prevent the app from serving traffic."""

    # (a) LMS schema validation — warns loudly if expected tables/columns
    # are missing so integration drift is visible at boot, not at first
    # student request. Defensive: validate_lms_schema is landing in a
    # parallel change to data_collector, so both the import and the call
    # are guarded.
    try:
        from app.services import data_collector as _dc
        validate = getattr(_dc, "validate_lms_schema", None)
        if validate is None:
            logger.info("validate_lms_schema not available — skipping LMS schema check")
        else:
            db = None
            try:
                from app.api.deps import SessionLocal  # built from app.models.db_models factory
                db = SessionLocal()
                warnings = validate(db)
                for w in (warnings or []):
                    logger.warning("LMS schema check: %s", w)
                if not warnings:
                    logger.info("LMS schema check passed")
            finally:
                if db is not None:
                    try:
                        db.close()
                    except Exception:
                        pass
    except Exception as e:
        logger.warning("LMS schema check skipped (non-fatal): %s", e)

    # (b) Template version — helps correlate rendered profiles with the
    # template that produced them.
    try:
        from app.services import profile_renderer as _pr
        tv = getattr(_pr, "TEMPLATE_VERSION", None)
        if tv is not None:
            logger.info("template version: %s", tv)
    except Exception as e:
        logger.warning("Could not read template version: %s", e)

    yield


app = FastAPI(
    lifespan=lifespan,
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="AI Profile Builder & Rubric Grading Engine for Upskillize",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — allow frontend domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://upskillize.com",
        "https://lms.upskillize.com",
        "https://www.upskillize.com",
        "https://upskillize.netlify.app",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
# NOTE: No prefix here — router already declares prefix="/api/v1" internally.
# Adding a prefix here would double it → /api/v1/api/v1/... (broken).
app.include_router(router)
# v5.4.2 — clean share URLs (/p/{token}); see routes.pretty_router
from app.api.routes import pretty_router
app.include_router(pretty_router)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
    }


@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <html>
    <head><title>Upskillize Profile Agent</title>
    <style>
        body{font-family:system-ui;background:#0A0E17;color:#F1F5F9;display:flex;
        justify-content:center;align-items:center;min-height:100vh;margin:0}
        .card{background:#111827;border:1px solid rgba(255,255,255,0.06);
        border-radius:16px;padding:48px;text-align:center;max-width:480px}
        h1{font-size:24px;margin-bottom:8px}
        p{color:#94A3B8;font-size:14px;margin-bottom:24px}
        a{color:#63B3ED;text-decoration:none;font-weight:500;
        padding:10px 24px;border:1px solid rgba(99,179,237,0.3);
        border-radius:8px;display:inline-block;transition:all .2s}
        a:hover{background:rgba(99,179,237,0.1)}
    </style></head>
    <body><div class="card">
        <h1>Upskillize AI Profile Agent</h1>
        <p>Bridging Academia and Industry</p>
        <a href="/docs">Open API Docs →</a>
    </div></body></html>
    """