from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from app.api.routes import router
from app.config import get_settings

settings = get_settings()

app = FastAPI(
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
app.include_router(router)


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
