"""
Dependencies — DB sessions and JWT authentication.
Connected to the same JWT system as your Node.js LMS backend.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, Header
from app.config import get_settings
from app.models.db_models import get_engine, get_session_maker
import jwt
import logging

logger = logging.getLogger(__name__)

settings = get_settings()

# FIX 1: Wrap engine/session creation in a try/except so a bad DATABASE_URL
# does not crash the entire app on startup and cause 503 on every request.
try:
    engine = get_engine(settings.DATABASE_URL)
    SessionLocal = get_session_maker(engine)
    logger.info("Database engine created successfully.")
except Exception as e:
    logger.critical(f"STARTUP FAILURE: Could not connect to database: {e}")
    raise RuntimeError(
        f"DATABASE_URL is missing or incorrect. "
        f"Set it in HuggingFace Space secrets. Error: {e}"
    )

# FIX 2: Read JWT_SECRET from settings (which reads from .env / HuggingFace secrets)
# instead of os.environ directly with a dummy fallback.
# The old code fell back to "YOUR_REAL_25_CHAR_SECRET_HERE" when the env var
# was missing — causing every token to fail verification silently with a 401.
JWT_SECRET = settings.JWT_SECRET
if not JWT_SECRET:
    logger.critical(
        "STARTUP WARNING: JWT_SECRET is not set. "
        "All authenticated requests will fail. "
        "Add JWT_SECRET to your HuggingFace Space secrets."
    )

JWT_ALGORITHM = "HS256"

# SECURITY: fail closed. PyJWT will happily verify HS256 tokens signed with an
# EMPTY key, so a missing JWT_SECRET would let anyone forge a login token.
def _require_jwt_secret() -> str:
    secret = settings.JWT_SECRET
    if not secret or len(secret) < 16:
        logger.critical("JWT_SECRET missing or too short — refusing all authenticated requests.")
        raise HTTPException(503, "Authentication is not configured on this service.")
    return secret


# ── Minimal in-memory rate limiter (per process) ─────────────────────────────
# Good enough for the single-worker HF Space. Resets on restart; replace with
# Redis if the service is ever scaled to multiple workers.
import time as _time
from collections import defaultdict as _dd, deque as _dq

_RATE_BUCKETS = _dd(_dq)


def rate_limit(key: str, limit: int, window_seconds: int) -> None:
    """Raise 429 if `key` made more than `limit` calls in the last window."""
    now = _time.monotonic()
    bucket = _RATE_BUCKETS[key]
    while bucket and now - bucket[0] > window_seconds:
        bucket.popleft()
    if len(bucket) >= limit:
        raise HTTPException(429, "Too many requests. Please wait a while and try again.")
    bucket.append(now)
    if len(_RATE_BUCKETS) > 20000:  # keep memory bounded
        for k in [k for k, v in _RATE_BUCKETS.items() if not v][:5000]:
            _RATE_BUCKETS.pop(k, None)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def get_current_student(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(401, "No authentication token provided")

    try:
        if authorization.startswith("Bearer "):
            token = authorization[7:]
        else:
            token = authorization

        # Frontend sent "Bearer null"/"Bearer undefined"/empty (token cleared
        # after logout or session revoked). Say so instead of a JWT decode error.
        if not token or token.strip().lower() in ("null", "undefined", "none"):
            raise HTTPException(401, "No authentication token provided")

        decoded = jwt.decode(token, _require_jwt_secret(), algorithms=[JWT_ALGORITHM])

        class AuthenticatedUser:
            def __init__(self, payload):
                self.id = payload.get("id")
                self.role = payload.get("role", "student")
                self.email = payload.get("email", "")
                self.name = payload.get("full_name", "")

        user = AuthenticatedUser(decoded)
        if not user.id:
            raise HTTPException(401, "Invalid token: no user ID")
        return user

    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token has expired. Please log in again.")
    except jwt.InvalidTokenError as e:
        logger.warning(f"JWT verification failed: {e}")
        raise HTTPException(401, "Invalid or expired token")


async def get_current_admin(authorization: str = Header(None)):
    user = await get_current_student(authorization)
    if user.role != "admin":
        raise HTTPException(403, "Admin access required")
    return user

def get_current_corporate(
         authorization: str = Header(None),
         db: Session = Depends(get_db),
    ):
         """
         Extract JWT from Authorization: Bearer <token>, decode it,
         look up the user, confirm role == 'corporate'.
        """
         if not authorization or not authorization.startswith("Bearer "):
             raise HTTPException(401, "Corporate authentication required.")
         token = authorization[len("Bearer "):]
         try:
             payload = jwt.decode(token, _require_jwt_secret(), algorithms=["HS256"])
             user_id = payload.get("id") or payload.get("sub")
         except jwt.PyJWTError:
             raise HTTPException(401, "Invalid token.")
         # v5.1 FIX: there is no SQLAlchemy `User` model in this service —
         # the old line `db.query(User)` raised NameError → 500 on every
         # corporate request. Verify the role straight from the users table.
         row = db.execute(
             text("SELECT id, role FROM users WHERE id = :uid LIMIT 1"),
             {"uid": user_id},
         ).mappings().first()
         if not row or (row.get("role") or "").lower() != "corporate":
             raise HTTPException(403, "Corporate access only.")

         class CorporateUser:
             def __init__(self, r):
                 self.id = r["id"]
                 self.role = r["role"]

         return CorporateUser(row)