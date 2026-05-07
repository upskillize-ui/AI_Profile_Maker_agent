"""
Dependencies — DB sessions and JWT authentication.
Connected to the same JWT system as your Node.js LMS backend.
"""

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

        decoded = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])

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