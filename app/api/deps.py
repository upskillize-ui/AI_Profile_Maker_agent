"""
Dependencies — DB sessions and JWT authentication.
Connected to the same JWT system as your Node.js LMS backend.
"""

from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, Header
from app.config import get_settings
from app.models.db_models import get_engine, get_session_maker
import os
import jwt
import logging

logger = logging.getLogger(__name__)

settings = get_settings()
engine = get_engine(settings.DATABASE_URL)
SessionLocal = get_session_maker(engine)

# Same JWT secret as your Node.js backend (.env JWT_SECRET)
JWT_SECRET = os.environ.get("JWT_SECRET", "YOUR_REAL_25_CHAR_SECRET_HERE")

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
        raise HTTPException(401, "Token has expired. Please login again.")
    except jwt.InvalidTokenError as e:
        logger.warning(f"JWT verification failed: {e}")
        raise HTTPException(401, "Invalid or expired token")


async def get_current_admin(authorization: str = Header(None)):
    user = await get_current_student(authorization)
    if user.role != "admin":
        raise HTTPException(403, "Admin access required")
    return user