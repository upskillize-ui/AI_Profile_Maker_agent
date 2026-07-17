"""Pytest setup — path + safe env defaults for OFFLINE runs.

app/api/deps.py builds the DB engine at import time, so importing anything
from app.api.* without a DATABASE_URL would crash the offline suite. These
setdefault() calls only apply when the real env vars are absent — the live
E2E suite (which sets real values) is unaffected.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("JWT_SECRET", "offline-test-secret")
