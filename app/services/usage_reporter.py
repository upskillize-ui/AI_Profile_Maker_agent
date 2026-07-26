"""Per-student cost attribution for ProfileIQ — Student Cost Sheet bridge.

The API route sets the current student with set_student(); every Claude call
site then calls report(model, usage_dict) with the `usage` object from the
Anthropic HTTP response JSON. Fires one POST to the LMS, which logs
agent_usage (per-student cost) and handles credits. Best-effort: attribution
must never break profile generation.

Env needed where ProfileIQ runs:
  LMS_BASE_URL            e.g. https://lms.upskillize.com
  INTERNAL_CREDIT_SECRET  same value as the LMS backend
"""

import contextvars
import json
import os
import urllib.request

_student = contextvars.ContextVar("profileiq_student_id", default=None)


def set_student(student_id) -> None:
    """Call at route entry. None/0 clears (no attribution)."""
    _student.set(student_id or None)


def report(model: str, usage: dict | None) -> None:
    base = os.getenv("LMS_BASE_URL", "").rstrip("/")
    secret = os.getenv("INTERNAL_CREDIT_SECRET", "")
    student_id = _student.get()
    if not base or not secret:
        print("[usage] OFF: LMS_BASE_URL / INTERNAL_CREDIT_SECRET not set in this Space")
        return
    if not student_id:
        print("[usage] skipped: no student set for this generation")
        return
    if not isinstance(usage, dict):
        return
    try:
        eff_in = (int(usage.get("input_tokens") or 0)
                  + round(0.10 * int(usage.get("cache_read_input_tokens") or 0))
                  + round(1.25 * int(usage.get("cache_creation_input_tokens") or 0)))
        body = json.dumps({
            "userId": student_id, "agent": "ProfileIQ", "model": model,
            "input_tokens": eff_in,
            "output_tokens": int(usage.get("output_tokens") or 0),
        }).encode()
        req = urllib.request.Request(
            f"{base}/api/ai-credits/consume-internal", data=body,
            headers={"Content-Type": "application/json",
                     "X-Internal-Secret": secret}, method="POST")
        urllib.request.urlopen(req, timeout=5).read()
        print(f"[usage] ProfileIQ -> LMS user {student_id}: {model}")
    except Exception as e:
        print(f"[usage] report skipped: {e}")
