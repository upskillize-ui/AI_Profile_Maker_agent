"""
ProfileIQ Data Collector — v6.3 (SCHEMA-VERIFIED)
═══════════════════════════════════════════════════════════════════════════

Every SQL query in this file has been verified against real DESCRIBE
output from your production database. Column names are no longer guessed.

CORRECTIONS FROM v6.2 (all found via HF Space runtime errors):

  1. courses.name  →  courses.course_name    (aliased AS course_name)
  2. enrollments.status  →  enrollments.completion_status
  3. enrollments.enrolled_at  →  enrollments.created_at
  4. capstones: removed non-existent c.reviewed_at
  5. test_history.completed_at  →  test_history.created_at only
  6. vyom_sessions.session_type  →  vyom_sessions.mode
  7. student_attendance now uses user_id directly (no students.id hop)
  8. student_attendance.session_id  →  student_attendance.lesson_id
  9. student_attendance.marked_at  →  student_attendance.joined_at
  10. Removed _get_students_id() entirely — no longer needed

SCHEMA REALITY NOTES:
  - test_history.student_id is varchar(100), not int — MySQL coerces the
    int parameter to string, so passing user_id as int works.
  - vyom_sessions.user_id is varchar(64) — same story.
  - vyom_sessions has 34 columns; we pull the ones useful for a profile.
  - student_attendance has BOTH student_id (=students.id, legacy) AND
    user_id (=users.id, current). We use user_id — one less lookup.

EVERYTHING ELSE FROM v6.2 IS UNCHANGED:
  - 8-axis Performance Snapshot (Assignments, Case Studies, Capstones,
    Industry Sessions, Mock Test, Mock Interview, Assessments, Punctuality)
  - Flat lists at section keys (template compatible)
  - Legacy aliases: capstone_projects, industry_interactions, test_scores
  - Punctuality graceful N/A fallback
  - Empty state = [] or None, never NaN or 0.00 sentinel
"""

import asyncio
import json
import os
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class DataCollector:
    """
    Reads student data from defaultdb for profile generation.
    Every method is best-effort — a query failure logs and returns []
    instead of crashing collect_all().
    """

    def __init__(self, db: Session):
        self.db = db

    # =====================================================================
    # PUBLIC ENTRY POINT
    # =====================================================================

    async def collect_all(self, user_id: int) -> Dict[str, Any]:
        try:
            personal = await self._get_personal(user_id)

            # v6.5 — ID CONVENTION (verified against production 18 Jul 2026):
            # Some activity tables key student_id on users.id, others on
            # students.id. Resolve students.id ONCE and route each read to the
            # correct id. Verified per-table:
            #   users.id  → test_history, vyom_sessions, capstones, attendance(user_id),
            #               punctuality, psychometric, certifications,
            #               session_understandings / session_attendance (industry
            #               sessions — LMS routes/industrySession.js writes req.user.id)
            #   students.id → enrollments (courses), case_study_submissions,
            #               assignment_submissions, quiz_attempts (assessments)
            # NOT hardcoded — _resolve_students_id maps EACH user to their own
            # students.id. Falls back to user_id if no students row (query then
            # simply returns nothing, gracefully).
            students_id = self._resolve_students_id(user_id)
            sid = students_id if students_id is not None else user_id

            # 12 concurrent reads
            results = await asyncio.gather(
                # students.id pre-resolved once above — no per-method re-lookup
                self._get_courses(user_id, students_id),  # 0  students.id
                self._get_case_studies(sid),            # 1  students.id
                self._get_assignments(sid),             # 2  students.id
                self._get_capstones(user_id),           # 3  users.id
                self._get_industry_sessions(user_id),   # 4  users.id (v6.6 fix)
                self._get_mock_tests(user_id),          # 5  users.id (varchar)
                self._get_mock_interviews(user_id),     # 6  users.id (varchar)
                self._get_attendance(user_id),          # 7  users.id
                self._get_punctuality(user_id),         # 8  users.id
                self._get_psychometric(user_id),        # 9  users.id
                self._get_certifications(user_id),      # 10 users.id
                self._get_assessments(sid),             # 11 students.id
                return_exceptions=True,
            )

            # Type-aware failure defaults: attendance/punctuality/psycho are
            # dicts — replacing them with [] crashes every downstream .get().
            defaults = [
                [], [], [], [], [], [], [],          # 0-6 list sections
                self._empty_attendance(),             # 7 attendance
                {"score": None, "band": None,
                 "events_counted": 0, "status": "no_data"},   # 8 punctuality
                {"personality_type": None, "traits": [],
                 "summary": "", "status": "no_data"},          # 9 psychometric
                [], [],                                # 10-11 lists
            ]
            (courses, case_studies, assignments, capstones,
             industry_sessions, mock_tests, mock_interviews,
             attendance, punctuality, psycho, certifications,
             assessments) = [
                (r if not isinstance(r, Exception) else self._safe_default(r, defaults[i]))
                for i, r in enumerate(results)
            ]

            # v6.8: proctored exams join the Assessments list.
            try:
                assessments = list(assessments) + await self._get_proctored_exams(user_id)
            except Exception as e:
                logger.warning(f"_get_proctored_exams failed for user_id={user_id}: {e}")

            # v6.7: hackathons (AIthon) — synchronous, after the gather.
            try:
                hackathons = await self._get_hackathons(user_id, sid, personal.get("email"))
            except Exception as e:
                logger.warning(f"_get_hackathons failed for user_id={user_id}: {e}")
                hackathons = []

            snapshot = self._compute_snapshot(
                assignments        = assignments,
                case_studies       = case_studies,
                capstones          = capstones,
                industry_sessions  = industry_sessions,
                mock_tests         = mock_tests,
                mock_interviews    = mock_interviews,
                assessments        = assessments,
                punctuality        = punctuality,
            )

            return {
                "personal":              personal,
                "courses":               courses,
                "case_studies":          case_studies,
                "assignments":           assignments,
                "capstones":             capstones,
                "capstone_projects":     capstones,          # legacy alias for renderer
                "industry_sessions":     industry_sessions,
                "industry_interactions": industry_sessions,  # legacy alias for renderer
                "hackathons":            hackathons,
                "mock_tests":            mock_tests,
                "mock_interviews":       mock_interviews,
                "assessments":           assessments,
                "test_scores":           assessments,        # legacy alias — renderer's
                                                             # "assessment" axis reads this
                "attendance":            attendance,
                "punctuality":           punctuality,
                "personality":           psycho,
                "certifications":        certifications,
                "performance_snapshot":  snapshot,
                "batch_info":            {},
                "computed": self._compute_metrics(
                    user_id, snapshot,
                    courses=courses, mock_tests=mock_tests,
                    assessments=assessments, case_studies=case_studies,
                    assignments=assignments, capstones=capstones,
                    industry_sessions=industry_sessions,
                    mock_interviews=mock_interviews,
                ),
            }

        except Exception as e:
            logger.exception(f"collect_all failed for user_id={user_id}")
            return self._empty_payload(user_id)

    # =====================================================================
    # PERSONAL / IDENTITY (users table)
    # =====================================================================

    # Identity columns _get_personal reads when present (verified names).
    _PERSONAL_BASE_COLS = [
        "id", "full_name", "email", "phone",
        "profile_photo", "bio", "gender", "city", "state", "country",
        "current_designation", "current_employer",
        "work_experience_years",
        "education_level", "edu_institution", "institution",
        "field_of_study", "graduation_year", "edu_year",
        "key_skills", "career_goals",
        "preferred_salary_min", "preferred_salary_max",
        "linkedin", "github", "portfolio", "twitter", "website",
        "resume_url", "resume_name",
        "languages_known", "hobbies", "industries",
    ]

    # "Open To" card fields — LMS deployments name these columns
    # differently, so each clean output key maps to its candidate column
    # names in preference order. Only columns that actually exist in
    # `users` (per SHOW COLUMNS) ever reach the SELECT.
    _OPEN_TO_CANDIDATES = {
        "preferred_role":     ["preferred_role", "desired_role", "target_role"],
        "expected_ctc":       ["expected_ctc", "ctc_expectation",
                               "expected_salary", "desired_ctc"],
        "current_ctc":        ["current_ctc"],
        "notice_period":      ["notice_period"],
        "employment_type":    ["employment_type"],
        "work_mode":          ["work_mode", "preferred_mode", "job_mode"],
        "preferred_location": ["preferred_location"],
        "open_to_relocation": ["open_to_relocation"],
    }

    # Process-lifetime cache for SHOW COLUMNS FROM users.
    _users_cols_cache: Optional[set] = None

    @classmethod
    def _users_columns(cls, db) -> set:
        """Actual column names of `users`, lowercased. Cached at class
        level after the first successful introspection; returns an empty
        set (and retries next call) if the query fails."""
        if cls._users_cols_cache is not None:
            return cls._users_cols_cache
        try:
            rows = db.execute(text("SHOW COLUMNS FROM users")).all()
            cols = {str(r[0]).lower() for r in rows}
            if cols:
                cls._users_cols_cache = cols
            return cols
        except Exception as e:
            logger.warning(f"_users_columns introspection failed: {e}")
            return set()

    async def _get_personal(self, user_id: int) -> Dict[str, Any]:
        try:
            # v6.5 "Open To" fix: build the SELECT from columns that
            # actually exist, so a deployment naming the role/CTC/notice
            # columns differently no longer 500s the whole query (which is
            # why the card showed "—" despite the student filling it in).
            cols = self._users_columns(self.db)
            if cols:
                select_cols = [c for c in self._PERSONAL_BASE_COLS if c in cols]
                for candidates in self._OPEN_TO_CANDIDATES.values():
                    select_cols.extend(c for c in candidates if c in cols)
                if "id" not in select_cols:
                    select_cols.insert(0, "id")
            else:
                # Introspection unavailable — fall back to the verified
                # v6.3 static list (previous behavior).
                select_cols = self._PERSONAL_BASE_COLS + [
                    "employment_type", "preferred_role", "preferred_location",
                    "notice_period", "open_to_relocation", "work_mode",
                ]
            # select_cols is a fixed whitelist intersected with SHOW COLUMNS
            # output — identifiers only, never user input. The :uid value
            # stays parameterized as always.
            sql = ("SELECT " + ", ".join(select_cols) +
                   " FROM users WHERE id = :uid LIMIT 1")
            row = self.db.execute(text(sql), {"uid": user_id}).mappings().first()

            if not row:
                return self._empty_personal(user_id)

            r = dict(row)

            def _first(candidates: List[str]) -> str:
                """First non-empty value among the candidate columns."""
                for c in candidates:
                    v = r.get(c)
                    if v not in (None, ""):
                        return str(v) if not isinstance(v, str) else v
                return ""

            # Each "Open To" field lands in its OWN normalized key —
            # downstream never has to guess which slot holds what (this is
            # what fixes the notice-period-under-CTC caption cross-wiring).
            open_to = {
                key: _first(candidates)
                for key, candidates in self._OPEN_TO_CANDIDATES.items()
            }
            return {
                "user_id":               r["id"],
                "full_name":             r.get("full_name") or "",
                "email":                 r.get("email") or "",
                "phone":                 r.get("phone") or "",
                "photo_url":             r.get("profile_photo") or "",
                "bio":                   r.get("bio") or "",
                "about_me":              r.get("bio") or "",
                "gender":                r.get("gender") or "",
                "city":                  r.get("city") or "",
                "state":                 r.get("state") or "",
                "country":               r.get("country") or "",
                "location":              self._join_location(r),
                "current_designation":   r.get("current_designation") or "",
                "current_employer":      r.get("current_employer") or "",
                "work_experience_years": r.get("work_experience_years") or "",
                "employment_type":       open_to["employment_type"],
                "education_level":       r.get("education_level") or "",
                "institution":           r.get("edu_institution") or r.get("institution") or "",
                "field_of_study":        r.get("field_of_study") or "",
                "graduation_year":       r.get("graduation_year") or r.get("edu_year") or "",
                "key_skills":            r.get("key_skills") or "",
                "career_goals":          r.get("career_goals") or "",
                # Normalized "Open To" keys — one dedicated key per field,
                # resolved from whichever candidate column this LMS uses.
                "preferred_role":        open_to["preferred_role"],
                "preferred_location":    open_to["preferred_location"],
                "notice_period":         open_to["notice_period"],
                "open_to_relocation":    open_to["open_to_relocation"],
                "expected_ctc":          open_to["expected_ctc"],
                "current_ctc":           open_to["current_ctc"],
                "preferred_salary_min":  r.get("preferred_salary_min") or "",
                "preferred_salary_max":  r.get("preferred_salary_max") or "",
                "work_mode":             open_to["work_mode"],
                "linkedin_url":          r.get("linkedin") or "",
                "github_url":            r.get("github") or "",
                "portfolio_url":         r.get("portfolio") or "",
                "website_url":           r.get("website") or "",
                "twitter_url":           r.get("twitter") or "",
                "resume_url":            r.get("resume_url") or "",
                "resume_name":           r.get("resume_name") or "",
                "languages_known":       r.get("languages_known") or "",
                "hobbies":               r.get("hobbies") or "",
                "industries":            r.get("industries") or "",
            }
        except Exception as e:
            logger.warning(f"_get_personal failed for user_id={user_id}: {e}")
            return self._empty_personal(user_id)

    # =====================================================================
    # ENROLLED COURSES
    # =====================================================================

    async def _get_courses(self, user_id: int,
                           students_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        enrollments columns (verified via DESCRIBE):
          id, student_id, course_id, completion_status, payment_status,
          progress_percentage, completed_at, created_at, updated_at
        NO `status` column, NO `enrolled_at` column.

        v6.4 CONVENTION (verified against production data, 17 Jul 2026):
        enrollments.student_id stores students.id, NOT users.id.
        Confirmed via: SELECT student_id, course_id FROM enrollments
        WHERE student_id IN (87, 52) → all rows under 52 (students.id)
        for Ranjana (users.id=87, students.id=52), courses 38/46/47.

        So: resolve users.id → students.id ONCE, query with students.id
        ONLY. No try-both fallback — querying users.id against a
        students.id column can numerically collide with ANOTHER student's
        enrollments, which is worse than returning nothing.
        """
        try:
            # v6.5 fix: use the students.id collect_all resolved once up
            # front; only resolve here when called standalone.
            sid = students_id if students_id is not None else self._resolve_students_id(user_id)
            if sid is None:
                logger.warning(
                    f"_get_courses: no students-table row for user_id={user_id}; "
                    f"cannot read enrollments (convention: students.id)")
                return []
            rows = self._query_courses(sid)
            certs = self._query_certificates(user_id, sid)   # course_id → cert row
            out = [
                {
                    "enrollment_id": r["enrollment_id"],
                    "course_id":     r["course_id"],
                    "course_name":   r.get("course_name") or "",
                    "description":   r.get("description") or "",
                    "category":      r.get("category") or "",
                    "status":        r.get("completion_status") or "",
                    # Both key spellings — template reads completion_status /
                    # progress_percentage; older code reads status / progress.
                    "completion_status":   r.get("completion_status") or "",
                    "progress":            float(r["progress_percentage"]) if r.get("progress_percentage") is not None else 0.0,
                    "progress_percentage": float(r["progress_percentage"]) if r.get("progress_percentage") is not None else 0.0,
                    "completed_at":  str(r.get("completed_at") or ""),
                    "enrolled_at":   str(r.get("enrolled_at") or ""),
                    "certificate":   None,
                }
                for r in rows
            ]
            # v6.6: an issued, non-revoked LMS certificate is proof of
            # completion even when enrollments.completion_status lags the
            # progress engine (it only flips at 100% or on manual override).
            for c in out:
                cert = certs.get(c["course_id"])
                if cert:
                    c["certificate"] = cert
                    if (c["completion_status"] or "").lower() != "completed":
                        c["status"] = c["completion_status"] = "completed"
                        c["progress"] = c["progress_percentage"] = 100.0
                        c["completed_at"] = c["completed_at"] or cert.get("issued_at", "")
            return out
        except Exception as e:
            logger.warning(f"_get_courses failed for user_id={user_id}: {e}")
            return []

    def _query_certificates(self, user_id: int, students_id: Optional[int]) -> Dict[int, Dict[str, Any]]:
        """LMS `certificates` table — written by routes/certificates.js,
        routes/enrollments.js and routes/student.js. Rows carry user_id
        (users.id) and/or student_id (students.id); the date column is
        issued_at in some writers and issued_date in others, so SELECT *."""
        try:
            rows = self.db.execute(text("""
                SELECT * FROM certificates
                WHERE (user_id = :uid OR student_id = :sid)
                  AND COALESCE(is_revoked, 0) = 0
            """), {"uid": user_id, "sid": students_id if students_id is not None else -1}
            ).mappings().all()
        except Exception as e:
            logger.info(f"_query_certificates: unavailable ({e})")
            return {}
        out: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            cid = r.get("course_id")
            if cid is None:
                continue
            out[int(cid)] = {
                "certificate_id": r.get("certificate_id"),
                "course_name":    r.get("course_name") or "",
                "final_score":    r.get("final_score"),
                "grade":          r.get("grade"),
                "png_url":        r.get("png_url") or r.get("pdf_url") or r.get("certificate_url") or "",
                "code":           (r.get("certificate_code") or r.get("certificate_number")
                                   or r.get("programme_code") or r.get("code") or ""),
                "issued_at":      str(r.get("issued_at") or r.get("issued_date") or r.get("created_at") or ""),
            }
        return out

    def _query_courses(self, enrollment_student_id: int):
        """Raw enrollments query for one candidate id (users.id OR students.id)."""
        return self.db.execute(text("""
            SELECT
                e.id                    AS enrollment_id,
                e.course_id,
                e.completion_status,
                e.progress_percentage,
                e.completed_at,
                e.created_at            AS enrolled_at,
                c.course_name,
                c.description           AS description,
                c.category
            FROM enrollments e
            LEFT JOIN courses c ON c.id = e.course_id
            WHERE e.student_id = :uid
            ORDER BY e.created_at DESC
        """), {"uid": enrollment_student_id}).mappings().all()

    @staticmethod
    def _pct(grade, max_marks, rubric=None) -> Optional[float]:
        """v6.8 — admin grades coursework OUT OF total_marks (or the rubric's
        point total), not out of 100 (see frontend AdminCoursework.jsx:
        `{r.grade}/{item.total_marks || 100}`). Convert to a percentage.
        A grade above the maximum is a 0-100 score written by the AI
        reviewer (same file, line ~1401) — keep it as-is, capped at 100."""
        if grade is None:
            return None
        try:
            g = float(grade)
        except (TypeError, ValueError):
            return None
        mx = None
        if rubric:
            try:
                rb = json.loads(rubric) if isinstance(rubric, str) else rubric
                cats = (rb or {}).get("categories") or []
                tot = sum(float(c.get("points") or 0) for c in cats if isinstance(c, dict))
                if tot > 0:
                    mx = tot
            except Exception:
                mx = None
        if mx is None:
            try:
                mx = float(max_marks) if max_marks else None
            except (TypeError, ValueError):
                mx = None
        if not mx or mx <= 0:
            mx = 100.0
        if g > mx:
            return round(min(g, 100.0), 2)
        return round(g / mx * 100.0, 2)

    def _resolve_students_id(self, user_id: int) -> Optional[int]:
        """users.id → students.id via the students table (students.user_id)."""
        try:
            row = self.db.execute(text("""
                SELECT id FROM students WHERE user_id = :uid LIMIT 1
            """), {"uid": user_id}).first()
            return int(row[0]) if row else None
        except Exception as e:
            logger.warning(f"_resolve_students_id failed for user_id={user_id}: {e}")
            return None

    # =====================================================================
    # ACTIVITY FLOW 1 — CASE STUDIES
    # =====================================================================

    async def _get_case_studies(self, user_id: int) -> List[Dict[str, Any]]:
        """
        courses column is `course_name`, not `name`.
        """
        try:
            rows = self.db.execute(text("""
                SELECT
                    css.id                AS submission_id,
                    css.case_study_id,
                    css.grade,
                    css.status,
                    css.rubric_scores,
                    css.feedback,
                    css.submitted_at,
                    css.reviewed_at,
                    cs.title              AS case_title,
                    cs.description        AS case_brief,
                    cs.course_id,
                    cs.total_marks        AS total_marks,
                    co.course_name        AS course_name
                FROM case_study_submissions css
                LEFT JOIN case_studies cs ON cs.id = css.case_study_id
                LEFT JOIN courses      co ON co.id = cs.course_id
                WHERE css.student_id = :uid
                ORDER BY css.grade DESC, css.submitted_at DESC
            """), {"uid": user_id}).mappings().all()

            items = []
            for r in rows:
                grade = r.get("grade")
                pct = self._pct(grade, r.get("total_marks"))
                items.append({
                    "submission_id":  r["submission_id"],
                    "case_study_id":  r["case_study_id"],
                    "title":          r.get("case_title") or "Case Study",
                    "brief":          r.get("case_brief") or "",
                    "score":          pct,
                    "ai_score":       pct,
                    "faculty_score":  pct,
                    "raw_grade":      float(grade) if grade is not None else None,
                    "total_marks":    r.get("total_marks"),
                    "max_score":      100,
                    "ai_grade":       self._grade_letter(pct),
                    "ai_grade_label": self._grade_label(pct),
                    "status":         r.get("status") or "",
                    "course_name":    r.get("course_name") or "",
                    "module_name":    "",
                    "submitted_at":   str(r.get("submitted_at") or ""),
                    "reviewed_at":    str(r.get("reviewed_at") or ""),
                    "feedback":       r.get("feedback") or "",
                    "rubric_scores":  r.get("rubric_scores") or {},
                })
            return items
        except Exception as e:
            logger.warning(f"_get_case_studies failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 2 — ASSIGNMENTS
    # =====================================================================

    async def _get_assignments(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            rows = self.db.execute(text("""
                SELECT
                    asub.id               AS submission_id,
                    asub.assignment_id,
                    asub.grade,
                    asub.status,
                    asub.feedback,
                    asub.submitted_at,
                    a.title               AS assignment_title,
                    a.description         AS assignment_brief,
                    a.course_id,
                    a.total_marks         AS total_marks,
                    a.rubric              AS rubric,
                    co.course_name        AS course_name
                FROM assignment_submissions asub
                LEFT JOIN assignments  a ON a.id = asub.assignment_id
                LEFT JOIN courses      co ON co.id = a.course_id
                WHERE asub.student_id = :uid
                  -- v6.9: hackathon-type assignments (AIthon) are shown in the
                  -- Hackathon tab only — never double-counted as assignments.
                  AND LOWER(COALESCE(a.title, '')) NOT LIKE '%aithon%'
                  AND LOWER(COALESCE(a.title, '')) NOT LIKE '%hackathon%'
                ORDER BY asub.grade DESC, asub.submitted_at DESC
            """), {"uid": user_id}).mappings().all()

            items = []
            for r in rows:
                grade = r.get("grade")
                pct = self._pct(grade, r.get("total_marks"), r.get("rubric"))
                items.append({
                    "submission_id":   r["submission_id"],
                    "assignment_id":   r["assignment_id"],
                    "title":           r.get("assignment_title") or "Assignment",
                    "brief":           r.get("assignment_brief") or "",
                    "score":           pct,
                    "ai_score":        pct,
                    "faculty_score":   pct,
                    "rubric_pct":      pct,
                    "raw_grade":       float(grade) if grade is not None else None,
                    "total_marks":     r.get("total_marks"),
                    "max_score":       100,
                    "ai_grade_label":  self._grade_label(pct),
                    "rubric_grade":    self._grade_letter(pct),
                    "status":          r.get("status") or "",
                    "course_name":     r.get("course_name") or "",
                    "module_name":     "",
                    "submitted_at":    str(r.get("submitted_at") or ""),
                    "feedback":        r.get("feedback") or "",
                })
            return items
        except Exception as e:
            logger.warning(f"_get_assignments failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 3 — CAPSTONES
    # =====================================================================

    async def _get_capstones(self, user_id: int) -> List[Dict[str, Any]]:
        """
        capstones columns (verified via DESCRIBE):
          id, title, description, course_id, due_date, total_marks,
          status, grade, feedback, submitted_at, created_at, updated_at,
          company, file_url
        NO `reviewed_at` column.
        """
        try:
            rows = self.db.execute(text("""
                SELECT
                    c.id, c.title, c.description, c.course_id,
                    c.due_date, c.total_marks, c.status,
                    c.grade, c.feedback,
                    c.submitted_at,
                    co.course_name        AS course_name
                FROM capstones c
                LEFT JOIN courses co ON co.id = c.course_id
                WHERE c.student_id = :uid
                ORDER BY c.grade DESC, c.submitted_at DESC
            """), {"uid": user_id}).mappings().all()

            items = []
            for r in rows:
                grade = r.get("grade")
                total = r.get("total_marks") or 100
                pct = None
                if grade is not None and total:
                    try:
                        pct = round(float(grade) / float(total) * 100, 2)
                    except Exception:
                        pct = None
                items.append({
                    "capstone_id":   r["id"],
                    "title":         r.get("title") or "Capstone",
                    "brief":         r.get("description") or "",
                    "description":   r.get("description") or "",
                    "score":         pct,
                    "score_pct":     pct,
                    "ai_score":      pct,
                    "faculty_score": pct,
                    "grade":         self._grade_letter(pct),
                    "raw_grade":     float(grade) if grade is not None else None,
                    "total_marks":   float(total) if total else None,
                    "max_score":     100,
                    "status":        r.get("status") or "",
                    "course_name":   r.get("course_name") or "",
                    "due_date":      str(r.get("due_date") or ""),
                    "submitted_at":  str(r.get("submitted_at") or ""),
                    "feedback":      r.get("feedback") or "",
                })
            return items
        except Exception as e:
            logger.warning(f"_get_capstones failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 4 — INDUSTRY SESSIONS
    # =====================================================================

    async def _get_industry_sessions(self, user_id: int) -> List[Dict[str, Any]]:
        """
        v6.6 FIX (21 Oct 2026): the LMS never writes `industry_session_submissions`.
        Industry-session work lives in (backend/routes/industrySession.js +
        routes/adminCoursework.js):

          session_understandings  — id, session_id, student_id (= users.id),
              content, ai_review (JSON), submitted_at, attachment_url/_type/_name,
              and lazily-added grade DECIMAL, max_marks INT, feedback, status
              ('submitted' | 'graded')
          session_attendance      — id, session_id, student_id (= users.id),
              joined_at, attended BOOL, marked_by
          industry_sessions       — id, title, description, speaker, company,
              type, date, duration, platform, course_id, status

        Score = grade / max_marks * 100 when admin graded, else None (an
        attended / submitted-but-ungraded session is listed with no score and
        therefore does not move the snapshot axis).
        """
        try:
            # SELECT * — grade/max_marks/feedback/status columns are added at
            # runtime by the LMS (ensureSessionGradeCols) and may be absent.
            rows = self.db.execute(text("""
                SELECT u.*, u.id AS submission_id,
                       ise.title       AS session_title,
                       ise.description AS session_brief,
                       ise.speaker     AS speaker,
                       ise.company     AS company,
                       ise.type        AS session_type,
                       ise.duration    AS duration,
                       ise.date        AS session_date
                FROM session_understandings u
                LEFT JOIN industry_sessions ise ON ise.id = u.session_id
                WHERE u.student_id = :uid
                ORDER BY u.submitted_at DESC
            """), {"uid": user_id}).mappings().all()

            items: List[Dict[str, Any]] = []
            seen_sessions = set()
            for r in rows:
                grade = r.get("grade")
                max_marks = r.get("max_marks")
                score = None
                if grade is not None:
                    try:
                        mm = float(max_marks) if max_marks else 100.0
                        score = round(float(grade) / mm * 100.0, 2) if mm > 0 else None
                    except (TypeError, ValueError):
                        score = None
                seen_sessions.add(r.get("session_id"))
                items.append({
                    "submission_id": r["submission_id"],
                    "session_id":    r.get("session_id"),
                    "title":         r.get("session_title") or "Industry Session",
                    "brief":         r.get("session_brief") or "",
                    "score":         score,
                    "band":          "",
                    "grade":         str(grade) if grade is not None else "",
                    "status":        r.get("status") or ("graded" if grade is not None else "submitted"),
                    "speaker":       r.get("speaker") or "",
                    "company":       r.get("company") or "",
                    "type":          r.get("session_type") or "",
                    "insight":       r.get("content") or "",
                    "insight_text":  r.get("content") or "",
                    "feedback":      r.get("feedback") or "",
                    "held_at":       str(r.get("session_date") or r.get("submitted_at") or ""),
                    "duration":      r.get("duration") or "",
                    "submitted_at":  str(r.get("submitted_at") or ""),
                    "attended":      True,
                })

            # Attended-only sessions (no written insight) still count as
            # participation — list them without a score.
            try:
                att = self.db.execute(text("""
                    SELECT sa.session_id, sa.joined_at, sa.attended,
                           ise.title AS session_title, ise.speaker, ise.company,
                           ise.type AS session_type, ise.duration, ise.date AS session_date
                    FROM session_attendance sa
                    LEFT JOIN industry_sessions ise ON ise.id = sa.session_id
                    WHERE sa.student_id = :uid AND sa.attended = 1
                """), {"uid": user_id}).mappings().all()
                for r in att:
                    if r.get("session_id") in seen_sessions:
                        continue
                    items.append({
                        "submission_id": None,
                        "session_id":    r.get("session_id"),
                        "title":         r.get("session_title") or "Industry Session",
                        "brief":         "",
                        "score":         None,
                        "band":          "",
                        "grade":         "",
                        "status":        "attended",
                        "speaker":       r.get("speaker") or "",
                        "company":       r.get("company") or "",
                        "type":          r.get("session_type") or "",
                        "insight":       "",
                        "insight_text":  "",
                        "feedback":      "",
                        "held_at":       str(r.get("session_date") or r.get("joined_at") or ""),
                        "duration":      r.get("duration") or "",
                        "submitted_at":  str(r.get("joined_at") or ""),
                        "attended":      True,
                    })
            except Exception as e:
                logger.info(f"_get_industry_sessions: attendance-only read skipped ({e})")

            return items
        except Exception as e:
            logger.warning(f"_get_industry_sessions failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # HACKATHONS — v6.7 (21 Oct 2026)
    # =====================================================================
    # Verified in production (defaultdb):
    #   • AIthon submissions are rows of assignment_submissions for the
    #     assignment whose title is "Aithon" (id 50) — keyed on students.id.
    #     Columns: notes, share_note, file_name, file_path, grade, feedback,
    #     status, submitted_at. The assignment's description holds the
    #     problem statement.
    #   • upski_coin_ledger: task_key 'aithon_participation'
    #     (ref_id 'aithon2026:participation', coins 50, user_id = users.id)
    #     marks felicitated participants.
    #   • aithon_coin_sheet (email, participation, winner, offline, prize)
    #     is the organiser's sheet — winner / prize flags live here.
    # No dedicated hackathon table exists; this reads the sources directly.

    _HACK_TITLE_PATTERNS = ("%aithon%", "%hackathon%", "%hack-a-thon%")
    _LMS_PUBLIC_BASE = os.environ.get("LMS_PUBLIC_BASE_URL",
                                      "https://upskillize-lms-backend.onrender.com")

    def _abs_link(self, path: Optional[str]) -> str:
        if not path:
            return ""
        p = str(path).strip()
        if p.startswith("http://") or p.startswith("https://"):
            return p
        return self._LMS_PUBLIC_BASE.rstrip("/") + ("/" + p.lstrip("/"))

    async def _get_hackathons(self, user_id: int, students_id: Optional[int],
                              email: Optional[str] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        seen_assignment: set = set()

        # 1. Hackathon-type assignments the student submitted
        if students_id is not None:
            rows = self.db.execute(text("""
                SELECT s.id AS submission_id, s.assignment_id, s.status, s.grade,
                       s.feedback, s.notes, s.share_note, s.file_name, s.file_path,
                       s.submitted_at,
                       a.title, a.description, a.due_date, a.course_id,
                       a.total_marks, a.rubric,
                       co.course_name
                FROM assignment_submissions s
                JOIN assignments a ON a.id = s.assignment_id
                LEFT JOIN courses co ON co.id = a.course_id
                WHERE s.student_id = :sid
                  AND (LOWER(a.title) LIKE :p1 OR LOWER(a.title) LIKE :p2
                       OR LOWER(a.title) LIKE :p3 OR LOWER(a.description) LIKE :p1)
                ORDER BY s.submitted_at DESC
            """), {"sid": students_id,
                   "p1": self._HACK_TITLE_PATTERNS[0],
                   "p2": self._HACK_TITLE_PATTERNS[1],
                   "p3": self._HACK_TITLE_PATTERNS[2]}).mappings().all()
            for r in rows:
                seen_assignment.add(r["assignment_id"])
                grade = r.get("grade")
                pct = self._pct(grade, r.get("total_marks"), r.get("rubric"))
                # v6.9 rule: a graded hackathon entry must score ≥ 50% to be
                # shown on the profile. Ungraded entries (participation) stay.
                if pct is not None and pct < 50:
                    continue
                items.append({
                    "source":            "assignment",
                    "assignment_id":     r["assignment_id"],
                    "submission_id":     r["submission_id"],
                    "title":             r.get("title") or "Hackathon",
                    "name":              r.get("title") or "Hackathon",
                    "organiser":         "Upskillize Solutions",
                    "category":          "Hackathon",
                    "problem_statement": r.get("description") or "",
                    "solution":          (r.get("notes") or r.get("share_note") or ""),
                    "description":       (r.get("notes") or r.get("share_note") or "")[:220],
                    "link":              self._abs_link(r.get("file_path")),
                    "link_label":        r.get("file_name") or "View submission",
                    "score":             pct,
                    "raw_grade":         float(grade) if grade is not None else None,
                    "total_marks":       r.get("total_marks"),
                    "feedback":          r.get("feedback") or "",
                    "status":            r.get("status") or "submitted",
                    "held_at":           str(r.get("due_date") or r.get("submitted_at") or "")[:10],
                    "submitted_at":      str(r.get("submitted_at") or ""),
                    "course_name":       r.get("course_name") or "",
                    "participated":      True,
                    "winner":            False,
                    "prize":             "",
                    "award":             "Participant",
                    "badge":             "Participant",
                })

        # 2. Felicitated participation (coin ledger) — may exist without a submission
        try:
            led = self.db.execute(text("""
                SELECT task_key, coins, note, ref_id, earned_at
                FROM upski_coin_ledger
                WHERE user_id = :uid
                  AND (task_key LIKE 'aithon%' OR task_key LIKE '%hackathon%')
                ORDER BY earned_at DESC
            """), {"uid": user_id}).mappings().all()
        except Exception as e:
            logger.info(f"_get_hackathons: ledger read skipped ({e})")
            led = []
        coins_total = sum(int(r.get("coins") or 0) for r in led)
        if led:
            if items:
                items[0]["coins"] = coins_total
                items[0]["award"] = "Felicitated participant"
                items[0]["badge"] = "Felicitated"
            else:
                r = led[0]
                items.append({
                    "source": "ledger", "title": "AIthon 2026", "name": "AIthon 2026",
                    "organiser": "Upskillize Solutions × DCCL", "category": "Hackathon",
                    "problem_statement": "", "solution": "", "description": r.get("note") or "",
                    "link": "", "link_label": "", "score": None, "feedback": "",
                    "status": "participated", "held_at": str(r.get("earned_at") or "")[:10],
                    "submitted_at": "", "course_name": "", "participated": True,
                    "winner": False, "prize": "", "award": "Felicitated participant",
                    "badge": "Felicitated", "coins": coins_total,
                })

        # 3. Organiser sheet — winner / prize
        if email and items:
            try:
                sh = self.db.execute(text("""
                    SELECT * FROM aithon_coin_sheet WHERE LOWER(email) = LOWER(:em) LIMIT 1
                """), {"em": email}).mappings().first()
            except Exception as e:
                logger.info(f"_get_hackathons: aithon_coin_sheet skipped ({e})")
                sh = None
            if sh:
                def _truthy(v):
                    return str(v).strip().lower() not in ("", "0", "no", "false", "none", "null")
                win = _truthy(sh.get("winner"))
                prize = sh.get("prize")
                items[0]["winner"] = win
                items[0]["prize"] = str(prize) if prize not in (None, "", 0, "0") else ""
                if win:
                    items[0]["award"] = "Winner" + (f" · {items[0]['prize']}" if items[0]["prize"] else "")
                    items[0]["badge"] = "Winner"
                elif items[0]["prize"]:
                    items[0]["award"] = items[0]["prize"]
                    items[0]["badge"] = "Prize"

        return items

    # =====================================================================
    # ACTIVITY FLOW 5 — MOCK TESTS (TestGen)
    # =====================================================================

    async def _get_mock_tests(self, user_id: int) -> List[Dict[str, Any]]:
        """
        test_history columns (verified via DESCRIBE):
          id, test_id, student_id (varchar!), course_id, lecture_id,
          topic, difficulty, total_questions, correct_answers,
          score_percentage, performance_band, duration_minutes,
          time_taken_seconds, overall_feedback, created_at
        NO `completed_at` column.
        student_id is varchar(100) — MySQL coerces int → varchar for
        WHERE = clause, so passing user_id as int is fine.
        """
        try:
            rows = self.db.execute(text("""
                SELECT
                    id, test_id, topic, difficulty,
                    total_questions, correct_answers,
                    score_percentage, performance_band,
                    duration_minutes, time_taken_seconds,
                    created_at
                FROM test_history
                WHERE student_id = :uid
                  -- v6.9: an attempt with zero correct answers, or no topic, is an
                  -- opened-and-abandoned test (verified 21 Oct: all 22 such rows
                  -- for user 87 had correct_answers=0, most under 30s). Excluding
                  -- them keeps the axis an honest average of real attempts.
                  AND COALESCE(correct_answers, 0) > 0
                  AND COALESCE(topic, '') <> ''
                ORDER BY score_percentage DESC, created_at DESC
            """), {"uid": str(user_id)}).mappings().all()

            items = []
            for r in rows:
                pct = r.get("score_percentage")
                items.append({
                    "test_id":         r.get("test_id"),
                    "title":           r.get("topic") or "TestGen Practice",
                    "topic":           r.get("topic") or "",
                    "test_name":       r.get("topic") or "",
                    "exam_name":       r.get("topic") or "",
                    "difficulty":      r.get("difficulty") or "",
                    "total_questions": r.get("total_questions") or 0,
                    "correct_answers": r.get("correct_answers") or 0,
                    "score":           float(pct) if pct is not None else None,
                    "percentage":      float(pct) if pct is not None else None,
                    "total_marks":     100,
                    "max_score":       100,
                    "grade":           r.get("performance_band") or "",
                    "band":            r.get("performance_band") or "",
                    "test_type":       "mock",
                    "attempted_at":    str(r.get("created_at") or ""),
                    "submitted_at":    str(r.get("created_at") or ""),
                    "course_name":     "",
                })
            return items
        except Exception as e:
            logger.warning(f"_get_mock_tests failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 6 — MOCK INTERVIEWS
    # =====================================================================

    async def _get_mock_interviews(self, user_id: int) -> List[Dict[str, Any]]:
        """
        vyom_sessions columns (verified via DESCRIBE):
          id (varchar!), user_id (varchar!), role, level, mode,
          session_mode, round, round_label, round_index, focus,
          company, name, difficulty, duration_min, started_at,
          ended_at, completion_type, current_stage, status,
          answer_count, assistant_message_count, user_message_count,
          camera_at_join, awaiting_rating, deleted_at,
          actual_duration_seconds, ...

        NO `session_type` column. Use `mode` or `round_label` instead.
        NO `created_at` — use `started_at`.
        NO `completed_at` — use `ended_at`.
        user_id is varchar(64), so we pass user_id as string.
        """
        try:
            rows = self.db.execute(text("""
                SELECT
                    id, user_id, mode, round_label, role, level,
                    status, current_stage, completion_type,
                    company, focus, duration_min, answer_count,
                    started_at, ended_at
                FROM vyom_sessions
                WHERE user_id = :uid
                  AND deleted_at IS NULL
                ORDER BY started_at DESC
            """), {"uid": str(user_id)}).mappings().all()

            items = []
            for row in rows:
                sess = dict(row)
                sess_id = sess["id"]
                # Aggregate ratings from vyom_answer_ratings
                try:
                    agg = self.db.execute(text("""
                        SELECT AVG(rating) AS avg_rating, COUNT(*) AS answer_count
                        FROM vyom_answer_ratings
                        WHERE session_id = :sid
                    """), {"sid": sess_id}).mappings().first()
                except Exception:
                    agg = None

                avg = agg["avg_rating"] if agg else None
                cnt = int(agg["answer_count"]) if agg and agg["answer_count"] else 0
                score = None
                if avg is not None and cnt > 0:
                    try:
                        avg_f = float(avg)
                        if avg_f <= 5:
                            score = round(avg_f / 5.0 * 100, 2)
                        elif avg_f <= 10:
                            score = round(avg_f / 10.0 * 100, 2)
                        else:
                            score = round(min(avg_f, 100), 2)
                    except Exception:
                        score = None

                items.append({
                    "session_id":   sess_id,
                    "title":        (sess.get("role") or sess.get("round_label") or sess.get("mode") or "Mock Interview"),
                    "session_type": sess.get("mode") or "",
                    "mode":         sess.get("mode") or "",
                    "role":         sess.get("role") or "",
                    "level":        sess.get("level") or "",
                    "company":      sess.get("company") or "",
                    "focus":        sess.get("focus") or "",
                    "round":        sess.get("round_label") or "",
                    "status":       sess.get("status") or "",
                    "current_stage": sess.get("current_stage") or "",
                    "completion":   sess.get("completion_type") or "",
                    "duration_min":     sess.get("duration_min") or 0,
                    "duration_minutes": sess.get("duration_min") or 0,  # template key
                    "answer_count": cnt or sess.get("answer_count") or 0,
                    "score":        score,
                    "percentage":   score,
                    "max_score":    100,
                    "completed_at": str(sess.get("ended_at") or ""),
                    "submitted_at": str(sess.get("ended_at") or sess.get("started_at") or ""),  # template key
                    "created_at":   str(sess.get("started_at") or ""),
                })
            items.sort(key=lambda x: (x["score"] or 0), reverse=True)
            return items
        except Exception as e:
            logger.warning(f"_get_mock_interviews failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ATTENDANCE — now uses user_id directly (no students.id hop)
    # =====================================================================

    async def _get_attendance(self, user_id: int) -> Dict[str, Any]:
        """
        student_attendance columns (verified via DESCRIBE):
          id, student_id (=students.id, legacy), user_id (=users.id, current),
          course_id, lesson_id, lesson_title, session_date,
          joined_at, left_at, duration_minutes, watch_percent,
          status, device, ip_address, created_at, updated_at

        NO `session_id` column — use `lesson_id`.
        NO `marked_at` column — use `joined_at` or `session_date`.
        NO `student_attendance.session_id` — use `lesson_id`.
        """
        try:
            row = self.db.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'present' OR status = 'joined' THEN 1 ELSE 0 END) AS present
                FROM student_attendance
                WHERE user_id = :uid
            """), {"uid": user_id}).mappings().first()

            total = int((row["total"] or 0)) if row else 0
            present = int((row["present"] or 0)) if row else 0
            pct = round(present / total * 100, 2) if total > 0 else None

            recent = self.db.execute(text("""
                SELECT lesson_id, lesson_title, session_date, joined_at, status
                FROM student_attendance
                WHERE user_id = :uid
                ORDER BY session_date DESC, joined_at DESC
                LIMIT 10
            """), {"uid": user_id}).mappings().all()

            return {
                "sessions_attended":  present,
                "sessions_total":     total,
                "attendance_percent": pct,
                "recent_sessions":    [dict(r) for r in recent],
            }
        except Exception as e:
            logger.warning(f"_get_attendance failed for user_id={user_id}: {e}")
            return self._empty_attendance()

    # =====================================================================
    # PUNCTUALITY (8th snapshot axis)
    # =====================================================================

    async def _get_punctuality(self, user_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(text("""
                SELECT
                    student_id, score, band, events_counted, computed_at, breakdown
                FROM punctuality_scores
                WHERE student_id = :uid
                ORDER BY computed_at DESC
                LIMIT 1
            """), {"uid": user_id}).mappings().first()

            if not row:
                return {"score": None, "band": None, "events_counted": 0, "status": "no_data"}

            bd = row.get("breakdown")
            if isinstance(bd, (str, bytes)):
                try:
                    bd = json.loads(bd)
                except Exception:
                    bd = {}
            if not isinstance(bd, dict):
                bd = {}
            return {
                "score":          float(row["score"]) if row["score"] is not None else None,
                "band":           row.get("band") or "",
                "events_counted": int(row.get("events_counted") or 0),
                "computed_at":    str(row.get("computed_at") or ""),
                "breakdown":      bd,
                "status":         "available" if row["score"] is not None else "cold_start",
            }
        except Exception as e:
            logger.info(f"_get_punctuality: unavailable for user_id={user_id} ({e})")
            return {"score": None, "band": None, "events_counted": 0, "status": "table_missing"}

    # =====================================================================
    # PSYCHOMETRIC
    # =====================================================================

    async def _get_psychometric(self, user_id: int) -> Dict[str, Any]:
        """v6.8 — the LMS stores psychometric results in
        lms_profile_psychometric_test (routes/psychometric.js): user_id,
        attempt_no, dominant_trait, dominant_icon, top_traits JSON,
        trait_scores JSON, conflict_style, learning_style, archetype,
        work_values JSON, role_matches JSON, is_latest, completed_at.
        users.psycho_result (the old source) is never written by the LMS;
        it is kept only as a fallback."""
        empty = {"personality_type": None, "traits": [], "summary": "", "status": "no_data"}

        def _j(v):
            if v in (None, ""):
                return None
            if isinstance(v, (dict, list)):
                return v
            try:
                return json.loads(v)
            except Exception:
                return None

        try:
            row = self.db.execute(text("""
                SELECT * FROM lms_profile_psychometric_test
                WHERE user_id = :uid
                ORDER BY is_latest DESC, attempt_no DESC, completed_at DESC
                LIMIT 1
            """), {"uid": user_id}).mappings().first()
        except Exception as e:
            logger.info(f"_get_psychometric: lms_profile_psychometric_test unavailable ({e})")
            row = None

        if row:
            top = _j(row.get("top_traits")) or []
            traits = []
            for t in top:
                if isinstance(t, dict):
                    nm = t.get("trait") or t.get("name") or t.get("label")
                    if nm:
                        traits.append(str(nm))
                elif t:
                    traits.append(str(t))
            scores = _j(row.get("trait_scores")) or {}
            values = _j(row.get("work_values")) or []
            roles  = _j(row.get("role_matches")) or []
            ptype  = row.get("archetype") or row.get("dominant_trait") or ""
            bits = []
            if row.get("dominant_trait"):
                bits.append(f"Dominant trait: {row['dominant_trait']}")
            if row.get("learning_style"):
                bits.append(f"learning style: {row['learning_style']}")
            if row.get("conflict_style"):
                bits.append(f"conflict style: {row['conflict_style']}")
            return {
                "personality_type": ptype,
                "archetype":        row.get("archetype") or "",
                "dominant_trait":   row.get("dominant_trait") or "",
                "traits":           traits,
                "strengths":        traits[:3],
                "trait_scores":     scores if isinstance(scores, dict) else {},
                "learning_style":   row.get("learning_style") or "",
                "conflict_style":   row.get("conflict_style") or "",
                "work_values":      values,
                "role_matches":     roles,
                "summary":          "; ".join(bits),
                "completed_at":     str(row.get("completed_at") or ""),
                "status":           "available",
            }

        # fallback: legacy users.psycho_result
        try:
            r2 = self.db.execute(text("""
                SELECT psycho_result FROM users WHERE id = :uid LIMIT 1
            """), {"uid": user_id}).first()
            if not r2 or not r2[0]:
                return empty
            data = _j(r2[0]) or {}
            if not isinstance(data, dict):
                return empty
            return {
                "personality_type": data.get("type") or data.get("mbti") or "",
                "traits":           data.get("traits") or [],
                "strengths":        data.get("strengths") or [],
                "summary":          data.get("summary") or "",
                "status":           "available",
            }
        except Exception as e:
            logger.warning(f"_get_psychometric failed for user_id={user_id}: {e}")
            return empty

    async def _get_certifications(self, user_id: int) -> List[Dict[str, Any]]:
        """users.certifications (self-reported JSON/CSV) + v6.6: LMS-issued
        course certificates from the `certificates` table, flagged
        upskillize_verified so the template renders the verified badge."""
        out: List[Dict[str, Any]] = []
        try:
            row = self.db.execute(text("""
                SELECT certifications FROM users WHERE id = :uid LIMIT 1
            """), {"uid": user_id}).first()
            raw = row[0] if row and row[0] else None
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        out = [
                            (c if isinstance(c, dict) else {"name": str(c), "certificate_name": str(c)})
                            for c in parsed
                        ]
                except Exception:
                    out = [
                        {"name": c.strip(), "certificate_name": c.strip()}
                        for c in raw.split(",") if c.strip()
                    ]
        except Exception as e:
            logger.warning(f"_get_certifications failed for user_id={user_id}: {e}")

        try:
            sid = self._resolve_students_id(user_id)
            have = {(c.get("certificate_name") or c.get("name") or "").strip().lower() for c in out}
            for cid, cert in self._query_certificates(user_id, sid).items():
                name = cert.get("course_name") or f"Course #{cid}"
                if name.strip().lower() in have:
                    continue
                out.append({
                    "name":                name,
                    "certificate_name":    name,
                    "course_name":         cert.get("course_name") or "",
                    "issuer":              "Upskillize Solutions",
                    "issued_at":           (cert.get("issued_at") or "")[:10],
                    "score":               cert.get("final_score"),
                    "completion_pct":      100,
                    "upskillize_verified": True,
                    "certificate_id":      cert.get("certificate_id"),
                    "code":                cert.get("code") or "",
                    "url":                 cert.get("png_url") or "",
                })
        except Exception as e:
            logger.info(f"_get_certifications: LMS certificates skipped ({e})")
        return out

    # =====================================================================
    # ASSESSMENTS (faculty-uploaded quizzes)
    # =====================================================================

    async def _get_proctored_exams(self, user_id: int) -> List[Dict[str, Any]]:
        """v6.8 — Proctored exams (routes/examStudent.js): exam_attempts
        keyed on users.id; score = total_score / max_score. Only graded or
        published attempts count. Listed alongside faculty quizzes in the
        Assessments tab (flagged test_type='proctored')."""
        try:
            rows = self.db.execute(text("""
                SELECT ea.id, ea.exam_id, ea.status, ea.total_score, ea.max_score,
                       ea.passed, ea.submitted_at,
                       e.exam_name, e.exam_type, e.total_marks, e.course_id,
                       co.course_name
                FROM exam_attempts ea
                LEFT JOIN exams   e  ON e.id  = ea.exam_id
                LEFT JOIN courses co ON co.id = e.course_id
                WHERE ea.student_id = :uid
                  AND ea.status IN ('graded', 'published')
                ORDER BY ea.submitted_at DESC
            """), {"uid": user_id}).mappings().all()
        except Exception as e:
            logger.info(f"_get_proctored_exams: unavailable ({e})")
            return []
        items = []
        for r in rows:
            mx = r.get("max_score") or r.get("total_marks")
            pct = self._pct(r.get("total_score"), mx)
            items.append({
                "quiz_id":      f"exam-{r.get('exam_id')}",
                "attempt_id":   r["id"],
                "title":        r.get("exam_name") or "Proctored Exam",
                "quiz_title":   r.get("exam_name") or "Proctored Exam",
                "score":        pct,
                "percentage":   pct,
                "raw_score":    float(r["total_score"]) if r.get("total_score") is not None else None,
                "total_marks":  float(mx) if mx else None,
                "max_score":    100,
                "passed":       bool(r.get("passed")) if r.get("passed") is not None else None,
                "status":       r.get("status") or "",
                "test_type":    "proctored",
                "course_name":  r.get("course_name") or "",
                "submitted_at": str(r.get("submitted_at") or ""),
            })
        return items

    async def _get_assessments(self, user_id: int) -> List[Dict[str, Any]]:
        """
        quizzes columns (verified via DESCRIBE):
          id, course_id, title, description, pass_percentage,
          time_limit_minutes, is_active, created_at, updated_at
        quiz_attempts columns (verified via DESCRIBE):
          id, quiz_id, student_id, score, total_marks, passed,
          submitted_at, time_taken_seconds, answers, created_at, updated_at
        """
        try:
            rows = self.db.execute(text("""
                SELECT
                    qa.id           AS attempt_id,
                    qa.quiz_id,
                    qa.score,
                    qa.total_marks,
                    qa.passed,
                    qa.submitted_at,
                    qa.time_taken_seconds,
                    qa.created_at,
                    q.title         AS quiz_title,
                    q.description   AS quiz_description,
                    q.course_id,
                    q.pass_percentage,
                    q.time_limit_minutes,
                    co.course_name  AS course_name
                FROM quiz_attempts qa
                LEFT JOIN quizzes q  ON q.id  = qa.quiz_id
                LEFT JOIN courses co ON co.id = q.course_id
                WHERE qa.student_id = :uid
                ORDER BY qa.score DESC, qa.submitted_at DESC
            """), {"uid": user_id}).mappings().all()

            items = []
            for r in rows:
                score = r.get("score")
                total = r.get("total_marks") or 100
                pct = None
                if score is not None and total:
                    try:
                        pct = round(float(score) / float(total) * 100, 2)
                    except Exception:
                        pct = None
                items.append({
                    "attempt_id":     r["attempt_id"],
                    "quiz_id":        r["quiz_id"],
                    "title":          r.get("quiz_title") or "Assessment",
                    "test_name":      r.get("quiz_title") or "",
                    "exam_name":      r.get("quiz_title") or "",
                    "brief":          r.get("quiz_description") or "",
                    "description":    r.get("quiz_description") or "",
                    "score":          pct,
                    "percentage":     pct,
                    "raw_score":      float(score) if score is not None else None,
                    "total_marks":    float(total) if total else None,
                    "max_score":      100,
                    "grade":          self._grade_letter(pct),
                    "ai_grade_label": self._grade_label(pct),
                    "passed":         bool(r.get("passed")),
                    "pass_percentage": r.get("pass_percentage") or 60,
                    "time_taken_sec": r.get("time_taken_seconds") or 0,
                    "time_limit_min": r.get("time_limit_minutes") or 0,
                    "course_name":    r.get("course_name") or "",
                    "module_name":    "",
                    "test_type":      "quiz",
                    "attempted_at":   str(r.get("submitted_at") or r.get("created_at") or ""),
                    "submitted_at":   str(r.get("submitted_at") or ""),
                })
            return items
        except Exception as e:
            logger.warning(f"_get_assessments failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # PERFORMANCE SNAPSHOT — 8 axes with FAIR AVERAGE
    # =====================================================================

    # LOCKED AGGREGATION RULE (user-confirmed, Jul 2026):
    #   1. Per axis: dedupe to the BEST attempt per item (a retried mock
    #      test counts once, at its highest score), then average those
    #      best attempts across the category.
    #   2. Overall Average Score: sum of all 8 axis values ÷ 8, with an
    #      empty/no-data axis counting as 0. The score doubles as a
    #      programme-completion signal — locked product decision.

    @staticmethod
    def _dedupe_best(items: list, *key_fields: str) -> list:
        """One row per item: group by the first present key field
        (id-based key preferred, title fallback), keep the highest-scored
        row, and annotate it with attempt_count."""
        if not items:
            return []
        best: Dict[Any, Dict] = {}
        order: List[Any] = []
        for row in items:
            key = None
            for f in key_fields:
                v = row.get(f)
                if v not in (None, ""):
                    key = (f, str(v).strip().lower())
                    break
            if key is None:
                key = ("__row__", id(row))
            try:
                sc = float(row.get("score")) if row.get("score") is not None else None
            except (TypeError, ValueError):
                sc = None
            if key not in best:
                entry = dict(row)
                entry["attempt_count"] = 1
                entry["_best"] = sc
                best[key] = entry
                order.append(key)
            else:
                cur = best[key]
                cur["attempt_count"] += 1
                if sc is not None and (cur["_best"] is None or sc > cur["_best"]):
                    n = cur["attempt_count"]
                    entry = dict(row)
                    entry["attempt_count"] = n
                    entry["_best"] = sc
                    best[key] = entry
        out = [best[k] for k in order]
        for r in out:
            r.pop("_best", None)
        out.sort(key=lambda r: (r.get("score") is not None,
                                float(r.get("score") or 0)), reverse=True)
        return out

    def _compute_snapshot(self, **sections) -> Dict[str, Any]:
        def _axis_avg(items: list) -> Optional[float]:
            if not items:
                return None
            scores = [x.get("score") for x in items if x.get("score") is not None]
            if not scores:
                return None
            try:
                return round(sum(float(s) for s in scores) / len(scores), 2)
            except Exception:
                return None

        # Best attempt per item FIRST, then average per category.
        assignments = self._dedupe_best(sections.get("assignments") or [], "assignment_id", "title")
        case_studies = self._dedupe_best(sections.get("case_studies") or [], "case_study_id", "title")
        capstones = self._dedupe_best(sections.get("capstones") or [], "capstone_id", "title")
        industry = self._dedupe_best(sections.get("industry_sessions") or [], "session_id", "title")
        # NOTE: TestGen mints a NEW test_id per attempt, so test_id is
        # useless as a grouping key (32 attempts looked like 32 tests).
        # Group by topic — retaking "Banking Basics" = one item, best score.
        mock_tests = self._dedupe_best(sections.get("mock_tests") or [], "topic", "title")
        mock_interviews = self._dedupe_best(sections.get("mock_interviews") or [], "session_id", "title")
        assessments = self._dedupe_best(sections.get("assessments") or [], "quiz_id", "title")

        punct = sections.get("punctuality") or {}
        axes = {
            "assignments":       _axis_avg(assignments),
            "case_studies":      _axis_avg(case_studies),
            "capstones":         _axis_avg(capstones),
            "industry_sessions": _axis_avg(industry),
            "mock_tests":        _axis_avg(mock_tests),
            "mock_interviews":   _axis_avg(mock_interviews),
            "assessments":       _axis_avg(assessments),
            "punctuality":       punct.get("score") if punct.get("score") is not None else None,
        }

        active_values = [v for v in axes.values() if v is not None and v > 0]
        # Locked rule: divide by all 8 axes; empty axis counts as 0.
        overall_avg = round(sum(float(v or 0) for v in axes.values()) / 8.0, 1)

        return {
            "axes":              axes,
            "active_axis_count": len(active_values),
            "fair_average":      overall_avg,
            "average":           overall_avg,
            "axis_labels": {
                "assignments":       "Assignments",
                "case_studies":      "Case Studies",
                "capstones":         "Capstones",
                "industry_sessions": "Industry Sessions",
                "mock_tests":        "Mock Tests",
                "mock_interviews":   "Mock Interviews",
                "assessments":       "Assessments",
                "punctuality":       "Punctuality",
            },
            "punctuality_status": punct.get("status", "no_data"),
            "punctuality_band":   punct.get("band", ""),
        }

    # =====================================================================
    # COMPUTED METRICS
    # =====================================================================
    # Downstream consumers (orchestrator._performance, _testgen, DataMerger
    # top_subjects skills path, routes richness guards) read these keys.
    # v6.3 never produced them — everything read 0. v6.4 computes them.

    def _compute_metrics(self, user_id: int, snapshot: Dict, *,
                         courses, mock_tests, assessments, case_studies,
                         assignments, capstones, industry_sessions,
                         mock_interviews) -> Dict[str, Any]:
        def _f(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        # topic-first grouping — TestGen's test_id is unique per attempt
        best_tests = self._dedupe_best(mock_tests, "topic", "title")
        test_scores = [_f(t.get("score")) for t in best_tests]
        test_scores = [s for s in test_scores if s is not None]

        # top_subjects: best score per topic (feeds LMS-verified skills)
        by_topic: Dict[str, float] = {}
        for t in best_tests:
            topic = (t.get("topic") or "").strip()
            sc = _f(t.get("score"))
            if topic and sc is not None:
                if topic not in by_topic or sc > by_topic[topic]:
                    by_topic[topic] = sc
        top_subjects = sorted(
            [(k, round(v, 1)) for k, v in by_topic.items()],
            key=lambda x: x[1], reverse=True,
        )[:8]

        completed_courses = sum(
            1 for c in courses
            if (c.get("completion_status") or c.get("status") or "").lower() == "completed"
            or float(c.get("progress_percentage") or c.get("progress") or 0) >= 100
        )

        axes = snapshot.get("axes", {}) or {}
        avg_case = _f(axes.get("case_studies"))
        avg_quiz = _f(axes.get("assessments"))

        return {
            "user_id":            user_id,
            "total_case_studies": len(case_studies),
            "total_assignments":  len(assignments),
            "total_capstones":    len(capstones),
            "total_industry":     len(industry_sessions),
            "total_mock_tests":   len(mock_tests),
            "total_interviews":   len(mock_interviews),
            "total_assessments":  len(assessments),
            # Metrics the rest of the pipeline reads:
            "overall_score":        snapshot.get("average") or 0,
            "best_test_score":      round(max(test_scores), 1) if test_scores else 0,
            "avg_test_score":       round(sum(test_scores) / len(test_scores), 1) if test_scores else 0,
            "avg_case_study_score": avg_case or 0,
            "avg_quiz_score":       avg_quiz or 0,
            "total_tests":          len(best_tests),
            "total_quizzes":        len(assessments),
            "total_courses":        len(courses),
            "completed_courses":    completed_courses,
            "top_subjects":         top_subjects,
        }

    # =====================================================================
    # HELPERS
    # =====================================================================

    def _join_location(self, r: Dict) -> str:
        parts = [r.get(k) for k in ("city", "state", "country") if r.get(k)]
        return ", ".join(str(p) for p in parts if p)

    def _grade_letter(self, score) -> str:
        if score is None:
            return ""
        try:
            s = float(score)
        except Exception:
            return ""
        if s >= 90: return "A+"
        if s >= 80: return "A"
        if s >= 70: return "B"
        if s >= 60: return "C"
        if s >= 50: return "D"
        return "F"

    def _grade_label(self, score) -> str:
        if score is None:
            return ""
        try:
            s = float(score)
        except Exception:
            return ""
        if s >= 90: return "Excellent"
        if s >= 80: return "Very Good"
        if s >= 70: return "Good"
        if s >= 60: return "Satisfactory"
        if s >= 50: return "Needs Improvement"
        return "Insufficient"

    def _empty_personal(self, user_id: int) -> Dict[str, Any]:
        return {
            "user_id": user_id, "full_name": "", "email": "",
            "phone": "", "photo_url": "", "bio": "", "about_me": "",
            "location": "",
            "current_designation": "", "current_employer": "",
            "work_experience_years": "", "employment_type": "",
            "education_level": "", "institution": "",
            "field_of_study": "", "graduation_year": "",
            "key_skills": "", "career_goals": "", "preferred_role": "",
            "preferred_location": "", "notice_period": "", "open_to_relocation": "",
            "expected_ctc": "", "current_ctc": "",
            "preferred_salary_min": "", "preferred_salary_max": "", "work_mode": "",
            "linkedin_url": "", "github_url": "", "portfolio_url": "",
            "website_url": "", "twitter_url": "",
            "resume_url": "", "resume_name": "",
            "languages_known": "", "hobbies": "", "industries": "",
        }

    def _empty_attendance(self) -> Dict[str, Any]:
        return {
            "sessions_attended":   0,
            "sessions_total":      0,
            "attendance_percent":  None,
            "recent_sessions":     [],
        }

    def _empty_payload(self, user_id: int) -> Dict[str, Any]:
        return {
            "personal":              self._empty_personal(user_id),
            "courses":               [],
            "case_studies":          [],
            "assignments":           [],
            "capstones":             [],
            "capstone_projects":     [],
            "industry_sessions":     [],
            "hackathons":            [],
            "industry_interactions": [],
            "mock_tests":            [],
            "mock_interviews":       [],
            "assessments":           [],
            "test_scores":           [],
            "attendance":            self._empty_attendance(),
            "punctuality":           {"score": None, "band": None,
                                      "events_counted": 0, "status": "no_data"},
            "personality":           {"personality_type": None, "traits": [],
                                      "summary": "", "status": "no_data"},
            "certifications":        [],
            "performance_snapshot":  self._compute_snapshot(),
            "batch_info":            {},
            "computed": {
                "user_id":            user_id,
                "total_case_studies": 0,
                "total_assignments":  0,
                "total_capstones":    0,
                "total_industry":     0,
                "total_mock_tests":   0,
                "total_interviews":   0,
                "total_assessments":  0,
            },
        }

    def _safe_default(self, exc, default):
        logger.warning(f"Collector task failed: {exc}")
        return default


# =========================================================================
# SCHEMA CONTRACT VALIDATION — called once at app startup
# =========================================================================

# Every table + key column the SQL in this file depends on. Derived from
# the queries above — keep in sync when a query changes.
# NOTE: punctuality_scores is known-optional (the collector already
# degrades to status="table_missing"); a warning for it is informational.
_SCHEMA_CONTRACT: Dict[str, List[str]] = {
    "users":                        ["id", "full_name", "email",
                                     "certifications", "psycho_result"],
    "students":                     ["id", "user_id"],
    "courses":                      ["id", "course_name", "category"],
    "enrollments":                  ["id", "student_id", "course_id",
                                     "completion_status",
                                     "progress_percentage", "created_at"],
    "case_studies":                 ["id", "title", "course_id"],
    "case_study_submissions":       ["id", "student_id", "case_study_id",
                                     "grade", "status", "submitted_at"],
    "assignments":                  ["id", "title", "course_id"],
    "assignment_submissions":       ["id", "student_id", "assignment_id",
                                     "grade", "submitted_at"],
    "capstones":                    ["id", "student_id", "title", "grade",
                                     "total_marks", "submitted_at"],
    "industry_sessions":            ["id", "title", "speaker"],
    "session_understandings":       ["id", "student_id", "session_id",
                                     "content", "submitted_at"],
    "session_attendance":           ["session_id", "student_id", "attended"],
    "certificates":                 ["course_id", "is_revoked"],
    "upski_coin_ledger":            ["user_id", "task_key", "coins", "note"],
    "lms_profile_psychometric_test":["user_id", "is_latest", "dominant_trait"],
    "exam_attempts":                ["student_id", "status", "total_score", "max_score"],
    "exams":                        ["id", "exam_name"],
    "test_history":                 ["id", "student_id", "score_percentage",
                                     "created_at"],
    "vyom_sessions":                ["id", "user_id", "mode",
                                     "started_at", "deleted_at"],
    "vyom_answer_ratings":          ["session_id", "rating"],
    "student_attendance":           ["user_id", "status",
                                     "session_date", "lesson_id"],
    "punctuality_scores":           ["student_id", "score", "band",
                                     "computed_at"],
    "quizzes":                      ["id", "title", "course_id"],
    "quiz_attempts":                ["id", "student_id", "quiz_id",
                                     "score", "total_marks", "submitted_at"],
}


def validate_lms_schema(db) -> List[str]:
    """
    Check that every table/column this collector's SQL depends on exists
    in the connected LMS database. Returns human-readable warning strings
    (empty list = healthy). NEVER raises — on any error it returns a
    single "could not run" warning so startup is never blocked.
    """
    try:
        rows = db.execute(text("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
        """)).all()

        live: Dict[str, set] = {}
        for r in rows:
            live.setdefault(str(r[0]).lower(), set()).add(str(r[1]).lower())

        warnings: List[str] = []
        if not live:
            return ["schema validation could not run: information_schema "
                    "returned no columns for the current database"]

        for table, needed in _SCHEMA_CONTRACT.items():
            live_cols = live.get(table)
            if live_cols is None:
                warnings.append(
                    f"missing table `{table}` — its profile section will "
                    f"render empty")
                continue
            missing = [c for c in needed if c.lower() not in live_cols]
            if missing:
                warnings.append(
                    f"table `{table}` is missing column(s): "
                    f"{', '.join(missing)} — queries against it may fail")
        return warnings
    except Exception as e:
        return [f"schema validation could not run: {e}"]