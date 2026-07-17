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
            #               punctuality, psychometric, certifications
            #   students.id → enrollments (courses), case_study_submissions,
            #               assignment_submissions, quiz_attempts (assessments),
            #               industry_session_submissions
            # NOT hardcoded — _resolve_students_id maps EACH user to their own
            # students.id. Falls back to user_id if no students row (query then
            # simply returns nothing, gracefully).
            students_id = self._resolve_students_id(user_id)
            sid = students_id if students_id is not None else user_id

            # 11 concurrent reads
            results = await asyncio.gather(
                self._get_courses(user_id),             # 0  (resolves students.id itself)
                self._get_case_studies(sid),            # 1  students.id
                self._get_assignments(sid),             # 2  students.id
                self._get_capstones(user_id),           # 3  users.id
                self._get_industry_sessions(sid),       # 4  students.id
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

    async def _get_personal(self, user_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(text("""
                SELECT
                    id, full_name, email, phone,
                    profile_photo, bio, gender, city, state, country,
                    current_designation, current_employer,
                    work_experience_years, employment_type,
                    education_level, edu_institution, institution,
                    field_of_study, graduation_year, edu_year,
                    key_skills, career_goals, preferred_role,
                    preferred_location, notice_period, open_to_relocation,
                    preferred_salary_min, preferred_salary_max, work_mode,
                    linkedin, github, portfolio, twitter, website,
                    resume_url, resume_name,
                    languages_known, hobbies, industries
                FROM users
                WHERE id = :uid
                LIMIT 1
            """), {"uid": user_id}).mappings().first()

            if not row:
                return self._empty_personal(user_id)

            r = dict(row)
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
                "employment_type":       r.get("employment_type") or "",
                "education_level":       r.get("education_level") or "",
                "institution":           r.get("edu_institution") or r.get("institution") or "",
                "field_of_study":        r.get("field_of_study") or "",
                "graduation_year":       r.get("graduation_year") or r.get("edu_year") or "",
                "key_skills":            r.get("key_skills") or "",
                "career_goals":          r.get("career_goals") or "",
                "preferred_role":        r.get("preferred_role") or "",
                "preferred_location":    r.get("preferred_location") or "",
                "notice_period":         r.get("notice_period") or "",
                "open_to_relocation":    r.get("open_to_relocation") or "",
                "preferred_salary_min":  r.get("preferred_salary_min") or "",
                "preferred_salary_max":  r.get("preferred_salary_max") or "",
                "work_mode":             r.get("work_mode") or "",
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

    async def _get_courses(self, user_id: int) -> List[Dict[str, Any]]:
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
            sid = self._resolve_students_id(user_id)
            if sid is None:
                logger.warning(
                    f"_get_courses: no students-table row for user_id={user_id}; "
                    f"cannot read enrollments (convention: students.id)")
                return []
            rows = self._query_courses(sid)
            return [
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
                }
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"_get_courses failed for user_id={user_id}: {e}")
            return []

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
                items.append({
                    "submission_id":  r["submission_id"],
                    "case_study_id":  r["case_study_id"],
                    "title":          r.get("case_title") or "Case Study",
                    "brief":          r.get("case_brief") or "",
                    "score":          float(grade) if grade is not None else None,
                    "ai_score":       float(grade) if grade is not None else None,
                    "faculty_score":  float(grade) if grade is not None else None,
                    "max_score":      100,
                    "ai_grade":       self._grade_letter(grade),
                    "ai_grade_label": self._grade_label(grade),
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
                    co.course_name        AS course_name
                FROM assignment_submissions asub
                LEFT JOIN assignments  a ON a.id = asub.assignment_id
                LEFT JOIN courses      co ON co.id = a.course_id
                WHERE asub.student_id = :uid
                ORDER BY asub.grade DESC, asub.submitted_at DESC
            """), {"uid": user_id}).mappings().all()

            items = []
            for r in rows:
                grade = r.get("grade")
                items.append({
                    "submission_id":   r["submission_id"],
                    "assignment_id":   r["assignment_id"],
                    "title":           r.get("assignment_title") or "Assignment",
                    "brief":           r.get("assignment_brief") or "",
                    "score":           float(grade) if grade is not None else None,
                    "ai_score":        float(grade) if grade is not None else None,
                    "faculty_score":   float(grade) if grade is not None else None,
                    "rubric_pct":      float(grade) if grade is not None else None,
                    "max_score":       100,
                    "ai_grade_label":  self._grade_label(grade),
                    "rubric_grade":    self._grade_letter(grade),
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
        industry_sessions columns (from result 13, DESCRIBE):
          id, title, description, speaker (not speaker_name), course_id,
          date, duration, type, status, company, mentor_name, key_topics
        industry_session_submissions columns (from result 14):
          id, session_id, student_id, score, band, grade, insight_text,
          feedback_json, attempt_number, has_feedback, submitted_at,
          reviewed_at, file_url, file_name
        """
        try:
            rows = self.db.execute(text("""
                SELECT
                    iss.id                AS submission_id,
                    iss.session_id,
                    iss.score,
                    iss.band,
                    iss.grade,
                    iss.insight_text,
                    iss.feedback_json,
                    iss.has_feedback,
                    iss.attempt_number,
                    iss.submitted_at,
                    iss.reviewed_at,
                    ise.title             AS session_title,
                    ise.description       AS session_brief,
                    ise.speaker           AS speaker,
                    ise.company           AS company,
                    ise.type              AS session_type,
                    ise.duration          AS duration
                FROM industry_session_submissions iss
                LEFT JOIN industry_sessions ise ON ise.id = iss.session_id
                WHERE iss.student_id = :uid
                ORDER BY iss.score DESC, iss.submitted_at DESC
            """), {"uid": user_id}).mappings().all()

            items = []
            for r in rows:
                score = r.get("score")
                items.append({
                    "submission_id": r["submission_id"],
                    "session_id":    r["session_id"],
                    "title":         r.get("session_title") or "Industry Session",
                    "brief":         r.get("session_brief") or "",
                    "score":         float(score) if score is not None else None,
                    "band":          r.get("band") or "",
                    "grade":         r.get("grade") or "",
                    "speaker":       r.get("speaker") or "",
                    "company":       r.get("company") or "",
                    "type":          r.get("session_type") or "",
                    "insight":       r.get("insight_text") or "",
                    "insight_text":  r.get("insight_text") or "",
                    "held_at":       str(r.get("submitted_at") or ""),
                    "duration":      r.get("duration") or "",
                    "submitted_at":  str(r.get("submitted_at") or ""),
                })
            return items
        except Exception as e:
            logger.warning(f"_get_industry_sessions failed for user_id={user_id}: {e}")
            return []

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
                    student_id, score, band, events_counted, computed_at
                FROM punctuality_scores
                WHERE student_id = :uid
                ORDER BY computed_at DESC
                LIMIT 1
            """), {"uid": user_id}).mappings().first()

            if not row:
                return {"score": None, "band": None, "events_counted": 0, "status": "no_data"}

            return {
                "score":          float(row["score"]) if row["score"] is not None else None,
                "band":           row.get("band") or "",
                "events_counted": int(row.get("events_counted") or 0),
                "computed_at":    str(row.get("computed_at") or ""),
                "status":         "available",
            }
        except Exception as e:
            logger.info(f"_get_punctuality: unavailable for user_id={user_id} ({e})")
            return {"score": None, "band": None, "events_counted": 0, "status": "table_missing"}

    # =====================================================================
    # PSYCHOMETRIC
    # =====================================================================

    async def _get_psychometric(self, user_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(text("""
                SELECT psycho_result FROM users WHERE id = :uid LIMIT 1
            """), {"uid": user_id}).first()
            if not row or not row[0]:
                return {"personality_type": None, "traits": [], "summary": "", "status": "no_data"}
            raw = row[0]
            data = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(data, dict):
                data = {}
            return {
                "personality_type": data.get("type") or data.get("mbti") or "",
                "traits":           data.get("traits") or [],
                "strengths":        data.get("strengths") or [],
                "summary":          data.get("summary") or "",
                "status":           "available",
            }
        except Exception as e:
            logger.warning(f"_get_psychometric failed for user_id={user_id}: {e}")
            return {"personality_type": None, "traits": [], "summary": "", "status": "no_data"}

    # =====================================================================
    # CERTIFICATIONS
    # =====================================================================

    async def _get_certifications(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            row = self.db.execute(text("""
                SELECT certifications FROM users WHERE id = :uid LIMIT 1
            """), {"uid": user_id}).first()
            if not row or not row[0]:
                return []
            raw = row[0]
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        return [
                            (c if isinstance(c, dict) else {"name": str(c), "certificate_name": str(c)})
                            for c in parsed
                        ]
                except Exception:
                    return [
                        {"name": c.strip(), "certificate_name": c.strip()}
                        for c in raw.split(",") if c.strip()
                    ]
            return []
        except Exception as e:
            logger.warning(f"_get_certifications failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ASSESSMENTS (faculty-uploaded quizzes)
    # =====================================================================

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