"""
ProfileIQ Data Collector — v6.2 (ASSESSMENTS ADDED)
═══════════════════════════════════════════════════════════════════════════

Full replacement of v6.1 with one addition and one clarification.

CHANGES FROM v6.1
──────────────────
  1. NEW method: _get_assessments() — reads quiz_attempts joined to quizzes.
     Faculty-uploaded quizzes ARE the "Assessments" tab on the LMS.
     (pf_assessments = Pathfinder AI career-guidance = deliberately excluded)
  2. asyncio.gather() now has 12 tasks (was 11). Unpacking updated to match.
  3. Snapshot is now 8 axes:
       1. Assignments  2. Case Studies  3. Capstones
       4. Industry     5. Mock Test     6. Mock Interview
       7. Assessments  8. Punctuality
  4. Legacy alias `test_scores` now routes to the assessments list
     (renderer's _compute_perf_snapshot line 168 reads test_scores for
     the "assessment" axis — this naming alignment makes it work
     without any renderer change).

STILL CORRECT FROM v6.1
────────────────────────
  - All activity tables join on users.id via `student_id` column
    (except student_attendance which is students.id)
  - Section keys hold FLAT LISTS (template does its own dedup + top-N)
  - Legacy aliases: capstone_projects, industry_interactions (for renderer)
  - Punctuality returns None-shape when table missing (graceful N/A)
  - Never references the dropped `visibility` column
  - Empty state is [] or None — never NaN or 0.00 sentinel

PERFORMANCE SNAPSHOT DESIGN (unchanged)
────────────────────────────────────────
  Axis score = mean of `score` across all items in that section's list.
  Fair average = mean of axes with real activity (score > 0), N/A excluded.
  Attendance folds into Punctuality (per the LMS-side Punctuality Score
  design, not as its own snapshot axis).

Author: Phase 0 data-flow rewire — v6.2
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
    Assembles all data needed to render a student's profile.

    Reads only. Never writes. Every method is best-effort — a query
    failure logs and returns an empty list rather than crashing
    collect_all(). Every activity method expects users.id as user_id.
    """

    def __init__(self, db: Session):
        self.db = db

    # =====================================================================
    # PUBLIC ENTRY POINT
    # =====================================================================

    async def collect_all(self, user_id: int) -> Dict[str, Any]:
        """
        Assemble the full data payload for one student.
        `user_id` is users.id — the authenticated student's id.
        """
        try:
            personal    = await self._get_personal(user_id)
            students_id = await self._get_students_id(user_id)

            # 12 concurrent reads — assessments is now #12
            results = await asyncio.gather(
                self._get_courses(user_id),             # 0
                self._get_case_studies(user_id),        # 1
                self._get_assignments(user_id),         # 2
                self._get_capstones(user_id),           # 3
                self._get_industry_sessions(user_id),   # 4
                self._get_mock_tests(user_id),          # 5
                self._get_mock_interviews(user_id),     # 6
                self._get_attendance(students_id),      # 7
                self._get_punctuality(user_id),         # 8
                self._get_psychometric(user_id),        # 9
                self._get_certifications(user_id),      # 10
                self._get_assessments(user_id),         # 11 — NEW
                return_exceptions=True,
            )

            (courses, case_studies, assignments, capstones,
             industry_sessions, mock_tests, mock_interviews,
             attendance, punctuality, psycho, certifications,
             assessments) = [
                (r if not isinstance(r, Exception) else self._safe_default_list(r))
                for r in results
            ]

            # Build the 8-axis Performance Snapshot from the flat lists
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
                "case_studies":          case_studies,       # LIST[dict]
                "assignments":           assignments,        # LIST[dict]
                "capstones":             capstones,          # LIST[dict]
                "capstone_projects":     capstones,          # LEGACY ALIAS (renderer)
                "industry_sessions":     industry_sessions,  # LIST[dict]
                "industry_interactions": industry_sessions,  # LEGACY ALIAS (renderer)
                "mock_tests":            mock_tests,         # LIST[dict]
                "mock_interviews":       mock_interviews,    # LIST[dict]
                "assessments":           assessments,        # NEW — LIST[dict]
                "test_scores":           assessments,        # LEGACY ALIAS — renderer's
                                                             # "assessment" axis reads this
                "attendance":            attendance,         # dict summary
                "punctuality":           punctuality,        # dict summary
                "personality":           psycho,             # dict
                "certifications":        certifications,     # LIST[dict]
                "performance_snapshot":  snapshot,           # 8-axis dict
                "batch_info":            {},
                "computed": {
                    "user_id":            user_id,
                    "students_id":        students_id,
                    "total_case_studies": len(case_studies),
                    "total_assignments":  len(assignments),
                    "total_capstones":    len(capstones),
                    "total_industry":     len(industry_sessions),
                    "total_mock_tests":   len(mock_tests),
                    "total_interviews":   len(mock_interviews),
                    "total_assessments":  len(assessments),
                },
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
                "about_me":              r.get("bio") or "",   # legacy alias
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

    async def _get_students_id(self, user_id: int) -> Optional[int]:
        """users.id → students.id lookup, for attendance queries only."""
        try:
            row = self.db.execute(text("""
                SELECT id FROM students WHERE user_id = :uid LIMIT 1
            """), {"uid": user_id}).first()
            return row[0] if row else None
        except Exception as e:
            logger.warning(f"_get_students_id failed for user_id={user_id}: {e}")
            return None

    async def _get_courses(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            rows = self.db.execute(text("""
                SELECT
                    e.id           AS enrollment_id,
                    e.course_id,
                    e.status,
                    e.enrolled_at,
                    c.name         AS course_name,
                    c.description  AS description,
                    c.category     AS category
                FROM enrollments e
                LEFT JOIN courses c ON c.id = e.course_id
                WHERE e.student_id = :uid
                ORDER BY e.enrolled_at DESC
            """), {"uid": user_id}).mappings().all()
            return [
                {
                    "enrollment_id": r["enrollment_id"],
                    "course_id":     r["course_id"],
                    "course_name":   r.get("course_name") or "",
                    "description":   r.get("description") or "",
                    "category":      r.get("category") or "",
                    "status":        r.get("status") or "",
                    "enrolled_at":   str(r.get("enrolled_at") or ""),
                }
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"_get_courses failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 1 — CASE STUDIES (flat list)
    # =====================================================================

    async def _get_case_studies(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            rows = self.db.execute(text("""
                SELECT
                    css.id           AS submission_id,
                    css.case_study_id,
                    css.grade,
                    css.status,
                    css.rubric_scores,
                    css.feedback,
                    css.submitted_at,
                    css.reviewed_at,
                    cs.title         AS case_title,
                    cs.description   AS case_brief,
                    cs.course_id,
                    co.name          AS course_name
                FROM case_study_submissions css
                LEFT JOIN case_studies cs ON cs.id = css.case_study_id
                LEFT JOIN courses    co ON co.id = cs.course_id
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
    # ACTIVITY FLOW 2 — ASSIGNMENTS (flat list)
    # =====================================================================

    async def _get_assignments(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            rows = self.db.execute(text("""
                SELECT
                    asub.id           AS submission_id,
                    asub.assignment_id,
                    asub.grade,
                    asub.status,
                    asub.feedback,
                    asub.submitted_at,
                    a.title           AS assignment_title,
                    a.description     AS assignment_brief,
                    a.course_id,
                    co.name           AS course_name
                FROM assignment_submissions asub
                LEFT JOIN assignments a ON a.id = asub.assignment_id
                LEFT JOIN courses    co ON co.id = a.course_id
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
    # ACTIVITY FLOW 3 — CAPSTONES (flat list)
    # =====================================================================

    async def _get_capstones(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            rows = self.db.execute(text("""
                SELECT
                    c.id, c.title, c.description, c.course_id,
                    c.due_date, c.total_marks, c.status,
                    c.grade, c.feedback,
                    c.submitted_at, c.reviewed_at,
                    co.name AS course_name
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
    # ACTIVITY FLOW 4 — INDUSTRY SESSIONS (flat list)
    # =====================================================================

    async def _get_industry_sessions(self, user_id: int) -> List[Dict[str, Any]]:
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
                    ise.speaker_name      AS speaker
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
                    "insight":       r.get("insight_text") or "",
                    "insight_text":  r.get("insight_text") or "",
                    "held_at":       str(r.get("submitted_at") or ""),
                    "duration":      "",
                    "submitted_at":  str(r.get("submitted_at") or ""),
                })
            return items
        except Exception as e:
            logger.warning(f"_get_industry_sessions failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 5 — MOCK TESTS (TestGen) (flat list)
    # =====================================================================

    async def _get_mock_tests(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            rows = self.db.execute(text("""
                SELECT
                    id, test_id, topic, difficulty,
                    total_questions, correct_answers,
                    score_percentage, performance_band,
                    created_at, completed_at
                FROM test_history
                WHERE student_id = :uid
                ORDER BY score_percentage DESC, completed_at DESC
            """), {"uid": user_id}).mappings().all()

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
                    "attempted_at":    str(r.get("completed_at") or r.get("created_at") or ""),
                    "submitted_at":    str(r.get("completed_at") or r.get("created_at") or ""),
                    "course_name":     "",
                })
            return items
        except Exception as e:
            logger.warning(f"_get_mock_tests failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 6 — MOCK INTERVIEWS (flat list)
    # =====================================================================

    async def _get_mock_interviews(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            rows = self.db.execute(text("""
                SELECT
                    id, user_id, session_type, role, status,
                    created_at, completed_at, ended_at
                FROM vyom_sessions
                WHERE user_id = :uid
                ORDER BY created_at DESC
            """), {"uid": user_id}).mappings().all()

            items = []
            for row in rows:
                sess = dict(row)
                sess_id = sess["id"]
                agg = self.db.execute(text("""
                    SELECT AVG(rating) AS avg_rating, COUNT(*) AS answer_count
                    FROM vyom_answer_ratings
                    WHERE session_id = :sid
                """), {"sid": sess_id}).mappings().first()

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
                    "title":        (sess.get("role") or sess.get("session_type") or "Mock Interview"),
                    "session_type": sess.get("session_type") or "",
                    "role":         sess.get("role") or "",
                    "status":       sess.get("status") or "",
                    "answer_count": cnt,
                    "score":        score,
                    "percentage":   score,
                    "max_score":    100,
                    "completed_at": str(sess.get("completed_at") or sess.get("ended_at") or ""),
                    "created_at":   str(sess.get("created_at") or ""),
                })
            items.sort(key=lambda x: (x["score"] or 0), reverse=True)
            return items
        except Exception as e:
            logger.warning(f"_get_mock_interviews failed for user_id={user_id}: {e}")
            return []

    # =====================================================================
    # ACTIVITY FLOW 7 — ASSESSMENTS (NEW — faculty-uploaded quizzes)
    # =====================================================================

    async def _get_assessments(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Faculty-uploaded assessments live in the `quizzes` table.
        Student attempts live in `quiz_attempts`.

          quizzes:        id, course_id, title, description, pass_percentage,
                          time_limit_minutes, is_active
          quiz_attempts:  id, quiz_id, student_id, score, total_marks,
                          passed, submitted_at, time_taken_seconds

        NOT to be confused with pf_assessments (Pathfinder = AI career-
        guidance, deliberately excluded from ProfileIQ).

        student_id in quiz_attempts stores users.id (consistent with all
        other activity tables in this schema).
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
                    co.name         AS course_name
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
                    "score":          pct,             # normalized 0-100 for snapshot
                    "percentage":     pct,
                    "raw_score":      float(score) if score is not None else None,
                    "total_marks":    float(total) if total else None,
                    "max_score":      100,
                    "grade":          self._grade_letter(pct),
                    "ai_grade_label": self._grade_label(pct),
                    "passed":         bool(r.get("passed")),
                    "pass_percentage":r.get("pass_percentage") or 60,
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
    # ATTENDANCE (uses students.id — folds into Punctuality, not its own axis)
    # =====================================================================

    async def _get_attendance(self, students_id: Optional[int]) -> Dict[str, Any]:
        if students_id is None:
            return self._empty_attendance()
        try:
            row = self.db.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'present' THEN 1 ELSE 0 END) AS present
                FROM student_attendance
                WHERE student_id = :sid
            """), {"sid": students_id}).mappings().first()

            total = int((row["total"] or 0)) if row else 0
            present = int((row["present"] or 0)) if row else 0
            pct = round(present / total * 100, 2) if total > 0 else None

            recent = self.db.execute(text("""
                SELECT session_id, status, marked_at
                FROM student_attendance
                WHERE student_id = :sid
                ORDER BY marked_at DESC
                LIMIT 10
            """), {"sid": students_id}).mappings().all()

            return {
                "sessions_attended":  present,
                "sessions_total":     total,
                "attendance_percent": pct,
                "recent_sessions":    [dict(r) for r in recent],
            }
        except Exception as e:
            logger.warning(f"_get_attendance failed for students_id={students_id}: {e}")
            return self._empty_attendance()

    # =====================================================================
    # PUNCTUALITY (8th axis)
    # =====================================================================

    async def _get_punctuality(self, user_id: int) -> Dict[str, Any]:
        """
        Reads from the punctuality_scores table (LMS-side cron output).
        Returns None-shaped dict if the table doesn't exist yet — snapshot
        treats as N/A. When the cron ships, this axis lights up automatically.

        Attendance is one component INSIDE Punctuality per the design.
        We don't count attendance as a separate axis.
        """
        try:
            row = self.db.execute(text("""
                SELECT
                    student_id,
                    score,
                    band,
                    events_counted,
                    computed_at
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
    # PSYCHOMETRIC (Beyond Work section)
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
    # PERFORMANCE SNAPSHOT — 8 axes with FAIR AVERAGE
    # =====================================================================

    def _compute_snapshot(self, **sections) -> Dict[str, Any]:
        """
        Build the 8-axis snapshot:
          1. Assignments
          2. Case Studies
          3. Capstones
          4. Industry Sessions
          5. Mock Test
          6. Mock Interview
          7. Assessments
          8. Punctuality (N/A until LMS cron ships)

        Axis score = mean of `score` across items in that section's list.
        Fair average = mean of axes with real activity (score > 0); N/A axes
        excluded. Attendance folds into Punctuality per the LMS-side design.
        """
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

        punct = sections.get("punctuality") or {}
        axes = {
            "assignments":       _axis_avg(sections.get("assignments")       or []),
            "case_studies":      _axis_avg(sections.get("case_studies")      or []),
            "capstones":         _axis_avg(sections.get("capstones")         or []),
            "industry_sessions": _axis_avg(sections.get("industry_sessions") or []),
            "mock_tests":        _axis_avg(sections.get("mock_tests")        or []),
            "mock_interviews":   _axis_avg(sections.get("mock_interviews")   or []),
            "assessments":       _axis_avg(sections.get("assessments")       or []),
            "punctuality":       punct.get("score") if punct.get("score") is not None else None,
        }

        # Fair average: only axes with score > 0
        active_values = [v for v in axes.values() if v is not None and v > 0]
        fair_avg = round(sum(active_values) / len(active_values), 1) if active_values else None

        return {
            "axes":              axes,
            "active_axis_count": len(active_values),
            "fair_average":      fair_avg,
            "average":           fair_avg,   # legacy alias for template
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
                "students_id":        None,
                "total_case_studies": 0,
                "total_assignments":  0,
                "total_capstones":    0,
                "total_industry":     0,
                "total_mock_tests":   0,
                "total_interviews":   0,
                "total_assessments":  0,
            },
        }

    def _safe_default_list(self, exc):
        """Return an empty list when a section-collector task raises."""
        logger.warning(f"Collector task failed: {exc}")
        return []