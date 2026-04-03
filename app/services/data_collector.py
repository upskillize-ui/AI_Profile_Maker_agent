"""
Data Collector Service — REVISED
═════════════════════════════════
Changes:
  - Fixed _get_personality(): was always returning 'default', now reads real data
  - Added _derive_personality(): derives traits from actual quiz/course behavior
  - Removed hardcoded "Analytical Strategist" as universal default
"""

from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, List
from decimal import Decimal
from datetime import date, datetime
import logging
import json

logger = logging.getLogger(__name__)


def clean_data(obj):
    """Recursively convert Decimal, date, datetime to JSON-safe types."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: clean_data(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_data(i) for i in obj]
    if isinstance(obj, tuple):
        return tuple(clean_data(i) for i in obj)
    return obj


class DataCollector:
    """Collects and aggregates all student data from the LMS database."""

    def __init__(self, db: Session):
        self.db = db

    async def collect_all(self, student_id: int) -> Dict[str, Any]:
        # student_id = users.id. But enrollments/quizzes use students.id
        sid_row = self.db.execute(
            text("SELECT id FROM students WHERE user_id = :uid LIMIT 1"),
            {"uid": student_id},
        ).mappings().first()
        stu_id = sid_row["id"] if sid_row else student_id
        logger.info(f"Mapped user_id={student_id} -> students.id={stu_id}")

        personal = self._get_personal_info(student_id)
        courses = self._get_courses(stu_id)
        test_scores = self._get_test_scores(stu_id)
        case_studies = self._get_case_studies(stu_id)
        assignments = self._get_assignments(stu_id)
        quiz_scores = self._get_quiz_scores(stu_id)
        personality = self._get_personality(student_id)
        platform_activity = self._get_platform_activity(stu_id)
        forum_activity = self._get_forum_activity(student_id)
        batch_info = self._get_batch_info(stu_id)

        computed = self._compute_metrics(
            test_scores=test_scores,
            case_studies=case_studies,
            assignments=assignments,
            quiz_scores=quiz_scores,
            courses=courses,
            platform_activity=platform_activity,
            forum_activity=forum_activity,
        )

        result = {
            "student_id": student_id,
            "personal": personal,
            "courses": courses,
            "test_scores": test_scores,
            "case_studies": case_studies,
            "assignments": assignments,
            "quiz_scores": quiz_scores,
            "projects": [],
            "certifications": [],
            "personality": personality,
            "platform_activity": platform_activity,
            "forum_activity": forum_activity,
            "batch_info": batch_info,
            "computed": computed,
        }
        return clean_data(result)

    # ─── Personal Info ───────────────────────────────────

    def _get_personal_info(self, student_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(
                text("""
                    SELECT
                        u.id, u.full_name, u.email, u.phone,
                        u.profile_photo AS photo_url,
                        s.city, s.state, s.country,
                        s.date_of_birth, s.enrollment_number, s.batch_id
                    FROM users u
                    LEFT JOIN students s ON s.user_id = u.id
                    WHERE u.id = :sid
                    LIMIT 1
                """),
                {"sid": student_id},
            ).mappings().first()

            if not row:
                return {"full_name": "Student", "email": ""}

            d = dict(row)
            parts = (d.get("full_name") or "Student").split(" ", 1)
            d["first_name"] = parts[0]
            d["last_name"] = parts[1] if len(parts) > 1 else ""
            return clean_data(d)
        except Exception as e:
            logger.warning(f"personal_info failed (student {student_id}): {e}")
            return {"first_name": "Student", "last_name": "", "email": ""}

    # ─── Courses & Enrollments ───────────────────────────

    def _get_courses(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(
                text("""
                    SELECT
                        c.id AS course_id, c.course_name, c.category,
                        c.difficulty_level,
                        e.progress_percentage, e.completion_status,
                        e.completed_at, e.created_at AS enrolled_at,
                        (SELECT COUNT(*) FROM course_modules cm WHERE cm.course_id = c.id) AS total_modules
                    FROM enrollments e
                    JOIN courses c ON c.id = e.course_id
                    WHERE e.student_id = :sid
                    ORDER BY e.created_at
                """),
                {"sid": student_id},
            ).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.warning(f"courses failed: {e}")
            return []

    # ─── Exam/Test Scores ────────────────────────────────

    def _get_test_scores(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(
                text("""
                    SELECT
                        r.id, r.score, r.total_marks, r.percentage,
                        r.grade, r.time_taken_minutes, r.submitted_at,
                        e.exam_name AS subject, e.exam_type AS topic,
                        c.course_name AS course_name
                    FROM results r
                    JOIN exams e ON e.id = r.exam_id
                    LEFT JOIN courses c ON c.id = e.course_id
                    WHERE r.student_id = :sid
                    ORDER BY r.submitted_at
                """),
                {"sid": student_id},
            ).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.warning(f"test_scores failed: {e}")
            return []

    # ─── Case Studies ────────────────────────────────────

    def _get_case_studies(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(
                text("""
                    SELECT
                        css.id AS submission_id, csd.title, csd.key_concepts,
                        csd.max_score, css.ai_score AS score, css.ai_grade,
                        css.ai_feedback, css.ai_rubric_scores,
                        css.ai_strengths, css.ai_improvements,
                        css.ai_missing_concepts, css.attempt_number,
                        css.word_count, css.submitted_at, css.status,
                        c.course_name
                    FROM case_study_submissions css
                    JOIN case_studies csd ON csd.id = css.case_study_id
                    LEFT JOIN courses c ON c.id = csd.course_id
                    WHERE css.student_id = :sid
                      AND css.status IN ('graded', 'mentor_reviewed')
                    ORDER BY css.ai_score DESC
                """),
                {"sid": student_id},
            ).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.warning(f"case_studies failed: {e}")
            return []

    # ─── Assignments ─────────────────────────────────────

    def _get_assignments(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(
                text("""
                    SELECT
                        asub.id, asub.grade AS score, asub.feedback,
                        asub.status, asub.submitted_at, a.title,
                        a.total_marks AS max_score, a.course_id,
                        c.course_name
                    FROM assignment_submissions asub
                    JOIN assignments a ON a.id = asub.assignment_id
                    LEFT JOIN courses c ON c.id = a.course_id
                    WHERE asub.student_id = :sid
                    ORDER BY asub.submitted_at
                """),
                {"sid": student_id},
            ).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.warning(f"assignments failed: {e}")
            return []

    # ─── Quiz Scores ─────────────────────────────────────

    def _get_quiz_scores(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(
                text("""
                    SELECT
                        qa.id, qa.score, qa.total_marks,
                        qa.passed, qa.time_taken_seconds, qa.submitted_at,
                        q.title AS quiz_title, c.course_name
                    FROM quiz_attempts qa
                    JOIN quizzes q ON q.id = qa.quiz_id
                    LEFT JOIN courses c ON c.id = q.course_id
                    WHERE qa.student_id = :sid
                    ORDER BY qa.submitted_at
                """),
                {"sid": student_id},
            ).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.warning(f"quiz_scores failed: {e}")
            return []

    # ─── Personality — FIXED ─────────────────────────────
    # OLD CODE: Always returned hardcoded "default" string
    # NEW CODE: Tries to read real psycho_result, falls back to behavior-derived traits

    def _get_personality(self, student_id: int) -> Dict[str, Any]:
        try:
            # First: check if users table has psycho_result column with real data
            row = self.db.execute(
                text("""
                    SELECT psycho_result FROM users WHERE id = :sid LIMIT 1
                """),
                {"sid": student_id},
            ).mappings().first()

            if row and row.get("psycho_result") and row["psycho_result"] != "default":
                raw = row["psycho_result"]
                try:
                    data = json.loads(raw) if isinstance(raw, str) else raw
                    if isinstance(data, dict) and data.get("personality_type"):
                        return {
                            "personality_type": data.get("personality_type", ""),
                            "traits_json": data.get("traits", ""),
                            "work_style": data.get("work_style", ""),
                            "communication_profile": data.get("communication", ""),
                            "leadership_indicators": data.get("leadership", ""),
                        }
                except (json.JSONDecodeError, TypeError):
                    pass

            # Column doesn't exist or has no real data — derive from behavior
            return self._derive_personality(student_id)

        except Exception as e:
            # psycho_result column might not exist at all
            logger.info(f"psycho_result not available, deriving personality: {e}")
            return self._derive_personality(student_id)

    def _derive_personality(self, student_id: int) -> dict:
        """Derive personality traits from ACTUAL student behavior on the platform."""
        try:
            quiz_row = self.db.execute(
                text("SELECT COUNT(*) AS cnt FROM quiz_attempts WHERE student_id = :sid"),
                {"sid": student_id},
            ).mappings().first()

            course_row = self.db.execute(
                text("SELECT COUNT(*) AS cnt FROM enrollments WHERE student_id = :sid"),
                {"sid": student_id},
            ).mappings().first()

            case_row = self.db.execute(
                text("""SELECT COUNT(*) AS cnt FROM case_study_submissions
                        WHERE student_id = :sid AND status IN ('graded','mentor_reviewed')"""),
                {"sid": student_id},
            ).mappings().first()

            quizzes = int(quiz_row["cnt"]) if quiz_row else 0
            courses = int(course_row["cnt"]) if course_row else 0
            cases = int(case_row["cnt"]) if case_row else 0

            total_activity = quizzes + cases

            # Derive personality type from actual engagement level
            if total_activity >= 15:
                ptype = "Strategic Achiever"
                traits = "High-performer, Assessment-driven, Detail-oriented"
                work_style = "Structured and goal-oriented"
            elif total_activity >= 8:
                ptype = "Analytical Strategist"
                traits = "Methodical, Self-motivated, Consistent"
                work_style = "Structured and methodical"
            elif total_activity >= 3:
                ptype = "Active Learner"
                traits = "Curious, Engaged, Growing"
                work_style = "Self-paced with regular engagement"
            elif total_activity >= 1:
                ptype = "Curious Explorer"
                traits = "Self-initiated, Building foundations"
                work_style = "Self-paced learning"
            else:
                ptype = "Getting Started"
                traits = "Enrolled and ready to learn"
                work_style = "Self-paced"

            return {
                "personality_type": ptype,
                "traits_json": traits,
                "work_style": work_style,
                "communication_profile": "Clear and concise",
                "leadership_indicators": "Collaborative" if cases >= 2 else "Individual contributor",
            }
        except Exception as e:
            logger.warning(f"derive_personality failed: {e}")
            return {
                "personality_type": "",
                "traits_json": "",
                "work_style": "",
                "communication_profile": "",
                "leadership_indicators": "",
            }

    # ─── Platform Activity ───────────────────────────────

    def _get_platform_activity(self, student_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(
                text("""
                    SELECT
                        COALESCE(SUM(total_watch_time), 0) AS total_watch_seconds,
                        COUNT(DISTINCT lesson_id) AS lessons_watched,
                        COUNT(DISTINCT DATE(last_watched_at)) AS active_days,
                        MIN(created_at) AS first_activity,
                        MAX(last_watched_at) AS last_activity
                    FROM video_watch_history
                    WHERE student_id = :sid
                """),
                {"sid": student_id},
            ).mappings().first()

            d = dict(row) if row else {}
            total_seconds = d.get("total_watch_seconds", 0) or 0
            d["total_minutes"] = round(float(total_seconds) / 60, 1)
            return clean_data(d)
        except Exception as e:
            logger.warning(f"platform_activity failed: {e}")
            return {"total_minutes": 0, "active_days": 0, "lessons_watched": 0}

    # ─── Forum Activity ──────────────────────────────────

    def _get_forum_activity(self, student_id: int) -> Dict[str, Any]:
        try:
            threads = self.db.execute(
                text("SELECT COUNT(*) AS cnt FROM forum_threads WHERE author_id = :sid"),
                {"sid": student_id},
            ).mappings().first()

            replies = self.db.execute(
                text("SELECT COUNT(*) AS cnt FROM forum_replies WHERE author_id = :sid"),
                {"sid": student_id},
            ).mappings().first()

            answers = self.db.execute(
                text("SELECT COUNT(*) AS cnt FROM forum_replies WHERE author_id = :sid AND is_answer = 1"),
                {"sid": student_id},
            ).mappings().first()

            return {
                "threads_created": int(threads["cnt"]) if threads else 0,
                "replies_given": int(replies["cnt"]) if replies else 0,
                "answers_accepted": int(answers["cnt"]) if answers else 0,
            }
        except Exception as e:
            logger.warning(f"forum_activity failed: {e}")
            return {"threads_created": 0, "replies_given": 0, "answers_accepted": 0}

    # ─── Batch Info ──────────────────────────────────────

    def _get_batch_info(self, student_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(
                text("""
                    SELECT b.name AS batch_name, b.start_date, b.end_date, b.status
                    FROM batch_students bs
                    JOIN batches b ON b.id = bs.batch_id
                    WHERE bs.student_id = :sid
                    ORDER BY b.id DESC LIMIT 1
                """),
                {"sid": student_id},
            ).mappings().first()
            return clean_data(dict(row)) if row else {}
        except Exception as e:
            logger.warning(f"batch_info failed: {e}")
            return {}

    # ─── Computed Metrics (unchanged) ────────────────────

    def _compute_metrics(self, **data) -> Dict[str, Any]:
        test_scores = data.get("test_scores", [])
        case_studies = data.get("case_studies", [])
        assignments = data.get("assignments", [])
        quiz_scores = data.get("quiz_scores", [])
        courses = data.get("courses", [])
        activity = data.get("platform_activity", {})
        forum = data.get("forum_activity", {})

        test_pcts = [
            float(t["percentage"]) for t in test_scores
            if t.get("percentage") and float(t.get("percentage", 0)) > 0
        ]
        best_test = max(test_pcts) if test_pcts else 0
        avg_test = round(sum(test_pcts) / len(test_pcts), 1) if test_pcts else 0

        subj_map: Dict[str, list] = {}
        for t in test_scores:
            s = t.get("subject") or t.get("course_name") or "General"
            pct = float(t.get("percentage", 0))
            if pct > 0:
                subj_map.setdefault(s, []).append(pct)
        subj_avgs = {s: round(sum(v) / len(v), 1) for s, v in subj_map.items()}
        top_subjects = sorted(subj_avgs.items(), key=lambda x: -x[1])

        case_scores = [
            float(c["score"]) for c in case_studies
            if c.get("score") and float(c.get("score", 0)) > 0
        ]
        avg_case = round(sum(case_scores) / len(case_scores), 1) if case_scores else 0
        best_case = max(case_scores) if case_scores else 0

        quiz_pcts = []
        for q in quiz_scores:
            tm = q.get("total_marks", 0) or 0
            sc = q.get("score", 0) or 0
            if int(tm) > 0:
                quiz_pcts.append(round(int(sc) / int(tm) * 100, 1))
        avg_quiz = round(sum(quiz_pcts) / len(quiz_pcts), 1) if quiz_pcts else 0

        all_pcts = test_pcts + quiz_pcts
        improvement = 0.0
        if len(all_pcts) >= 4:
            half = len(all_pcts) // 2
            first_half = sum(all_pcts[:half]) / half
            second_half = sum(all_pcts[half:]) / (len(all_pcts) - half)
            improvement = round(second_half - first_half, 1)

        completed_courses = [c for c in courses if c.get("completion_status") == "completed"]

        overall = round(
            (avg_test * 0.30) + (avg_case * 0.30) + (avg_quiz * 0.15) +
            (min(len(completed_courses), 10) * 2.5),
            1,
        )

        total_hours = round(float(activity.get("total_minutes", 0)) / 60, 1)

        if len(all_pcts) > 2:
            mean = sum(all_pcts) / len(all_pcts)
            std = (sum((x - mean) ** 2 for x in all_pcts) / len(all_pcts)) ** 0.5
            consistency = max(0, round(100 - std, 1))
        else:
            consistency = 85.0

        return {
            "overall_score": min(overall, 100),
            "best_test_score": best_test,
            "avg_test_score": avg_test,
            "total_tests": len(test_scores),
            "total_case_studies": len(case_studies),
            "avg_case_study_score": avg_case,
            "best_case_study_score": best_case,
            "total_assignments": len(assignments),
            "total_quizzes": len(quiz_scores),
            "avg_quiz_score": avg_quiz,
            "total_courses": len(courses),
            "completed_courses": len(completed_courses),
            "subject_averages": subj_avgs,
            "top_subjects": top_subjects,
            "improvement_pct": improvement,
            "total_hours": total_hours,
            "active_days": int(activity.get("active_days", 0) or 0),
            "lessons_watched": int(activity.get("lessons_watched", 0) or 0),
            "consistency_score": consistency,
            "forum_threads": int(forum.get("threads_created", 0) or 0),
            "forum_replies": int(forum.get("replies_given", 0) or 0),
            "forum_answers": int(forum.get("answers_accepted", 0) or 0),
        }
