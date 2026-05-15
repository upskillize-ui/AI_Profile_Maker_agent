"""
Data Collector Service — v6 COMPLETE
═════════════════════════════════════
ALL fixes applied:
  • Personality: direct JSON fallback when AI interpretation fails
  • Hobbies: wired from users.hobbies column → hobbies_list
  • Course descriptions: c.description added to SELECT
  • TestGen filter: brain_drill/practice tests excluded
  • Rubric JOIN: drops student_id check, broadens evaluation_type
  • Job preferences: already wired (was in v4)
  • Capstone: defensively tries multiple table names
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
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, datetime): return obj.isoformat()
    if isinstance(obj, date): return obj.isoformat()
    if isinstance(obj, dict): return {k: clean_data(v) for k, v in obj.items()}
    if isinstance(obj, list): return [clean_data(i) for i in obj]
    if isinstance(obj, tuple): return tuple(clean_data(i) for i in obj)
    return obj


class DataCollector:

    def __init__(self, db: Session):
        self.db = db

    async def collect_all(self, student_id: int) -> Dict[str, Any]:
        sid_row = self.db.execute(
            text("SELECT id FROM students WHERE user_id = :uid LIMIT 1"),
            {"uid": student_id},
        ).mappings().first()
        stu_id = sid_row["id"] if sid_row else student_id
        logger.info(f"Mapped user_id={student_id} -> students.id={stu_id}")

        # v12.6: one-shot schema discovery — logs the REAL table names that exist
        # for capstones / mock interviews / industry / tests so we stop guessing.
        # Read-only INFORMATION_SCHEMA query, runs once per profile generation.
        self._probe_schema(stu_id)

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
        capstone_projects = self._get_capstone_projects(stu_id)
        semester_results = self._get_semester_results(stu_id)
        job_preferences = self._get_job_preferences(student_id)

        # v12.4: real data for the perf tabs that were previously empty
        mock_tests        = self._get_mock_tests(stu_id)
        mock_interviews   = self._get_mock_interviews(stu_id)
        industry_sessions = self._get_industry_sessions(stu_id)
        hackathons        = self._get_hackathons(stu_id)

        computed = self._compute_metrics(
            test_scores=test_scores, case_studies=case_studies,
            assignments=assignments, quiz_scores=quiz_scores,
            courses=courses, platform_activity=platform_activity,
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
            "projects": self._get_projects(stu_id),
            "certifications": self._get_certifications(stu_id),
            "personality": personality,
            "platform_activity": platform_activity,
            "forum_activity": forum_activity,
            "batch_info": batch_info,
            "computed": computed,
            "capstone_projects": capstone_projects,
            "semester_results": semester_results,
            "job_preferences": job_preferences,
            "hobbies_data": personal.get("hobbies_list", []),
            "lms_education": personal.get("lms_education", []),
            "lms_work_experience": personal.get("lms_work_experience", []),
            # v12.4
            "mock_tests": mock_tests,
            "mock_interviews": mock_interviews,
            "industry_sessions": industry_sessions,
            "hackathons": hackathons,
        }
        return clean_data(result)

    # ─── v12.6 Schema Discovery ──────────────────────────
    # Runs once per profile generation. Logs the actual table names that exist
    # for the buckets we keep failing to query (capstones, mock interviews,
    # industry sessions, tests) plus the per-table foreign-key columns. The
    # idea is: instead of guessing table+FK combinations forever, dump what's
    # actually there and let the operator paste the log line back so v12.7's
    # SQL can be written against real tables.

    def _probe_schema(self, student_id: int) -> None:
        try:
            buckets = {
                "capstone":  ["%capstone%"],
                "mock_intv": ["%mock_interview%", "%interview_review%", "%mock_int%"],
                "industry":  ["%industry%", "%guest_lecture%", "%masterclass%"],
                "tests":     ["%exam%", "%test_result%", "%practice_test%", "%testgen%", "%mock_test%"],
                "rubric":    ["%rubric%", "%airev%", "%case_study_review%"],
            }
            for bucket, patterns in buckets.items():
                found = []
                for pat in patterns:
                    try:
                        rows = self.db.execute(text("""
                            SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE :pat
                        """), {"pat": pat}).fetchall()
                        for r in rows: found.append(r[0])
                    except Exception:
                        continue
                found = sorted(set(found))
                if found:
                    logger.info(f"[schema] {bucket}: {found}")
                    # For each found table, log its FK-shaped columns so we know
                    # whether to filter on student_id, user_id, or something else.
                    for tname in found[:4]:
                        try:
                            cols = self.db.execute(text("""
                                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t
                                  AND (COLUMN_NAME LIKE '%user%id%' OR COLUMN_NAME LIKE '%student%id%'
                                       OR COLUMN_NAME = 'uid' OR COLUMN_NAME = 'sid')
                            """), {"t": tname}).fetchall()
                            fk_cols = [c[0] for c in cols]
                            cnt = self.db.execute(
                                text(f"SELECT COUNT(*) FROM `{tname}`")
                            ).scalar()
                            logger.info(f"[schema]   {tname} fk_cols={fk_cols} total_rows={cnt}")
                        except Exception as e:
                            logger.info(f"[schema]   {tname} probe failed: {e}")
                else:
                    logger.info(f"[schema] {bucket}: (no tables matched any of {patterns})")
        except Exception as e:
            logger.warning(f"[schema] probe failed: {e}")

    # ─── Personal Info (with hobbies) ────────────────────

    def _get_personal_info(self, student_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(text("""
                SELECT u.id, u.full_name, u.email, u.phone,
                       u.profile_photo AS photo_url,
                       s.city, s.state, s.country,
                       s.date_of_birth, s.enrollment_number, s.batch_id
                FROM users u
                LEFT JOIN students s ON s.user_id = u.id
                WHERE u.id = :sid LIMIT 1
            """), {"sid": student_id}).mappings().first()
            if not row: return {"full_name": "Student", "email": ""}
            d = dict(row)
            parts = (d.get("full_name") or "Student").split(" ", 1)
            d["first_name"] = parts[0]
            d["last_name"] = parts[1] if len(parts) > 1 else ""
        except Exception as e:
            logger.warning(f"personal_info basic query failed: {e}")
            return {"first_name": "Student", "last_name": "", "email": ""}

        # Extra columns on USERS table — includes hobbies
        users_columns = {
            "linkedin": "linkedin_url", "github": "github_url",
            "portfolio": "portfolio_url", "twitter": "twitter_url",
            "resume_url": "resume_url", "resume_name": "resume_name",
            "bio": "about_me", "skills": "skills",
            "certifications": "certifications",
            "education_level": "education_level", "institution": "institution",
            "graduation_year": "graduation_year",
            "field_of_study": "field_of_study",
            "work_experience_years": "work_experience_years",
            "current_employer": "current_employer",
            "current_designation": "current_designation",
            "key_skills": "key_skills", "career_goals": "career_goals",
            "preferred_role": "preferred_role",
            "hobbies": "hobbies",
        }
        for db_col, key_name in users_columns.items():
            try:
                extra_row = self.db.execute(
                    text(f"SELECT {db_col} FROM users WHERE id = :sid LIMIT 1"),
                    {"sid": student_id},
                ).mappings().first()
                if extra_row and extra_row.get(db_col):
                    d[key_name] = extra_row[db_col]
            except Exception:
                pass

        # Parse hobbies into a list
        raw_hobbies = d.get("hobbies", "") or ""
        if raw_hobbies and raw_hobbies.strip():
            d["hobbies_list"] = [h.strip() for h in raw_hobbies.replace("\n", ",").split(",") if h.strip() and len(h.strip()) > 1]
        else:
            d["hobbies_list"] = []

        # Build structured education from LMS profile fields
        lms_education = []
        if d.get("education_level") or d.get("institution") or d.get("graduation_year"):
            lms_education.append({
                "degree": d.get("education_level", "") or "",
                "institution": d.get("institution", "") or "",
                "year": str(d.get("graduation_year", "") or ""),
                "field_of_study": d.get("field_of_study", "") or "",
                "percentage": "", "source": "lms_profile",
            })
        else:
            bio = d.get("about_me", "") or ""
            if bio:
                parsed_edu = self._parse_education_from_bio(bio)
                if parsed_edu:
                    lms_education.append(parsed_edu)
        d["lms_education"] = lms_education

        lms_work_experience = []
        if d.get("current_designation") or d.get("current_employer"):
            years = d.get("work_experience_years", "") or ""
            duration = f"{years} years" if years else ""
            lms_work_experience.append({
                "title": d.get("current_designation", "") or "",
                "company": d.get("current_employer", "") or "",
                "duration": duration, "description": "",
                "source": "lms_profile",
            })
        d["lms_work_experience"] = lms_work_experience

        return clean_data(d)

    # ─── Bio → Education Parser ──────────────────────────

    def _parse_education_from_bio(self, bio: str):
        import re
        if not bio or len(bio.strip()) < 10: return None
        bio_lower = bio.lower()
        degree_patterns = [
            (r'\bb\.?\s*tech\b', 'B.Tech'), (r'\bb\.?\s*e\b', 'B.E'),
            (r'\bb\.?\s*c\.?\s*a\b', 'BCA'), (r'\bm\.?\s*c\.?\s*a\b', 'MCA'),
            (r'\bm\.?\s*b\.?\s*a\b', 'MBA'), (r'\bm\.?\s*tech\b', 'M.Tech'),
            (r'\bb\.?\s*com\b', 'B.Com'), (r'\bm\.?\s*com\b', 'M.Com'),
            (r'\bb\.?\s*sc\b', 'B.Sc'), (r'\bm\.?\s*sc\b', 'M.Sc'),
            (r'\bph\.?\s*d\b', 'Ph.D'),
            (r'\bbachelor\b', "Bachelor's Degree"),
            (r'\bmaster\b', "Master's Degree"),
            (r'\bdiploma\b', 'Diploma'),
        ]
        degree = ""
        for pat, name in degree_patterns:
            if re.search(pat, bio_lower):
                degree = name; break
        if not degree: return None
        institution = ""
        from_match = re.search(r'\bfrom\s+([^.;]+?)(?:\.\s*$|$|;)', bio, re.IGNORECASE)
        if from_match:
            institution = from_match.group(1).strip().rstrip(',.')
            if len(institution) < 100:
                words = [w.upper() if len(w) <= 4 and w.isalpha() else w.title() for w in institution.split()]
                institution = ' '.join(words)
        year_match = re.search(r'\b(19|20)\d{2}\b', bio)
        year = year_match.group(0) if year_match else ""
        field = ""
        for pat, name in [(r'computer\s+science', 'Computer Science'), (r'\bcse\b', 'Computer Science'),
                          (r'information\s+technology', 'Information Technology'),
                          (r'electronics', 'Electronics'), (r'commerce', 'Commerce'), (r'e-?commerce', 'E-Commerce')]:
            if re.search(pat, bio_lower): field = name; break
        return {"degree": degree, "institution": institution, "year": year,
                "field_of_study": field, "percentage": "", "source": "lms_bio_parsed"}

    # ─── Courses (with description) ──────────────────────

    def _get_courses(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(text("""
                SELECT c.id AS course_id, c.course_name, c.category,
                       c.difficulty_level, c.description,
                       e.progress_percentage, e.completion_status,
                       e.completed_at, e.created_at AS enrolled_at,
                       (SELECT COUNT(*) FROM course_modules cm WHERE cm.course_id = c.id) AS total_modules
                FROM enrollments e
                JOIN courses c ON c.id = e.course_id
                WHERE e.student_id = :sid
                ORDER BY e.created_at
            """), {"sid": student_id}).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            # Fallback without description column
            try:
                rows = self.db.execute(text("""
                    SELECT c.id AS course_id, c.course_name, c.category,
                           c.difficulty_level,
                           e.progress_percentage, e.completion_status,
                           e.completed_at, e.created_at AS enrolled_at
                    FROM enrollments e
                    JOIN courses c ON c.id = e.course_id
                    WHERE e.student_id = :sid ORDER BY e.created_at
                """), {"sid": student_id}).mappings().all()
                return clean_data([dict(r) for r in rows])
            except Exception as e2:
                logger.warning(f"courses failed: {e2}")
                return []

    # ─── Test Scores (EXCLUDES TestGen) ──────────────────

    def _get_test_scores(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(text("""
                SELECT r.id, r.score, r.total_marks, r.percentage,
                       r.grade, r.time_taken_minutes, r.submitted_at,
                       e.exam_name AS subject, e.exam_type AS topic,
                       c.course_name AS course_name
                FROM results r
                JOIN exams e ON e.id = r.exam_id
                LEFT JOIN courses c ON c.id = e.course_id
                WHERE r.student_id = :sid
                  AND (e.exam_type IS NULL OR LOWER(e.exam_type) NOT IN
                       ('brain_drill','testgen','practice_test','ai_generated',
                        'practice','braindrill','brain-drill'))
                ORDER BY r.percentage DESC
            """), {"sid": student_id}).mappings().all()
            logger.info(f"test_scores: {len(rows)} real assessments (TestGen excluded)")
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.warning(f"test_scores failed: {e}")
            return []

    # ─── Case Studies ────────────────────────────────────

    def _get_case_studies(self, student_id: int) -> List[Dict]:
        # v12.4: exposes BOTH faculty_score (manual mentor grade) AND ai_score (AiRev agent grade)
        query_variants = [
            ("with rubric_results join", """
               SELECT css.id AS submission_id, csd.title,
                      csd.max_score, css.grade AS score,
                      css.grade AS faculty_score,
                      css.feedback AS faculty_feedback, css.rubric_scores,
                      css.notes, css.submitted_at, css.status,
                      c.course_name,
                      rr.total_score AS ai_score,
                      rr.percentage AS ai_percentage,
                      rr.grade AS ai_grade,
                      rr.grade_label AS ai_grade_label,
                      rr.overall_feedback AS ai_feedback
               FROM case_study_submissions css
               JOIN case_studies csd ON csd.id = css.case_study_id
               LEFT JOIN courses c ON c.id = csd.course_id
               LEFT JOIN rubric_results rr ON rr.submission_id = css.id
                   AND (rr.evaluation_type = 'case_study' OR rr.evaluation_type IS NULL)
               WHERE css.student_id = :sid
                 AND css.status IN ('reviewed','graded','mentor_reviewed','completed')
               ORDER BY css.grade DESC"""),
            ("dedicated columns", """
               SELECT css.id AS submission_id, csd.title,
                      csd.max_score, css.grade AS score,
                      css.grade AS faculty_score,
                      css.ai_score, css.ai_feedback,
                      css.feedback AS faculty_feedback, css.rubric_scores,
                      css.notes, css.submitted_at, css.status,
                      c.course_name
               FROM case_study_submissions css
               JOIN case_studies csd ON csd.id = css.case_study_id
               LEFT JOIN courses c ON c.id = csd.course_id
               WHERE css.student_id = :sid
                 AND css.status IN ('reviewed','graded','mentor_reviewed','completed')
               ORDER BY css.grade DESC"""),
            ("legacy minimal", """
               SELECT css.id AS submission_id, csd.title,
                      css.grade AS score, css.grade AS faculty_score,
                      css.feedback AS faculty_feedback,
                      css.notes, css.status, css.submitted_at
               FROM case_study_submissions css
               JOIN case_studies csd ON csd.id = css.case_study_id
               WHERE css.student_id = :sid
               ORDER BY css.grade DESC"""),
        ]
        for label, query in query_variants:
            try:
                rows = self.db.execute(text(query), {"sid": student_id}).mappings().all()
                if rows:
                    logger.info(f"case_studies: {len(rows)} rows ('{label}')")
                    return clean_data([dict(r) for r in rows])
            except Exception as e:
                logger.info(f"case_studies '{label}' failed: {e}")
        return []
               

    # ─── Assignments (rubric JOIN FIXED) ─────────────────

    def _get_assignments(self, student_id: int) -> List[Dict]:
        # v12.4: returns BOTH faculty_score and ai_score so the template can show both
        primary_query = """
            SELECT asub.id,
                asub.grade AS faculty_score,
                COALESCE(rr.total_score, 0) AS ai_score,
                rr.percentage AS ai_percentage,
                COALESCE(rr.total_score, asub.grade, 0) AS score,
                COALESCE(rr.max_score, a.total_marks, 100) AS max_score,
                rr.percentage AS rubric_pct,
                rr.grade AS ai_grade, rr.grade_label AS ai_grade_label,
                rr.grade AS rubric_grade,
                COALESCE(rr.overall_feedback, asub.feedback) AS feedback,
                rr.overall_feedback AS ai_feedback,
                asub.feedback AS faculty_feedback,
                rr.strengths, rr.top_competencies,
                asub.status, asub.submitted_at, a.title,
                a.course_id, c.course_name
            FROM assignment_submissions asub
            JOIN assignments a ON a.id = asub.assignment_id
            LEFT JOIN courses c ON c.id = a.course_id
            LEFT JOIN rubric_results rr ON rr.submission_id = asub.id
                AND (rr.evaluation_type IN ('assignment','rubric_assignment',
                     'final_assessment','case_study') OR rr.evaluation_type IS NULL)
            WHERE asub.student_id = :sid
            ORDER BY rr.percentage DESC, asub.submitted_at DESC
        """
        fallback_query = """
            SELECT asub.id, asub.grade AS score, asub.grade AS faculty_score,
                   asub.feedback, asub.status, asub.submitted_at, a.title,
                   a.total_marks AS max_score, a.course_id, c.course_name
            FROM assignment_submissions asub
            JOIN assignments a ON a.id = asub.assignment_id
            LEFT JOIN courses c ON c.id = a.course_id
            WHERE asub.student_id = :sid ORDER BY asub.submitted_at
        """
        for label, query in (("primary+rubric", primary_query), ("legacy", fallback_query)):
            try:
                rows = self.db.execute(text(query), {"sid": student_id}).mappings().all()
                result = clean_data([dict(r) for r in rows])
                if label == "primary+rubric":
                    matched = sum(1 for r in result if r.get("rubric_pct") is not None)
                    logger.info(f"assignments: {len(result)} rows, {matched} with rubric data")
                return result
            except Exception as e:
                logger.info(f"assignments {label} query failed: {e}")
        return []

    # ─── Mock Tests (TestGen / BrainDrill — INVERSE of test_scores) ──
    def _get_mock_tests(self, student_id: int) -> List[Dict]:
        """TestGen / BrainDrill practice attempts — the user's `lms.upskillize.com/student/testgen`
        page. INVERSE filter from _get_test_scores (which excludes these)."""
        queries = [
            ("results+exams (testgen filter)", """
                SELECT r.id, r.score, r.total_marks, r.percentage,
                       r.grade, r.time_taken_minutes,
                       r.submitted_at AS attempted_at,
                       e.exam_name AS title, e.exam_type AS test_type,
                       c.course_name
                FROM results r
                JOIN exams e ON e.id = r.exam_id
                LEFT JOIN courses c ON c.id = e.course_id
                WHERE r.student_id = :sid
                  AND LOWER(e.exam_type) IN
                      ('brain_drill','testgen','practice_test','ai_generated',
                       'practice','braindrill','brain-drill')
                ORDER BY r.submitted_at DESC
            """),
            ("testgen_attempts table", """
                SELECT * FROM testgen_attempts WHERE student_id = :sid
                ORDER BY id DESC LIMIT 20
            """),
            ("brain_drill_attempts table", """
                SELECT * FROM brain_drill_attempts WHERE student_id = :sid
                ORDER BY id DESC LIMIT 20
            """),
            ("test_attempts table", """
                SELECT * FROM test_attempts WHERE student_id = :sid
                ORDER BY id DESC LIMIT 20
            """),
        ]
        for label, q in queries:
            try:
                rows = self.db.execute(text(q), {"sid": student_id}).mappings().all()
                if rows:
                    logger.info(f"mock_tests (TestGen): {len(rows)} rows from '{label}'")
                    return clean_data([dict(r) for r in rows])
            except Exception as e:
                logger.info(f"mock_tests '{label}' failed: {e}")
        return []

    # ─── Mock Interviews (InterviewIQ) ────────────────────
    def _get_mock_interviews(self, student_id: int) -> List[Dict]:
        queries = [
            ("mock_interviews + reviews", """
                SELECT mi.id,
                       mi.score AS faculty_score,
                       mi.overall_score AS faculty_score_alt,
                       mi.feedback AS faculty_feedback,
                       mi.duration_minutes, mi.scheduled_at AS submitted_at,
                       COALESCE(mi.interview_type, mi.topic, 'Mock Interview') AS title,
                       mi.interviewer,
                       ir.overall_score AS ai_score, ir.feedback AS ai_feedback
                FROM mock_interviews mi
                LEFT JOIN interview_reviews ir ON ir.interview_id = mi.id
                WHERE mi.student_id = :sid
                ORDER BY mi.scheduled_at DESC LIMIT 20
            """),
            ("mock_interviews simple", """
                SELECT id, score, feedback, scheduled_at AS submitted_at,
                       interview_type AS title, status
                FROM mock_interviews WHERE student_id = :sid
                ORDER BY id DESC LIMIT 20
            """),
            ("interview_attempts", """
                SELECT * FROM interview_attempts WHERE student_id = :sid
                ORDER BY id DESC LIMIT 20
            """),
            ("interview_sessions", """
                SELECT * FROM interview_sessions WHERE student_id = :sid
                ORDER BY id DESC LIMIT 20
            """),
            ("interviewiq_sessions", """
                SELECT * FROM interviewiq_sessions WHERE student_id = :sid
                ORDER BY id DESC LIMIT 20
            """),
        ]
        for label, q in queries:
            try:
                rows = self.db.execute(text(q), {"sid": student_id}).mappings().all()
                if rows:
                    logger.info(f"mock_interviews: {len(rows)} rows from '{label}'")
                    return clean_data([dict(r) for r in rows])
            except Exception as e:
                logger.info(f"mock_interviews '{label}' failed: {e}")
        return []

    # ─── Industry Sessions (live mentor masterclasses) ────
    def _get_industry_sessions(self, student_id: int) -> List[Dict]:
        # Try dedicated tables first
        for table in ("industry_sessions", "industry_session_attendance",
                      "session_attendance", "masterclass_attendance",
                      "live_session_attendance"):
            try:
                rows = self.db.execute(
                    text(f"SELECT * FROM {table} WHERE student_id = :sid ORDER BY id DESC LIMIT 15"),
                    {"sid": student_id},
                ).mappings().all()
                if rows:
                    logger.info(f"industry_sessions from {table}: {len(rows)}")
                    return clean_data([dict(r) for r in rows])
            except Exception:
                continue

        # Fall back to assignment_submissions filtered by type/category — try several columns
        type_filters = [
            ("a.type", "industry_session"),
            ("a.type", "industry"),
            ("a.assignment_type", "industry_session"),
            ("a.category", "industry_session"),
            ("a.coursework_type", "industry_session"),
        ]
        for col, val in type_filters:
            try:
                rows = self.db.execute(text(f"""
                    SELECT asub.id, asub.grade AS faculty_score, asub.feedback,
                           asub.status, asub.submitted_at, a.title,
                           a.total_marks AS max_score, c.course_name,
                           rr.total_score AS ai_score, rr.percentage AS ai_percentage,
                           rr.overall_feedback AS ai_feedback
                    FROM assignment_submissions asub
                    JOIN assignments a ON a.id = asub.assignment_id
                    LEFT JOIN courses c ON c.id = a.course_id
                    LEFT JOIN rubric_results rr ON rr.submission_id = asub.id
                    WHERE asub.student_id = :sid AND LOWER({col}) = :tv
                    ORDER BY asub.submitted_at DESC LIMIT 15
                """), {"sid": student_id, "tv": val.lower()}).mappings().all()
                if rows:
                    logger.info(f"industry_sessions from coursework where {col}='{val}': {len(rows)}")
                    return clean_data([dict(r) for r in rows])
            except Exception:
                continue
        return []

    # ─── Hackathons ───────────────────────────────────────
    def _get_hackathons(self, student_id: int) -> List[Dict]:
        for table in ("hackathons", "hackathon_participation",
                      "hackathon_submissions", "student_hackathons"):
            try:
                rows = self.db.execute(
                    text(f"SELECT * FROM {table} WHERE student_id = :sid ORDER BY id DESC LIMIT 10"),
                    {"sid": student_id},
                ).mappings().all()
                if rows:
                    logger.info(f"hackathons from {table}: {len(rows)}")
                    return clean_data([dict(r) for r in rows])
            except Exception:
                continue
        return []

    # ─── Quiz Scores (EXCLUDES TestGen) ──────────────────

    def _get_quiz_scores(self, student_id: int) -> List[Dict]:
        try:
            rows = self.db.execute(text("""
                SELECT qa.id, qa.score, qa.total_marks,
                       qa.passed, qa.time_taken_seconds, qa.submitted_at,
                       q.title AS quiz_title, c.course_name
                FROM quiz_attempts qa
                JOIN quizzes q ON q.id = qa.quiz_id
                LEFT JOIN courses c ON c.id = q.course_id
                WHERE qa.student_id = :sid AND q.course_id IS NOT NULL
                ORDER BY qa.submitted_at
            """), {"sid": student_id}).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.warning(f"quiz_scores failed: {e}")
            return []

    # ─── Personality (with direct JSON fallback) ─────────

    def _get_personality(self, student_id: int) -> Dict[str, Any]:
        empty = {"personality_type": "", "traits_json": "", "traits": "",
                 "work_style": "", "communication_profile": "", "communication": "",
                 "leadership_indicators": "", "leadership": ""}
        try:
            row = self.db.execute(
                text("SELECT psycho_result, full_name FROM users WHERE id = :sid LIMIT 1"),
                {"sid": student_id},
            ).mappings().first()
            if not row: return empty
            raw = row.get("psycho_result")
            name = row.get("full_name", "Student") or "Student"
            if not raw or raw == "default": return empty

            # Try AI interpretation first
            try:
                from app.agents.personality_agent import PersonalityAgent
                agent = PersonalityAgent()
                result = agent.interpret(raw, student_name=name)
                if result and result.get("personality_type"):
                    return result
            except Exception as e:
                logger.warning(f"PersonalityAgent AI failed: {e}")

            # DIRECT FALLBACK — read from JSON without AI
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
                ptype = data.get("type") or data.get("personality_type") or ""
                desc = data.get("desc") or data.get("description") or ""
                return {
                    "personality_type": ptype,
                    "traits_json": desc,
                    "traits": desc,
                    "work_style": data.get("work_style") or data.get("workStyle") or "",
                    "communication_profile": "",
                    "communication": data.get("communication") or "",
                    "leadership_indicators": "",
                    "leadership": data.get("leadership") or data.get("teamRole") or "",
                }
            except Exception:
                logger.warning("Direct psycho_result JSON parse also failed")
                return empty

        except Exception as e:
            logger.info(f"psycho_result not available: {e}")
            return empty

    # ─── Capstone Projects ───────────────────────────────

    def _get_capstone_projects(self, student_id: int) -> list:
        # v12.5: expanded table + FK hunt. The LMS exposes /student/capstones endpoint
        # but the underlying table name varies. Try every plausible combination.
        table_variants = [
            ("capstones",            "student_id"),
            ("capstones",            "user_id"),
            ("capstone_projects",    "student_id"),
            ("capstone_projects",    "user_id"),
            ("capstone_submissions", "student_id"),
            ("capstone_submissions", "user_id"),
            ("student_capstones",    "student_id"),
            ("student_capstones",    "user_id"),
        ]
        for table, fk in table_variants:
            try:
                rows = self.db.execute(
                    text(f"SELECT * FROM {table} WHERE {fk} = :sid ORDER BY id DESC LIMIT 8"),
                    {"sid": student_id},
                ).mappings().all()
                if rows:
                    logger.info(f"capstones from {table}.{fk}: {len(rows)}")
                    return clean_data([dict(r) for r in rows])
            except Exception as e:
                logger.debug(f"capstone table {table}.{fk} failed: {e}")

        # Coursework-table fallback (capstone stored as assignment.type='capstone' etc)
        type_filters = [
            ("a.type", "capstone"),
            ("a.assignment_type", "capstone"),
            ("a.category", "capstone"),
            ("a.coursework_type", "capstone"),
            ("a.type", "capstone_project"),
            ("a.type", "Capstone"),
        ]
        for col, val in type_filters:
            try:
                rows = self.db.execute(text(f"""
                    SELECT asub.id, asub.grade AS faculty_score, asub.feedback AS faculty_feedback,
                           asub.status, asub.submitted_at, a.title,
                           a.total_marks AS max_score, c.course_name,
                           rr.total_score AS ai_score, rr.percentage AS ai_percentage,
                           rr.grade AS ai_grade, rr.grade_label AS ai_grade_label,
                           rr.overall_feedback AS ai_feedback,
                           COALESCE(rr.total_score, asub.grade, 0) AS score
                    FROM assignment_submissions asub
                    JOIN assignments a ON a.id = asub.assignment_id
                    LEFT JOIN courses c ON c.id = a.course_id
                    LEFT JOIN rubric_results rr ON rr.submission_id = asub.id
                    WHERE asub.student_id = :sid AND LOWER({col}) = :tv
                    ORDER BY asub.submitted_at DESC LIMIT 8
                """), {"sid": student_id, "tv": val.lower()}).mappings().all()
                if rows:
                    logger.info(f"capstones from coursework where {col}='{val}': {len(rows)}")
                    return clean_data([dict(r) for r in rows])
            except Exception:
                continue
        logger.info("capstones: NONE found across all 8 table variants + 6 coursework filters")
        return []

    # ─── Semester Results ────────────────────────────────

    def _get_semester_results(self, student_id: int) -> list:
        for table in ("semester_results", "final_results", "academic_results"):
            try:
                rows = self.db.execute(
                    text(f"SELECT * FROM {table} WHERE student_id = :sid ORDER BY id DESC"),
                    {"sid": student_id},
                ).mappings().all()
                if rows: return clean_data([dict(r) for r in rows])
            except Exception:
                continue
        return []

    # ─── Job Preferences ─────────────────────────────────

    def _get_job_preferences(self, student_id: int) -> dict:
        for table in ("job_preferences", "student_job_preferences"):
            try:
                row = self.db.execute(
                    text(f"SELECT * FROM {table} WHERE user_id = :sid OR student_id = :sid LIMIT 1"),
                    {"sid": student_id},
                ).mappings().first()
                if row: return clean_data(dict(row))
            except Exception:
                continue
        prefs = {}
        for col in ("preferred_role", "preferred_industry", "preferred_location",
                     "work_mode", "expected_salary", "notice_period", "open_to_relocate",
                     "employment_type", "expected_salary_min", "expected_salary_max",
                     "company_size"):
            try:
                r = self.db.execute(
                    text(f"SELECT {col} FROM users WHERE id = :sid LIMIT 1"),
                    {"sid": student_id},
                ).mappings().first()
                if r and r.get(col): prefs[col] = r[col]
            except Exception:
                continue
        return clean_data(prefs)

    # ─── Projects ────────────────────────────────────────

    def _get_projects(self, student_id: int) -> list:
        try:
            rows = self.db.execute(text("""
                SELECT title, description, technologies_used, mentor_feedback,
                       github_url, demo_url, submitted_at
                FROM student_projects WHERE student_id = :sid
                ORDER BY submitted_at DESC
            """), {"sid": student_id}).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception:
            return []

    # ─── Certifications ──────────────────────────────────

    def _get_certifications(self, student_id: int) -> list:
        try:
            rows = self.db.execute(text("""
                SELECT certificate_name, course_name, issued_at,
                       verification_url, certificate_url
                FROM certificates WHERE student_id = :sid
                ORDER BY issued_at DESC
            """), {"sid": student_id}).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception:
            return []

    # ─── Platform Activity ───────────────────────────────

    def _get_platform_activity(self, student_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(text("""
                SELECT COALESCE(SUM(total_watch_time),0) AS total_watch_seconds,
                       COUNT(DISTINCT lesson_id) AS lessons_watched,
                       COUNT(DISTINCT DATE(last_watched_at)) AS active_days,
                       MIN(created_at) AS first_activity,
                       MAX(last_watched_at) AS last_activity
                FROM video_watch_history WHERE student_id = :sid
            """), {"sid": student_id}).mappings().first()
            d = dict(row) if row else {}
            d["total_minutes"] = round(float(d.get("total_watch_seconds", 0) or 0) / 60, 1)
            return clean_data(d)
        except Exception:
            return {"total_minutes": 0, "active_days": 0, "lessons_watched": 0}

    # ─── Forum Activity ──────────────────────────────────

    def _get_forum_activity(self, student_id: int) -> Dict[str, Any]:
        try:
            threads = self.db.execute(text("SELECT COUNT(*) AS cnt FROM forum_threads WHERE author_id = :sid"), {"sid": student_id}).mappings().first()
            replies = self.db.execute(text("SELECT COUNT(*) AS cnt FROM forum_replies WHERE author_id = :sid"), {"sid": student_id}).mappings().first()
            answers = self.db.execute(text("SELECT COUNT(*) AS cnt FROM forum_replies WHERE author_id = :sid AND is_answer = 1"), {"sid": student_id}).mappings().first()
            return {"threads_created": int(threads["cnt"]) if threads else 0,
                    "replies_given": int(replies["cnt"]) if replies else 0,
                    "answers_accepted": int(answers["cnt"]) if answers else 0}
        except Exception:
            return {"threads_created": 0, "replies_given": 0, "answers_accepted": 0}

    # ─── Batch Info ──────────────────────────────────────

    def _get_batch_info(self, student_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(text("""
                SELECT b.name AS batch_name, b.start_date, b.end_date, b.status
                FROM batch_students bs JOIN batches b ON b.id = bs.batch_id
                WHERE bs.student_id = :sid ORDER BY b.id DESC LIMIT 1
            """), {"sid": student_id}).mappings().first()
            return clean_data(dict(row)) if row else {}
        except Exception:
            return {}

    # ─── Computed Metrics ────────────────────────────────

    def _compute_metrics(self, **data) -> Dict[str, Any]:
        test_scores = data.get("test_scores", [])
        case_studies = data.get("case_studies", [])
        assignments = data.get("assignments", [])
        quiz_scores = data.get("quiz_scores", [])
        courses = data.get("courses", [])
        activity = data.get("platform_activity", {})
        forum = data.get("forum_activity", {})

        test_pcts = [float(t["percentage"]) for t in test_scores
                     if t.get("percentage") and float(t.get("percentage", 0)) > 0]
        best_test = max(test_pcts) if test_pcts else 0
        avg_test = round(sum(test_pcts) / len(test_pcts), 1) if test_pcts else 0

        subj_map = {}
        for t in test_scores:
            s = t.get("subject") or t.get("course_name") or "General"
            pct = float(t.get("percentage", 0))
            if pct > 0: subj_map.setdefault(s, []).append(pct)
        subj_avgs = {s: round(sum(v)/len(v), 1) for s, v in subj_map.items()}
        top_subjects = sorted(subj_avgs.items(), key=lambda x: -x[1])

        case_scores = [float(c["score"]) for c in case_studies
                       if c.get("score") and float(c.get("score", 0)) > 0]
        avg_case = round(sum(case_scores)/len(case_scores), 1) if case_scores else 0
        best_case = max(case_scores) if case_scores else 0

        quiz_pcts = []
        for q in quiz_scores:
            tm = q.get("total_marks", 0) or 0
            sc = q.get("score", 0) or 0
            if int(tm) > 0: quiz_pcts.append(round(int(sc)/int(tm)*100, 1))
        avg_quiz = round(sum(quiz_pcts)/len(quiz_pcts), 1) if quiz_pcts else 0

        all_pcts = test_pcts + quiz_pcts
        improvement = 0.0
        if len(all_pcts) >= 4:
            half = len(all_pcts) // 2
            improvement = round(sum(all_pcts[half:])/(len(all_pcts)-half) - sum(all_pcts[:half])/half, 1)

        completed_courses = [c for c in courses if c.get("completion_status") == "completed"]
        overall = round((avg_test*0.30)+(avg_case*0.30)+(avg_quiz*0.15)+(min(len(completed_courses),10)*2.5), 1)
        total_hours = round(float(activity.get("total_minutes", 0))/60, 1)

        consistency = 85.0
        if len(all_pcts) > 2:
            mean = sum(all_pcts)/len(all_pcts)
            std = (sum((x-mean)**2 for x in all_pcts)/len(all_pcts))**0.5
            consistency = max(0, round(100-std, 1))

        return {
            "overall_score": min(overall, 100),
            "best_test_score": best_test, "avg_test_score": avg_test,
            "total_tests": len(test_scores), "total_case_studies": len(case_studies),
            "avg_case_study_score": avg_case, "best_case_study_score": best_case,
            "total_assignments": len(assignments), "total_quizzes": len(quiz_scores),
            "avg_quiz_score": avg_quiz, "total_courses": len(courses),
            "completed_courses": len(completed_courses),
            "subject_averages": subj_avgs, "top_subjects": top_subjects,
            "improvement_pct": improvement, "total_hours": total_hours,
            "active_days": int(activity.get("active_days", 0) or 0),
            "lessons_watched": int(activity.get("lessons_watched", 0) or 0),
            "consistency_score": consistency,
            "forum_threads": int(forum.get("threads_created", 0) or 0),
            "forum_replies": int(forum.get("replies_given", 0) or 0),
            "forum_answers": int(forum.get("answers_accepted", 0) or 0),
        }