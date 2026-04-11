"""
Data Collector Service — v4 FIXED
═════════════════════════════════
- linkedin, github, resume_url are on USERS table (not students)
- Tries each extra column individually — never crashes
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

    def __init__(self, db: Session):
        self.db = db

    async def collect_all(self, student_id: int) -> Dict[str, Any]:
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

        # NEW v9: capstone projects, semester results, job preferences
        capstone_projects = self._get_capstone_projects(stu_id)
        semester_results = self._get_semester_results(stu_id)
        job_preferences = self._get_job_preferences(student_id)

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
            "lms_education": personal.get("lms_education", []),
            "lms_work_experience": personal.get("lms_work_experience", []),
        }
        return clean_data(result)

    # ─── NEW: Capstone Projects ──────────────────────────

    def _get_capstone_projects(self, student_id: int) -> list:
        """Fetch capstone project submissions with scores and status.
        Tries multiple table names defensively."""
        for table in ("capstone_projects", "capstone_submissions", "student_capstones"):
            try:
                rows = self.db.execute(
                    text(f"""
                        SELECT * FROM {table}
                        WHERE student_id = :sid
                        ORDER BY id DESC
                        LIMIT 5
                    """),
                    {"sid": student_id},
                ).mappings().all()
                if rows:
                    return clean_data([dict(r) for r in rows])
            except Exception:
                continue
        return []

    # ─── NEW: Semester / Final Results ───────────────────

    def _get_semester_results(self, student_id: int) -> list:
        """Fetch semester or final results."""
        for table in ("semester_results", "final_results", "academic_results"):
            try:
                rows = self.db.execute(
                    text(f"""
                        SELECT * FROM {table}
                        WHERE student_id = :sid
                        ORDER BY id DESC
                    """),
                    {"sid": student_id},
                ).mappings().all()
                if rows:
                    return clean_data([dict(r) for r in rows])
            except Exception:
                continue
        return []

    # ─── NEW: Job Preferences ────────────────────────────

    def _get_job_preferences(self, student_id: int) -> dict:
        """Fetch from job_preferences table OR fall back to user table fields."""
        # Try a dedicated table first
        for table in ("job_preferences", "student_job_preferences"):
            try:
                row = self.db.execute(
                    text(f"SELECT * FROM {table} WHERE user_id = :sid OR student_id = :sid LIMIT 1"),
                    {"sid": student_id},
                ).mappings().first()
                if row:
                    return clean_data(dict(row))
            except Exception:
                continue

        # Fall back to user-level columns
        prefs = {}
        for col in ("preferred_role", "preferred_industry", "preferred_location",
                    "work_mode", "expected_salary", "notice_period", "open_to_relocate"):
            try:
                r = self.db.execute(
                    text(f"SELECT {col} FROM users WHERE id = :sid LIMIT 1"),
                    {"sid": student_id},
                ).mappings().first()
                if r and r.get(col):
                    prefs[col] = r[col]
            except Exception:
                continue
        return clean_data(prefs)

    # ─── NEW v9.2: Parse education from free-text bio ───────

    def _parse_education_from_bio(self, bio: str) -> Dict[str, str]:
        """Extract degree + institution from a free-text bio like
        'i completed my b.tech from sec, Sasaram.' or
        'BCA graduate from XYZ University 2024'."""
        import re
        if not bio or len(bio.strip()) < 10:
            return None

        bio_lower = bio.lower()

        # Detect degree
        degree_patterns = [
            (r'\bb\.?\s*tech\b', 'B.Tech'),
            (r'\bb\.?\s*e\b', 'B.E'),
            (r'\bb\.?\s*c\.?\s*a\b', 'BCA'),
            (r'\bm\.?\s*c\.?\s*a\b', 'MCA'),
            (r'\bm\.?\s*b\.?\s*a\b', 'MBA'),
            (r'\bm\.?\s*tech\b', 'M.Tech'),
            (r'\bb\.?\s*com\b', 'B.Com'),
            (r'\bm\.?\s*com\b', 'M.Com'),
            (r'\bb\.?\s*sc\b', 'B.Sc'),
            (r'\bm\.?\s*sc\b', 'M.Sc'),
            (r'\bph\.?\s*d\b', 'Ph.D'),
            (r'\bbachelor\b', "Bachelor's Degree"),
            (r'\bmaster\b', "Master's Degree"),
            (r'\bdiploma\b', 'Diploma'),
        ]

        degree = ""
        for pat, name in degree_patterns:
            if re.search(pat, bio_lower):
                degree = name
                break

        if not degree:
            return None

        # Try to extract institution after "from" keyword
        institution = ""
        # Match "from X" where X can contain commas and spaces, until end of string or sentence
        from_match = re.search(r'\bfrom\s+([^.;]+?)(?:\.\s*$|$|;)', bio, re.IGNORECASE)
        if from_match:
            institution = from_match.group(1).strip().rstrip(',.')
            # Smart casing: keep short all-caps words (acronyms like SEC, IIT, NIT) uppercase,
            # title-case the rest
            if len(institution) < 100 and institution:
                words = []
                for w in institution.split():
                    if len(w) <= 4 and w.isalpha():
                        words.append(w.upper())
                    else:
                        words.append(w.title())
                institution = ' '.join(words)

        # Try to extract year
        year_match = re.search(r'\b(19|20)\d{2}\b', bio)
        year = year_match.group(0) if year_match else ""

        # Try to extract field of study
        field = ""
        field_patterns = [
            (r'computer\s+science', 'Computer Science'),
            (r'\bcse\b', 'Computer Science'),
            (r'information\s+technology', 'Information Technology'),
            (r'\bit\b', 'Information Technology'),
            (r'electronics', 'Electronics'),
            (r'mechanical', 'Mechanical Engineering'),
            (r'civil', 'Civil Engineering'),
            (r'electrical', 'Electrical Engineering'),
            (r'commerce', 'Commerce'),
            (r'e-?commerce', 'E-Commerce'),
        ]
        for pat, name in field_patterns:
            if re.search(pat, bio_lower):
                field = name
                break

        return {
            "degree": degree,
            "institution": institution,
            "year": year,
            "field_of_study": field,
            "percentage": "",
            "source": "lms_bio_parsed",
        }

    # ─── Personal Info ───────────────────────────────────

    def _get_personal_info(self, student_id: int) -> Dict[str, Any]:
        # Basic query that always works
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
        except Exception as e:
            logger.warning(f"personal_info basic query failed: {e}")
            return {"first_name": "Student", "last_name": "", "email": ""}

        # All extra columns are on the USERS table (not students!)
        users_columns = {
            "linkedin": "linkedin_url",
            "github": "github_url",
            "portfolio": "portfolio_url",
            "twitter": "twitter_url",
            "resume_url": "resume_url",
            "resume_name": "resume_name",
            "bio": "about_me",
            "skills": "skills",
            "certifications": "certifications",
            "education_level": "education_level",
            "institution": "institution",
            "graduation_year": "graduation_year",
            "field_of_study": "field_of_study",
            "work_experience_years": "work_experience_years",
            "current_employer": "current_employer",
            "current_designation": "current_designation",
            "key_skills": "key_skills",
            "career_goals": "career_goals",
            "preferred_role": "preferred_role",
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
                pass  # Column doesn't exist — skip silently

        # ── v6: Build structured education + work_experience from LMS profile fields ──
        # These act as the LAST fallback when resume + LinkedIn don't provide them.
        # The data_merger will pick these up via lms_data["lms_education"] / ["lms_work_experience"].
        lms_education = []
        if d.get("education_level") or d.get("institution") or d.get("graduation_year"):
            lms_education.append({
                "degree": d.get("education_level", "") or "",
                "institution": d.get("institution", "") or "",
                "year": str(d.get("graduation_year", "") or ""),
                "field_of_study": d.get("field_of_study", "") or "",
                "percentage": "",
                "source": "lms_profile",
            })
        else:
            # NEW v9.2: Try to parse education from the about_me bio field
            # e.g. "i completed my b.tech from sec, Sasaram." → degree="B.Tech", institution="SEC, Sasaram"
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
                "duration": duration,
                "description": "",
                "source": "lms_profile",
            })
        d["lms_work_experience"] = lms_work_experience

        return clean_data(d)

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
        """Fetch graded case study submissions. Defensively tries multiple
        column combinations since LMS schemas vary."""

        # Define progressive query variants — try richest first, fall back gracefully
        query_variants = [
            # Variant 1: full schema with key_concepts
            """SELECT
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
            ORDER BY css.ai_score DESC""",

            # Variant 2: without key_concepts
            """SELECT
                css.id AS submission_id, csd.title,
                csd.max_score, css.ai_score AS score, css.ai_grade,
                css.ai_feedback, css.submitted_at, css.status,
                c.course_name
            FROM case_study_submissions css
            JOIN case_studies csd ON csd.id = css.case_study_id
            LEFT JOIN courses c ON c.id = csd.course_id
            WHERE css.student_id = :sid
              AND css.status IN ('graded', 'mentor_reviewed')
            ORDER BY css.ai_score DESC""",

            # Variant 3: minimal — just title, score, status
            """SELECT
                css.id AS submission_id, csd.title,
                css.ai_score AS score, css.status, css.submitted_at
            FROM case_study_submissions css
            JOIN case_studies csd ON csd.id = css.case_study_id
            WHERE css.student_id = :sid
              AND css.status IN ('graded', 'mentor_reviewed')
            ORDER BY css.ai_score DESC""",

            # Variant 4: even more minimal — drop status filter
            """SELECT
                css.id AS submission_id, csd.title,
                css.ai_score AS score
            FROM case_study_submissions css
            JOIN case_studies csd ON csd.id = css.case_study_id
            WHERE css.student_id = :sid
            ORDER BY css.ai_score DESC""",
        ]

        for i, query in enumerate(query_variants):
            try:
                rows = self.db.execute(text(query), {"sid": student_id}).mappings().all()
                if i > 0:
                    logger.info(f"case_studies fetched using fallback variant #{i+1}")
                return clean_data([dict(r) for r in rows])
            except Exception as e:
                logger.info(f"case_studies variant {i+1} failed: {e}")
                continue

        logger.warning("All case_studies query variants failed — returning empty list")
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

    # ─── Projects ────────────────────────────────────────

    def _get_projects(self, student_id: int) -> list:
        try:
            rows = self.db.execute(
                text("""
                    SELECT title, description, technologies_used, mentor_feedback,
                           github_url, demo_url, submitted_at
                    FROM student_projects
                    WHERE student_id = :sid
                    ORDER BY submitted_at DESC
                """),
                {"sid": student_id},
            ).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.info(f"projects table not found or empty: {e}")
            return []

    # ─── Certifications ──────────────────────────────────

    def _get_certifications(self, student_id: int) -> list:
        try:
            rows = self.db.execute(
                text("""
                    SELECT certificate_name, course_name, issued_at,
                           verification_url, certificate_url
                    FROM certificates
                    WHERE student_id = :sid
                    ORDER BY issued_at DESC
                """),
                {"sid": student_id},
            ).mappings().all()
            return clean_data([dict(r) for r in rows])
        except Exception as e:
            logger.info(f"certificates table not found or empty: {e}")
            return []

    # ─── Personality ─────────────────────────────────────

    def _get_personality(self, student_id: int) -> Dict[str, Any]:
        try:
            row = self.db.execute(
                text("SELECT psycho_result FROM users WHERE id = :sid LIMIT 1"),
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
            return self._derive_personality(student_id)
        except Exception as e:
            logger.info(f"psycho_result not available: {e}")
            return self._derive_personality(student_id)

    def _derive_personality(self, student_id: int) -> dict:
        try:
            quiz_row = self.db.execute(
                text("SELECT COUNT(*) AS cnt FROM quiz_attempts WHERE student_id = :sid"),
                {"sid": student_id},
            ).mappings().first()
            case_row = self.db.execute(
                text("""SELECT COUNT(*) AS cnt FROM case_study_submissions
                        WHERE student_id = :sid AND status IN ('graded','mentor_reviewed')"""),
                {"sid": student_id},
            ).mappings().first()

            quizzes = int(quiz_row["cnt"]) if quiz_row else 0
            cases = int(case_row["cnt"]) if case_row else 0
            total = quizzes + cases

            if total >= 15:
                ptype, traits = "Strategic Achiever", "High-performer, Assessment-driven, Detail-oriented"
                ws = "Structured and goal-oriented"
            elif total >= 8:
                ptype, traits = "Analytical Strategist", "Methodical, Self-motivated, Consistent"
                ws = "Structured and methodical"
            elif total >= 3:
                ptype, traits = "Active Learner", "Curious, Engaged, Growing"
                ws = "Self-paced with regular engagement"
            elif total >= 1:
                ptype, traits = "Curious Explorer", "Self-initiated, Building foundations"
                ws = "Self-paced learning"
            else:
                # No assessments — return empty so the section is hidden from recruiters
                return {"personality_type": "", "traits_json": "", "work_style": "",
                        "communication_profile": "", "leadership_indicators": ""}

            return {
                "personality_type": ptype, "traits_json": traits, "work_style": ws,
                "communication_profile": "Clear and concise",
                "leadership_indicators": "Collaborative" if cases >= 2 else "Individual contributor",
            }
        except Exception as e:
            logger.warning(f"derive_personality failed: {e}")
            return {"personality_type": "", "traits_json": "", "work_style": "",
                    "communication_profile": "", "leadership_indicators": ""}

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
            d["total_minutes"] = round(float(d.get("total_watch_seconds", 0) or 0) / 60, 1)
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

    # ─── Computed Metrics ────────────────────────────────

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
            (min(len(completed_courses), 10) * 2.5), 1,
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