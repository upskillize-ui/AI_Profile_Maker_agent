# 00 — Discovery (STEP 0, read-only)

ProfileIQ / Upskillize Profile Agent — backend end-to-end audit.
All claims below are grounded in files read in this session. Line numbers are 1-indexed.

- Service: `Upskillize Profile Agent` v`4.0.0` (`app/config.py:6-7`), FastAPI.
- HF Space: `upskill25/Ai_Enhancer` (`README.md`, sdk: docker). Runtime base URL used in code: `https://upskill25-ai-enhancer.hf.space` (`app/api/routes.py:43`).
- Role: downstream profile generator. `DataCollector` reads the LMS MySQL DB (read-only, all `SELECT`s), agents shape the data, Jinja renders `profile_template.html`. **Frontend is server-rendered Jinja HTML, not React. No npm build.**
- DB: MySQL (`pymysql`, `app/config.py:12`, `requirements.txt`). Default `DATABASE_URL` is a localhost placeholder — the real LMS DB creds must come from the operator (HF Space secret).

---

## 0.a Endpoint map (`app/main.py`, `app/api/routes.py`)

Router prefix `/api/v1` (`routes.py:39`). Auth via `Authorization` header JWT (`deps.py:53`).

| Method | Path | Auth | Request | Response | Source |
|---|---|---|---|---|---|
| GET | `/health` | none | — | JSON status | `main.py:40` |
| GET | `/` | none | — | HTML splash | `main.py:49` |
| POST | `/api/v1/profile/generate` | student JWT | `ProfileGenerateRequest{student_id?, force_regenerate}` | JSON slug/urls | `routes.py:118` |
| GET | `/api/v1/profile/me` | student JWT | — | JSON profile snapshot | `routes.py:371` |
| GET | `/api/v1/profile/debug/me` | student JWT | — | JSON diagnostic (full PII of self) | `routes.py:419` |
| GET | `/api/v1/profile/public/{slug}` | **none** | — | rendered HTML (visibility-gated) | `routes.py:527` |
| GET | `/api/v1/profile/download/{slug}` | **none** | — | PDF/HTML (**NOT visibility-gated**) | `routes.py:559` |
| POST | `/api/v1/profile/toggle-visibility` | student JWT | `VisibilityToggleRequest{visibility}` | JSON | `routes.py:590` |
| POST | `/api/v1/rubric/grade/case-study` | student JWT | `GradeCaseStudyRequest` | JSON result | `routes.py:620` |
| GET | `/api/v1/rubric/result/{evaluation_type}/{submission_id}` | student JWT | — | JSON result (own only) | `routes.py:698` |
| GET | `/api/v1/rubric/my-results` | student JWT | `?evaluation_type&limit` | JSON list (own only) | `routes.py:713` |
| POST | `/api/v1/rubric/admin/template` | admin JWT | `RubricTemplateCreate` | JSON | `routes.py:724` |
| GET | `/api/v1/rubric/admin/templates` | admin JWT | — | JSON list | `routes.py:747` |

Note: `/docs` and `/redoc` are enabled unconditionally (`main.py:13-14`).

## 0.d Auth model (`app/api/deps.py`)

- `get_current_student` (`deps.py:53`): reads `Authorization` header, strips optional `Bearer `, `jwt.decode(token, JWT_SECRET, algorithms=["HS256"])`. Builds a user from claims `id`, `role`, `email`, `full_name`. 401 if header missing / token bad / no `id`.
- `get_current_admin` (`deps.py:84`): wraps student, requires `role == "admin"` else 403.
- Shared `JWT_SECRET` with the Node LMS (`deps.py:34`, `.env.example`). Algorithm pinned to HS256 → no `alg:none` confusion.
- **No per-object ownership check inside `/profile/generate`** — see finding S1.

## 0.e Config surface (`app/config.py`)

| Var | Default | Secret? | Notes |
|---|---|---|---|
| `DATABASE_URL` | localhost placeholder | **yes** | LMS DB DSN |
| `JWT_SECRET` | `""` | **yes** | must match LMS; empty ⇒ all auth 401 (`deps.py:35`) |
| `ANTHROPIC_API_KEY` | `""` | **yes** | read via `os.environ` in agents (see 0.g); optional (rule-based fallback) |
| `REDIS_URL`/`CELERY_*` | localhost | no | cache/celery |
| `MAIL_*` | gmail/empty | password is secret | not used on hot profile path |
| `BASE_URL`, `AI_MODEL`, `PROFILE_CACHE_TTL`, `MAX_CASE_STUDIES_SHOWN`, `MAX_SKILLS_SHOWN` | — | no | |

Secrets audit (C3 preview): `git grep` for hardcoded keys found **only** `os.environ.get("ANTHROPIC_API_KEY","")` reads and the placeholder DSN — **no leaked secrets in the tree**. `app.zip` (tracked) contains source only, no `.env`. `.env`/`.env.*` are gitignored.

## 0.f Deploy (`Dockerfile`, `requirements.txt`, `README.md`)

- `python:3.12-slim`, installs weasyprint native deps, `uvicorn app.main:app --host 0.0.0.0 --port 7860`.
- No `slowapi`/rate-limit dependency. No test suite in repo.

---

## 0.b DataCollector SQL inventory (`app/services/data_collector.py`)

**Parameterisation:** every query binds the student id via a named param `:sid`/`:uid` (safe). Several queries use `text(f"... FROM {table} WHERE {fk} = :sid")` where `{table}`/`{fk}`/`{col}` are **hardcoded literals from in-code lists**, never request input → not SQL-injectable from the client. `_probe_schema` interpolates a table name from `INFORMATION_SCHEMA` results into a back-ticked `COUNT(*)` — also not client-controlled. (Full C2 verdict deferred to live run, but the static surface is clean.)

**ID mapping quirk:** `collect_all` maps `user_id → students.id` (`stu_id`) at `:41-46`. Most collectors take `stu_id` (students.id); but `_get_personal_info`, `_get_personality`, `_get_forum_activity`, `_get_job_preferences` take the original `student_id` (= user_id). Any collector using the wrong id domain silently returns empty. Flag for live verification.

| Collector | Tables (join keys) | Notes / fragility |
|---|---|---|
| `_get_personal_info` `:165` | `users` JOIN `students`(user_id); then per-column probes on `users` | 20+ hardcoded `users` columns probed one-by-one (`:186-211`) |
| `_get_courses` `:293` | `enrollments`(student_id) JOIN `courses`; subquery `course_modules` | has description-fallback variant |
| `_get_test_scores` `:326` | `results`(student_id) JOIN `exams` LEFT `courses` | excludes TestGen exam_types |
| `_get_case_studies` `:350` | `case_study_submissions`(student_id) JOIN `case_studies` LEFT `rubric_results`(submission_id) | 3 query variants; needs status IN (reviewed/graded/…) |
| `_get_assignments` `:410` | `assignment_submissions`(student_id) JOIN `assignments` LEFT `rubric_results` | primary+rubric / legacy |
| `_get_mock_tests` `:459` | `results`+`exams` (testgen filter) **or** `testgen_attempts`/`brain_drill_attempts`/`test_attempts` | **guessed tables** |
| `_get_mock_interviews` `:502` | `mock_interviews`(student_id) LEFT `interview_reviews` **or** `interview_attempts`/`interview_sessions`/`interviewiq_sessions` | **guessed tables** |
| `_get_industry_sessions` `:548` | 5 candidate tables, else `assignment_submissions` type-filter | **guessed** |
| `_get_hackathons` `:595` | 4 candidate tables | **guessed** |
| `_get_quiz_scores` `:612` | `quiz_attempts`(student_id) JOIN `quizzes` | |
| `_get_personality` `:631` | `users.psycho_result` | AI interpret + JSON fallback |
| `_get_capstone_projects` `:680` | 8 (table,fk) variants + 6 coursework filters | **heavily guessed**; logs "NONE found" |
| `_get_semester_results` `:741` | 3 candidate tables | guessed |
| `_get_job_preferences` `:755` | 2 tables or `users` columns | |
| `_get_projects` `:782` | `student_projects`(student_id) | |
| `_get_certifications` `:796` | `certificates`(student_id) | |
| `_get_platform_activity` `:810` | `video_watch_history`(student_id) | |
| `_get_forum_activity` `:828` | `forum_threads`/`forum_replies`(author_id) | author_id = user_id domain |
| `_get_batch_info` `:841` | `batch_students` JOIN `batches` | |

**Every collector is wrapped in try/except that returns `[]`/`{}` on any error** → schema drift degrades silently, no error surfaced to the operator. This is the central wire-format risk (C5).

## 0.g Hardcoded LMS wire-format assumptions (C5 seed)

Tables/columns the service will silently render empty for if the LMS renames them:
`users`(id, full_name, email, phone, profile_photo, psycho_result, linkedin_url, github_url, resume_url, key_skills, hobbies, current_designation, …), `students`(user_id, batch_id, city…), `enrollments`(student_id, course_id, progress_percentage, completion_status), `courses`(id, course_name, category, difficulty_level), `results`(student_id, exam_id, score, percentage, submitted_at), `exams`(exam_type, exam_name), `case_study_submissions`(student_id, case_study_id, grade, status), `case_studies`(title, max_score), `assignment_submissions`(student_id, assignment_id, grade), `assignments`(title, total_marks), `rubric_results`(submission_id, evaluation_type, total_score, percentage, grade, grade_label, overall_feedback), `quiz_attempts`/`quizzes`, `certificates`, `student_projects`, `video_watch_history`, `forum_threads`/`forum_replies`, `batch_students`/`batches`. Plus **entirely guessed** tables for mock tests / mock interviews / capstones / industry / hackathons (see 0.b).

---

## 0.c Seven-flow trace (LMS table → collector → template var → HTML section)

| # | Flow | Collector → student_data key | Renderer context var | Template section (line) | Static verdict |
|---|---|---|---|---|---|
| 1 | Attendance → Enrolled Courses | **none** (no attendance collector) | `courses_data` (enriched courses) | Enrolled Courses `:664-696` | **attendance metrics MISSING** — section renders course *progress %*, not attendance %/weekly/streak/absences |
| 2 | TestGen → Mock Tests | `_get_mock_tests` → `mock_tests` | `mock_tests_raw` | Mock Tests `:846-870` | schema aligned; population depends on guessed tables |
| 3 | AiRev → Case Studies | `_get_case_studies` → `case_studies` | `case_studies_raw` | Case Studies `:723-758` | aligned (`module_name` absent but guarded) |
| 4 | AiRev → Assignments | `_get_assignments` → `assignments` | `assignments_raw` | Assignments `:761-794` | aligned (`module_name` guarded) |
| 5 | AiRev → Assessments | `_combine_assessments(test_scores+quiz_scores)` | `test_scores_raw` | Assessments `:797-821` | aligned; **source is exams+quizzes, not AiRev rubric** (framing mismatch vs brief) |
| 6 | AiRev → Capstones | `_get_capstone_projects` → `capstone_projects` | `capstone_projects` | Capstones `:699-720` | aligned; **population highly uncertain** (8 tables + 6 filters, code logs "NONE found") |
| 7 | InterviewIQ → Mock Interviews | `_get_mock_interviews` → `mock_interviews` | `mock_interviews_raw` | Mock Interviews `:824-843` | single score per row only — **no rounds / per-round bands / overall band / calibration** as the brief's contract expects |

Counts driving the tab chips come from `_compute_activity_counts` (`profile_renderer.py:214-228`); the perf heptagon from `_compute_perf_snapshot` (`:163`).

---

## Preliminary code-level findings (receipt = file:line read this session)

- **S1 (P1, broken object-level authz / IDOR):** `routes.py:132` `student_id = body.student_id or student.id`. An authenticated student can generate/regenerate **another** student's profile by supplying `student_id` in the POST body. New profiles are created `VisibilityMode.PUBLIC` (`:154`) and are then world-readable via `/profile/public/{slug}`, exposing the victim's real name/email/phone/scores pulled from LMS.
- **S2 (P1, missing auth + privacy bypass):** `/profile/download/{slug}` (`routes.py:559`) has **no auth dependency and no `visibility` check** (contrast `/profile/public` `:545`). Anyone who knows/guesses a slug downloads the full PDF even for `private` profiles. Slugs are `slugify(name-student_id)` (`:178`,`:258`) → enumerable.
- **S3 (P2, privacy default → product review):** profiles created PUBLIC by default (`routes.py:154`) though the model default is PRIVATE (`db_models.py:67`).
- **S4 (P3, info disclosure):** raw exception text returned to client on 500 (`routes.py:228`, `:364`).
- **S5 (P2, availability):** no rate limiting anywhere (no `slowapi`/limiter in code or `requirements.txt`); unauthenticated `/profile/public` and `/profile/download` (weasyprint PDF render) are DoS-able.
- **S6 (P1 latent, wire-format):** pervasive `try/except → []/{}` silent degradation on LMS schema drift (0.b, 0.g).
- **Attendance (Flow 1):** no `_get_attendance`/streak/weekly/sincerity/absences collector exists (`git grep` empty); `orchestrator.py:271` reads `student_data.get("attendance", {})` which `DataCollector` never populates, and `attendance_data` is not even passed into the render context.
- **Secrets:** clean (no hardcoded keys); `app.zip` + `backup/` tracked (hygiene, not a leak).

---

## Blocked: inputs required before STEP 2 (execution)

STEP 2/3 need live access I do not have. Cannot produce receipts (DB rows, collector output, rendered HTML, IDOR curls) without:

1. **Read-only** LMS MySQL creds (host, port, db, user, password). Read-only is a hard requirement.
2. Three real ids — `student_A` (activity across most flows), `student_B` (IDOR victim), `student_C` (empty-state). Note whether the id is `users.id` or `students.id` (the code maps user_id→students.id).
3. Run target: (i) deployed `Ai_Enhancer` URL + a student JWT, or (ii) run `uvicorn app.main:app` locally against the LMS DB (preferred — direct logs).

Until provided, flows are recorded here with **static verdicts only**; the final report will mark data-population and render checks `NOT_TESTED` rather than claim they work.
