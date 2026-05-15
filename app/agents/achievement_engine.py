"""
Achievement Engine v6 — Dynamic Headlines, Clean
═════════════════════════════════════════════════
FIX: Removed duplicate _generate_headline dead code that was
unreachable after the new dynamic version's return statement.
"""
import hashlib
import logging
from typing import Dict, List, Any
from app.agents.course_intelligence import CourseIntelligence

logger = logging.getLogger(__name__)

ACHIEVEMENT_VERBS = [
    "Analyzed", "Evaluated", "Assessed", "Investigated", "Examined",
    "Developed", "Designed", "Implemented", "Executed", "Delivered",
    "Demonstrated", "Showcased", "Applied", "Utilized", "Leveraged",
]
LEARNING_VERBS = [
    "Mastered", "Completed", "Achieved", "Accomplished", "Attained",
    "Built expertise in", "Gained proficiency in", "Acquired skills in",
]
PERFORMANCE_VERBS = [
    "Outperformed", "Excelled in", "Achieved distinction in",
    "Secured top marks in", "Demonstrated proficiency in",
]


def _hash_pick(name: str, items: list, offset: int = 0) -> str:
    h = int(hashlib.md5(f"{name}{offset}".encode()).hexdigest()[:8], 16)
    return items[h % len(items)]


class AchievementEngine:

    def __init__(self):
        self.course_intel = CourseIntelligence()

    def generate_all(self, student_data: Dict[str, Any], role_matches: list = None) -> Dict[str, Any]:
        name = (student_data.get("personal", {}).get("full_name") or "Student").strip()
        computed = student_data.get("computed", {})
        return {
            "headline": self._generate_headline(student_data, role_matches),
            "top_achievements": self._top_achievements(student_data, name),
            "case_study_highlights": self._reframe_case_studies(student_data, name),
            "test_highlights": self._reframe_test_scores(student_data, name),
            "assignment_highlights": self._reframe_assignments(student_data, name),
            "project_highlights": self._reframe_projects(student_data, name),
            "learning_metrics": self._learning_metrics(student_data),
            "consistency_statement": self._consistency_statement(computed, name),
            "growth_statement": self._growth_statement(computed, name),
            "engagement_statement": self._engagement_statement(student_data, name),
        }

    def _generate_headline(self, d: Dict, role_matches: list = None) -> str:
        """v12.6: Single best-fit role + concise punch line.
        Previously this returned the top 3 roles concatenated with ' | ' which
        crowded the hero. Now: one role (the highest match) followed by a
        ' · ' separator and a tagline derived from the student's strongest
        differentiator (education / projects / current course track).
        """
        # Priority 1: scored role matches from RoleMatcher
        primary = None
        if role_matches and len(role_matches) > 0:
            primary = role_matches[0].get("role_title")

        # Priority 2: derive directly from enrolled courses
        if not primary:
            courses = d.get("courses", []) or []
            if courses:
                try:
                    intel = self.course_intel.analyze(courses)
                    if intel.get("roles"):
                        primary = intel["roles"][0]
                except Exception as e:
                    logger.info(f"course_intel headline derivation failed: {e}")

        # Priority 3: current designation
        if not primary:
            personal = d.get("personal", {}) or {}
            designation = (personal.get("current_designation") or "").strip()
            employer = (personal.get("current_employer") or "").strip()
            if designation:
                return f"{designation} at {employer}" if employer else designation

        if not primary:
            return "Financial Services Professional"

        tagline = self._build_punch_line(d, primary, role_matches)
        return f"{primary} · {tagline}" if tagline else primary

    def _build_punch_line(self, d: Dict, primary_role: str, role_matches: list = None) -> str:
        """Compose a short, factual tagline that highlights the student's edge.
        Examples this produces:
          'B.Tech CSE bridging Full-Stack Python into FinTech'
          'BFSI track with 3 Industry-Validated Certificates'
          'Banking Foundation + Payments & Cards specialist'
        Always factual — pulls from real data only, never invented.
        """
        bits = []

        # Education signal
        edu_list = d.get("education", []) or d.get("lms_education", []) or []
        edu_short = None
        for edu in edu_list[:1]:
            deg = (edu.get("degree") or "").strip()
            field = (edu.get("field_of_study") or "").strip()
            if deg and field:
                edu_short = f"{deg} {field}"
            elif deg:
                edu_short = deg

        # Strongest tech skill cluster (if primary role is non-BFSI)
        tech_cluster = None
        skills_obj = d.get("all_skills") or {}
        tech_skills = skills_obj.get("technical_skills") or []
        if tech_skills:
            top = tech_skills[0] if isinstance(tech_skills[0], str) else (tech_skills[0].get("name") if isinstance(tech_skills[0], dict) else "")
            if top:
                tech_cluster = top

        # Current LMS track (for BFSI roles)
        completed = []
        for c in (d.get("courses", []) or []):
            cs = (c.get("completion_status") or "").lower()
            pct = c.get("progress_percentage") or 0
            if cs == "completed" or pct >= 100:
                completed.append(c.get("course_name") or "")
        completed = [c for c in completed if c][:2]

        # Build tagline based on role category cues
        role_lower = primary_role.lower()
        is_tech_role = any(t in role_lower for t in ("developer", "engineer", "data analyst", "data scientist", "fintech", "product analyst"))

        if is_tech_role and edu_short and tech_cluster:
            bits.append(f"{edu_short} bridging {tech_cluster} into FinTech")
        elif edu_short and completed:
            bits.append(f"{edu_short} · {' + '.join(completed)} certified")
        elif completed:
            bits.append(f"{' + '.join(completed)} certified")
        elif edu_short:
            bits.append(edu_short)

        return bits[0] if bits else ""

    def _top_achievements(self, d: Dict, name: str) -> List[Dict]:
        achievements = []
        case_studies = d.get("case_studies", [])
        if case_studies:
            best_case = max(case_studies, key=lambda x: float(x.get("score", 0) or 0))
            score = float(best_case.get("score", 0) or 0)
            max_score = float(best_case.get("max_score", 100) or 100)
            pct = round(score / max(max_score, 1) * 100, 1)
            title = best_case.get("title", "Case Study")
            concepts = best_case.get("key_concepts", [])
            concept_text = f" covering {', '.join(concepts[:3])}" if concepts else ""
            achievements.append({
                "type": "case_study", "score": pct,
                "statement": f"{_hash_pick(name, ACHIEVEMENT_VERBS, 1)} {title}{concept_text}, achieving {pct}% score",
                "metric": f"{pct}%", "label": title,
            })
        test_scores = d.get("test_scores", [])
        if test_scores:
            best_test = max(test_scores, key=lambda x: float(x.get("percentage", 0) or 0))
            pct = float(best_test.get("percentage", 0) or 0)
            subject = best_test.get("subject", "Assessment")
            achievements.append({
                "type": "test", "score": pct,
                "statement": f"Scored {pct}% in {subject}",
                "metric": f"{pct}%", "label": subject,
            })
        courses = d.get("courses", [])
        completed_courses = [c for c in courses if c.get("completed_at")]
        if completed_courses:
            achievements.append({
                "type": "course_completion", "score": len(completed_courses) * 25,
                "statement": f"Successfully completed {len(completed_courses)} professional training course{'s' if len(completed_courses) > 1 else ''}",
                "metric": str(len(completed_courses)), "label": "Courses completed",
            })
        computed = d.get("computed", {})
        avg_quiz = computed.get("avg_quiz_score", 0)
        total_quizzes = computed.get("total_quizzes", 0)
        if total_quizzes >= 3 and avg_quiz >= 60:
            achievements.append({
                "type": "quiz_consistency", "score": avg_quiz,
                "statement": f"Maintained {avg_quiz}% average across {total_quizzes} assessments",
                "metric": f"{avg_quiz}%", "label": f"Avg across {total_quizzes} quizzes",
            })
        improvement = computed.get("improvement_pct", 0)
        if improvement > 10:
            achievements.append({
                "type": "growth", "score": improvement,
                "statement": f"Demonstrated {improvement}% performance improvement from first to recent assessments",
                "metric": f"+{improvement}%", "label": "Score improvement",
            })
        achievements.sort(key=lambda x: x["score"], reverse=True)
        return achievements[:5]

    def _reframe_case_studies(self, d: Dict, name: str) -> List[Dict]:
        cases = d.get("case_studies", [])
        if not cases: return []
        sorted_cases = sorted(cases, key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:3]
        highlights = []
        for i, cs in enumerate(sorted_cases):
            score = float(cs.get("score", 0) or 0)
            max_score = float(cs.get("max_score", 100) or 100)
            pct = round(score / max(max_score, 1) * 100, 1)
            title = cs.get("title", "Case Study")
            concepts = cs.get("key_concepts", [])
            strengths = cs.get("ai_strengths", [])
            word_count = cs.get("word_count", 0) or 0
            verb = _hash_pick(name, ACHIEVEMENT_VERBS, i + 10)
            parts = [f"{verb} '{title}'"]
            if concepts and isinstance(concepts, list):
                parts.append(f"covering {', '.join(concepts[:3])}")
            description = " ".join(parts) + f". Scored {pct}%"
            if word_count > 500:
                description += f" with a {word_count}-word analytical submission"
            if strengths and isinstance(strengths, list) and isinstance(strengths[0], str):
                description += f". Key strength: {strengths[0][:100]}"
            highlights.append({
                "title": title, "course": "", "score_pct": pct,
                "description": description,
                "concepts": concepts[:5] if isinstance(concepts, list) else [],
                "grade": cs.get("ai_grade", ""),
            })
        return highlights

    def _reframe_test_scores(self, d: Dict, name: str) -> List[Dict]:
        tests = d.get("test_scores", [])
        if not tests: return []
        sorted_tests = sorted(tests, key=lambda x: float(x.get("percentage", 0) or 0), reverse=True)[:3]
        highlights = []
        for i, t in enumerate(sorted_tests):
            pct = float(t.get("percentage", 0) or 0)
            subject = t.get("subject", "Assessment")
            grade = t.get("grade", "")
            time_taken = t.get("time_taken_minutes", 0) or 0
            verb = _hash_pick(name, PERFORMANCE_VERBS, i + 20)
            desc = f"{verb} {subject} with {pct}% score"
            if grade: desc += f" — Grade: {grade}"
            if time_taken > 0: desc += f". Completed in {time_taken} minutes"
            highlights.append({"subject": subject, "score_pct": pct, "description": desc, "grade": grade, "course": ""})
        return highlights

    def _reframe_assignments(self, d: Dict, name: str) -> List[Dict]:
        assignments = d.get("assignments", [])
        if not assignments: return []
        graded = [a for a in assignments if a.get("score") and a.get("status") in ("graded", "reviewed", "completed")]
        if not graded: return []
        sorted_asgn = sorted(graded, key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:3]
        highlights = []
        for i, a in enumerate(sorted_asgn):
            score = float(a.get("score", 0) or 0)
            max_score = float(a.get("max_score", 100) or 100)
            pct = round(score / max(max_score, 1) * 100, 1) if max_score > 0 else 0
            title = a.get("title", "Assignment")
            feedback = (a.get("feedback") or "")[:150]
            verb = _hash_pick(name, LEARNING_VERBS, i + 30)
            desc = f"{verb} '{title}'"
            if pct > 0: desc += f" — scored {pct}%"
            if feedback: desc += f". Feedback: {feedback}"
            highlights.append({"title": title, "score_pct": pct, "description": desc, "course": ""})
        return highlights

    def _reframe_projects(self, d: Dict, name: str) -> List[Dict]:
        projects = d.get("projects", [])
        if not projects: return []
        highlights = []
        for i, p in enumerate(projects[:5]):
            title = p.get("title", "Project")
            desc = p.get("description", "")
            techs = p.get("technologies_used", [])
            feedback = p.get("mentor_feedback", "")
            tech_text = f" using {', '.join(techs[:4])}" if techs else ""
            feedback_text = f'. Mentor noted: "{feedback[:100]}"' if feedback else ""
            statement = f"{_hash_pick(name, ACHIEVEMENT_VERBS, i + 40)} {title}{tech_text}{feedback_text}"
            highlights.append({"title": title, "description": desc[:200] if desc else "", "technologies": techs, "statement": statement})
        return highlights

    def _learning_metrics(self, d: Dict) -> Dict:
        computed = d.get("computed", {})
        return {
            "overall_score": computed.get("overall_score", 0),
            "total_courses": computed.get("total_courses", 0),
            "completed_courses": computed.get("completed_courses", 0),
            "total_tests": computed.get("total_tests", 0),
            "total_quizzes": computed.get("total_quizzes", 0),
            "total_case_studies": computed.get("total_case_studies", 0),
            "total_assignments": computed.get("total_assignments", 0),
            "best_test_score": computed.get("best_test_score", 0),
            "avg_test_score": computed.get("avg_test_score", 0),
            "avg_quiz_score": computed.get("avg_quiz_score", 0),
            "avg_case_study_score": computed.get("avg_case_study_score", 0),
            "total_hours": computed.get("total_hours", 0),
            "improvement_pct": computed.get("improvement_pct", 0),
            "consistency_score": computed.get("consistency_score", 0),
        }

    def _consistency_statement(self, computed: Dict, name: str) -> str:
        consistency = computed.get("consistency_score", 0)
        total = computed.get("total_tests", 0) + computed.get("total_quizzes", 0) + computed.get("total_case_studies", 0)
        if total < 3: return ""
        if consistency >= 85: return f"Exceptional consistency with {consistency}% score stability across {total} assessments"
        if consistency >= 70: return f"Reliable performance with {consistency}% consistency across {total} assessments"
        if consistency >= 50: return f"Building consistency across {total} assessments"
        return ""

    def _growth_statement(self, computed: Dict, name: str) -> str:
        improvement = computed.get("improvement_pct", 0)
        if improvement > 20: return f"Rapid growth — {improvement}% improvement from early to recent assessments"
        if improvement > 10: return f"Positive growth with {improvement}% score improvement"
        if improvement > 5: return f"Steady upward trajectory with {improvement}% cumulative improvement"
        return ""

    def _engagement_statement(self, d: Dict, name: str) -> str:
        computed = d.get("computed", {})
        parts = []
        hours = computed.get("total_hours", 0)
        if hours > 0: parts.append(f"{round(hours, 1)} hours of learning")
        active_days = computed.get("active_days", 0)
        if active_days > 0: parts.append(f"{active_days} active days")
        lessons = computed.get("lessons_watched", 0)
        if lessons > 0: parts.append(f"{lessons} lessons completed")
        if not parts: return ""
        return f"Platform engagement: {', '.join(parts)}"