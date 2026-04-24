"""
Achievement Engine v5 — Course-First Headlines
═══════════════════════════════════════════════
KEY CHANGE: Headline is derived from LMS-driven role matches FIRST.
Only falls back to education/resume data when no courses are enrolled.

Headline priority:
  1. LMS-driven role matches (from role_matcher with lms_driven=True)
  2. Course-name-derived professional roles
  3. Education + designation fallback
  4. Generic "Finance & Banking Professional"
"""

import hashlib
import logging
from typing import Dict, List, Any, Optional

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

    # ─── Dynamic Headline — COURSE-FIRST ─────────────────

    def _generate_headline(self, d: Dict, role_matches: list = None) -> str:
        """Generate headline with LMS courses as PRIMARY signal.

        Priority order:
        1. LMS-driven role matches (lms_driven=True from role_matcher)
        2. Course-name-derived professional roles
        3. Current designation + domain
        4. Generic fallback
        """
        # ── Priority 1: Use LMS-driven role matches ──
        if role_matches and len(role_matches) > 0:
            # Filter to only LMS-driven roles
            lms_roles = [r for r in role_matches if r.get("lms_driven")]
            if lms_roles:
                top_roles = [r["role_title"] for r in lms_roles[:3]]
                return " | ".join(top_roles)

            # If no LMS-driven roles, use top matches but cap at 2
            top_roles = [r["role_title"] for r in role_matches[:2]]
            return " | ".join(top_roles)

        # ── Priority 2: Derive from course names directly ──
        courses = d.get("courses", [])
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]

        if course_names:
            text = " ".join(course_names).lower()

            roles = []
            if "payment" in text or "card" in text or "upi" in text:
                roles.extend(["Digital Payments Specialist", "Payment Operations Analyst"])
            if "banking" in text or "bank" in text:
                roles.extend(["Banking Operations Executive", "Branch Operations Analyst"])
            if "credit" in text or "lending" in text:
                roles.extend(["Credit Analyst", "Lending Operations Associate"])
            if "risk" in text or "compliance" in text:
                roles.extend(["Risk & Compliance Analyst", "Compliance Associate"])
            if "fintech" in text:
                roles.extend(["FinTech Product Analyst", "Digital Banking Associate"])
            if "insurance" in text:
                roles.extend(["Insurance Analyst", "Underwriting Associate"])
            if "data" in text or "analytics" in text:
                roles.extend(["Data Analyst", "Business Intelligence Analyst"])
            if "investment" in text or "wealth" in text:
                roles.extend(["Investment Analyst", "Wealth Management Associate"])
            if "finance" in text:
                roles.extend(["Financial Analyst", "Finance Executive"])
            if "python" in text or "sql" in text:
                roles.extend(["Data Analyst", "Technology Analyst"])
            if "cbaf" in text:
                roles.extend(["Credit Analyst", "Banking Operations Executive"])
            if "adfba" in text or "pgdfba" in text:
                roles.extend(["Banking Professional", "Financial Services Analyst"])
            if "cfbm" in text:
                roles.extend(["Business Strategy Analyst", "Family Business Consultant"])

            if roles:
                seen = set()
                unique = []
                for r in roles:
                    if r not in seen:
                        seen.add(r)
                        unique.append(r)
                    if len(unique) >= 3:
                        break
                return " | ".join(unique)

        # ── Priority 3: Current designation ──
        personal = d.get("personal", {})
        designation = personal.get("current_designation", "") or ""
        employer = personal.get("current_employer", "") or ""
        if designation:
            if employer:
                return f"{designation} at {employer}"
            return designation

        # ── Priority 4: Generic fallback ──
        return "Finance & Banking Professional"

    # ─── Everything below is UNCHANGED from v4 ───────────

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
            feedback_text = f". Mentor noted: \"{feedback[:100]}\"" if feedback else ""
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
        total = (computed.get("total_tests", 0) + computed.get("total_quizzes", 0) + computed.get("total_case_studies", 0))
        if total < 3: return ""
        if consistency >= 85: return f"Demonstrates exceptional consistency with {consistency}% score stability across {total} assessments"
        if consistency >= 70: return f"Shows reliable performance with {consistency}% consistency across {total} assessments"
        if consistency >= 50: return f"Building consistency across {total} assessments with diverse performance areas"
        return ""

    def _growth_statement(self, computed: Dict, name: str) -> str:
        improvement = computed.get("improvement_pct", 0)
        if improvement > 20: return f"Rapid growth trajectory — {improvement}% improvement from early to recent assessments"
        if improvement > 10: return f"Positive growth trend with {improvement}% score improvement over the learning journey"
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