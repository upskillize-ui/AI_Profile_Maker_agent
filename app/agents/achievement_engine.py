"""
Achievement Engine v4 — The Translator
═══════════════════════════════════════
Converts raw LMS data into professional achievements.
Every sentence is backed by REAL numbers from the database.
Zero invented data. Zero generic phrases.

This is what makes each profile unique — the student's actual
numbers, courses, and projects are different, so the output
is automatically unique even without AI.
"""

import hashlib
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# VERB BANKS — rotated per student via name hash
# ═══════════════════════════════════════════════

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
    """Pick from list using name hash — ensures variety per student."""
    h = int(hashlib.md5(f"{name}{offset}".encode()).hexdigest()[:8], 16)
    return items[h % len(items)]


class AchievementEngine:
    """Transforms raw student data into professional-grade achievements."""

    def generate_all(self, student_data: Dict[str, Any], role_matches: list = None) -> Dict[str, Any]:
        """Generate all achievement sections from real student data."""
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

    # ─── Dynamic Headline ────────────────────────────────

    def _generate_headline(self, d: Dict, role_matches: list = None) -> str:
        """Generate headline from ELIGIBLE JOB ROLES — not course names.
        
        Shows what the student CAN BECOME, not what they're studying.
        Looks like a LinkedIn headline, not an LMS report.
        """
        # If we have role matches, use top 2-3 role titles as headline
        if role_matches and len(role_matches) > 0:
            top_roles = [r["role_title"] for r in role_matches[:3]]
            return " | ".join(top_roles)

        # Fallback: derive eligible roles from course keywords
        courses = d.get("courses", [])
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")]

        if not course_names:
            return "Finance & Banking Professional"

        text = " ".join(course_names).lower()

        # Map courses to professional role titles (not course names!)
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

        if not roles:
            roles = ["Banking Professional", "Financial Services Analyst"]

        # Pick top 2-3 unique roles
        seen = set()
        unique_roles = []
        for r in roles:
            if r not in seen:
                seen.add(r)
                unique_roles.append(r)
            if len(unique_roles) >= 3:
                break

        return " | ".join(unique_roles)

    # ─── Top Achievements (best 3-5 across all categories) ───

    def _top_achievements(self, d: Dict, name: str) -> List[Dict]:
        """Find the absolute best achievements across all data."""
        achievements = []

        # Best case study
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
                "type": "case_study",
                "score": pct,
                "statement": f"{_hash_pick(name, ACHIEVEMENT_VERBS, 1)} {title}{concept_text}, achieving {pct}% score",
                "metric": f"{pct}%",
                "label": title,
            })

        # Best test score
        test_scores = d.get("test_scores", [])
        if test_scores:
            best_test = max(test_scores, key=lambda x: float(x.get("percentage", 0) or 0))
            pct = float(best_test.get("percentage", 0) or 0)
            subject = best_test.get("subject", "Assessment")
            course = best_test.get("course_name", "")
            context = ""

            achievements.append({
                "type": "test",
                "score": pct,
                "statement": f"Scored {pct}% in {subject}{context}",
                "metric": f"{pct}%",
                "label": subject,
            })

        # Course completion
        courses = d.get("courses", [])
        completed_courses = [c for c in courses if c.get("completed_at")]
        if completed_courses:
            achievements.append({
                "type": "course_completion",
                "score": len(completed_courses) * 25,
                "statement": f"Successfully completed {len(completed_courses)} professional training course{'s' if len(completed_courses) > 1 else ''}",
                "metric": str(len(completed_courses)),
                "label": "Courses completed",
            })

        # Quiz performance
        computed = d.get("computed", {})
        avg_quiz = computed.get("avg_quiz_score", 0)
        total_quizzes = computed.get("total_quizzes", 0)
        if total_quizzes >= 3 and avg_quiz >= 60:
            achievements.append({
                "type": "quiz_consistency",
                "score": avg_quiz,
                "statement": f"Maintained {avg_quiz}% average across {total_quizzes} assessments, demonstrating consistent knowledge retention",
                "metric": f"{avg_quiz}%",
                "label": f"Avg across {total_quizzes} quizzes",
            })

        # Improvement trend
        improvement = computed.get("improvement_pct", 0)
        if improvement > 10:
            achievements.append({
                "type": "growth",
                "score": improvement,
                "statement": f"Demonstrated {improvement}% performance improvement from first to recent assessments",
                "metric": f"+{improvement}%",
                "label": "Score improvement",
            })

        # Forum contribution
        forum = d.get("forum_activity", {})
        total_forum = (forum.get("threads_created", 0) or 0) + (forum.get("replies_given", 0) or 0)
        if total_forum >= 5:
            answers = forum.get("answers_accepted", 0) or 0
            achievements.append({
                "type": "community",
                "score": total_forum * 5,
                "statement": f"Active knowledge contributor with {total_forum} forum contributions" +
                             (f" including {answers} accepted answers" if answers > 0 else ""),
                "metric": str(total_forum),
                "label": "Forum contributions",
            })

        # Sort by score, return top 5
        achievements.sort(key=lambda x: x["score"], reverse=True)
        return achievements[:5]

    # ─── Case Study Reframing ────────────────────────────

    def _reframe_case_studies(self, d: Dict, name: str) -> List[Dict]:
        """Transform case study scores into professional achievements."""
        cases = d.get("case_studies", [])
        if not cases:
            return []

        # Sort by score, take top 3
        sorted_cases = sorted(cases, key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:3]
        highlights = []

        for i, cs in enumerate(sorted_cases):
            score = float(cs.get("score", 0) or 0)
            max_score = float(cs.get("max_score", 100) or 100)
            pct = round(score / max(max_score, 1) * 100, 1)
            title = cs.get("title", "Case Study")
            course = cs.get("course_name", "")
            concepts = cs.get("key_concepts", [])
            strengths = cs.get("ai_strengths", [])
            word_count = cs.get("word_count", 0) or 0

            # Build professional description
            verb = _hash_pick(name, ACHIEVEMENT_VERBS, i + 10)

            parts = [f"{verb} '{title}'"]
            if False:  # Do not add course name to description
                pass  # Do not mention course name
            if concepts and isinstance(concepts, list) and len(concepts) > 0:
                parts.append(f"covering {', '.join(concepts[:3])}")

            description = " ".join(parts)
            description += f". Scored {pct}%"

            if word_count > 500:
                description += f" with a {word_count}-word analytical submission"

            if strengths and isinstance(strengths, list) and len(strengths) > 0:
                # Take first strength as highlight
                if isinstance(strengths[0], str):
                    description += f". Key strength: {strengths[0][:100]}"

            highlights.append({
                "title": title,
                "course": "",  # Hidden from profile
                "score_pct": pct,
                "description": description,
                "concepts": concepts[:5] if isinstance(concepts, list) else [],
                "grade": cs.get("ai_grade", ""),
            })

        return highlights

    # ─── Test Score Reframing ────────────────────────────

    def _reframe_test_scores(self, d: Dict, name: str) -> List[Dict]:
        """Transform test scores into professional achievements."""
        tests = d.get("test_scores", [])
        if not tests:
            return []

        sorted_tests = sorted(tests, key=lambda x: float(x.get("percentage", 0) or 0), reverse=True)[:3]
        highlights = []

        for i, t in enumerate(sorted_tests):
            pct = float(t.get("percentage", 0) or 0)
            subject = t.get("subject", "Assessment")
            course = t.get("course_name", "")
            time_taken = t.get("time_taken_minutes", 0) or 0
            grade = t.get("grade", "")

            verb = _hash_pick(name, PERFORMANCE_VERBS, i + 20)

            desc = f"{verb} {subject}"
            if False:  # Do not add course name to description
                desc += f" ({course})"
            desc += f" with {pct}% score"
            if grade:
                desc += f" — Grade: {grade}"
            if time_taken and time_taken > 0:
                desc += f". Completed in {time_taken} minutes"

            highlights.append({
                "subject": subject,
                "score_pct": pct,
                "description": desc,
                "grade": grade,
                "course": "",  # Hidden from profile
            })

        return highlights

    # ─── Assignment Reframing ────────────────────────────

    def _reframe_assignments(self, d: Dict, name: str) -> List[Dict]:
        """Transform assignments into professional achievements."""
        assignments = d.get("assignments", [])
        if not assignments:
            return []

        # Filter to graded assignments with scores
        graded = [a for a in assignments if a.get("score") and a.get("status") in ("graded", "reviewed", "completed")]
        if not graded:
            return []

        sorted_asgn = sorted(graded, key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:3]
        highlights = []

        for i, a in enumerate(sorted_asgn):
            score = float(a.get("score", 0) or 0)
            max_score = float(a.get("max_score", 100) or 100)
            pct = round(score / max(max_score, 1) * 100, 1) if max_score > 0 else 0
            title = a.get("title", "Assignment")
            course = a.get("course_name", "")
            feedback = (a.get("feedback") or "")[:150]

            verb = _hash_pick(name, LEARNING_VERBS, i + 30)

            desc = f"{verb} '{title}'"
            if False:  # Do not add course name to description
                desc += f" in {course}"
            if pct > 0:
                desc += f" — scored {pct}%"
            if feedback:
                desc += f". Feedback: {feedback}"

            highlights.append({
                "title": title,
                "score_pct": pct,
                "description": desc,
                "course": "",  # Hidden from profile
            })

        return highlights

    # ─── Project Reframing ───────────────────────────────

    def _reframe_projects(self, d: Dict, name: str) -> List[Dict]:
        """Transform projects into portfolio-ready highlights."""
        projects = d.get("projects", [])
        if not projects:
            return []

        highlights = []
        for i, p in enumerate(projects[:5]):
            title = p.get("title", "Project")
            desc = p.get("description", "")
            techs = p.get("technologies_used", [])
            feedback = p.get("mentor_feedback", "")

            tech_text = f" using {', '.join(techs[:4])}" if techs else ""
            feedback_text = f". Mentor noted: \"{feedback[:100]}\"" if feedback else ""

            statement = f"{_hash_pick(name, ACHIEVEMENT_VERBS, i + 40)} {title}{tech_text}{feedback_text}"

            highlights.append({
                "title": title,
                "description": desc[:200] if desc else "",
                "technologies": techs,
                "statement": statement,
            })

        return highlights

    # ─── Metric Statements ───────────────────────────────

    def _learning_metrics(self, d: Dict) -> Dict:
        """Compile key learning metrics — all from real data."""
        computed = d.get("computed", {})
        activity = d.get("platform_activity", {})

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
            "active_days": computed.get("active_days", 0),
            "lessons_watched": computed.get("lessons_watched", 0),
            "improvement_pct": computed.get("improvement_pct", 0),
            "consistency_score": computed.get("consistency_score", 0),
        }

    def _consistency_statement(self, computed: Dict, name: str) -> str:
        """Generate consistency statement from real variance data."""
        consistency = computed.get("consistency_score", 0)
        total_assessments = (computed.get("total_tests", 0) +
                             computed.get("total_quizzes", 0) +
                             computed.get("total_case_studies", 0))

        if total_assessments < 3:
            return ""

        if consistency >= 85:
            return f"Demonstrates exceptional consistency with {consistency}% score stability across {total_assessments} assessments"
        elif consistency >= 70:
            return f"Shows reliable performance with {consistency}% consistency across {total_assessments} assessments"
        elif consistency >= 50:
            return f"Building consistency across {total_assessments} assessments with diverse performance areas"
        return ""

    def _growth_statement(self, computed: Dict, name: str) -> str:
        """Generate growth statement from real improvement data."""
        improvement = computed.get("improvement_pct", 0)
        if improvement > 20:
            return f"Rapid growth trajectory — {improvement}% improvement from early to recent assessments, indicating strong learning agility"
        elif improvement > 10:
            return f"Positive growth trend with {improvement}% score improvement over the learning journey"
        elif improvement > 5:
            return f"Steady upward trajectory with {improvement}% cumulative improvement"
        return ""

    def _engagement_statement(self, d: Dict, name: str) -> str:
        """Generate engagement statement from real activity data."""
        computed = d.get("computed", {})
        hours = computed.get("total_hours", 0)
        active_days = computed.get("active_days", 0)
        lessons = computed.get("lessons_watched", 0)
        forum_threads = computed.get("forum_threads", 0)
        forum_replies = computed.get("forum_replies", 0)

        parts = []
        if hours > 0:
            parts.append(f"{round(hours, 1)} hours of learning")
        if active_days > 0:
            parts.append(f"{active_days} active days")
        if lessons > 0:
            parts.append(f"{lessons} lessons completed")
        if forum_threads + forum_replies > 0:
            parts.append(f"{forum_threads + forum_replies} community contributions")

        if not parts:
            return ""

        return f"Platform engagement: {', '.join(parts)}"

    # ─── Helpers ─────────────────────────────────────────

    def _derive_domain(self, course_names: List[str]) -> str:
        """Derive professional domain from actual course names."""
        text = " ".join(course_names).lower()
        domains = [
            (["fintech", "fin tech", "digital banking"], "FinTech & Digital Banking"),
            (["banking", "bank"], "Banking & Financial Services"),
            (["insurance", "insur"], "Insurance & Risk"),
            (["payment", "upi"], "Payment Systems & Digital Transactions"),
            (["risk", "compliance"], "Risk & Compliance"),
            (["data", "analytics", "ml", "ai"], "Data Analytics & AI"),
            (["finance", "financial"], "Finance"),
            (["investment", "wealth", "portfolio"], "Investment & Wealth Management"),
            (["credit", "lending"], "Credit & Lending Operations"),
        ]
        for keywords, domain in domains:
            if any(kw in text for kw in keywords):
                return domain
        return "Financial Services"
