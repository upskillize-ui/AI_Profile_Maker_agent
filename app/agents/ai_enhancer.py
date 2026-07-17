"""
AI Enhancer — v1.0 (Companion module)
═══════════════════════════════════════════════════════════════════════════

Companion module that PLUGS INTO the existing summary_agent + ai_polisher
pipeline. Does not replace either.

Adds five capabilities the existing pipeline doesn't have:

  1. Abusive-input filter for student-provided text fields (bio, hobbies,
     about_me, career_goals). Runs BEFORE anything reaches the AI. If the
     student wrote something offensive, that field is silently skipped
     from the profile (not replaced with "Not provided" — just excluded).

  2. Groundedness gate. After summary_agent or ai_polisher produces
     content, we check that every skill/claim it makes traces back to
     raw source data. Failing content is rejected and re-run once with
     stronger constraints; if still failing, fall back to next tier.

  3. Semantic filler check. Soft signal — flags content that reads as
     mostly generic filler ("dedicated learner", "passionate about
     growth", etc.) even after banned-phrase stripping. Triggers a re-run.

  4. Layered fallback tiers:
       Tier 1: LMS data → strict grounding
       Tier 2: External sources (LinkedIn + resume + GitHub) → strict grounding
       Tier 3: Contextual minimal (enrolled courses + career goal only)
       Tier 4: Skip field entirely (nothing to say honestly)
     Every fallback is logged so we can review which students hit low tiers.

  5. Cache-then-regenerate strategy. The summary is cached under a hash
     of the underlying data. If the data hasn't changed, we serve the
     cached summary — no LLM call. Cheap and consistent.

INTEGRATION POINT
──────────────────
In profile_orchestrator.py's generation flow, wrap the summary call:

    # Old:
    summary = await self.summary_agent.generate(student_data)

    # New:
    from app.agents.ai_enhancer import AIEnhancer
    enhancer = AIEnhancer(self.summary_agent, self.ai_polisher)
    summary = await enhancer.generate_summary(student_data, merged_data)

    # Same for other text fields:
    hobbies_clean  = enhancer.clean_field(personal.get("hobbies"),      "hobbies")
    bio_clean      = enhancer.clean_field(personal.get("bio"),          "bio")
    goals_clean    = enhancer.clean_field(personal.get("career_goals"), "career_goals")

The wrapping is minimal. Everything else in the pipeline stays as-is.
"""

import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from app.services.cache_service import CacheService

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# ABUSE FILTER
# ═══════════════════════════════════════════════════════════════════════
#
# Two lists — profanity and hate speech — checked as substrings on the
# lowercased input. Also matches common leetspeak substitutions and
# common Hindi transliterations of the same words.
#
# The lists are intentionally not exhaustive; they catch the obvious
# cases (slurs, curse words) without over-blocking normal speech. Casual
# words like "damn" or "hell" are NOT blocked — the goal is to catch
# clearly offensive content, not to be a swear-word filter.
#
# When any word matches, the ENTIRE field is discarded. We don't try to
# "clean" abusive input — it goes straight to skip.

_PROFANITY = {
    # Explicit English profanity
    "fuck", "fucking", "fucked", "fck", "f*ck", "f**k",
    "shit", "sh1t", "sh!t", "shitty",
    "bitch", "b1tch", "biatch",
    "asshole", "a**hole", "arsehole",
    "cunt", "twat",
    "dick", "d1ck", "cock",
    "bastard", "wanker",

    # Hindi/Urdu transliterations of common slurs
    "bhosdi", "bhosdike", "bhosdiwale", "bhosdika",
    "madarchod", "madarchood", "mc", "mchd",
    "behenchod", "behnchod", "bc", "bhnchod",
    "chutiya", "chutiye", "chodu", "chodo",
    "randi", "raand", "kutta", "kutti", "kutte",
    "gaandu", "gaand", "gand", "chinaal", "chinal",

    # Slurs (any language, any target group)
    "nigger", "nigga", "n1gger", "nigg3r",
    "faggot", "fag",
    "retard", "retarded",
}

_HATE_INDICATORS = {
    # Only very high-signal phrases; don't over-block normal discussion
    "kill yourself", "kys", "go die",
    "should die", "deserves to die",
    "hate all", "hate every",
}


def _is_abusive(text: str) -> bool:
    """Return True if the input contains any profanity or hate indicator."""
    if not text or not isinstance(text, str):
        return False
    normalized = text.lower()
    # Collapse repeated chars (e.g. "fuuuuck" → "fuck")
    normalized = re.sub(r"(.)\1{2,}", r"\1\1", normalized)
    # Strip non-alphanumeric for substring matching
    stripped = re.sub(r"[^a-z0-9\s]", "", normalized)
    words = set(stripped.split())

    if _PROFANITY & words:
        return True
    for phrase in _HATE_INDICATORS:
        if phrase in normalized:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════
# GROUNDEDNESS CHECK
# ═══════════════════════════════════════════════════════════════════════
#
# For each claim in the output, verify a matching signal exists in the
# raw source data. Because natural language claims are fuzzy (e.g. "wrote
# Python microservices" vs raw data "Python: 4 repositories"), we do
# noun-and-skill extraction, not sentence-level entailment. Claims that
# introduce nouns/skills NOT anchored in source data trigger a rejection.

# Words that appear in output but should NEVER count as invented (they're
# grammatical glue, not claims).
_GROUND_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "in", "to", "for", "at", "on",
    "with", "by", "as", "is", "was", "are", "were", "be", "been", "being",
    "has", "have", "had", "do", "does", "did", "will", "would", "should",
    "could", "may", "might", "can", "shall", "must",
    "this", "that", "these", "those", "his", "her", "its", "their", "our",
    "student", "candidate", "learner", "professional", "fresher", "graduate",
    "course", "courses", "program", "programs", "training", "class", "classes",
    "score", "scores", "grade", "grades", "test", "tests", "assessment",
    "case", "study", "studies", "assignment", "assignments", "project",
    "projects", "work", "role", "roles", "job", "jobs", "career", "goal",
    "goals", "domain", "industry", "field", "sector", "team", "teams",
    "upskillize", "lms", "platform", "system", "tool", "tools",
    "developed", "built", "created", "implemented", "designed", "used",
    "applied", "learned", "trained", "completed", "achieved", "focused",
    "attempted", "submitted", "reviewed", "graded", "practiced",
    "one", "two", "three", "four", "five", "six", "seven", "eight",
    "nine", "ten", "first", "second", "third",
    "percent", "percentage", "average", "best", "top", "high", "strong",
    "good", "solid", "clear", "specific", "particular",
    "year", "years", "month", "months", "week", "weeks", "day", "days",
    "banking", "finance", "financial",  # domain terms, usually safe
    "python", "sql", "excel", "java",  # tools frequently in LMS skills
    "on", "at", "in", "from", "to", "into", "onto",
    # v1.1 — platform-activity vocabulary. These describe WHAT KIND of
    # activity happened, not invented claims; treating them as claims
    # made the gate reject virtually every honest summary (prod 17 Jul
    # 2026, "All tiers failed"). Facts (names, scores, employers) are
    # still checked — none of these words can smuggle in a fake skill.
    "enrolled", "enrolling", "enrollment", "mock", "interview", "interviews",
    "capstone", "capstones", "practice", "practiced", "practising",
    "scored", "scoring", "targeting", "targeted", "band", "bands",
    "reached", "reaching", "including", "included", "scenario", "scenarios",
    "session", "sessions", "quiz", "quizzes", "module", "modules",
    "certificate", "certificates", "certification", "certifications",
    "hackathon", "hackathons", "punctuality", "attendance", "attended",
    "highest", "lowest", "latest", "recent", "recently", "currently",
    "coursework", "curriculum", "cohort", "batch", "track", "tracks",
    "earned", "finished", "finishing", "pursuing", "preparing",
}


def _extract_content_words(text: str) -> set:
    """Extract meaningful content words (potential claims) from output."""
    if not text:
        return set()
    words = re.findall(r"[a-zA-Z][a-zA-Z0-9-]+", text.lower())
    return {w for w in words if len(w) > 2 and w not in _GROUND_STOPWORDS}


# ═══════════════════════════════════════════════════════════════════════
# v1.2 TARGETED GROUNDEDNESS
# ═══════════════════════════════════════════════════════════════════════
#
# The v1.0/v1.1 gate compared EVERY content word against source vocabulary.
# In production (17 Jul 2026) that rejected every honest summary — the
# "invented" words were connective English ('across', 'spanning', 'both',
# 'suggests') that no stopword list can enumerate. Each rejection burned
# up to 6 LLM calls and shipped the minimal fallback line.
#
# The rewritten gate checks only what hallucination actually looks like:
#   1. TECH/SKILL terms in the output that are absent from source
#      (fake "Kubernetes", "TensorFlow", "AWS" expertise)
#   2. Mid-sentence PROPER NOUNS absent from source
#      (fake employers, institutions, certifications)
#   3. NUMBERS absent from source (fake "scored 92%", "500+ users";
#      small counts ≤ 12 are allowed — "2 of 4 courses" is arithmetic,
#      not invention)
# Zero tolerance on those three. Ordinary prose can never trip it.

_TECH_LEXICON = {
    "python", "java", "javascript", "typescript", "golang", "rust", "kotlin",
    "swift", "ruby", "php", "scala", "matlab", "cobol",
    "react", "angular", "vue", "svelte", "nextjs", "django", "flask",
    "fastapi", "spring", "express", "rails", "laravel",
    "nodejs", "node", "dotnet",
    "mysql", "postgresql", "postgres", "mongodb", "redis", "oracle",
    "sqlite", "dynamodb", "cassandra", "elasticsearch",
    "aws", "azure", "gcp", "kubernetes", "docker", "terraform", "ansible",
    "jenkins", "graphql", "kafka", "spark", "hadoop", "airflow",
    "tensorflow", "pytorch", "keras", "scikit-learn", "pandas", "numpy",
    "tableau", "powerbi", "selenium", "figma", "photoshop",
    "blockchain", "solidity", "ethereum", "devops", "microservices",
    "scrum", "kanban",
}

_SAFE_PROPER_NOUNS = {
    "upskillize", "i", "ai", "bfsi", "lms", "india", "indian", "english",
    "hindi", "career", "profile", "summary",
    "january", "february", "march", "april", "may", "june", "july",
    "august", "september", "october", "november", "december",
}


def _collect_source_parts(source_data: Dict) -> List[str]:
    """All raw text fragments from source data (shared by vocab + blob)."""
    parts = []

    personal = source_data.get("personal", {}) or {}
    for k in ("full_name", "current_designation", "current_employer",
              "education_level", "institution", "field_of_study",
              "key_skills", "career_goals", "preferred_role", "hobbies",
              "industries", "languages_known", "bio", "about_me",
              "work_experience_years", "graduation_year"):
        v = personal.get(k)
        if v:
            parts.append(str(v))

    for c in source_data.get("courses", []) or []:
        parts.extend([str(c.get("course_name", "")), str(c.get("description", "")),
                      str(c.get("category", "")), str(c.get("progress_percentage", ""))])

    for section in ("case_studies", "assignments", "capstones",
                    "industry_sessions", "mock_tests", "mock_interviews",
                    "assessments", "test_scores"):
        for item in source_data.get(section, []) or []:
            if not isinstance(item, dict):
                continue
            for f in ("title", "topic", "brief", "course_name", "band",
                      "insight_text", "session_type", "role", "quiz_title",
                      "level", "company", "focus", "speaker", "difficulty",
                      "exam_name", "test_name", "grade", "status",
                      "score", "percentage"):
                v = item.get(f)
                if v is not None and v != "":
                    parts.append(str(v))

    for cert in source_data.get("certifications", []) or []:
        parts.extend([str(cert.get("name", "")), str(cert.get("certificate_name", "")),
                      str(cert.get("issuer", ""))])

    # v1.3: merged education / work_experience / skills / projects so the
    # summary can cite experience & qualifications without the gate flagging
    # those words as "invented".
    for edu in source_data.get("education", []) or []:
        if isinstance(edu, dict):
            parts.extend([str(edu.get("degree", "")), str(edu.get("field_of_study", "")),
                          str(edu.get("institution", "")), str(edu.get("year", ""))])
    for w in source_data.get("work_experience", []) or []:
        if isinstance(w, dict):
            parts.extend([str(w.get("title", "")), str(w.get("company", "")),
                          str(w.get("duration", "")), str(w.get("description", ""))])
    allsk = source_data.get("all_skills", {}) or {}
    if isinstance(allsk, dict):
        for grp in allsk.values():
            for s in (grp or []):
                parts.append(str(s.get("name", "")) if isinstance(s, dict) else str(s))
    for p in source_data.get("projects", []) or []:
        if isinstance(p, dict):
            parts.extend([str(p.get("title", "")), str(p.get("name", "")),
                          str(p.get("description", ""))])
    gh = source_data.get("github_profile", {}) or {}
    if isinstance(gh, dict):
        for lang in (gh.get("languages", {}) or {}):
            parts.append(str(lang))

    perso = source_data.get("personality", {}) or {}
    parts.append(str(perso.get("personality_type", "")))
    parts.append(str(perso.get("summary", "")))
    parts.extend([str(t) for t in perso.get("traits", []) or []])

    computed = source_data.get("computed", {}) or {}
    for k in ("overall_score", "best_test_score", "avg_test_score",
              "completed_courses", "total_courses"):
        v = computed.get(k)
        if v is not None:
            parts.append(str(v))

    for section_key in ("linkedin_data", "github_data", "resume_data"):
        ext = source_data.get(section_key) or {}
        if isinstance(ext, dict):
            for v in ext.values():
                if isinstance(v, str):
                    parts.append(v)
                elif isinstance(v, list):
                    parts.extend(str(x) for x in v if x)

    return parts


def _invented_numbers(output: str, source_blob: str) -> List[str]:
    """Numbers in output that don't trace to source. Small counts (≤ 12)
    are allowed — '3 courses', '2 of 4' are arithmetic, not invention."""
    allowed = set()
    for n in re.findall(r"\d+(?:\.\d+)?", source_blob):
        allowed.add(n)
        try:
            allowed.add(str(int(float(n))))
            allowed.add(str(round(float(n))))
        except ValueError:
            pass
    bad = []
    for n in re.findall(r"\d+(?:\.\d+)?", output):
        try:
            val = float(n)
        except ValueError:
            continue
        if val <= 12:
            continue
        if n in allowed or str(int(val)) in allowed or str(round(val)) in allowed:
            continue
        bad.append(n)
    return bad


def _invented_proper_nouns(output: str, source_vocab: set) -> List[str]:
    """Mid-sentence capitalized tokens (names/orgs/products) not in source."""
    bad = []
    for sentence in re.split(r"(?<=[.!?])\s+|\n", output):
        words = re.findall(r"[A-Za-z][A-Za-z0-9&.+'-]*", sentence)
        # Skip leading bullet word(s): first alphabetic token of a sentence
        for i, w in enumerate(words):
            if i == 0:
                continue
            if not w[0].isupper() or len(w) < 3:
                continue
            wl = w.lower().strip(".&'-")
            if wl in _SAFE_PROPER_NOUNS or wl in _GROUND_STOPWORDS:
                continue
            if wl not in source_vocab:
                bad.append(w)
    return bad


def _extract_source_vocabulary(source_data: Dict) -> set:
    """Collect every meaningful noun/skill/term from the source data."""
    return _extract_content_words(" ".join(_collect_source_parts(source_data)))


def _extract_source_vocabulary_legacy(source_data: Dict) -> set:
    """(unused since v1.2 — kept for reference during launch week)"""
    parts = []

    personal = source_data.get("personal", {}) or {}
    for k in ("full_name", "current_designation", "current_employer",
              "education_level", "institution", "field_of_study",
              "key_skills", "career_goals", "preferred_role", "hobbies",
              "industries", "languages_known", "bio", "about_me"):
        v = personal.get(k)
        if v:
            parts.append(str(v))

    for c in source_data.get("courses", []) or []:
        parts.extend([c.get("course_name", ""), c.get("description", ""),
                      c.get("category", "")])

    # v1.1: mock_interviews added (was missing — interview roles/companies
    # counted as "invented" words and helped fail the gate), plus extra
    # per-item fields that legitimately appear in summaries.
    for section in ("case_studies", "assignments", "capstones",
                    "industry_sessions", "mock_tests", "mock_interviews",
                    "assessments", "test_scores"):
        for item in source_data.get(section, []) or []:
            if not isinstance(item, dict):
                continue
            for f in ("title", "topic", "brief", "course_name", "band",
                      "insight_text", "session_type", "role", "quiz_title",
                      "level", "company", "focus", "speaker", "difficulty",
                      "exam_name", "test_name", "grade", "status"):
                v = item.get(f)
                if v:
                    parts.append(str(v))

    for cert in source_data.get("certifications", []) or []:
        parts.extend([cert.get("name", ""), cert.get("certificate_name", ""),
                      cert.get("issuer", "")])

    perso = source_data.get("personality", {}) or {}
    parts.append(str(perso.get("personality_type", "")))
    parts.append(str(perso.get("summary", "")))
    parts.extend([str(t) for t in perso.get("traits", []) or []])

    # External sources (from data_merger — after merge)
    for section_key in ("linkedin_data", "github_data", "resume_data"):
        ext = source_data.get(section_key) or {}
        if isinstance(ext, dict):
            for v in ext.values():
                if isinstance(v, str):
                    parts.append(v)
                elif isinstance(v, list):
                    parts.extend(str(x) for x in v if x)

    return _extract_content_words(" ".join(parts))


def _check_groundedness(output: str, source_data: Dict) -> Tuple[bool, List[str]]:
    """
    Return (is_grounded, invented_words). is_grounded=False when the output
    contains meaningful content words that don't appear in the source.
    """
    # v1.2 TARGETED gate — see block comment above _TECH_LEXICON.
    # Checks the three shapes hallucination actually takes; never polices
    # connective English (the v1.0/v1.1 failure mode).
    source_vocab = _extract_source_vocabulary(source_data)
    source_blob = " ".join(_collect_source_parts(source_data)).lower()

    output_lower = output.lower()
    output_words = _extract_content_words(output)

    invented = []

    # 1. Tech/skill terms claimed but absent from source
    for term in _TECH_LEXICON:
        if term in output_words and term not in source_vocab \
                and term not in source_blob:
            invented.append(f"tech:{term}")

    # 2. Proper nouns (employers, institutions, products) absent from source
    for noun in _invented_proper_nouns(output, source_vocab):
        invented.append(f"name:{noun}")

    # 3. Metrics absent from source (fake scores, fake user counts)
    for num in _invented_numbers(output, source_blob):
        invented.append(f"number:{num}")

    # Zero tolerance — these are precise checks, not fuzzy ones.
    is_grounded = len(invented) == 0
    return is_grounded, invented[:20]  # cap for logging


# ═══════════════════════════════════════════════════════════════════════
# SEMANTIC FILLER CHECK
# ═══════════════════════════════════════════════════════════════════════
#
# Even after banned-phrase stripping, some output reads as generic filler.
# We detect this by counting the ratio of "signal words" (specific
# nouns/skills from source data) to total meaningful words. If the ratio
# is too low, the output is mostly filler.

_FILLER_MIN_SIGNAL_RATIO = 0.25   # at least 25% of content words must map to source


def _check_semantic_signal(output: str, source_data: Dict) -> Tuple[bool, float]:
    """
    Return (has_signal, ratio). has_signal=False when the output is
    dominated by generic language rather than specific facts from source.
    """
    output_words = _extract_content_words(output)
    if not output_words:
        return True, 0.0  # empty output is a separate problem, don't false-flag
    source_words = _extract_source_vocabulary(source_data)
    matched = output_words & source_words
    ratio = len(matched) / len(output_words)
    return ratio >= _FILLER_MIN_SIGNAL_RATIO, ratio


# ═══════════════════════════════════════════════════════════════════════
# LAYERED FALLBACK
# ═══════════════════════════════════════════════════════════════════════
#
# Four tiers, in order. Each tier passes a different `source_data` to
# the summary_agent, from richest to poorest.

TIER_LMS = "lms"
TIER_EXTERNAL = "external"
TIER_CONTEXTUAL = "contextual"
TIER_SKIP = "skip"


def _build_tier_source(base_data: Dict, tier: str) -> Optional[Dict]:
    """
    Return a source_data dict scoped to what the given tier is allowed to see.
    Tier lms: everything.
    Tier external: personal + LinkedIn + resume + GitHub (no LMS activity).
    Tier contextual: only courses + career_goal + preferred_role.
    Tier skip: None → caller should return empty.
    """
    if tier == TIER_LMS:
        return base_data

    if tier == TIER_EXTERNAL:
        # Keep external sources, strip LMS activity keys
        d = dict(base_data)
        for k in ("case_studies", "assignments", "capstones",
                  "industry_sessions", "mock_tests", "mock_interviews",
                  "assessments", "test_scores",
                  "attendance", "punctuality", "computed"):
            d[k] = [] if isinstance(base_data.get(k), list) else {}
        return d

    if tier == TIER_CONTEXTUAL:
        personal = base_data.get("personal", {}) or {}
        return {
            "personal": {
                "full_name":      personal.get("full_name", ""),
                "career_goals":   personal.get("career_goals", ""),
                "preferred_role": personal.get("preferred_role", ""),
            },
            "courses": base_data.get("courses", [])[:4],  # first 4 courses only
            "case_studies": [], "assignments": [], "capstones": [],
            "industry_sessions": [], "mock_tests": [], "mock_interviews": [],
            "attendance": {}, "punctuality": {}, "personality": {},
            "certifications": [], "computed": {},
        }

    return None


# ═══════════════════════════════════════════════════════════════════════
# HASH-BASED CACHING
# ═══════════════════════════════════════════════════════════════════════

def _summary_cache_key(user_id: int, source_data: Dict) -> str:
    """Deterministic key over the fields that affect the summary."""
    signature_parts = []
    personal = source_data.get("personal", {}) or {}
    for k in ("full_name", "current_designation", "current_employer",
              "work_experience_years", "education_level", "institution",
              "field_of_study", "key_skills", "career_goals", "preferred_role"):
        signature_parts.append(str(personal.get(k, "")))
    signature_parts.append(str(len(source_data.get("courses", []) or [])))
    signature_parts.append(str(len(source_data.get("case_studies", []) or [])))
    signature_parts.append(str(len(source_data.get("assignments", []) or [])))
    signature_parts.append(str(len(source_data.get("capstones", []) or [])))
    signature_parts.append(str(len(source_data.get("mock_tests", []) or [])))
    signature_parts.append(str(len(source_data.get("mock_interviews", []) or [])))
    signature_parts.append(str(len(source_data.get("assessments", []) or [])))

    # Include best-score numbers so a new high score invalidates the cache
    for section in ("case_studies", "assignments", "capstones", "assessments"):
        items = source_data.get(section, []) or []
        scores = [i.get("score") for i in items if i.get("score") is not None]
        signature_parts.append(f"{section}:{max(scores) if scores else 0}")

    sig = "|".join(signature_parts)
    return f"summary:v1:{user_id}:{hashlib.md5(sig.encode()).hexdigest()}"


# ═══════════════════════════════════════════════════════════════════════
# MAIN CLASS
# ═══════════════════════════════════════════════════════════════════════

class AIEnhancer:
    """
    Wraps summary_agent + ai_polisher with strict validation, layered
    fallback, and hash-based caching. Also filters abusive student input.

    Usage in profile_orchestrator.py:

        enhancer = AIEnhancer(self.summary_agent, self.ai_polisher)
        summary = await enhancer.generate_summary(student_data, merged_data)
        clean_hobbies = enhancer.clean_field(personal.get("hobbies"), "hobbies")
    """

    def __init__(self, summary_agent, ai_polisher=None):
        self.summary_agent = summary_agent
        self.ai_polisher = ai_polisher

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC: summary with strict validation + layered fallback
    # ══════════════════════════════════════════════════════════════════

    async def generate_summary(self,
                                student_data: Dict[str, Any],
                                merged_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a validated summary using layered fallback.
        Returns a non-empty string in all cases (Tier 4 returns a minimal
        contextual line, never a raw NaN or empty string).
        """
        user_id = (student_data.get("personal", {}) or {}).get("user_id", 0)
        merged = merged_data or {}

        # v1.3 FIX: generate FROM the merged profile (LMS + resume + LinkedIn +
        # GitHub), not raw LMS data. merged is a superset of student_data and
        # adds parsed education, work_experience, and all_skills — so the
        # summary can actually use experience + qualifications, not just LMS.
        source_data = dict(merged) if merged else dict(student_data)
        # Guard: if a thin merged dict was passed, keep the LMS essentials.
        for _k in ("personal", "computed", "personality", "courses",
                   "case_studies", "assignments", "capstones", "mock_tests",
                   "mock_interviews", "assessments", "certifications"):
            if not source_data.get(_k) and student_data.get(_k):
                source_data[_k] = student_data[_k]
        # nested raw external dicts (extra grounding vocab)
        if merged.get("linkedin_data"):
            source_data["linkedin_data"] = merged["linkedin_data"]
        if merged.get("resume_data"):
            source_data["resume_data"] = merged["resume_data"]
        if merged.get("github_data"):
            source_data["github_data"] = merged["github_data"]

        # ─── Cache check ──────────────────────────────────────────
        cache_key = _summary_cache_key(user_id, source_data)
        cached = CacheService.get(cache_key)
        if cached and isinstance(cached, dict) and cached.get("summary"):
            logger.info(f"Summary cache hit for user_id={user_id}")
            return cached["summary"]

        # ─── Try each tier in order ────────────────────────────────
        for tier in (TIER_LMS, TIER_EXTERNAL, TIER_CONTEXTUAL):
            tier_source = _build_tier_source(source_data, tier)
            if tier_source is None:
                continue

            summary = await self._try_tier(tier, tier_source, retry_once=True)
            if summary:
                # Cache the winning summary
                CacheService.set(cache_key, {"summary": summary, "tier": tier}, ttl=86400)
                logger.info(f"Summary generated at tier={tier} for user_id={user_id}")
                return summary

        # ─── Tier 4: absolute last resort ──────────────────────────
        logger.warning(f"All tiers failed for user_id={user_id}, using minimal fallback")
        return self._minimal_fallback(source_data)

    async def _try_tier(self, tier: str, source_data: Dict, retry_once: bool = True) -> Optional[str]:
        """Attempt one tier. Retries once with a stronger constraint hint."""
        try:
            summary = await self.summary_agent.generate(source_data)
        except Exception as e:
            logger.warning(f"summary_agent.generate raised at tier={tier}: {e}")
            return None

        if not summary or not summary.strip():
            return None

        # ─── Groundedness gate ─────────────────────────────────────
        grounded, invented = _check_groundedness(summary, source_data)
        if not grounded:
            # warning (not info): app loggers default to WARNING on the
            # Space, so info-level rejections were invisible in logs.
            logger.warning(f"Groundedness failed at tier={tier}, invented={invented}")
            if retry_once:
                # Retry once — summary_agent has its own variety_seed so
                # a second call will produce different phrasing.
                return await self._try_tier(tier, source_data, retry_once=False)
            return None

        # ─── Semantic filler gate (soft) ───────────────────────────
        has_signal, ratio = _check_semantic_signal(summary, source_data)
        if not has_signal:
            logger.info(f"Filler signal low at tier={tier}, ratio={ratio:.2f}")
            if retry_once:
                return await self._try_tier(tier, source_data, retry_once=False)
            # Accept anyway — better than nothing at this tier

        return summary.strip()

    def _minimal_fallback(self, source_data: Dict) -> str:
        """Absolute last resort — a factual single line, never empty."""
        personal = source_data.get("personal", {}) or {}
        courses = source_data.get("courses", []) or []
        course_names = [c.get("course_name", "") for c in courses if c.get("course_name")][:2]

        parts = []
        if course_names:
            parts.append(f"Enrolled in {' and '.join(course_names)} at Upskillize.")
        goal = (personal.get("career_goals") or personal.get("preferred_role") or "")
        goal = goal.strip().rstrip(".")   # avoid "years.." double-period
        if goal:
            parts.append(f"Career direction: {goal}.")

        if not parts:
            return "Upskillize learner building career readiness through structured coursework."
        return " ".join(parts)

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC: per-field text cleaning
    # ══════════════════════════════════════════════════════════════════

    def clean_field(self, raw_text: Optional[str], field_name: str) -> str:
        """
        Sanitize a single student-provided text field (bio, hobbies,
        about_me, career_goals). Returns empty string if:
          - Input is None or empty
          - Input contains profanity or hate indicators (silently skipped)
          - Input is too short to be meaningful

        Otherwise returns the input with basic grammar/case fixes.

        This does NOT re-run through the LLM. Field-level rewriting happens
        only for the summary. Individual fields are cleaned in-place.
        """
        if not raw_text or not isinstance(raw_text, str):
            return ""

        text = raw_text.strip()
        if len(text) < 3:
            return ""

        if _is_abusive(text):
            logger.warning(f"Abusive input detected in field={field_name}, skipping")
            return ""

        # Basic grammar/case fixes
        text = self._basic_clean(text)
        return text

    def _basic_clean(self, text: str) -> str:
        """Capitalize sentences, collapse whitespace, fix common misspells."""
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()

        # Capitalize first letter of each sentence
        sentences = re.split(r"(?<=[.!?])\s+", text)
        cleaned = []
        for s in sentences:
            s = s.strip()
            if s:
                s = s[0].upper() + s[1:] if len(s) > 1 else s.upper()
                cleaned.append(s)
        text = " ".join(cleaned)

        # Ensure trailing period
        if text and text[-1] not in ".!?":
            text += "."

        # Fix common lowercase-i-as-pronoun
        text = re.sub(r"\bi\b", "I", text)
        text = re.sub(r"\bi'm\b", "I'm", text, flags=re.IGNORECASE)
        text = re.sub(r"\bi've\b", "I've", text, flags=re.IGNORECASE)
        text = re.sub(r"\bi'll\b", "I'll", text, flags=re.IGNORECASE)

        return text