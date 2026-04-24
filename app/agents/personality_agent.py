"""
Personality Agent v1
════════════════════
Reads raw psychometric test responses and interprets them into
a structured personality profile using Claude Haiku.

Flow:
  1. data_collector passes raw psycho_result from users table
  2. If already interpreted (has personality_type) → use as-is
  3. If raw responses exist → AI interprets them
  4. If no psychometric data → returns empty (section hidden)

NEVER invents personality labels from quiz counts.
If there's no real psychometric data, the section stays empty.
"""

import os
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAS_API = bool(ANTHROPIC_API_KEY.strip())

# Empty result — profile hides the personality section
EMPTY_PERSONALITY = {
    "personality_type": "",
    "traits_json": "",
    "work_style": "",
    "communication_profile": "",
    "leadership_indicators": "",
}


class PersonalityAgent:

    def __init__(self):
        self.has_api = HAS_API

    def interpret(self, psycho_result: Any, student_name: str = "Student") -> Dict[str, str]:
        """Interpret psychometric test data into a personality profile.

        Args:
            psycho_result: Raw value from users.psycho_result column.
                           Could be: None, "default", JSON string, or dict.
            student_name: For AI prompt context.

        Returns:
            Dict with personality_type, traits_json, work_style,
            communication_profile, leadership_indicators.
            Returns EMPTY_PERSONALITY if no real data.
        """

        # ── Step 1: Parse the raw data ──
        if not psycho_result or psycho_result == "default":
            return EMPTY_PERSONALITY

        data = psycho_result
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                logger.info(f"psycho_result is not valid JSON: {data[:100] if len(data) > 100 else data}")
                return EMPTY_PERSONALITY

        if not isinstance(data, (dict, list)):
            return EMPTY_PERSONALITY

        # ── Step 2: Check if already interpreted ──
        if isinstance(data, dict) and data.get("personality_type"):
            return {
                "personality_type": data.get("personality_type", ""),
                "traits_json": data.get("traits", data.get("traits_json", "")),
                "work_style": data.get("work_style", ""),
                "communication_profile": data.get("communication", data.get("communication_profile", "")),
                "leadership_indicators": data.get("leadership", data.get("leadership_indicators", "")),
            }

        # ── Step 3: Has raw responses — interpret with AI ──
        raw_responses = self._extract_responses(data)
        if not raw_responses:
            logger.info("psycho_result exists but no interpretable responses found")
            return EMPTY_PERSONALITY

        if self.has_api:
            try:
                result = self._ai_interpret(raw_responses, student_name)
                if result:
                    return result
            except Exception as e:
                logger.warning(f"AI personality interpretation failed: {e}")

        # ── Step 4: Rule-based interpretation if AI unavailable ──
        return self._rule_based_interpret(raw_responses)

    def _extract_responses(self, data: Any) -> Optional[str]:
        """Extract a readable summary of responses from various formats.

        Handles:
          - {"responses": [...]} — array of scenario answers
          - {"scenarios": [...]} — array of scenario objects
          - {"answers": {...}} — dict of scenario_id → ranking
          - [{"scenario": "...", "ranking": [...]}] — direct array
          - {"dimension_scores": {...}} — pre-scored dimensions
          - Any dict with score-like values
        """
        if isinstance(data, dict):
            # Pre-scored dimensions (best case — already aggregated)
            if data.get("dimension_scores") or data.get("dimensions"):
                scores = data.get("dimension_scores") or data.get("dimensions")
                if isinstance(scores, dict):
                    return "Dimension scores: " + ", ".join(
                        f"{k}: {v}" for k, v in scores.items()
                    )

            # Raw responses array
            responses = data.get("responses") or data.get("scenarios") or data.get("answers")
            if responses:
                if isinstance(responses, list):
                    # Truncate to keep prompt manageable
                    summary_parts = []
                    for i, r in enumerate(responses[:25]):
                        if isinstance(r, dict):
                            scenario = r.get("scenario") or r.get("question") or r.get("title") or f"Q{i+1}"
                            ranking = r.get("ranking") or r.get("answer") or r.get("response") or r.get("options_ranked")
                            summary_parts.append(f"Scenario: {scenario}\nResponse: {ranking}")
                        else:
                            summary_parts.append(f"Q{i+1}: {r}")
                    return "\n\n".join(summary_parts)
                elif isinstance(responses, dict):
                    return "Responses: " + json.dumps(responses, indent=1, default=str)[:2000]

            # Catch-all: if it has enough keys that look like scores
            score_keys = [k for k, v in data.items() if isinstance(v, (int, float)) and k not in ("id", "student_id", "created_at")]
            if len(score_keys) >= 3:
                return "Scores: " + ", ".join(f"{k}: {data[k]}" for k in score_keys)

        elif isinstance(data, list) and len(data) >= 5:
            summary_parts = []
            for i, item in enumerate(data[:25]):
                if isinstance(item, dict):
                    scenario = item.get("scenario") or item.get("question") or f"Q{i+1}"
                    ranking = item.get("ranking") or item.get("answer") or item.get("response")
                    summary_parts.append(f"Scenario: {scenario}\nResponse: {ranking}")
                else:
                    summary_parts.append(f"Q{i+1}: {item}")
            return "\n\n".join(summary_parts)

        return None

    def _ai_interpret(self, raw_responses: str, student_name: str) -> Optional[Dict[str, str]]:
        """Use Claude Haiku to interpret psychometric responses."""
        import httpx

        prompt = f"""You are an organizational psychologist analyzing a psychometric assessment.

The student ({student_name}) completed a 25-scenario workplace psychometric test where they ranked 4 behavioral options from most to least likely for each scenario.

RAW RESPONSES:
{raw_responses[:3000]}

Based on these responses, provide a structured personality assessment. Return ONLY valid JSON:

{{
  "personality_type": "A 2-3 word professional personality label (e.g., 'Analytical Strategist', 'Collaborative Leader', 'Detail-Oriented Planner', 'Innovative Problem-Solver')",
  "traits": "3-5 key personality traits as a comma-separated string (e.g., 'Methodical, Data-driven, Collaborative, Detail-oriented, Structured')",
  "work_style": "1 sentence describing their preferred way of working (e.g., 'Prefers structured environments with clear processes and measurable outcomes')",
  "communication": "1 sentence on communication style (e.g., 'Direct and evidence-based communicator who values clarity over diplomacy')",
  "leadership": "1 sentence on team role (e.g., 'Natural coordinator who organizes team efforts around shared objectives')"
}}

RULES:
- Base assessment ONLY on the actual responses provided
- Use professional, recruiter-appropriate language
- Personality type should be 2-3 words, professional, and positive
- Do NOT use generic filler — every statement must reflect the specific response patterns
- Return ONLY JSON, no markdown, no explanation"""

        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 400,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )
                resp.raise_for_status()
                text = resp.json()["content"][0]["text"].strip()

                # Clean markdown fences
                if text.startswith("```"):
                    import re
                    text = re.sub(r'^```\w*\n?', '', text)
                    text = re.sub(r'\n?```$', '', text)

                parsed = json.loads(text)
                logger.info(f"AI personality interpretation: {parsed.get('personality_type', '')}")

                return {
                    "personality_type": parsed.get("personality_type", ""),
                    "traits_json": parsed.get("traits", ""),
                    "work_style": parsed.get("work_style", ""),
                    "communication_profile": parsed.get("communication", ""),
                    "leadership_indicators": parsed.get("leadership", ""),
                }
        except Exception as e:
            logger.warning(f"AI personality interpretation failed: {e}")
            return None

    def _rule_based_interpret(self, raw_responses: str) -> Dict[str, str]:
        """Basic rule-based interpretation when AI is unavailable.
        Only returns a generic label — much less useful than AI."""

        lower = raw_responses.lower()

        # Try to detect dominant patterns from keywords
        analytical = sum(1 for kw in ["data", "analysis", "evidence", "research", "systematic", "logical"] if kw in lower)
        collaborative = sum(1 for kw in ["team", "collaborate", "group", "together", "consensus", "discuss"] if kw in lower)
        leadership = sum(1 for kw in ["lead", "initiative", "delegate", "direct", "decision", "responsibility"] if kw in lower)
        creative = sum(1 for kw in ["creative", "innovative", "new approach", "brainstorm", "experiment"] if kw in lower)
        structured = sum(1 for kw in ["plan", "organize", "schedule", "process", "systematic", "step-by-step"] if kw in lower)

        scores = {
            "Analytical Thinker": analytical,
            "Collaborative Team Player": collaborative,
            "Strategic Leader": leadership,
            "Creative Problem-Solver": creative,
            "Structured Planner": structured,
        }

        if max(scores.values()) == 0:
            # Can't determine from text — return generic
            return {
                "personality_type": "Professional",
                "traits_json": "Assessment completed",
                "work_style": "Adaptable work approach",
                "communication_profile": "",
                "leadership_indicators": "",
            }

        top_type = max(scores, key=scores.get)

        traits_map = {
            "Analytical Thinker": "Data-driven, Methodical, Evidence-based",
            "Collaborative Team Player": "Team-oriented, Communicative, Consensus-building",
            "Strategic Leader": "Decisive, Initiative-taking, Goal-oriented",
            "Creative Problem-Solver": "Innovative, Flexible, Experimental",
            "Structured Planner": "Organized, Process-oriented, Detail-focused",
        }

        return {
            "personality_type": top_type,
            "traits_json": traits_map.get(top_type, ""),
            "work_style": "Identified through psychometric assessment",
            "communication_profile": "",
            "leadership_indicators": "",
        }