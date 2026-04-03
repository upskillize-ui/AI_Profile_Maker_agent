"""Rubric Grading Agent — disabled in v4. Profile generation doesn't need it."""
class RubricGradingAgent:
    async def grade(self, *args, **kwargs):
        return {"error": "Rubric grading disabled in v4", "score": 0}
