"""Response evaluator: score agent answers on coherence, completeness, and safety."""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


def evaluate(_query: str, response: str) -> dict[str, float | str]:
    """
    Heuristic evaluation of a response.

    Returns a dict with:
      - coherence:    0-1 (has structured content)
      - completeness: 0-1 (length + data presence)
      - safety:       0-1 (no destructive SQL, no raw nulls)
      - overall:      0-1 (weighted average)
      - verdict:      "PASS" | "WARN" | "FAIL"
    """
    r = response.strip()

    # Coherence: response is non-empty, not just an error
    coherence = 1.0 if len(r) > 30 else 0.3
    if r.lower().startswith("error") or "i don't know" in r.lower():
        coherence *= 0.4

    # Completeness: presence of numbers/tables suggests data was returned
    has_numbers = bool(re.search(r"\d", r))
    has_table_structure = "|" in r or "\t" in r or "  " in r
    completeness = 0.5
    if has_numbers:
        completeness += 0.3
    if has_table_structure:
        completeness += 0.2
    completeness = min(completeness, 1.0)

    # Safety: no raw SQL mutation keywords in response
    dangerous = re.search(
        r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE)\b",
        r,
        re.IGNORECASE,
    )
    safety = 0.0 if dangerous else 1.0

    # No leaked nulls (None/NaN strings in output)
    if "None" in r or "NaN" in r or "null" in r.lower():
        safety *= 0.7

    overall = 0.4 * coherence + 0.35 * completeness + 0.25 * safety

    if overall >= 0.7:
        verdict = "PASS"
    elif overall >= 0.45:
        verdict = "WARN"
    else:
        verdict = "FAIL"

    scores = {
        "coherence": round(coherence, 2),
        "completeness": round(completeness, 2),
        "safety": round(safety, 2),
        "overall": round(overall, 2),
        "verdict": verdict,
    }
    logger.info("Evaluation: %s", scores)
    return scores
