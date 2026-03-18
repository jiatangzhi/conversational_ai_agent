"""Conversation memory: persist recent queries and responses to JSON."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MEMORY_PATH = Path(__file__).parent.parent / "logs" / "memory.json"
MAX_ENTRIES = 20


def _load() -> list[dict[str, Any]]:
    if MEMORY_PATH.exists():
        try:
            return json.loads(MEMORY_PATH.read_text())
        except json.JSONDecodeError:
            logger.warning("Memory file corrupted — resetting")
    return []


def _save(entries: list[dict[str, Any]]) -> None:
    MEMORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    MEMORY_PATH.write_text(json.dumps(entries, indent=2, ensure_ascii=False))


def save_interaction(query: str, response: str, metadata: dict[str, Any] | None = None) -> None:
    """Append a query/response pair to memory."""
    entries = _load()
    entries.append(
        {
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "response": response,
            "metadata": metadata or {},
        }
    )
    # Keep only the last MAX_ENTRIES
    entries = entries[-MAX_ENTRIES:]
    _save(entries)
    logger.debug("Memory updated (%d entries)", len(entries))


def get_recent(n: int = 5) -> list[dict[str, Any]]:
    """Return the last n interactions."""
    return _load()[-n:]


def get_last_context() -> str:
    """Return a formatted string of the last few interactions for prompt injection."""
    recent = get_recent(5)
    if not recent:
        return "No previous interactions."
    lines = []
    for entry in recent:
        ts = entry["timestamp"][:16].replace("T", " ")
        lines.append(f"[{ts}] User: {entry['query']}")
        lines.append(f"       Agent: {entry['response'][:200]}{'...' if len(entry['response']) > 200 else ''}")
    return "\n".join(lines)


def clear() -> None:
    """Wipe conversation memory."""
    _save([])
    logger.info("Memory cleared")
