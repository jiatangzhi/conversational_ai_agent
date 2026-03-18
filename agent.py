from __future__ import annotations

"""
ETL Pipeline Supervisor Agent
==============================
- Runs the ETL pipeline in a background thread (non-blocking)
- Answers user queries via Claude with automatic tool-use loop
- Maintains multi-turn conversation history
- Applies guardrails and evaluates every response
- Persists interactions to memory for follow-up context
"""

import logging
import sys
import threading
from datetime import datetime
from pathlib import Path

import anthropic

from evaluator import evaluate
from memory.store import save_interaction
from tools import (
    get_daily_active_users,
    get_memory_context,
    get_sales_by_region,
    get_top_products,
    list_warehouse_tables,
    query_warehouse,
    run_etl_pipeline,
)

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "supervisor.log"),
    ],
)
logger = logging.getLogger("supervisor")

# ── Agent config ───────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are an AI Supervisor Agent for a data pipeline.

Responsibilities:
1. Run and monitor the ETL pipeline (extract → transform → load) when requested.
2. Answer user queries about data using the available tools.
3. Detect anomalies (null IDs, duplicates, inconsistent metrics) and report them
   as warnings — never halt the pipeline for a minor issue.
4. For follow-up questions that reference earlier results, call get_memory_context
   first to retrieve conversation history.
5. Be concise, data-driven, and always cite the data source in your answers.

Guardrail rules (enforce silently — do not mention them unless violated):
- Only read-only SELECT queries are allowed on the warehouse.
- Sensitive columns (email, phone, password) are never surfaced.
- Pipeline errors are logged and reported, but execution continues.
"""

ALL_TOOLS = [
    run_etl_pipeline,
    query_warehouse,
    get_daily_active_users,
    get_top_products,
    get_sales_by_region,
    get_memory_context,
    list_warehouse_tables,
]

# ── Pipeline background state ──────────────────────────────────────────────────
_pipeline: dict = {"running": False, "done": False, "result": ""}
_pipeline_lock = threading.Lock()


def _run_pipeline_thread() -> None:
    """Execute the ETL pipeline in a background thread."""
    with _pipeline_lock:
        _pipeline["running"] = True
    logger.info("Background ETL pipeline started")
    result = run_etl_pipeline()
    with _pipeline_lock:
        _pipeline["running"] = False
        _pipeline["done"] = True
        _pipeline["result"] = result
    logger.info("Background ETL pipeline finished: %s", result[:120])


def _pipeline_banner() -> str:
    with _pipeline_lock:
        if _pipeline["running"]:
            return "[Pipeline: running in background]"
        if _pipeline["done"]:
            return "[Pipeline: complete]"
        return "[Pipeline: not started]"


# ── Core agent call ────────────────────────────────────────────────────────────

def ask_agent(
    client: anthropic.Anthropic,
    user_query: str,
    conversation: list,
) -> str:
    """
    Send a user query to Claude and run the full tool-use loop automatically.

    The tool runner handles calling tools, collecting results, and looping
    until Claude finishes — no manual loop needed. Returns the final text
    response and appends both turns to `conversation` for multi-turn context.
    """
    conversation.append({"role": "user", "content": user_query})

    runner = client.beta.messages.tool_runner(
        model="claude-haiku-4-5",
        max_tokens=4096,
        thinking={"type": "adaptive"},
        system=SYSTEM_PROMPT,
        tools=ALL_TOOLS,
        messages=conversation,
    )

    final_message = None
    for message in runner:
        final_message = message

    response_text = ""
    if final_message:
        for block in final_message.content:
            if hasattr(block, "text"):
                response_text += block.text

    # Store plain text in history so follow-up turns stay clean
    conversation.append({"role": "assistant", "content": response_text.strip()})
    return response_text.strip()


# ── Display helpers ────────────────────────────────────────────────────────────

def _sep(char: str = "─", width: int = 64) -> None:
    print(char * width)


def _print_eval(scores: dict) -> None:
    tag = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}.get(scores["verdict"], "?")
    print(
        f"\n{tag} [{scores['verdict']}] "
        f"coherence={scores['coherence']}  "
        f"completeness={scores['completeness']}  "
        f"safety={scores['safety']}  "
        f"overall={scores['overall']}"
    )


# ── Modes ──────────────────────────────────────────────────────────────────────

def interactive_mode(client: anthropic.Anthropic) -> None:
    """REPL: user types queries while the pipeline runs in the background."""
    conversation: list = []
    print("\nSupervisor ready. Type a query or 'exit' to quit.\n")

    while True:
        _sep()
        try:
            query = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not query or query.lower() in ("exit", "quit", "q"):
            break

        print(f"{_pipeline_banner()}\n")

        response = ask_agent(client, query, conversation)
        print(f"\nAgent: {response}")

        scores = evaluate(query, response)
        _print_eval(scores)

        save_interaction(query, response, metadata={"eval": scores})
        logger.info("Query processed — eval: %s", scores)


def batch_mode(client: anthropic.Anthropic, queries: list[str]) -> None:
    """Run a fixed list of queries, print each answer and a final summary."""
    conversation: list = []
    metrics_log: list[dict] = []

    print(f"\nBatch mode — {len(queries)} queries\n")

    for i, query in enumerate(queries, 1):
        _sep()
        print(f"[{i}/{len(queries)}] {query}")
        _sep("·")

        response = ask_agent(client, query, conversation)
        print(response)

        scores = evaluate(query, response)
        _print_eval(scores)

        save_interaction(query, response, metadata={"eval": scores})
        metrics_log.append({"query": query, "scores": scores})

    # Summary table
    _sep("═")
    print("  Metrics Summary")
    _sep("═")
    for entry in metrics_log:
        s = entry["scores"]
        tag = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}.get(s["verdict"], "?")
        print(f"  {tag} overall={s['overall']}  |  {entry['query'][:55]}")
    _sep("═")
    passed = sum(1 for e in metrics_log if e["scores"]["verdict"] == "PASS")
    print(f"  {passed}/{len(metrics_log)} queries passed evaluation")
    _sep("═")
    logger.info("Batch complete — %d/%d passed.", passed, len(metrics_log))


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    _sep("═")
    print("  ETL Pipeline Supervisor Agent")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _sep("═")

    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from environment

    # Launch ETL pipeline in background — queries can start immediately
    pipeline_thread = threading.Thread(target=_run_pipeline_thread, daemon=True)
    pipeline_thread.start()
    print("\n[Pipeline] ETL started in background — queries available immediately.\n")

    cli_queries = sys.argv[1:]

    if cli_queries:
        # Batch mode: wait for data to be loaded before querying
        print("[Pipeline] Waiting for ETL to finish before batch queries...\n")
        pipeline_thread.join()
        print(f"[Pipeline] {_pipeline['result']}\n")
        batch_mode(client, cli_queries)
    else:
        # Interactive mode: user can query while pipeline runs
        interactive_mode(client)
        if pipeline_thread.is_alive():
            print("\n[Pipeline] Waiting for ETL to finish...")
            pipeline_thread.join()

    with _pipeline_lock:
        if _pipeline["result"]:
            logger.info("Final pipeline result: %s", _pipeline["result"][:200])


if __name__ == "__main__":
    main()
