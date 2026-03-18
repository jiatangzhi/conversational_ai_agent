# CLAUDE.md — Project Context for AI Assistants

## What this project is

An ETL pipeline supervisor agent built with the Anthropic Claude API. It runs a full
extract-transform-load pipeline in a background thread and answers natural-language
queries about the resulting data in real time, with guardrails, episodic memory, and
automated response evaluation.

## Entry point

```bash
python3 agent.py          # interactive REPL
python3 agent.py "query"  # batch mode
```

Requires `ANTHROPIC_API_KEY` in the environment.

## Key files

| File | Purpose |
|---|---|
| `agent.py` | Main loop, thread management, `ask_agent()`, interactive/batch modes |
| `tools.py` | All 7 Claude tools (`@beta_tool`). This is where tool logic lives. |
| `evaluator.py` | Scores responses: coherence, completeness, safety → PASS/WARN/FAIL |
| `guardrails/validator.py` | SQL allowlist/blocklist + PII column stripping |
| `memory/store.py` | JSON-backed episodic memory (persist + retrieve interactions) |
| `etl/extract.py` | Loads CSVs from `data/` |
| `etl/transform.py` | Normalizes columns, removes null user_ids, computes aggregates |
| `etl/load.py` | Writes DataFrames to SQLite `warehouse.db` |

## Model

`claude-haiku-4-5` with `thinking: {"type": "adaptive"}`.
To change the model, update the `model=` argument in `ask_agent()` in `agent.py`.

## Tool pattern

All tools use `@beta_tool` from `anthropic`. Schemas are auto-generated from type
annotations and docstrings. To add a new tool:
1. Define it in `tools.py` with `@beta_tool`
2. Add it to `ALL_TOOLS` in `agent.py`

## Guardrails — never bypass

`guardrails/validator.py` enforces two rules on every `query_warehouse` call:
- Only `SELECT` / `WITH` queries allowed — no `DROP`, `DELETE`, `UPDATE`, `INSERT`, etc.
- Sensitive columns (`email`, `phone`, `password`, `credit_card`, `signup_date`) are
  stripped before any data reaches the response

These must remain active. Do not disable or weaken them.

## Memory

`memory/store.py` persists every interaction to `logs/memory.json` (rolling window of
20 entries). `get_memory_context()` is a registered tool — Claude calls it when
follow-up questions reference prior results.

## Evaluation

`evaluator.py` is heuristic-based (no LLM calls). Weights:
- coherence 40%, completeness 35%, safety 25%
- `PASS` ≥ 0.7 · `WARN` ≥ 0.45 · `FAIL` < 0.45

## Threading model

- `_run_pipeline_thread` runs as `daemon=True` — killed automatically if main exits.
- `_pipeline_lock` guards `_pipeline` dict — always acquire before read or write.
- Batch mode calls `pipeline_thread.join()` before queries. Interactive mode does not.

## Warehouse tables

| Table | Rows | Description |
|---|---|---|
| `curated_data` | 35 | Joined users + sales + products, normalized |
| `dau` | 7 | Daily Active Users (2026-03-11 → 2026-03-17) |
| `sales_by_product` | 10 | Revenue and units per product |

Tables are replaced on every ETL run (`if_exists="replace"`).

## Logging

- File: `logs/supervisor.log` (INFO level, file handler only)
- All tools use `logger = logging.getLogger(__name__)`
- Evaluation scores logged at INFO after every query

## What NOT to do

- Do not add `INSERT`, `UPDATE`, or schema-mutation logic to `etl/load.py` without
  updating guardrails accordingly.
- Do not store raw DataFrames in memory — only serializable strings go into `memory.json`.
- Do not remove `sanitize_dataframe()` from the `query_warehouse` tool path.
- Do not change `conversation.append({"role": "assistant", "content": response_text.strip()})`
  to store full content blocks — thinking blocks in history break multi-turn calls.
