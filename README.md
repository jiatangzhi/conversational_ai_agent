# Conversational AI Agent — ETL Pipeline Supervisor

A production-oriented conversational agent built with the **Anthropic Claude API** that monitors a data pipeline end-to-end, responds to natural-language queries in real time, enforces safety guardrails, persists episodic memory, and evaluates every response automatically.

> Designed to demonstrate applied AI engineering across conversational systems, memory & retrieval, agent orchestration, safety guardrails, and evaluation — the core competencies for production LLM development.

---

## System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        agent.py  (entry point)                   │
│                                                                  │
│   ┌──────────────┐        ┌───────────────────────────────────┐  │
│   │  Background  │        │       Conversational Layer        │  │
│   │   Thread     │        │                                   │  │
│   │              │        │  system_prompt (policy + persona) │  │
│   │  ETL Pipeline│        │  conversation[] (dialogue state)  │  │
│   │  extract()   │        │  ask_agent()  ──► tool_runner     │  │
│   │  transform() │        │                      │            │  │
│   │  load()      │        │            ┌─────────▼──────────┐ │  │
│   │              │        │            │   Tool Dispatcher  │ │  │
│   │  warehouse.db│        │            │  (7 @beta_tools)   │ │  │
│   └──────────────┘        │            └─────────┬──────────┘ │  │
│         │                 └───────────────────────┼───────────┘  │
│         │                                         │              │
│   ┌─────▼────────────────────────────────────────▼────────────┐  │
│   │                    Supporting Modules                     │  │
│   │                                                           │  │
│   │  guardrails/validator.py   ← SQL validation, PII filter   │  │
│   │  memory/store.py           ← episodic memory (JSON)       │  │
│   │  evaluator.py              ← coherence / safety scoring   │  │
│   │  logs/supervisor.log       ← structured runtime logs      │  │
│   └───────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Capability Map

| Component | File | Competency |
|---|---|---|
| System prompt + dialogue state | `agent.py` | Conversational system design |
| Multi-turn conversation history | `agent.py` → `ask_agent()` | Dialogue flow & coherence |
| Episodic memory (JSON store) | `memory/store.py` | Long-term memory & retrieval |
| `get_memory_context` tool | `tools.py` | Structured memory access patterns |
| 7 `@beta_tool` functions | `tools.py` | Agent orchestration & tool use |
| Background thread (ETL) | `agent.py` → `threading` | Async task execution |
| SQL guardrail + PII filter | `guardrails/validator.py` | Safety & compliance-by-design |
| Heuristic response evaluator | `evaluator.py` | Evaluation framework & KPIs |
| Structured logging | `logs/supervisor.log` | Monitoring & feedback loops |

---

## 1 · Conversational System Engineering

The agent uses a **layered architecture** that separates concerns cleanly:

```
Policy layer    →  SYSTEM_PROMPT   (rules, persona, escalation logic)
Memory layer    →  conversation[]  (dialogue state, full turn history)
Execution layer →  tool_runner     (Claude decides which tools to invoke)
```

The **system prompt** (`agent.py`) encodes the agent's behavioral policy:
- Role definition and responsibilities
- Anomaly detection instructions (report, never halt)
- Memory retrieval trigger (`get_memory_context` on ambiguous follow-ups)
- Silent guardrail enforcement rules

**Dialogue state** is maintained as a growing `conversation` list passed to every Claude call, enabling coherent multi-turn interactions:

```python
conversation.append({"role": "user", "content": user_query})
# ... tool_runner resolves all tool calls automatically ...
conversation.append({"role": "assistant", "content": response_text})
```

---

## 2 · Memory, Retrieval & Personalization

`memory/store.py` implements **episodic memory** — every user query and agent response is persisted to `logs/memory.json` with timestamps and evaluation metadata.

```python
# Persistence after every interaction
save_interaction(query, response, metadata={"eval": scores})

# Structured retrieval for follow-up context
def get_last_context() -> str:
    """Returns last 5 interactions formatted for prompt injection."""
```

The `get_memory_context` tool exposes memory to Claude as a **callable retrieval step**, allowing the agent to resolve references like *"that product"* or *"yesterday's anomaly"* by explicitly fetching prior context before answering.

Memory hygiene is enforced via a rolling window (`MAX_ENTRIES = 20`) and graceful JSON corruption recovery.

---

## 3 · Agent Orchestration & Tool Use

Seven tools registered with `@beta_tool` — the Anthropic SDK generates JSON schemas automatically from Python type annotations:

```python
@beta_tool
def get_daily_active_users(start_date: str = "", end_date: str = "") -> str:
    """DAU — distinct active users per day, filterable by date range."""

@beta_tool
def query_warehouse(sql: str) -> str:
    """Read-only SQL against warehouse.db. Guardrails enforced inside."""
```

The **tool runner** (`client.beta.messages.tool_runner`) handles the full agentic loop — Claude decides which tools to call, the SDK executes them and feeds results back, iteration continues until a final response is produced. No manual loop required.

The **ETL pipeline** runs in a `threading.Thread` (daemon), decoupling data ingestion from the conversational interface:

```
Thread 1 (background):  extract() → transform() → load() → warehouse.db
Thread 2 (main):        REPL → ask_agent() → tool_runner → response
```

---

## 4 · Safety, Guardrails & Compliance-by-Design

`guardrails/validator.py` implements two independent safety layers applied on every tool invocation:

**Layer 1 — SQL injection & destructive operation prevention**
```python
# Allowlist: only SELECT / WITH (CTEs)
if not re.match(r"^\s*(SELECT|WITH)\b", stripped, re.IGNORECASE):
    raise GuardrailError("Only SELECT queries are allowed.")

# Blocklist: no destructive keywords anywhere in the statement
_DESTRUCTIVE_PATTERNS = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE|EXEC|PRAGMA)\b",
    re.IGNORECASE,
)
```

**Layer 2 — PII / sensitive column filtering**
```python
_SENSITIVE_COLUMNS = {"email", "phone", "password", "credit_card", "signup_date"}

def sanitize_dataframe(df) -> pd.DataFrame:
    cols_to_drop = [c for c in df.columns if c.lower() in _SENSITIVE_COLUMNS]
    return df.drop(columns=cols_to_drop)
```

Guardrails surface as structured messages (`GUARDRAIL VIOLATION: ...`) rather than exceptions — the pipeline continues and the agent communicates the violation naturally. This mirrors **safe-completion patterns** in production LLM systems.

---

## 5 · Evaluation, Monitoring & Quality Assurance

`evaluator.py` scores every agent response on three axes and derives a verdict printed inline after each reply:

| Metric | Weight | What it measures |
|---|---|---|
| `coherence` | 40% | Response is substantive and not an error message |
| `completeness` | 35% | Presence of numeric data and/or table structure |
| `safety` | 25% | No destructive SQL keywords or leaked null values |
| `verdict` | — | `PASS` ≥ 0.7  ·  `WARN` ≥ 0.45  ·  `FAIL` < 0.45 |

Scores are persisted alongside every interaction in `memory.json`, enabling **retrospective analysis** of response quality over time. Structured runtime logs (`logs/supervisor.log`) capture every pipeline event, query, and tool result for monitoring and feedback loops.

---

## 6 · Production Engineering

- **Type-safe tool schemas** auto-generated from Python annotations via `@beta_tool` — no manual JSON schema maintenance
- **Thread-safe pipeline state** managed with `threading.Lock` — no race conditions on shared status flags
- **Graceful degradation** at every layer: tools return structured error strings (never raise into the agent loop), guardrail violations are surfaced as natural language
- **Two execution modes** sharing the same agent core: interactive REPL and batch CLI
- **Dual logging**: file (`supervisor.log`) + stdout for visibility in both terminal and log aggregators

---

## Data Model

```
data/users.csv      (15 rows)   user_id, Name, Region, signup_date
data/sales.csv      (35 rows)   sale_id, UserID, ProductID, Amount, Quantity, SaleDate
data/products.csv   (10 rows)   product_id, ProductName, Category, Price
          │
          ▼  etl/transform.py
          │
          │  normalize_columns()          snake_case all column names
          │  drop_null_user_ids()         anomaly detection + removal
          │  build_curated()              3-way join: users + sales + products
          │  compute_dau()                daily active users aggregate
          │  compute_sales_by_product()   revenue ranking aggregate
          ▼
warehouse.db
  ├── curated_data      (35 rows, 12 cols)   enriched transactions
  ├── dau               ( 7 rows,  2 cols)   daily unique buyers
  └── sales_by_product  (10 rows,  4 cols)   revenue per product
```

---

## Project Structure

```
conversational_ai_agent/
├── agent.py              # Entry point — supervisor agent (conversational loop)
├── tools.py              # @beta_tool definitions (Claude tool use)
├── evaluator.py          # Heuristic response evaluator (KPIs)
├── requirements.txt
│
├── etl/
│   ├── extract.py        # Load CSVs from data/
│   ├── transform.py      # Clean, normalize, aggregate
│   └── load.py           # Write to SQLite warehouse
│
├── guardrails/
│   └── validator.py      # SQL validation + sensitive column stripping
│
├── memory/
│   └── store.py          # Episodic memory (persist / retrieve interactions)
│
├── data/
│   ├── users.csv
│   ├── sales.csv
│   └── products.csv
│
└── logs/                 # Generated at runtime
    ├── supervisor.log    # Structured event log
    └── memory.json       # Conversation memory store
```

---

## Setup & Usage

```bash
# 1. Virtual environment
python3 -m venv .venv && source .venv/bin/activate

# 2. Dependencies
pip install -r requirements.txt

# 3. API key
export ANTHROPIC_API_KEY="sk-ant-..."

# Interactive mode — ETL runs in background, query immediately
python3 agent.py

# Batch mode — waits for ETL, runs all queries, prints metrics summary
python3 agent.py \
  "Were there any null user_id values detected?" \
  "Show DAU trend for all available dates" \
  "Top 3 products by revenue" \
  "Sales breakdown by region"
```

**Example session:**
```
════════════════════════════════════════════════════════════════
  ETL Pipeline Supervisor Agent  ·  2026-03-18 17:27:01
════════════════════════════════════════════════════════════════
[Pipeline] ETL started in background — queries available immediately.

You: Show me DAU for all dates
[Pipeline: complete]

Agent: **Daily Active Users**
Total unique users across period: 28  ·  Average DAU: 4.0

sale_date  | dau
---------- | ---
2026-03-11 | 5
2026-03-12 | 4
...

✓ [PASS] coherence=1.0  completeness=1.0  safety=1.0  overall=1.0
```

---

## Stack

| Layer | Technology |
|---|---|
| LLM | Claude Haiku 4.5 via Anthropic Python SDK |
| Tool use | `@beta_tool` decorator + `client.beta.messages.tool_runner` |
| Concurrency | `threading.Thread` (daemon, non-blocking ETL) |
| Data processing | pandas |
| Warehouse | SQLite |
| Memory | JSON flat file with rolling window |
| Logging | Python `logging` (rotating file + stdout) |
