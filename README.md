# ledger

The persistence layer of the CyberControl pipeline. Subscribes to every Kafka topic in the platform and writes each event into PostgreSQL as logs move through the pipeline stages.

The ledger is the only service in the platform that writes to the database. All other services read from PostgreSQL but never write to it. This means there is exactly one place where data enters the database and one clear audit trail for every log event from ingestion to resolution.

---

## Table of contents

- [How it fits in the pipeline](#how-it-fits-in-the-pipeline)
- [What it persists](#what-it-persists)
  - [logs.unfiltered](#logsunfiltered)
  - [logs.categories](#logscategories)
  - [logs.solver_plan](#logssolver_plan)
  - [logs.solution](#logssolution)
  - [analytics](#analytics)
  - [actions](#actions)
- [Timestamp coordination with the ingestor](#timestamp-coordination-with-the-ingestor)
- [Database schema](#database-schema)
- [Project structure](#project-structure)
- [Setup](#setup)
- [Environment variables](#environment-variables)
- [Running the ledger](#running-the-ledger)

---

## How it fits in the pipeline

```
logs.unfiltered  ──┐
logs.categories  ──┤
logs.solver_plan ──┤──► ledger ──► PostgreSQL ──► gateway ──► dashboard
logs.solution    ──┤
analytics        ──┤
actions          ──┘
```

The ledger sits off the main pipeline flow and consumes from all topics simultaneously. It does not block or slow down the pipeline — it is a passive observer that records everything it sees. The RAC agents (classifier, analyst, responder) publish to their topics and move on; the ledger picks up those messages and handles persistence independently.

---

## What it persists

The ledger maps each Kafka topic to one or more database tables. A single security event accumulates data across multiple topics as it progresses through the pipeline — the ledger stitches this together using `log_id` / `trace_id` as the common key.

### logs.unfiltered

The raw log document published by the ingestor. The ledger inserts a new row into the `logs` table for each message, setting `processing_stage = 'unfiltered'`.

**Fields written to `logs`:**

| DB column | Source field | Notes |
|---|---|---|
| `id` | `trace.id` | Used as the primary key and correlation ID throughout the pipeline |
| `timestamp` | `@timestamp` | Parsed from ISO 8601 |
| `message` | `message` | Raw log message text |
| `service_name` | `service.name` | Service or device that produced the log |
| `raw_severity` | `log.level` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `user_id` | `user.id` | Username if present |
| `http_status` | `http.response.status_code` | HTTP status if present |
| `process_pid` | `process.pid` | Process ID if present |
| `trace_id` | `trace.id` | Stored redundantly for query convenience |
| `processing_stage` | — | Set to `unfiltered` |
| `created_at` | — | Set to `NOW()` at insert time |

### logs.categories

The classifier's output. The ledger inserts a row into `classifications` and updates the corresponding `logs` row to set `processing_stage = 'classified'`.

**Fields written to `classifications`:**

| DB column | Source field |
|---|---|
| `log_id` | `log` → `trace.id` (correlation key) |
| `category` | `classification.category` |
| `severity` | `classification.severity` |
| `is_cybersecurity` | `classification.isCybersecurity` |
| `confidence` | `classification.classificationConfidence` |
| `reasoning` | `classification.reasoning` |
| `tags` | `classification.tags` (stored as array) |
| `created_at` | `NOW()` |

### logs.solver_plan

The analyst's investigation output. The ledger inserts a row into `threat_assessments` and updates `processing_stage = 'investigated'` on the corresponding `logs` row.

**Fields written to `threat_assessments`:**

| DB column | Source field |
|---|---|
| `log_id` | `log` → `trace.id` |
| `attack_vector` | `investigation.attackVector` |
| `ai_suggestion` | `investigation.aiSuggestion` |
| `complexity` | `investigation.complexity` |
| `recurrence_rate` | `investigation.recurrenceRate` |
| `confidence` | `investigation.confidence` |
| `auto_fixable` | `investigation.autoFixable` |
| `requires_approval` | `investigation.requiresHumanApproval` |
| `priority` | `investigation.priority` |
| `notify_teams` | `investigation.notifyTeams` (stored as array) |
| `created_at` | `NOW()` |

### logs.solution

The responder's full resolution output. This is the most data-rich write — the ledger inserts rows into `incidents`, `remediation_steps`, and `follow_up_actions`, and updates `processing_stage = 'resolved'` on the `logs` row.

**Fields written to `incidents`:**

| DB column | Source field |
|---|---|
| `incident_id` | Generated UUID |
| `log_id` | `log` → `trace.id` |
| `resolution_mode` | `resolution.resolutionMode` |
| `executive_summary` | `resolution.executiveSummary` |
| `what_happened` | `resolution.postIncidentSummary.whatHappened` |
| `impact_assessment` | `resolution.postIncidentSummary.impactAssessment` |
| `root_cause` | `resolution.postIncidentSummary.rootCause` |
| `lessons_learned` | `resolution.postIncidentSummary.lessonsLearned` |
| `resolved_at` | `resolved_at` from the payload |

**Fields written to `remediation_steps`** (one row per step in `resolution.immediateActions`):

| DB column | Source field |
|---|---|
| `log_id` | `log` → `trace.id` |
| `step_number` | `step.id` |
| `title` | `step.title` |
| `description` | `step.description` |
| `command` | `step.command` |
| `risk` | `step.risk` |
| `estimated_time` | `step.estimatedTime` |
| `rollback` | `step.rollback` |
| `auto_execute` | `step.autoExecute` |
| `requires_approval` | `step.requiresApproval` |
| `status` | `step.approvalStatus` — `auto` or `pending` |
| `created_at` | `NOW()` |

**Fields written to `follow_up_actions`** (one row per item in `resolution.followUpActions`):

| DB column | Source field |
|---|---|
| `incident_id` | The `incident_id` generated above |
| `title` | `action.title` |
| `description` | `action.description` |
| `owner` | `action.owner` |
| `deadline` | `action.deadline` |
| `created_at` | `NOW()` |

### analytics

Heartbeats and statistics published by the classifier, analyst, and responder. The ledger stores these for agent health monitoring and performance dashboards. Each message is written to an `analytics` table with the raw JSON payload preserved, keyed by `agent` and `event` type.

### actions

Human approval and denial decisions submitted via the dashboard (routed through the gateway). The ledger updates the `status` and `executed_at` columns on the corresponding `remediation_steps` row when it sees:

- `status: approved` → sets `status = 'approved'`, `executed_at = NOW()`
- `status: denied` → sets `status = 'denied'`, `executed_at = NOW()`

The ledger also handles the ingestor's timestamp coordination request on this topic — see [Timestamp coordination with the ingestor](#timestamp-coordination-with-the-ingestor).

---

## Timestamp coordination with the ingestor

When the Elasticsearch ingestor starts up it publishes a `get_last_unfiltered_timestamp` request to the `actions` topic. The ledger listens on `actions` and responds to these requests so the ingestor knows where to resume polling after a restart.

**Request message** (published by the ingestor to `actions`):
```json
{
  "action": "get_last_unfiltered_timestamp",
  "requester": "ingestor-elasticsearch-3a8f2b1c"
}
```

**What the ledger does:**
1. Detects `action == "get_last_unfiltered_timestamp"`
2. Queries the `logs` table for `MAX(timestamp)` where `processing_stage = 'unfiltered'`
3. Publishes the response back to `actions`:

```json
{
  "action": "response_last_unfiltered_timestamp",
  "requester": "ingestor-elasticsearch-3a8f2b1c",
  "timestamp": "2026-03-12T09:44:12.000Z"
}
```

The `requester` field is echoed back exactly so the ingestor can match the response to its own request, even if multiple ingestor instances are running simultaneously.

If the `logs` table is empty (first run), the ledger responds with `null` and the ingestor falls back to epoch.

---

## Database schema

The full schema the ledger writes to. All tables are created by the ledger on startup if they do not already exist.

```sql
CREATE TABLE IF NOT EXISTS logs (
    id               VARCHAR PRIMARY KEY,
    timestamp        TIMESTAMPTZ,
    message          TEXT,
    service_name     VARCHAR,
    raw_severity     VARCHAR,
    user_id          VARCHAR,
    http_status      INTEGER,
    process_pid      INTEGER,
    trace_id         VARCHAR,
    processing_stage VARCHAR,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS classifications (
    log_id           VARCHAR PRIMARY KEY REFERENCES logs(id),
    category         VARCHAR,
    severity         VARCHAR,
    is_cybersecurity BOOLEAN,
    confidence       INTEGER,
    reasoning        TEXT,
    tags             TEXT[],
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS threat_assessments (
    log_id            VARCHAR PRIMARY KEY REFERENCES logs(id),
    attack_vector     TEXT,
    ai_suggestion     TEXT,
    complexity        VARCHAR,
    recurrence_rate   INTEGER,
    confidence        INTEGER,
    auto_fixable      BOOLEAN,
    requires_approval BOOLEAN,
    priority          INTEGER,
    notify_teams      TEXT[],
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id       VARCHAR PRIMARY KEY,
    log_id            VARCHAR REFERENCES logs(id),
    resolution_mode   VARCHAR,
    executive_summary TEXT,
    outcome           VARCHAR,
    resolved_by       VARCHAR,
    what_happened     TEXT,
    impact_assessment TEXT,
    root_cause        TEXT,
    lessons_learned   TEXT,
    resolved_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS remediation_steps (
    id                SERIAL PRIMARY KEY,
    log_id            VARCHAR REFERENCES logs(id),
    step_number       INTEGER,
    title             VARCHAR,
    description       TEXT,
    command           TEXT,
    risk              VARCHAR,
    estimated_time    VARCHAR,
    rollback          TEXT,
    auto_execute      BOOLEAN,
    requires_approval BOOLEAN,
    status            VARCHAR,
    executed_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS follow_up_actions (
    id           SERIAL PRIMARY KEY,
    incident_id  VARCHAR REFERENCES incidents(incident_id),
    title        VARCHAR,
    description  TEXT,
    owner        VARCHAR,
    deadline     VARCHAR,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics (
    id         SERIAL PRIMARY KEY,
    agent      VARCHAR,
    event      VARCHAR,
    payload    JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Key relationships:**
- `classifications.log_id` → `logs.id`
- `threat_assessments.log_id` → `logs.id`
- `incidents.log_id` → `logs.id`
- `remediation_steps.log_id` → `logs.id`
- `follow_up_actions.incident_id` → `incidents.incident_id`

A single log event can be traced through all five tables using its `trace.id` / `logs.id`. The `processing_stage` column on the `logs` table tells you exactly how far through the pipeline any given log has progressed.

---

## Project structure

```
ledger/
  main.py           — Entry point: connects to Kafka, starts consumers, handles all topics
  config.py         — Reads KAFKA_BROKERS and DATABASE_URL from environment
  database.py       — PostgreSQL connection, schema creation, all INSERT/UPDATE queries
  requirements.txt
  .env
```

The ledger has no sub-modules — it is intentionally kept simple. All the logic lives in `main.py` and `database.py`.

---

## Setup

**Prerequisites:**
- Python 3.10+
- PostgreSQL running and accessible — default connection is `postgresql://admin:secret@localhost:5432/cybercontrol`
- Kafka running (via Docker Compose from the [.github repo](https://github.com/Admin-or-Admin/.github))

**1. Create and activate a virtual environment**
```bash
cd ledger
python -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Create your `.env` file**
```bash
cp .env.example .env
```

Edit `.env` with your values. See [Environment variables](#environment-variables).

**4. Create the database**

The ledger creates its own tables on startup — you only need to create the database itself:

```bash
psql -U admin -c "CREATE DATABASE cybercontrol;"
```

Or if using the Docker Compose setup from the `.github` repo, the database is created automatically. Just make sure the `postgres` container is running before starting the ledger.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:29092` | Kafka broker address. Use the LAN IP of the machine running Docker if Kafka is not on the same machine |
| `KAFKA_GROUP_ID` | `ledger-group` | Kafka consumer group ID. Changing this causes the ledger to reprocess all messages from the beginning of every topic — only do this intentionally |
| `DATABASE_URL` | `postgresql://admin:secret@localhost:5432/cybercontrol` | Full PostgreSQL connection string |

A complete `.env`:

```properties
KAFKA_BROKERS=localhost:29092
KAFKA_GROUP_ID=ledger-group
DATABASE_URL=postgresql://admin:secret@localhost:5432/cybercontrol
```

---

## Running the ledger

```bash
source venv/bin/activate
python main.py
```

**Expected startup output:**

```
[Ledger] Connecting to Kafka at localhost:29092...
[Ledger] Creating tables if not exist...
[Ledger] Tables ready.
[Ledger] Subscribed to: logs.unfiltered, logs.categories, logs.solver_plan, logs.solution, analytics, actions
[Ledger] Listening...
```

Once running, you will see a line logged for each message persisted:

```
[Ledger] logs.unfiltered   → inserted log a3f2c1d4-...
[Ledger] logs.categories   → inserted classification for a3f2c1d4-...
[Ledger] logs.solver_plan  → inserted threat assessment for a3f2c1d4-...
[Ledger] logs.solution     → inserted incident + 3 steps + 2 follow-ups for a3f2c1d4-...
[Ledger] analytics         → stored heartbeat from classifier
[Ledger] actions           → timestamp request from ingestor-elasticsearch-3a8f2b1c → responded
```

**Start order:** the ledger should be started before the ingestor so it is ready to respond to the timestamp coordination request. If the ingestor starts first it will wait up to 10 seconds for a response before falling back to epoch — so the order is not strictly required, just recommended.

The recommended full startup order for the platform is:

```
1. docker compose up -d        (Kafka, PostgreSQL, Elasticsearch)
2. python main.py              (ledger — ready to receive from all topics)
3. ingestor                    (begins publishing to logs.unfiltered)
4. classifier.py               (begins consuming logs.unfiltered)
5. analyst.py
6. responder.py
7. gateway python main.py
8. dashboard npm run dev
```

**Shutdown:** `Ctrl+C`. The ledger closes its Kafka consumer cleanly on `KeyboardInterrupt`.

---

## Related repos

| Repo | Description |
|---|---|
| [ingestor](https://github.com/Admin-or-Admin/ingestor) | Publishes to `logs.unfiltered` — requests timestamp on startup |
| [rac-agents](https://github.com/Admin-or-Admin/rac-agents) | Classifier, analyst, responder — publish to `logs.categories`, `logs.solver_plan`, `logs.solution`, `analytics` |
| [gateway](https://github.com/Admin-or-Admin/gateway) | Reads from PostgreSQL — publishes human decisions to `actions` |
| [.github](https://github.com/Admin-or-Admin/.github) | Docker Compose for Kafka, PostgreSQL, Elasticsearch |
