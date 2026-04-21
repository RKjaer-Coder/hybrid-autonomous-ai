PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS step_outcomes (
  event_id TEXT PRIMARY KEY,
  step_type TEXT NOT NULL,
  skill TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('PASS', 'FAIL', 'DEGRADED')),
  latency_ms INTEGER NOT NULL,
  quality_warning INTEGER DEFAULT 0 CHECK (quality_warning IN (0, 1)),
  recovery_tier INTEGER CHECK (recovery_tier IS NULL OR recovery_tier BETWEEN 1 AND 5),
  timestamp TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_step_outcomes_step_skill_timestamp ON step_outcomes(step_type, skill, timestamp);
CREATE INDEX IF NOT EXISTS idx_step_outcomes_chain_id ON step_outcomes(chain_id);
CREATE INDEX IF NOT EXISTS idx_step_outcomes_outcome_timestamp ON step_outcomes(outcome, timestamp);
CREATE INDEX IF NOT EXISTS idx_step_outcomes_skill_timestamp ON step_outcomes(skill, timestamp);

CREATE TABLE IF NOT EXISTS chain_definitions (
  chain_type TEXT PRIMARY KEY,
  steps TEXT NOT NULL CHECK (json_valid(steps)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS execution_traces (
  trace_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  role TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  harness_version TEXT NOT NULL,
  intent_goal TEXT NOT NULL,
  steps_json TEXT NOT NULL CHECK (json_valid(steps_json)),
  prompt_template TEXT NOT NULL,
  context_assembled TEXT NOT NULL,
  retrieval_queries_json TEXT NOT NULL CHECK (json_valid(retrieval_queries_json)),
  judge_verdict TEXT NOT NULL CHECK (judge_verdict IN ('PASS', 'FAIL', 'PARTIAL')),
  judge_reasoning TEXT NOT NULL,
  outcome_score REAL NOT NULL CHECK (outcome_score >= 0.0 AND outcome_score <= 1.0),
  cost_usd REAL NOT NULL CHECK (cost_usd >= 0.0),
  duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
  training_eligible INTEGER NOT NULL DEFAULT 1 CHECK (training_eligible IN (0, 1)),
  retention_class TEXT NOT NULL DEFAULT 'STANDARD' CHECK (retention_class IN ('STANDARD', 'FAILURE_AUDIT')),
  source_chain_id TEXT,
  source_session_id TEXT,
  source_trace_id TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_execution_traces_skill_created ON execution_traces(skill_name, created_at);
CREATE INDEX IF NOT EXISTS idx_execution_traces_training_created ON execution_traces(training_eligible, created_at);
CREATE INDEX IF NOT EXISTS idx_execution_traces_retention_created ON execution_traces(retention_class, created_at);

CREATE TABLE IF NOT EXISTS harness_variants (
  variant_id TEXT PRIMARY KEY,
  skill_name TEXT NOT NULL,
  parent_version TEXT NOT NULL,
  diff TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('track_a', 'operator', 'proposer')),
  status TEXT NOT NULL CHECK (status IN ('PROPOSED', 'SHADOW_EVAL', 'PROMOTED', 'REJECTED')),
  prompt_prelude TEXT NOT NULL DEFAULT '',
  retrieval_strategy_diff TEXT NOT NULL DEFAULT '',
  scoring_formula_diff TEXT NOT NULL DEFAULT '',
  context_assembly_diff TEXT NOT NULL DEFAULT '',
  touches_infrastructure INTEGER NOT NULL DEFAULT 0 CHECK (touches_infrastructure IN (0, 1)),
  reject_reason TEXT,
  eval_result_json TEXT CHECK (eval_result_json IS NULL OR json_valid(eval_result_json)),
  promoted_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_hv_skill_status ON harness_variants(skill_name, status);
CREATE INDEX IF NOT EXISTS idx_hv_created ON harness_variants(created_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_hv_active_skill ON harness_variants(skill_name)
WHERE status IN ('PROPOSED', 'SHADOW_EVAL');

CREATE VIEW IF NOT EXISTS harness_frontier AS
SELECT
  skill_name,
  variant_id,
  json_extract(eval_result_json, '$.quality_delta') AS quality_delta,
  json_extract(eval_result_json, '$.regression_rate') AS regression_rate,
  json_extract(eval_result_json, '$.traces_evaluated') AS traces_evaluated,
  promoted_at
FROM harness_variants
WHERE status = 'PROMOTED'
ORDER BY promoted_at DESC, variant_id DESC;

-- Rolling reliability by step_type x skill over 7d and 30d windows.
CREATE VIEW IF NOT EXISTS reliability_by_step AS
SELECT
  step_type,
  skill,
  SUM(CASE WHEN timestamp >= datetime('now', '-7 days') THEN
    CASE outcome WHEN 'PASS' THEN 1.0 WHEN 'DEGRADED' THEN 0.5 ELSE 0.0 END
  ELSE 0.0 END)
  /
  NULLIF(SUM(CASE WHEN timestamp >= datetime('now', '-7 days') THEN 1 ELSE 0 END), 0) AS reliability_7d,
  SUM(CASE WHEN timestamp >= datetime('now', '-30 days') THEN
    CASE outcome WHEN 'PASS' THEN 1.0 WHEN 'DEGRADED' THEN 0.5 ELSE 0.0 END
  ELSE 0.0 END)
  /
  NULLIF(SUM(CASE WHEN timestamp >= datetime('now', '-30 days') THEN 1 ELSE 0 END), 0) AS reliability_30d
FROM step_outcomes
GROUP BY step_type, skill;

-- Product of per-step reliabilities per chain_type based on chain_definitions steps JSON.
CREATE VIEW IF NOT EXISTS chain_reliability AS
SELECT
  cd.chain_type,
  exp(SUM(CASE
      WHEN r.reliability_7d IS NULL OR r.reliability_7d <= 0 THEN NULL
      ELSE ln(r.reliability_7d)
  END)) AS chain_reliability_7d,
  exp(SUM(CASE
      WHEN r.reliability_30d IS NULL OR r.reliability_30d <= 0 THEN NULL
      ELSE ln(r.reliability_30d)
  END)) AS chain_reliability_30d
FROM chain_definitions cd
JOIN json_each(cd.steps) s
LEFT JOIN reliability_by_step r
  ON r.step_type = json_extract(s.value, '$.step_type')
 AND r.skill = json_extract(s.value, '$.skill')
GROUP BY cd.chain_type;

CREATE INDEX IF NOT EXISTS idx_chain_definitions_created_at ON chain_definitions(created_at);
