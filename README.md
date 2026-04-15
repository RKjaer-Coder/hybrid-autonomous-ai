# Hybrid Autonomous AI

This repository is the implementation baseline for Hybrid Autonomous AI: a
hybrid, local-first autonomous intelligence system designed to run primarily on
operator-owned infrastructure and use cloud models selectively where they add
clear leverage or safety value.

This is a real, test-backed codebase, not just an idea dump. It already
contains the persistence layer, routing contracts, immune guardrails, Hermes
integration scaffolding, milestone evals, and a deterministic runtime proof.
It is still pre-live from a real Hermes deployment perspective.

## Latest Progress

As of April 15, 2026, GitHub `main` includes the latest validated Hermes
runtime attachment work:

- repo-owned Hermes profile generation under `~/.hermes/profiles/<profile>/`
- a docs-aligned `config.yaml` plus a spec-compat `profile.yaml` projection
- structural profile validation in both `--doctor` and `--readiness`
- an explicit `--bootstrap-live` CLI path that matches the generated launchers
- deterministic runtime proof coverage for install, bootstrap, workflow, and
  post-workflow doctor checks

The highest-priority remaining step is still hardware-gated: run the readiness
flow against a real Hermes installation on the Mac Studio and confirm the live
CLI/profile surface end to end.

## What Is In This Repo

Today, this repository includes:

- a five-database SQLite baseline with migrations and drift verification
- a typed financial router with spend controls and hard approval boundaries
- an immune subsystem with Sheriff, Judge, bootstrap patching, and verdict logs
- council contracts and orchestration support for structured deliberation
- research, strategic-memory, opportunity, operator, and observability skills
- milestone eval harnesses and deterministic fixtures
- a Hermes runtime integration layer that can prepare a local profile bundle,
  generate repo-owned Hermes profile artifacts, migrate the databases,
  bootstrap a runtime, run a doctor check that validates the runtime layout
  and generated profile files, run a real-Hermes readiness check against the
  operator bootstrap checklist, and prove a deterministic operator workflow
  plus a council-backed opportunity and phase-gate path against a mock runtime
- GitHub Actions CI that runs the full test suite on `main`, `codex/**`, and
  pull requests to `main`

## What This Repo Is Not Yet

This repository should not be described as:

- a confirmed live Hermes installation
- a production-attached runtime on a real operator machine
- the full target-state autonomous system described in the broader architecture
- proof that the richer long-horizon loops are already operating live

The current state is best described as a strong pre-live implementation
substrate with deterministic runtime proofs.

## Repo Map

```text
.
├── schemas/             # SQLite contracts for the five-database baseline
├── migrate.py           # Applies schemas and verifies structural drift
├── financial_router/    # Routing policy, cost logic, approval gates
├── immune/              # Fail-closed security and policy validation
├── council/             # Deliberation types, prompts, validators, orchestration
├── skills/              # Hermes-facing integration and runtime scaffolding
├── eval/                # Milestone runners, mock backends, fixtures, reporting
├── tests/               # Unit and integration coverage
└── .github/workflows/   # CI automation
```

## The Five Databases

Runtime state is intentionally split across five SQLite databases:

- `strategic_memory.db`: briefs, research tasks, opportunities, council outputs
- `telemetry.db`: chain definitions, step outcomes, reliability views
- `immune_system.db`: verdicts, alerts, revocations, security audit trails
- `financial_ledger.db`: routing decisions, costs, revenue, projects, kill data
- `operator_digest.db`: digests, alerts, gates, harvest requests, operator load

This separation keeps safety, finance, operator UX, and strategic state easier
to reason about and audit.

## Key Entry Points

- `migrate.py`
  Bootstraps and verifies the full SQLite baseline.
- `financial_router/`
  Enforces routing, quality, and spend policy.
- `immune/`
  Implements the fail-closed validation layer.
- `skills/runtime.py`
  The main Hermes-facing runtime bootstrap and workflow proof entry point.
- `eval/runner.py`
  Runs milestone-oriented evals against deterministic harnesses.

## Quick Verification

Install development dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Run the full test suite:

```bash
python3 -m pytest -q
```

Run the skill-focused suite:

```bash
python3 -m pytest -q tests/test_skills
```

Run the M2 eval:

```bash
python3 -m eval.runner --milestone M2
```

Run the deterministic runtime proof:

```bash
python3 -m skills.runtime --operator-workflow
```

Run the explicit runtime bootstrap path:

```bash
python3 -m skills.runtime --bootstrap-live
```

Run the runtime doctor check:

```bash
python3 -m skills.runtime --doctor
```

Run the real-Hermes readiness command:

```bash
python3 -m skills.runtime --readiness
```

That command prepares the repo-managed runtime bundle and canonical databases
under the selected Hermes paths, generates a repo-owned Hermes profile under
`~/.hermes/profiles/<profile>/` (`config.yaml` plus a spec-compat
`profile.yaml` projection), validates that generated profile/config shape
structurally, checks the live Hermes CLI/version/profile/seed-tool surface,
runs a live Hermes CLI smoke test when Hermes is available, verifies
`STEP_OUTCOME`/log evidence, creates a deterministic data snapshot under the
runtime checkpoints directory, and exits clearly when Hermes is not installed.

It also reports two current drifts explicitly instead of hiding them:

- `spec/00_manifest.md` declares Hermes `v0.9.0+`, while `spec/s07_hermes_config.md`
  §7.5c still says `v0.8.0+`
- `spec/s07_hermes_config.md` §7.5c D1-2 still names
  `~/.hermes/profiles/<profile>/profile.yaml`, while current Hermes docs/profile
  commands center `config.yaml` inside the profile directory, so the repo now
  emits both artifacts from one owned profile spec instead of pretending the
  surfaces already match

One remaining uncertainty is still called out by the command itself: current
Hermes public docs clearly describe `config.yaml` and `approvals.mode`, but
they do not clearly document a first-class `dangerous_commands` config schema,
so the repo continues to project and validate the §7.5c dangerous-command set
as a repo-owned contract until live Hermes proves the exact upstream key
shape.

On machines without Hermes installed yet, you can still verify the offline
attachment path without attempting the live chat smoke test:

```bash
python3 -m skills.runtime --readiness --skip-cli-smoke
```

That runtime proof now exercises this chain from a clean layout:

`heartbeat -> immune check -> route -> research brief -> opportunity routing ->
harvest request -> council review -> project handoff -> phase gate ->
operator alert -> digest -> observability readback`

If you only want to bootstrap and verify the databases directly:

```bash
python3 migrate.py --db-dir ./data --verify
```

## How To Read This Codebase

If you are new here, start in this order:

1. `README.md`
2. `migrate.py`
3. `schemas/*.sql`
4. the module you want to change
5. the nearest test file under `tests/`

Treat the code and tests as the source of truth for what is implemented today.
If you also have access to the broader architecture workspace, use that for
target-state intent and this repository for implemented behavior.

## Working Style

- contracts first
- local first
- fail closed
- auditability over hidden magic
- deterministic tests over vague claims
- small, verified increments over broad rewrites

## Short Version

This repository already contains a substantial, validated implementation
substrate for Hybrid Autonomous AI: databases, routing, immune guardrails,
council scaffolding, research and operator skills, evals, CI, and a
deterministic Hermes-facing runtime proof. It is not yet the fully live system,
but it is well past a skeleton.
