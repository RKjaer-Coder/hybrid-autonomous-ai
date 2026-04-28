# Mission Control Dashboard Design

Mission Control is a Hermes dashboard tab, not a standalone product surface.
The native Hermes sidebar, routing, themes, sessions, logs, analytics, config,
skills, gateway, and update controls remain the shell. This plugin contributes
only the project-specific operator layer.

## V1 Tab Map

1. Overview: runtime pulse, pending gates, replay readiness, milestone health,
   operator load, latest digest, and active alerts.
2. Workflow: lightweight view of how work moves through Opportunity Pipeline,
   Projects, Phase Engine, Research Tasks, and Operator Queues.
3. Projects: kanban lanes mapped to real project/phase states with project
   priority controls.
4. Tasks: operator-created tasks plus system research and harvest queues, with
   priority/status controls where safe.
5. Decisions: read-only G1-G4/G3/quarantine/runtime-halt queues until Hermes
   dashboard auth, audit, timeout, and replay semantics pass gate validation.

## Design Rules

- Reflect real workflow objects; do not abstract Council, Research, Gates, or
  Finance into generic productivity concepts.
- Keep the plugin bundle tiny: plain JavaScript IIFE, Hermes SDK components,
  dashboard theme variables, and no bundled React.
- Use `MissionControlService.snapshot()` as the backend contract so standalone
  and Hermes-native views cannot drift.
- Prefer read-only visibility first for high-consequence surfaces. Promote
  write actions only after CLI-equivalent tests exist.
- Keep visual density calm: the operator should see pressure, flow, and next
  action without reading implementation internals.

## Promotion Path

1. Validate plugin discovery with `hermes dashboard --no-open`.
2. Confirm `/api/plugins/hybrid-mission-control/health` and `/snapshot`.
3. Verify project priority, task priority, manual task creation, and alert
   acknowledgement write heartbeat rows with channel `hermes_dashboard`.
4. Only after gate-validation tests pass, add writable gate/quarantine review
   controls.
