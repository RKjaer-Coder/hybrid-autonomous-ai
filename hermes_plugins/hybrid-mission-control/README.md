# Hybrid Mission Control Hermes Plugin

This plugin is the Hermes-native replacement target for the standalone Mission
Control prototype. It keeps the useful panels and actions, but renders them as a
small dashboard tab inside Hermes instead of growing a separate frontend stack.

Install it through the runtime profile installer:

```bash
python3 -m skills.runtime --install-profile
```

The installer copies this directory to `~/.hermes/plugins/hybrid-mission-control`
and writes `runtime_config.json` with the repo root and data directory. Then run:

```bash
hermes dashboard --no-open
```

Gate and quarantine review actions are intentionally read-only in the first
plugin version. They should become writable only after Hermes dashboard auth,
audit logging, timeout handling, and replay semantics pass the same checks as
the CLI gate path.
