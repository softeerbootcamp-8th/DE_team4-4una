---
name: update-project-context
description: Synchronize DE4 project context and architectural records with durable implementation, schema, pipeline, service-boundary, source-data, simulation, scoring, or deployment changes. Use after code or contracts change, before opening a PR, when resolving an open question, or when asked to review context drift. Do not use for changes that have no lasting effect on project behavior or documentation.
---

# Update Project Context

Keep `context/` and relevant ADRs aligned with verified repository behavior without turning proposals into accepted decisions.

## Gather evidence

1. Read `AGENTS.md`, `context/README.md`, and `context/manifest.yaml`
2. Read only the task-specific context routed by `context/README.md`
3. Inspect the user request, current branch, `git status`, and relevant diffs
4. When a base is not supplied, include committed branch changes against the merge base with `develop` or `origin/develop` when available, plus staged and unstaged changes
5. Inspect affected executable contracts, migrations, service code, configuration, and tests
6. Treat issue or PR descriptions as intent, not proof that behavior is implemented

Do not fetch remotes, modify implementation, or overwrite unrelated user changes unless the user explicitly requests it.

## Decide whether documentation must change

Update context only for durable changes to requirements, contracts, data flow, ownership, operational behavior, assumptions, or accepted decisions. For an internal refactor, update only stale paths, service boundaries, or execution instructions. If no durable context changed, leave the documents unchanged and explain why.

Use the following routing:

| Change | Review and update |
| --- | --- |
| Product goal, scope, KPI, or requirement | `context/project.md` |
| Architecture, deployment, or service responsibility | `context/architecture.md`, `context/services.md` |
| Dataset grain, keys, fields, or service contract | `context/data/schema-catalog.md`, `context/data/contracts.md` |
| Source, snapshot, or ingestion policy | `context/data/sources.yaml`, relevant `context/runs/` evidence |
| Pipeline stages or dependencies | `context/data/lineage.md` |
| Validation, rejection, or quality threshold | `context/data/quality-rules.md` |
| Simulation behavior or assumptions | `context/simulation.md` |
| Comfort-score formula or aggregation | `context/comfort-score.md` |
| Shared terminology | `context/glossary.md` |
| Resolved or newly discovered decision | `context/open-questions.md` and, when architectural, `docs/adr/` |
| Added, moved, or removed context document | `context/manifest.yaml`, `context/README.md` |

Do not copy full executable schemas into prose when `libs/de4-core` or a migration is authoritative. Link to the authoritative artifact and document its grain, purpose, and constraints instead.

## Preserve decision status

- Mark a requirement **Confirmed** only when the user, an accepted ADR, or another authoritative project decision confirms it
- Mark behavior **Implemented** only when code exists and proportionate validation supports it
- Keep unaccepted designs **Proposed** and unresolved choices **Open**
- Never silently resolve an entry in `context/open-questions.md`
- Create or update an ADR only when the decision is accepted and materially affects multiple components, deployment, data contracts, or long-term operations
- Preserve superseded decisions through ADR history rather than rewriting history

Use the current date for changed `last_reviewed` metadata. Do not change review dates on untouched documents.

## Make focused edits

1. Update the smallest set of documents that fully describes the change
2. Keep a single canonical definition and repair stale paths or contradictory statements
3. Separate current behavior, target architecture, and future work explicitly
4. Record exact source files, snapshots, checksums, and commands only for reproducible real-data runs
5. Avoid implementation diaries, PR summaries, speculative claims, generated data, credentials, and large fixtures in `context/`

When evidence conflicts, follow the precedence in `context/README.md` and report the conflict instead of guessing.

## Validate and report

1. Re-read every changed context section against the supporting code or contract
2. Confirm new links and repository paths exist
3. Parse changed YAML with an available repository tool and run `git diff --check`
4. Run additional repository checks only when the context change depends on executable behavior that has not already been verified
5. Summarize updated files, supporting evidence, validation performed, and any remaining mismatch or open question

Do not commit or push unless the user requests it.
