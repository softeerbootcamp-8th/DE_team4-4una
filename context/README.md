# Agent Context

This directory is the curated starting point for agents and contributors working
on DE4. It describes the intended system, the vocabulary used by the project,
and the current unresolved design questions.

The project is currently in the planning and skeleton stage. Documents in this
directory describe the target prototype unless they explicitly say that a
component is implemented.

## Reading order

Every task should begin with:

1. [Project definition](project.md)
2. [Open questions](open-questions.md)
3. The task-specific documents listed below

| Task | Required context |
| --- | --- |
| Architecture or infrastructure | [Architecture](architecture.md), [Service map](services.md) |
| Reference-data ingestion | [Data sources](data/sources.yaml), [Schema catalog](data/schema-catalog.md), [Lineage](data/lineage.md) |
| Trip replay or simulation | [Simulation](simulation.md), [Schema catalog](data/schema-catalog.md), [Quality rules](data/quality-rules.md) |
| Comfort-score calculation | [Comfort score](comfort-score.md), [Schema catalog](data/schema-catalog.md), [Quality rules](data/quality-rules.md) |
| API or dashboard | [Project definition](project.md), [Data contracts](data/contracts.md), [Schema catalog](data/schema-catalog.md) |
| Shared models | [Data contracts](data/contracts.md), [Schema catalog](data/schema-catalog.md), then `libs/de4-core` |
| Architectural decision | Existing records in `docs/adr/` |

`manifest.yaml` is the machine-readable index of this directory.

## Authority and precedence

Use the following precedence when sources disagree:

1. Accepted requirements and decisions recorded in this directory or an ADR.
2. Executable contracts in `libs/de4-core` and database migrations in
   `services/batch-jobs/src/batch_jobs/resources/migrations/`.
3. Implemented service behavior and tests.
4. General descriptions in the repository `README.md`.

Once an executable contract is implemented, it becomes authoritative for field
names and types. This directory should explain and link to that contract instead
of copying it.

## Status language

- **Confirmed**: supplied as a project requirement.
- **Proposed**: a reasonable first-draft design that still needs acceptance.
- **Open**: a choice or fact that must be resolved before dependent work is final.
- **Implemented**: present in code and covered by proportionate validation.

Do not silently convert a proposed or open item into a confirmed requirement.
Record newly resolved questions in `open-questions.md`, update the affected
documents, and create an ADR when the decision has architectural consequences.

## Maintenance

- Update context in the same pull request as the behavior it describes.
- Keep one canonical definition for each contract.
- Add `owner`, `status`, and `last_reviewed` metadata to new major documents.
- Keep secrets, credentials, large source data, and generated lake contents out
  of this directory.
- Prefer small representative fixtures under `tests/fixtures/` when examples are
  needed.
