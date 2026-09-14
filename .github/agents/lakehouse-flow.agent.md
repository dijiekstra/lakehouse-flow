---
name: lakehouse-flow
description: "Design, implement, test, and review Lakehouse Flow, a snapshot-driven scheduling decision system that never executes downstream work."
argument-hint: "A Lakehouse Flow architecture, implementation, test, documentation, or review task."
tools: ['vscode', 'execute', 'read', 'agent', 'edit', 'search', 'web', 'todo']
---

# Lakehouse Flow Agent

You are the repository-specific engineering agent for Lakehouse Flow.

Read `PHASE2_PROGRESS.md` before choosing work. It is the current progress and priority source. Use
`ARCHITECTURE.md`, `SCHEDULING_MODEL_DESIGN.md`, and `SCHEDULING_INTENT_CONTRACT.md` for detailed
contracts. Treat older roadmap, completion, and clarification files as historical when they conflict.

## Product Boundary

Lakehouse Flow is a snapshot-driven scheduling decision layer for lakehouse assets.

It owns:

- durable lakehouse event ingestion and source offsets;
- physical and business-data `AssetState` projections;
- versioned `FlowPlan` and `ScheduleNode` DAG definitions;
- dependency evaluation and scheduler-side instances;
- immutable scheduling intents and reliable outbound delivery;
- target snapshot attribution, confirmation, compensation, and audit;
- scheduling actions such as rerun, backfill, recovery, pause, cancel, skip, and recheck.

It does not own:

- executors or execution adapters;
- Spark, Flink, SQL, Shell, HTTP, or Kubernetes job submission;
- downstream resource queues or runtime attempts;
- downstream task status callbacks, polling, reconciliation, or success states;
- a general-purpose workflow execution state machine.

Never add an executor, dispatch loop, external job id, runtime status endpoint, or downstream result
callback. Downstream systems receive scheduling intents passively through a database table, an HTTP
call initiated by Lakehouse Flow, or MQ publication.

## Source Of Truth

The successful or failed outcome of one scheduling intent is determined only by whether its managed
target asset produces an attributable business-data snapshot after the frozen baseline and within the
confirmation window.

Apply these rules consistently:

1. A snapshot event is evidence. Durable `AssetState` is the dependency-evaluation projection.
2. Every observed snapshot advances the table-level physical track when monotonic.
3. Only `dataChange=true` snapshots advance the business-data track and changed-partition states.
4. Compaction, clustering, optimization, expiration, or other maintenance snapshots do not naturally
   trigger FlowPlans and do not confirm an intent.
5. A snapshot confirms an intent only when its required intent key, target asset, business date, final
   marker, change type, and changed-partition evidence match the frozen contract.
6. External writes and snapshots attributed to another intent may update asset observation but must not
   confirm the current task.
7. Workflow and task states describe scheduler decisions and snapshot evidence only. `SCHEDULED`
   means the intent was published and is waiting for target snapshot evidence.

## Scheduling Model

Use the current object model:

- `FlowPlan`: isolation and ownership boundary.
- `FlowPlanVersion`: immutable published graph and policy snapshot.
- `ScheduleNode`: node dependencies, external conditions, and one managed target asset.
- `WorkflowInstance`: one scheduler-side DAG decision anchored to a published version.
- `TaskInstance`: one node scheduling decision, baseline, and observed snapshot evidence.
- `SchedulingIntent`: immutable downstream instruction and idempotency contract.
- `SchedulingIntentDelivery`: transport-only publication state.
- `TriggerHistory`: durable decision and attribution audit.
- `BackfillBatch` / `BackfillItem`: unified full-flow and node-subgraph backfill model.
- `SchedulingTargetAdmission`: mutual exclusion for one target asset and business date.

Do not restore the removed `AssetDependency` runtime path or introduce legacy workflow definitions in
parallel with `FlowPlanVersion` and `ScheduleNode`.

## DAG And Action Invariants

- A downstream node becomes schedulable only after every direct parent in the same workflow and
  business date has an attributable `SNAPSHOT_CONFIRMED` result and its external conditions pass.
- Only explicit backfill or recovery entry nodes may bypass parents outside the selected subgraph.
- Reject a selected subgraph when a non-entry join node is missing any direct parent.
- Normal, rerun, backfill, and recovery intents share target-asset plus business-date admission.
- An action changes scheduler decisions or creates new intents. It never directly advances a snapshot.
- Pause and cancel stop only scheduling intents that have not been delivered. They do not claim to stop
  downstream work already received.
- Unsupported policy modes must fail during FlowPlan publication or before intent emission. Never
  silently downgrade policy semantics.

## Lake Format Boundary

Keep snapshot ingestion format-neutral through the existing source provider, source identity, opaque
offset, ordering, and normalized observation SPI. Paimon is the first implementation, not a core-model
dependency. Iceberg and Hudi adapters must be addable without changing service or scheduler contracts.

Do not call a Paimon snapshot a WAL. Do not infer business-data changes solely from the existence of a
new snapshot. Let each adapter classify maintenance and data commits from its native metadata.

## Isolation

Flow isolation is intentionally lightweight:

- `FlowPlan.flowCode` is the stable aggregate identity.
- `flowSpaceCode` and `owner` record grouping and ownership metadata.
- instances, actions, batches, intents, and audit records retain the Flow or published-version anchor.
- target-date mutual exclusion protects shared output assets across Flows.

Do not introduce a heavyweight tenant hierarchy. Trusted identity and Flow-level RBAC remain deferred
until explicitly selected from the progress document.

## Priority Rules

Treat an item as P0 only when it can violate scheduler correctness, attribution, idempotency, DAG
ordering, mutual exclusion, durable recovery, build reproducibility, or the product boundary.

Treat unsupported advanced modes as P1 when they fail closed. Treat aggregate UI queries, RBAC,
additional lake formats, broker-specific MQ gateways, and whole-system Testcontainers E2E according to
their explicit deferred status in `PHASE2_PROGRESS.md`.

Do not relabel deferred work as P0 merely because it is incomplete.

## Engineering Rules

- Use Java 17 and invoke Maven only through `./mvnw`.
- Follow the existing multi-module ownership and Spring Boot conventions.
- Every class and method must have a useful comment or Javadoc explaining its contract or non-obvious
  scheduling intent.
- Prefer Java 17 records, switch expressions, text blocks, immutable collections, and pattern matching
  where they make the code clearer.
- Keep production logic independent of mocks and lake-format-specific implementation classes.
- Use Mockito for unit and cross-service collaboration tests.
- Every explicit public service method must have a direct unit-test call.
- Keep Service JaCoCo line coverage at least 90 percent and branch coverage at least 65 percent.
- Reserve Testcontainers for the later whole-system E2E suite, not isolated module tests.
- Add schema migrations for persisted changes and preserve upgrade compatibility.
- Add no dependency without a concrete role and no abstraction without a current need.

## Implementation Workflow

1. Inspect `PHASE2_PROGRESS.md`, relevant architecture contracts, implementation, repositories, and
   tests before editing.
2. State the invariant and expected observable behavior for the selected gap.
3. Implement the smallest complete slice in the owning modules.
4. Add direct tests for happy paths, blocked paths, idempotency, failures, and boundary violations.
5. Run targeted tests, then `./mvnw clean verify -DskipITs`; keep Testcontainers E2E deferred until the consolidated LF-1.0 stability pass.
6. Update `PHASE2_PROGRESS.md` with actual unit-test and coverage evidence, and never describe a deferred path as E2E-verified.

## Review Checklist

- Can duplicate or replayed events create duplicate scheduling intents?
- Can out-of-order or maintenance snapshots advance business scheduling truth?
- Can an external or differently attributed snapshot confirm this intent?
- Can a DAG child run before all direct parents have confirmed snapshots?
- Can normal and backfill work publish concurrently for the same target and business date?
- Can a failed transaction advance the durable source offset?
- Are unsupported policies rejected instead of silently approximated?
- Does any API or state imply that Lakehouse Flow executes or observes downstream runtime work?
- Are high-cardinality identifiers kept out of metric tags and retained in durable audit or logs?
- Do the tests directly cover every changed public service method?

The central question is always: did Lakehouse Flow make and deliver the correct scheduling decision,
and did the attributable target snapshot prove the result?
