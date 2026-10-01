# Part 1: Architecture and Design
- [Design document](docs/design-document.md)

# Part 2: Template publish fan-out worker

## To Execute Tests

```bash
#Requires JDK 25
./mvnw test
```
This repo implements the fan-out worker as a **regional planner plus a bounded regional worker**.

A shared `templatePublished` event is broadcast to every region. Each region uses its own metadata store to find 
candidate engagements, emits engagement-level review work, and processes that work through a bounded async worker. 
Migration is handled separately as a seeded backfill stream.

This keeps engagement-derived data in-region and keeps the expensive one-minute engagement load behind explicit 
concurrency limits.

## What is implemented

### Streams

- `templatePublished`: broadcast planning event
- `engagementUpdateReview`: engagement-level work created from publish events
- `engagementMigration`: seeded backfill work for engagements that do not yet have metadata

### Planner

`TemplatePublishedPlanner` consumes `templatePublished`, looks up regional metadata for the published template, and 
emits one `EngagementWorkItem` per candidate engagement to `engagementUpdateReview`.

### Worker

`TemplateUpdateConsumer` and `MigrationConsumer` both feed the same `EngagementWorkProcessor`.

The processor uses:

- one shared bounded executor with **no internal queue**
- per-topic semaphores so template updates get more capacity than migration
- manual Kafka acknowledgment only after async processing completes

### Downstream contract

`EngagementManagement` is treated as the slow downstream boundary. It loads an engagement and returns the authoritative 
metadata snapshot needed to update the projection. The worker writes that snapshot through 
`EngagementMetadataRepository`.

## Correctness

### Migration

Migration is a **gap-filling backfill**, not the steady-state path.

- the migration stream is seeded from the region’s existing engagements
- if metadata already exists, the worker skips the load
- missing metadata is loaded and written once

### Publish updates

Publish events do not directly trigger slow engagement loads. They first go through the planner, which uses existing 
metadata to identify candidate engagements and enqueue review work.

### Stale work

In-flight stale work is allowed, but stale results must not overwrite newer state.

That guard lives in the metadata repository:

- projection writes are conditional
- `upsertIfNewer(...)` can no-op stale snapshots
- if a user updates an engagement while older worker output is still in flight, the older write is ignored

This is the main race-condition protection in the implementation.

## Assumptions

- each engagement belongs to exactly one region
- each region has its own Engagement Management boundary
- each region has its own metadata store
- shared template content is global, but engagement-derived metadata stays regional
- `templatePublished` is broadcast to every region
- engagement create/apply/decline actions keep metadata current enough for stale worker writes to be rejected safely

## Failure handling

- transient failures are republished with an incremented attempt count
- persistent failures, or retries that exceed the configured limit, go to the DLQ
- Kafka is the durable backlog; the worker does not introduce another internal queue or scheduler

### Retry scope

This repo models retry and DLQ publishing, but it does **not** implement a delayed retry topology. In a production 
setup, exponential backoff and jitter would usually come from Kafka retry-topic wiring or platform-managed delayed 
delivery rather than from custom scheduling inside this worker.

## Tradeoffs

- a separate planner makes the publish fan-out explicit
- regional planning duplicates the publish signal per region, but keeps scans and metadata local
- conditional projection writes are simpler than adding reconciliation for this exercise
- a bounded executor protects Engagement Management first and leaves backlog management to Kafka

## Tests

The test suite covers:

- Spring context wiring
- acknowledgment only after async processing completes
- planner fan-out from publish events to engagement review work
- capacity exhaustion and retry/DLQ behavior
- migration skip when metadata already exists
- stale snapshot handling through conditional metadata upsert