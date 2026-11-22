# Coordinator and tracker integration

Reviewed November 22, 2026 against D's PR #12 at
`81e2f9f7d8f1c5fd5b45b6ae004527d63052a309`, and B at `f788c56` plus this change.
This is B's handoff for review, not approval or deployment of D's PR.

## Answers to D's B-related questions

- Status definitions and all 19 event types are in
  [contracts/status-values.md](../../contracts/status-values.md).
  [workflow.events.schema.json](../../contracts/events/workflow.events.schema.json)
  describes the actual B producer. `order.created.schema.json` records B's current
  accepted A input; it does not silently adopt a proposed new shared envelope.
- D's order and line enum sets match B, with tracker-only ACCEPTED added by D.
  Preserve `payload.status` and `payload.line_status` independently. `line.cancelled`
  cancels an attempt; the line proceeds through REPLACING/QUOTING, not CANCELLED.
- UNKNOWN, RECONCILING, CANCELLATION_PENDING and RECONCILIATION_REQUIRED are
  uncertain/attention states. D's "checking, not failed" presentation is correct.
- Last delivery observation is null, IN_TRANSIT or DELIVERED. PENDING is not
  currently emitted as delivery_status. Reason/error strings are extensible;
  known categories are listed in status-values.md.
- Topics order.created/workflow.events, order_id keys, per-order versions,
  stable event IDs and consumer groups agree. B retains fail-stop input handling;
  D's local dead-letter policy does not require B to skip a poisoned record.
- A's proposed top-level order_id/state_version=0 are **not** currently accepted
  by B's strict input model. Keep the published current input or agree an adapter
  change with A/B/D first. A chooses business IDs; B allows UUID strings as IDs.
- B does not approve A's proxy-path decision or C's physical dispatch/fencing
  guarantees. Those owners still need to confirm their respective contracts.

## Finding for D: cancellation does not always mean replacement

D's views.py currently sets `flags.replaced` from any CANCELLED attempt history.
The controlled cancellation/no-stock case demonstrates:

```
B final order/line: UNFULFILLED
B attempt history: one CANCELLED attempt, no successor
D API: terminal=true, replaced=true
```

The API description says a reservation was cancelled and the part re-sourced.
B recommends marking replaced only after a confirmed successor reservation:
correlate line.reserving.replaces_attempt_id with a prior CANCELLED attempt and
then line.reserved for the successor (or delivery proof for that successor).
`line.replacement_started` still names the old attempt and is insufficient.
Alternatively D/A must explicitly agree to rename/redefine the flag as
"a prior reservation was cancelled." This finding remains for D review; B emits
the required predecessor/successor evidence and does not modify D's owned code.

D logs version gaps and continues. B events do not contain a complete multi-line
snapshot every time. A gap can leave missing line/attempt history; do not present
it as a fully rebuilt projection. Replay retained ordered events to reconstruct
the view. No cross-system exactly-once or full rebuild-from-any-event claim.

## Reproduce the cross-component checks

Run from the repository root. Obtain D in a separate worktree, so its `app`
package cannot overwrite B's `app` package. If the temporary directory already
exists, inspect/reuse it instead of deleting work.

```bash
git fetch origin feature/package-d-tracker
git worktree add --detach /tmp/partstream-tracker-review origin/feature/package-d-tracker
cd services/coordinator
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pip check
COORDINATOR_TRACKER_SOURCE=/tmp/partstream-tracker-review/services/tracker python -m pytest -q
```

The optional cross-component cases generate events through B's actual persistence,
orchestrator, reconcilers and pollers. They then start a fresh interpreter using
D's actual decoder, Projector, SQLAlchemy read model and API response builder.
Every rendered response validates against D's OpenAPI schema. No events are
hand-edited to make the services agree. All databases are disposable SQLite;
seller replies are controlled fixtures. No cloud credentials or running services
are used. Without COORDINATOR_TRACKER_SOURCE, only the 12 external-D cases skip;
the 12 emitted-event schema cases and catalog drift check still run.

Coverage: reservation, IN_TRANSIT, delivery, lateness, overdue failed poll,
unknown reservation, reconciliation success/exhaustion, confirmed replacement,
lost cancellation reply, cancellation followed by no stock, and delivery winning
the cancellation race. Each case also checks duplicate last-event delivery,
stale lower version with a new event ID, and late A creation without regression.
The known replaced-flag discrepancy is documented above, not counted as a passed
flag-contract check. Status/attempt identity, counts, reason, terminal flag and
API schema compatibility are checked.

Set COORDINATOR_TRACKER_EVIDENCE_DIR to a scratch directory to retain each
synthetic scenario's real B events, B snapshot and D views/outcomes for inspection.
For the coordinator's real infrastructure checks, additionally enable
COORDINATOR_POSTGRES_TESTS=1 and COORDINATOR_KAFKA_TESTS=1 using the existing local
test services. These opt-ins do not turn the SQLite B/D projection test into a
live broker/cloud test. See VERIFICATION.md for recorded counts.

## D deployment handoff

D's current Compose definition starts only B's HTTP process. Running that alone
does not consume orders, execute jobs or publish saved events. The B-owned
[compose.integration.yaml](compose.integration.yaml) overlay supplies:

| Process | Command | Boundary |
| --- | --- | --- |
| coordinator-migrate | python -m alembic upgrade head | One-shot, only B's DB, after D's db-init |
| coordinator | Image default Uvicorn command | HTTP health/docs; /ready checks current B schema |
| coordinator-intake | python -m app.tools.consume_orders --watch | order.created -> durable B acceptance/job |
| coordinator-worker | python -m app.tools.run_workflows --watch | Saved Saga jobs -> seller HTTP/state/outbox |
| coordinator-relay | python -m app.tools.publish_outbox --watch | B outbox -> workflow.events |

All long-running processes wait for migration success and D's Kafka topic init.
Only migration runs Alembic; it does not initialize other services' DBs or rerun
shared init.sql. Existing schema head remains 0005_workflow_jobs; no new migration.
The role/DB agree with D's draft: coordinator/coordinator_db. Passwords are supplied
by D's infra/.env, never embedded in this handoff. D documents generated hex
passwords, which are URL-safe; otherwise URL-encode passwords before URL assembly.
Seller names/ports follow D's current topology and still require C confirmation.

The worker is in a separate coordinator-workflows profile until C's endpoints and
terminal fencing are reviewed. Intake/relay can integrate with D independently.
No worker host ports are exposed. HTTP readiness does not certify worker progress;
watch process exits/logs, queue lag, due-job age and outbox backlog separately.

From the repository root, after B and D changes are present and D's infra/.env is
configured, define this shell helper (Compose 2.24+ required):

```bash
coordinator_compose() {
  docker compose --env-file infra/.env \
    -f infra/docker-compose.yml \
    -f services/coordinator/compose.integration.yaml "$@"
}
```

Review configuration privately: `config` output can contain credentials. For a
syntax-only check use `coordinator_compose --profile apps config --quiet`.
First start B's HTTP and publication path (this runs the migration dependency):

```bash
coordinator_compose --profile apps up -d --build --wait coordinator coordinator-relay
```

For a brand-new B consumer group only, before any long-running intake is active,
deliberately initialize missing partition offsets and handle one bounded batch:

```bash
coordinator_compose --profile apps run --rm --no-deps coordinator-intake \
  python -m app.tools.consume_orders --bootstrap-earliest
```

Do not reset/delete an existing group's offsets. Ordinary restarts use:

```bash
coordinator_compose --profile apps up -d --build --wait coordinator-intake
```

After C compatibility is confirmed, enable automatic seller work explicitly:

```bash
coordinator_compose --profile coordinator-workflows up -d --build --wait coordinator-worker
coordinator_compose --profile apps --profile coordinator-workflows ps
```

Before later schema deployments, gracefully stop B's intake/worker/relay, run the
single migration job, then restart updated processes. Keep the data volumes.
D's deploy.sh currently loads only infra/docker-compose.yml; it will **not** pick
up this overlay automatically. D must add the second -f file there or incorporate
these service blocks into its owned Compose definition. No D deploy script or
shared infrastructure file was changed by B.

## Repository integration

PRs #3-#11 were merged into their stacked base branches, not all into main.
At review time main a7b9c13 contains only B's foundation; feature/package-b-coordinator
f788c56 contains the full service. Include this change on that development branch
and open a final PR **base main**, comparing the full coordinator implementation.
Preserve the historical review branches; do not reset/force-push them.

This completes B's preparation for D review. Real broker-to-tracker/cloud rollout,
C's live seller integration and deployment failure testing remain joint work;
benchmarks and final report evidence follow verified integration.
