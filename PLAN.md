# airflow-ops

## What this is

A small command-line tool that runs the routine maintenance tasks for our Airflow
setup (Airflow 3, running on AWS ECS Fargate with an RDS database). It replaces a
set of bash scripts that GitLab CI jobs used to call. Now each GitLab job just runs
one command from this tool.

Goal: keep it **simple and small** — easy for a new programmer to read top to bottom.

## Where it gets its settings

The tool hardcodes no AWS IDs. When it runs, it reads a small JSON blob from AWS
SSM Parameter Store and parses it into a typed object:

- `/airflow/persist` → the long-lived stuff: the database id, the load balancer's
  listener, and the DB password secret. (The `PersistConfig` type in `config-store.ts`.)
- `/airflow/<stack>` → one per Airflow version, e.g. `/airflow/airflow-3_2_1`. Holds
  that version's ECS cluster, task definition, subnets, and security groups.
  (The `RuntimeConfig` type.)

Region and credentials come from the environment: `AWS_REGION` plus whatever role
the GitLab job assumes. There is no `--env` flag.

## Commands that exist today

| Command                            | What it does                                                                                                                                                         |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `airflow exec --stack X -- <cmd>`  | Runs any airflow CLI command as a one-off task (e.g. `-- db migrate`, `-- pools set …`, `-- users create …`). A straight passthrough.                                |
| `airflow metadata-clean --stack X` | Runs `airflow db clean` with a retention policy: `--retention-days N` (default 60, min 30 unless `--dry-run`), `--mode clean_all\|exclude_dag_version`, `--dry-run`. |
| `alb describe-rules`               | Lists the load balancer's routing rules.                                                                                                                             |
| `bg describe`                      | Shows the RDS blue/green deployment status.                                                                                                                          |
| `bg green-db-conn`                 | Prints the new (green) database's connection string.                                                                                                                 |

The `airflow` commands can't log into a running server, so they run the airflow
command as a **one-off ECS task** (reusing the runtime stack's task definition, so
it already has the DB connection) and wait for it to finish — failing if it exits
non-zero. `exec` just forwards its args; `metadata-clean` is the one airflow op with
real policy (retention math, the 30-day floor, table modes), ported from the bash.

## How to run it

```bash
yarn build                                                   # tsc → dist/
node dist/index.js airflow exec --stack airflow-3_2_1 -- db migrate
node dist/index.js airflow metadata-clean --stack airflow-3_2_1 --retention-days 60
```

Needs `AWS_REGION` and AWS credentials in the environment. `--stack` (which Airflow
version to target) is a per-command flag. Run the compiled `dist/` — `exec`'s `--`
separator is swallowed by `yarn`/`npm` scripts. In GitLab, each job runs one command.

## How the code is laid out

Object-oriented — one class per file (familiar if you come from Java):

- `src/cli.ts` — the application. Builds the command-line program and wires the
  services into the command groups (constructor injection).
- `src/aws-clients.ts` — holds the AWS SDK clients, created once.
- `src/config-store.ts` — reads a stack's config from its SSM parameter into a typed
  object (`PersistConfig` / `RuntimeConfig`).
- `src/ecs-task-runner.ts` — runs a one-off Fargate task (how the airflow CLI runs).
- `src/log.ts` — console output (status to stderr, results to stdout). `Cli` catches
  any thrown `Error`, prints its message, and exits 1.
- `src/commands/*-commands.ts` — one class per group (`AirflowCommands`, `AlbCommands`,
  `BgCommands`); each registers its subcommands.
- `src/index.ts` — the entry point: `new Cli().run(process.argv)`.

## Still to add

The remaining GitLab maintenance jobs, still to port (grouped):

- **ALB routing:** add a route, delete route(s) by host or priority, point traffic at a stack.
- **RDS blue/green:** create, switch over, abandon, delete.
- **Database:** delete, snapshot, restore.
- **DNS:** add/update a record.
- **Maintenance page:** turn on / off.
- **Services:** set one service's count, set all counts, delete a stack.
- **Version upgrade:** the multi-step flow below.

## Upgrading to a new Airflow version

You can't upgrade in place, because the new version's database changes may break the
old version. So you bring the new version up next to the old one, move the data
across, then switch. Run these steps in order:

1. Deploy the new version's stack (done by the deploy job, not this tool).
2. Make a safety copy of the database — a snapshot, or a blue/green clone.
3. Stop the old version from writing: scale its services to 0 and show a maintenance page.
4. Run `airflow exec --stack <new> -- db migrate` to update the database.
5. (blue/green only) switch the clone to become the main database.
6. Start the new version's services.
7. Check the new version is healthy.
8. Point the load balancer at the new version and remove the maintenance page.
9. Delete the old version's stack.

If any step fails, undo the steps already done: start the old services again, remove
the maintenance page, point the load balancer back, and restore the snapshot.

## Deploying infrastructure changes (no version upgrade) — graceful worker drain

The runbook above is for a **version** change. Most deploys aren't: they change only the
**infrastructure** on the _same_ Airflow version — task sizing (CPU/memory), env vars, a
same-version image patch, autoscaling. Those deploy **in place**: you re-deploy the same
version-named stack and let ECS roll the tasks. There is **no** new stack, no DB snapshot, no
`db migrate` (the schema doesn't change), no ALB cutover, and no maintenance page.

The one real risk is the **worker** service. When ECS replaces a running worker task it sends
it a SIGTERM; if the worker isn't set up to drain, that SIGTERM (eventually a SIGKILL) **kills
whatever DAG task instances were mid-run**. The settings below make the old worker _drain_ —
stop taking new work, finish what it's running — before ECS kills it.

This works here because **our tasks run under ~2 minutes**, which fits inside Fargate's hard
**120-second `stopTimeout` cap**. If that stops being true, item 6 (retry/adoption) stops being
a safety net and becomes load-bearing.

### The deploy

1. Confirm it's a same-version change: the Airflow version / image tag and the stack name are
   unchanged. If the version changes, use the upgrade runbook above instead.
2. `cdk deploy` the same version-named runtime stack in place (the deploy job, not this tool).
   ECS registers a new task-def revision and rolls each service.
3. That's it — no `db migrate`, no snapshot, no ALB cutover, no maintenance page.
4. **Rollback:** the ECS deployment circuit breaker auto-reverts to the previous task-def
   revision (or re-deploy the previous CDK revision). No DB restore — the schema was untouched.

### The config that makes workers drain (set once, in the CDK stack / Airflow config)

These belong in the CDK task/service definitions and Airflow config, not in this tool. Verify
them once (see below); after that every in-place deploy drains cleanly.

1. **SIGTERM must reach Celery — the #1 cause of killed tasks.** ECS sends SIGTERM to the
   container's PID 1. Celery only warm-shuts-down if it actually receives it. Make the worker
   entrypoint `exec` the Celery worker (so it _is_ PID 1), **or** set the container's
   `linuxParameters.initProcessEnabled: true` (tini) to forward signals. A shell wrapper that
   swallows SIGTERM means no drain and every deploy kills running tasks. If you check only one
   thing, check this.
2. **Worker `stopTimeout: 120`** (the Fargate maximum). ECS waits this long after SIGTERM
   before SIGKILL — long enough for a sub-2-minute task to finish. Keep the Airflow task
   `execution_timeout` comfortably under 120s (≤ ~100s). Any task that runs past ~120s can
   still be killed and must fall back on retries (item 6).
3. **Keep Celery's default warm shutdown.** One SIGTERM = warm shutdown (stop consuming, finish
   active tasks, exit). ECS sends exactly one, then waits `stopTimeout`. Don't add anything that
   sends a _second_ SIGTERM — that forces a cold shutdown, which kills running tasks.
4. **`AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER = 1`.** With higher prefetch a worker reserves
   messages it hasn't started yet; on drain those sit stuck or get lost. Multiplier 1 means it
   only holds what it's actively running.
5. **ECS deployment config on every service:** `minimumHealthyPercent: 100`,
   `maximumPercent: 200`, and the **deployment circuit breaker with rollback** enabled. ECS then
   starts and health-checks the new tasks _before_ draining the old ones, so old workers get
   their full SIGTERM + `stopTimeout` drain window and the webserver/API stays reachable.
6. **Retry / adoption safety net** (for a task that outlives the window, or a worker that dies
   outright): give tasks `retries >= 1`; set the broker **visibility timeout above the max task
   runtime** (SQS visibility timeout / Redis `visibility_timeout`) with `task_acks_late` so an
   interrupted, unacked message is redelivered instead of lost; and confirm the scheduler
   adopts/re-queues orphaned `running` task instances so none are stranded.
7. **Worker container health check** (e.g. `celery inspect ping`), so ECS counts a new worker
   as healthy only once it's actually consuming — otherwise, with `minimumHealthyPercent: 100`,
   ECS could drain an old worker before its replacement is ready.
8. **ALB target-group `deregistration_delay` (~30s)** on the webserver/API, so in-flight HTTP
   requests to old tasks finish during the roll (finishes the zero-downtime story for the UI).

### Verify it once, on dev

1. Kick off a ~90-second dummy DAG task on a dev worker.
2. While it's running, `cdk deploy` in place (or otherwise force a new task-def revision) so ECS
   rolls the worker service.
3. In the worker logs, confirm SIGTERM triggers a **warm shutdown** and the task **runs to
   completion** (not SIGKILLed); confirm the task instance ends `success` in the UI.
4. Confirm the webserver/API stayed reachable and the deployment reached steady state (no
   circuit-breaker rollback).
5. Optional negative check: temporarily drop the worker `stopTimeout` to ~30s and repeat — the
   task should now be killed at ~30s, proving `stopTimeout` is the control that matters.

## Notes

- User creation, pool creation, etc. go through `airflow exec` — e.g.
  `airflow exec --stack X -- users create --username … --role …`. No wrapper code.
- The snapshot approach (step 2) has more downtime than the blue/green one; pick per case.
