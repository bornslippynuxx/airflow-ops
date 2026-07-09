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

## Notes

- User creation, pool creation, etc. go through `airflow exec` — e.g.
  `airflow exec --stack X -- users create --username … --role …`. No wrapper code.
- The snapshot approach (step 2) has more downtime than the blue/green one; pick per case.
