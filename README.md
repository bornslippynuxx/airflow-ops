# airflow-ops

A small, typed CLI for operating an Apache Airflow 3 deployment running on AWS
ECS Fargate + RDS. It replaces a pile of maintenance bash scripts (the kind you
end up calling from CI jobs) with one binary whose commands resolve everything
they need at runtime from **SSM parameters** (one JSON blob per stack) — no
hardcoded ARNs.

Built to run in CI (non-interactive): commands print status to stderr, machine
output to stdout (`--json`), and destructive ops require `--yes`.

## Topology it assumes

- **`airflow-persist`** — durable stack: RDS metadata DB, ALB + listener, secret.
- **`airflow-<M>_<m>_<p>`** — immutable, versioned runtime stack (ECS services +
  task def). e.g. `airflow-3_1_1`. The stack name is the version; `--to 3.2.1`
  resolves to `airflow-3_2_1`.

## Commands

```
airflow  migrate                 # airflow db migrate      (one-off ECS task)
airflow  metadata-clean --before # airflow db clean        (destructive)
airflow  create-task-pool        # airflow pools set
airflow  create-api-user         # airflow users create
alb      describe-rules          # list ALB listener rules
bg       describe                # RDS blue/green deployment status
bg       green-db-conn           # green (target) DB connection string
```

The `airflow *` commands can't shell into a running service, so they launch a
one-off Fargate task from the runtime stack's existing task definition with the
container command overridden — inheriting its DB connection config.

## Configure

Each stack publishes its outputs as a single JSON SSM parameter — persist at
`/airflow/persist`, each runtime stack at `/airflow/<stack>`. Confirm these in one
file, [`src/aws.ts`](./src/aws.ts), against your deployment:

- `ENVIRONMENTS` — region per `--env`.
- `SSM_PARAM` — the parameter paths.
- `PersistConfig` / `RuntimeConfig` — the interfaces each param's JSON is parsed
  into (field name = JSON key). Check the real keys with:
  ```
  aws ssm get-parameter --name /airflow/persist --query Parameter.Value --output text | jq keys
  ```
- `AIRFLOW_CONTAINER` — the container name in the task def that runs the airflow CLI.

## Usage

```bash
npm install
npm run ops -- alb describe-rules --env dev
npm run ops -- bg green-db-conn --env prod --no-password
npm run ops -- airflow migrate --env dev --to 3.2.1
```

Credentials come from the default AWS provider chain (env / SSO / role /
`AWS_PROFILE`). Region is pinned per `--env`, so `AWS_REGION` isn't needed.

## Build (for CI)

```bash
npm run build          # → dist/ops.cjs (single self-contained file)
node dist/ops.cjs alb describe-rules --env prod --json
```

## Global flags

`--env <dev|staging|prod>` · `--stack <name>` / `--to <semver>` · `--json` ·
`--dry-run` · `--yes`

## Development

```bash
npm run typecheck
```

Source layout: `src/aws.ts` (env, clients, SSM-param resolution, one-off task
runner), `src/cli.ts` (output, `--yes` guard, error→exit-code), `src/commands/*`
(thin command handlers), `src/index.ts` (wiring).

## License

MIT
