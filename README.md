# airflow-ops

A small, typed CLI for operating an Apache Airflow 3 deployment running on AWS
ECS Fargate + RDS. It replaces a pile of maintenance bash scripts (the kind you
end up calling from CI jobs) with one binary whose commands resolve everything
they need at runtime from **SSM parameters** (one JSON blob per stack) — no
hardcoded ARNs.

Built to run in GitLab CI (non-interactive): status goes to stderr, results to
stdout, and destructive ops require `--yes`. Region + credentials come from the
ambient AWS environment (`AWS_REGION` + the role the job assumes).

## Topology it assumes

- **`airflow-persist`** — durable stack: RDS metadata DB, ALB + listener, secret.
- **`airflow-<M>_<m>_<p>`** — immutable, versioned runtime stack (ECS services +
  task def), e.g. `airflow-3_1_1`. The stack name is the version; commands that
  target it take `--stack airflow-3_2_1`.

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

Each stack publishes its config as a single JSON SSM parameter — persist at
`/airflow/persist`, each runtime stack at `/airflow/<stack>`. Confirm these in one
file, [`src/aws.ts`](./src/aws.ts), against your deployment:

- `SSM_PARAM` — the parameter paths.
- `PersistConfig` / `RuntimeConfig` — the interfaces each param's JSON is parsed
  into (field name = JSON key). Check the real keys with:
  ```
  aws ssm get-parameter --name /airflow/persist --query Parameter.Value --output text | jq keys
  ```
- `AIRFLOW_CONTAINER` — the container name in the task def that runs the airflow CLI.

## Usage

```bash
yarn install
export AWS_REGION=us-east-1        # + credentials via env / SSO / role
yarn ops alb describe-rules
yarn ops bg green-db-conn --no-password
yarn ops airflow migrate --stack airflow-3_2_1
```

## Build (for CI)

```bash
yarn build          # → dist/ops.cjs (single self-contained file)
node dist/ops.cjs airflow migrate --stack airflow-3_2_1 --yes
```

## Global flags

`--stack <name>` · `--yes`

## Development

```bash
yarn typecheck
```

Source layout: `src/aws.ts` (clients, SSM-param resolution, one-off task runner),
`src/cli.ts` (`--yes` guard, error→exit-code), `src/commands/*` (thin handlers),
`src/index.ts` (wiring).

## License

MIT
