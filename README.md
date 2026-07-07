# airflow-ops

A small, typed CLI for operating an Apache Airflow 3 deployment running on AWS
ECS Fargate + RDS. It replaces a pile of maintenance bash scripts (the kind you
end up calling from CI jobs) with one binary whose commands resolve everything
they need from **CloudFormation stack outputs** at runtime — no hardcoded ARNs.

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

One file, [`src/aws.ts`](./src/aws.ts). Confirm against your deployment:

- `ENVIRONMENTS` — region + persist stack name per `--env`.
- `OUTPUT_KEYS` — the CloudFormation output keys the CLI reads. Check with:
  ```
  aws cloudformation describe-stacks --stack-name airflow-persist \
    --query 'Stacks[0].Outputs[].OutputKey'
  ```
- `AIRFLOW_CONTAINER` — the container name in the task def that runs the airflow CLI.

A missing/renamed output fails fast with the key name — fix it once, every command
inherits it.

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

Source layout: `src/aws.ts` (env, clients, stack-output resolution, one-off task
runner), `src/cli.ts` (output, `--yes` guard, error→exit-code), `src/commands/*`
(thin command handlers), `src/index.ts` (wiring).

## License

MIT
