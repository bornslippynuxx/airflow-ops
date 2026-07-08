# airflow-ops

A small, typed CLI for operating an Apache Airflow 3 deployment running on AWS
ECS Fargate + RDS. It replaces a pile of maintenance bash scripts (the kind you
end up calling from CI jobs) with one binary whose commands resolve everything
they need at runtime from **SSM parameters** (one JSON blob per stack) — no
hardcoded ARNs.

Built to run in GitLab CI (non-interactive): status goes to stderr, results to
stdout. Region + credentials come from the ambient AWS environment (`AWS_REGION` +
the role the job assumes).

## Topology it assumes

- **`airflow-persist`** — durable stack: RDS metadata DB, ALB + listener, secret.
- **`airflow-<M>_<m>_<p>`** — immutable, versioned runtime stack (ECS services +
  task def), e.g. `airflow-3_1_1`. The stack name is the version; commands that
  target it take `--stack airflow-3_2_1`.

## Commands

```
airflow  exec -- <cmd>   # run any airflow CLI command (db migrate, pools set, users create, …)
airflow  metadata-clean  # airflow db clean, with a retention policy (see below)
alb      describe-rules  # list ALB listener rules
bg       describe        # RDS blue/green deployment status
bg       green-db-conn   # green (target) DB connection string
```

The `airflow` commands can't shell into a running service, so they launch a one-off
Fargate task from the runtime stack's existing task definition with the container
command overridden — inheriting its DB connection config. `exec` is a straight
passthrough; `metadata-clean` wraps `airflow db clean` with policy:

- `--retention-days <N>` (default 60) → deletes metadata before _N days ago_.
- Refuses `N < 30` unless `--dry-run` (protects recent metadata).
- `--mode clean_all` (default) or `exclude_dag_version` (keeps `dag_version`/`dag`).

## Configure

Each stack publishes its config as a single JSON SSM parameter — persist at
`/airflow/persist`, each runtime stack at `/airflow/<stack>`. Confirm these against
your deployment:

- Parameter paths + the `PersistConfig` / `RuntimeConfig` interfaces (field name =
  JSON key) live in [`src/config-store.ts`](./src/config-store.ts). Check the real keys with:
  ```
  aws ssm get-parameter --name /airflow/persist --query Parameter.Value --output text | jq keys
  ```
- The container name that runs the airflow CLI is `AirflowCommands.CONTAINER` in
  [`src/commands/airflow-commands.ts`](./src/commands/airflow-commands.ts).

## Usage

```bash
yarn install && yarn build         # tsc → dist/
export AWS_REGION=us-east-1         # + credentials via env / SSO / role
node dist/index.js alb describe-rules
node dist/index.js bg green-db-conn --no-password
node dist/index.js airflow exec --stack airflow-3_2_1 -- db migrate
node dist/index.js airflow metadata-clean --stack airflow-3_2_1 --retention-days 60
```

Run the compiled `dist/` — the `airflow exec … -- <cmd>` passthrough relies on the
`--` separator, which `yarn`/`npm` scripts swallow.

## Build (for CI)

```bash
yarn build          # tsc → dist/ (compiled JS)
node dist/index.js airflow exec --stack airflow-3_2_1 -- db migrate
```

## Development

```bash
yarn typecheck
```

Object-oriented, one class per file: `Cli` (wiring), `AwsClients`, `ConfigStore`
(SSM → typed config), `EcsTaskRunner`, `Log`, and `commands/*Commands`
(one class per group). Entry point: `src/index.ts` → `new Cli().run(process.argv)`.

## License

MIT
