# Wiring `airflow-ops` into your stack

The CLI holds almost no knowledge of your infrastructure. Every command resolves
what it needs **at runtime from an SSM parameter** — one JSON blob per stack — then
calls AWS. So wiring it up is four steps:

1. Confirm the config (SSM paths + field names) in [`src/config-store.ts`](./src/config-store.ts).
2. Have your CDK stacks **publish the SSM params** the CLI reads.
3. Grant the **CI role** the AWS permissions the commands use.
4. **Call it** from GitLab jobs.

Then verify safest-first (read-only commands before anything that mutates).

---

## The contract (what the CLI reads and calls)

The bits you confirm:

- Parameter paths (`/airflow/persist`, `/airflow/<stack>`) + the `PersistConfig` /
  `RuntimeConfig` interfaces — in [`src/config-store.ts`](./src/config-store.ts).
- The container name that runs the airflow CLI — `AirflowCommands.CONTAINER` in
  [`src/commands/airflow-commands.ts`](./src/commands/airflow-commands.ts).

Region + credentials come from the ambient AWS environment (`AWS_REGION` + the
role the job assumes) — the CLI has no `--env` of its own.

| Command                           | Reads (SSM param → fields)                                                                        | AWS API calls                                                          |
| --------------------------------- | ------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `alb describe-rules`              | `/airflow/persist` → `httpsListenerArn`                                                           | `ssm:GetParameter`, `elasticloadbalancing:DescribeRules`               |
| `bg describe`                     | `/airflow/persist` → `dbInstanceIdentifier`                                                       | `ssm:GetParameter`, `rds:DescribeBlueGreenDeployments`                 |
| `bg green-db-conn`                | `/airflow/persist` → `dbInstanceIdentifier`, `dbSecretArn`                                        | + `rds:DescribeDBInstances`, `secretsmanager:GetSecretValue`           |
| `airflow exec` / `metadata-clean` | `/airflow/<stack>` → `clusterArn`, `taskDefinitionArn`, `privateSubnets`, `serviceSecurityGroups` | `ssm:GetParameter`, `ecs:RunTask`, `ecs:DescribeTasks`, `iam:PassRole` |

---

## Step 1 — Confirm the config

In [`src/config-store.ts`](./src/config-store.ts), the parameter paths and the config
interfaces:

```ts
export class ConfigStore {
  static readonly PERSIST_PARAM = "/airflow/persist";
  static runtimeParam(stack: string): string {
    return `/airflow/${stack}`; // e.g. /airflow/airflow-3_2_1
  }
  // ...
}
```

And the container name in [`src/commands/airflow-commands.ts`](./src/commands/airflow-commands.ts):

```ts
private static readonly CONTAINER = "airflow"; // must match the task-def container name
```

**Runtime param path.** `--stack airflow-3_2_1` → param `/airflow/airflow-3_2_1`. If
you'd rather key on the bare version (`/airflow/3_2_1`), change `runtimeParam` here
and the CDK side (Step 2) together.

---

## Step 2 — Publish the SSM params from your CDK stacks

Each stack writes **one** `StringParameter` holding a JSON object. The JSON field
names must match the `PersistConfig` / `RuntimeConfig` interfaces in
[`src/config-store.ts`](./src/config-store.ts).

**Persist stack** — `/airflow/persist`:

```ts
new ssm.StringParameter(this, "OpsConfig", {
  parameterName: "/airflow/persist",
  stringValue: JSON.stringify({
    dbInstanceIdentifier: db.instanceIdentifier,
    dbSecretArn: db.secret!.secretArn,
    httpsListenerArn: httpsListener.listenerArn,
  }),
});
```

**Each runtime stack** — `/airflow/<stackName>`:

```ts
new ssm.StringParameter(this, "OpsConfig", {
  parameterName: `/airflow/${this.stackName}`, // e.g. /airflow/airflow-3_2_1
  stringValue: JSON.stringify({
    clusterArn: cluster.clusterArn,
    taskDefinitionArn: taskDef.taskDefinitionArn,
    // JSON arrays — parsed straight into string[]
    privateSubnets: vpc.privateSubnets.map((s) => s.subnetId),
    serviceSecurityGroups: service.connections.securityGroups.map((sg) => sg.securityGroupId),
  }),
});
```

**Why the runtime task def / subnets / SGs?** `airflow migrate` etc. can't shell
into a running service, so they launch a **one-off Fargate task from your existing
service task definition** with the container command overridden to `airflow …`.
Reusing that task def is deliberate: it already carries the DB connection env /
secrets, so the one-off task connects to the metadata DB with no extra config.

> **Same-repo tip:** if the CLI becomes a subpackage of the CDK repo, export the
> field-name strings from one shared module and have both the `JSON.stringify({…})`
> and the config interfaces import it — then they can't drift.

---

## Step 3 — Grant the CI role permissions

The role your GitLab jobs assume needs (scope the resources down to taste):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadConfig",
      "Effect": "Allow",
      "Action": ["ssm:GetParameter"],
      "Resource": "arn:aws:ssm:REGION:ACCOUNT:parameter/airflow/*"
    },
    {
      "Sid": "Read",
      "Effect": "Allow",
      "Action": ["elasticloadbalancing:DescribeRules", "rds:DescribeBlueGreenDeployments", "rds:DescribeDBInstances"],
      "Resource": "*"
    },
    {
      "Sid": "DbSecret",
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:REGION:ACCOUNT:secret:airflow/db-*"
    },
    { "Sid": "RunAirflowCli", "Effect": "Allow", "Action": ["ecs:RunTask", "ecs:DescribeTasks"], "Resource": "*" },
    {
      "Sid": "PassTaskRoles",
      "Effect": "Allow",
      "Action": ["iam:PassRole"],
      "Resource": [
        "arn:aws:iam::ACCOUNT:role/airflow-task-execution-role",
        "arn:aws:iam::ACCOUNT:role/airflow-task-role"
      ],
      "Condition": { "StringEquals": { "iam:PassedToService": "ecs-tasks.amazonaws.com" } }
    }
  ]
}
```

If the params are `SecureString`, also allow `kms:Decrypt` on their key.
`iam:PassRole` is required because `RunTask` hands ECS the task's execution + task
roles; the one-off task's own DB/network access comes from **those** roles.

---

## Step 4 — Build and invoke

```bash
yarn install --frozen-lockfile
yarn build           # tsc → dist/ (compiled JS)
node dist/index.js alb describe-rules
```

Region + credentials come from the ambient AWS environment (`AWS_REGION` + the
OIDC/assumed role) — the CI job for a given account is already scoped to it.

### GitLab jobs (build once, thin wrappers consume the artifact)

One build job compiles `dist/`; the ops jobs `needs: [build]` and just run
`node dist/index.js`. The AWS SDK is **not** bundled, so the runtime also needs
`node_modules` — artifact it alongside `dist/` (or cache + reinstall per job).

```yaml
build:
  stage: build
  image: node:22-slim
  script:
    - yarn install --frozen-lockfile
    - yarn build # tsc → dist/
  artifacts:
    paths: [dist/, node_modules/]

.ops:
  stage: ops
  image: node:22-slim
  needs: [build]
  # AWS creds via OIDC (id_tokens + sts:AssumeRoleWithWebIdentity); sets AWS_REGION

alb_describe_rules:
  extends: .ops
  script: ["node dist/index.js alb describe-rules"]

db_migrate: # job name unchanged; airflow op goes through `exec`
  extends: .ops
  script: ["node dist/index.js airflow exec --stack $STACK -- db migrate"]

db_metadata_clean:
  extends: .ops
  script: ["node dist/index.js airflow metadata-clean --stack $STACK --retention-days $DAYS --mode $MODE"]
```

`metadata-clean` guards deletes with a 30-day retention floor + `--dry-run` (not a
`--yes` gate). Any failure exits non-zero, so the GitLab job goes red.

---

## Step 5 — Verify, safest-first

With your **dev** account's creds + `AWS_REGION` set, read-only before anything
that mutates:

1. `alb describe-rules` — proves config + SSM param + auth. Zero blast radius.
2. `bg describe` — proves the RDS path.
3. `bg green-db-conn --no-password` — proves secret access + the runtime data.
4. `airflow exec --stack airflow-<v> -- db migrate` — proves `RunTask` + the task
   def wiring end-to-end (it waits and reports the container exit code).

If step 1 fails, the JSON in the SSM param doesn't match `PersistConfig` — align the
param's keys (or the interface) in [`src/config-store.ts`](./src/config-store.ts).

---

## Gotchas

- **Entrypoint vs command.** `RunTask` overrides the container **command**, not its
  entrypoint. The airflow image's entrypoint must exec the given command so
  `["airflow","db","migrate"]` runs.
- **Private subnets need egress.** The one-off task pulls its image and reaches AWS
  APIs — the `privateSubnets` need a NAT gateway or VPC endpoints (the CLI sets
  `assignPublicIp: DISABLED`).
- **`exec` needs the `--` separator**, and `yarn`/`npm` scripts swallow it — invoke
  the compiled `node dist/index.js airflow exec --stack X -- <cmd>` directly.
