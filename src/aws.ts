import type { Command } from "commander";
import {
  DescribeTasksCommand,
  ECSClient,
  RunTaskCommand,
  waitUntilTasksStopped,
} from "@aws-sdk/client-ecs";
import { ElasticLoadBalancingV2Client } from "@aws-sdk/client-elastic-load-balancing-v2";
import { RDSClient } from "@aws-sdk/client-rds";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { GetParameterCommand, SSMClient } from "@aws-sdk/client-ssm";
import { fail, say } from "./cli.js";

// ── environments ──────────────────────────────────────────────────────────────
// CONFIRM regions against the real deployment (creds are per-env / per-account).
const ENVIRONMENTS = {
  dev: { region: "us-east-1" },
  staging: { region: "us-east-1" },
  prod: { region: "us-east-1" },
} as const;

export type EnvName = keyof typeof ENVIRONMENTS;

export interface GlobalOpts {
  env: EnvName;
  stack?: string; // explicit runtime stack, e.g. airflow-3_2_1
  to?: string; // version shorthand, e.g. 3.2.1
  json: boolean;
  dryRun: boolean;
  yes: boolean;
}

export interface EnvConfig {
  name: EnvName;
  region: string;
}

export function resolveEnv(name: string): EnvConfig {
  const e = ENVIRONMENTS[name as EnvName];
  if (!e) fail(`Unknown --env "${name}". Known: ${Object.keys(ENVIRONMENTS).join(", ")}`);
  return { name: name as EnvName, ...e };
}

/** Version "3.2.1" ⇄ runtime stack "airflow-3_2_1". */
export function versionToStack(v: string): string {
  const m = v.trim().match(/^(\d+)\.(\d+)\.(\d+)$/);
  if (!m) fail(`Invalid --to "${v}" (expected e.g. 3.2.1).`);
  return `airflow-${m[1]}_${m[2]}_${m[3]}`;
}

/** The runtime stack a command targets, from --stack or --to. */
export function runtimeStackName(opts: GlobalOpts): string {
  if (opts.stack) return opts.stack;
  if (opts.to) return versionToStack(opts.to);
  fail("This command needs a runtime stack: pass --stack airflow-3_2_1 or --to 3.2.1.");
}

// ── clients + session ─────────────────────────────────────────────────────────
export interface AwsClients {
  ssm: SSMClient;
  ecs: ECSClient;
  elbv2: ElasticLoadBalancingV2Client;
  rds: RDSClient;
  secrets: SecretsManagerClient;
}

export function makeClients(env: EnvConfig): AwsClients {
  const cfg = { region: env.region };
  return {
    ssm: new SSMClient(cfg),
    ecs: new ECSClient(cfg),
    elbv2: new ElasticLoadBalancingV2Client(cfg),
    rds: new RDSClient(cfg),
    secrets: new SecretsManagerClient(cfg),
  };
}

/** Bootstrap a command: global opts + resolved env + AWS clients. Plain data. */
export function session(cmd: Command): { opts: GlobalOpts; env: EnvConfig; aws: AwsClients } {
  const opts = cmd.optsWithGlobals() as GlobalOpts;
  const env = resolveEnv(opts.env);
  return { opts, env, aws: makeClients(env) };
}

// ── config from SSM ───────────────────────────────────────────────────────────
// Each stack publishes its config as a single JSON SSM parameter. The interfaces
// below ARE the JSON shape (field name = JSON key) — parse once, pass the struct
// around. CONFIRM the paths and field names against your stacks.
export const SSM_PARAM = {
  persist: "/airflow/persist",
  runtime: (stackName: string) => `/airflow/${stackName}`,
};

// CONFIRM: the container name in the task definition that runs the airflow CLI.
export const AIRFLOW_CONTAINER = "airflow";

/** `/airflow/persist` */
export interface PersistConfig {
  dbInstanceIdentifier: string;
  dbSecretArn: string;
  httpsListenerArn: string;
}

/** `/airflow/<stack>` */
export interface RuntimeConfig {
  clusterArn: string;
  taskDefinitionArn: string;
  privateSubnets: string[];
  serviceSecurityGroups: string[];
}

/** Read a JSON SSM parameter and parse it into `T`. */
async function readConfig<T>(ssm: SSMClient, name: string): Promise<T> {
  let value: string | undefined;
  try {
    value = (await ssm.send(new GetParameterCommand({ Name: name, WithDecryption: true }))).Parameter?.Value;
  } catch (e) {
    fail(`Could not read SSM parameter "${name}": ${(e as Error).message}`);
  }
  if (!value) fail(`SSM parameter "${name}" is empty or missing.`);
  try {
    return JSON.parse(value) as T;
  } catch {
    fail(`SSM parameter "${name}" is not valid JSON.`);
  }
}

export const persistConfig = (ssm: SSMClient): Promise<PersistConfig> =>
  readConfig<PersistConfig>(ssm, SSM_PARAM.persist);

export const runtimeConfig = (ssm: SSMClient, stackName: string): Promise<RuntimeConfig> =>
  readConfig<RuntimeConfig>(ssm, SSM_PARAM.runtime(stackName));

// ── one-off ECS task (how we run the airflow CLI) ─────────────────────────────
export interface ManualEcsTaskParams {
  cluster: string;
  taskDefinition: string;
  container: string;
  command: string[];
  subnets: string[];
  securityGroups: string[];
}

/** Run a one-off Fargate task with the container command overridden. By default
 *  waits for it to stop and fails on a non-zero exit, so CI fails when the CLI does. */
export async function runManualEcsTask(
  ecs: ECSClient,
  p: ManualEcsTaskParams,
  opts: { wait: boolean } = { wait: true },
): Promise<{ taskArn: string; exitCode: number | null }> {
  const started = await ecs.send(
    new RunTaskCommand({
      cluster: p.cluster,
      taskDefinition: p.taskDefinition,
      launchType: "FARGATE",
      networkConfiguration: {
        awsvpcConfiguration: { subnets: p.subnets, securityGroups: p.securityGroups, assignPublicIp: "DISABLED" },
      },
      overrides: { containerOverrides: [{ name: p.container, command: p.command }] },
    }),
  );

  const taskArn = started.tasks?.[0]?.taskArn;
  if (!taskArn) fail(`RunTask did not start a task: ${started.failures?.[0]?.reason ?? "unknown reason"}`);
  const shortId = taskArn.split("/").pop() ?? taskArn;
  say(`→ started task ${shortId}`);
  if (!opts.wait) return { taskArn, exitCode: null };

  await waitUntilTasksStopped({ client: ecs, maxWaitTime: 1800 }, { cluster: p.cluster, tasks: [taskArn] });
  const desc = await ecs.send(new DescribeTasksCommand({ cluster: p.cluster, tasks: [taskArn] }));
  const container = desc.tasks?.[0]?.containers?.find((c) => c.name === p.container);
  const exitCode = container?.exitCode ?? null;
  if (exitCode !== 0) {
    fail(`task ${shortId} failed (exit ${exitCode ?? "?"}: ${container?.reason ?? desc.tasks?.[0]?.stoppedReason ?? "no exit code"})`);
  }
  return { taskArn, exitCode };
}
