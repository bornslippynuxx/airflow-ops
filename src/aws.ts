import type { Command } from "commander";
import { CloudFormationClient, DescribeStacksCommand, type Stack } from "@aws-sdk/client-cloudformation";
import {
  DescribeTasksCommand,
  ECSClient,
  RunTaskCommand,
  waitUntilTasksStopped,
} from "@aws-sdk/client-ecs";
import { ElasticLoadBalancingV2Client } from "@aws-sdk/client-elastic-load-balancing-v2";
import { RDSClient } from "@aws-sdk/client-rds";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { fail, say } from "./cli.js";

// ── environments ──────────────────────────────────────────────────────────────
// CONFIRM regions + persist stack name against the real deployment.
const ENVIRONMENTS = {
  dev: { region: "us-east-1", persistStackName: "airflow-persist" },
  staging: { region: "us-east-1", persistStackName: "airflow-persist" },
  prod: { region: "us-east-1", persistStackName: "airflow-persist" },
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
  persistStackName: string;
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
  cfn: CloudFormationClient;
  ecs: ECSClient;
  elbv2: ElasticLoadBalancingV2Client;
  rds: RDSClient;
  secrets: SecretsManagerClient;
}

export function makeClients(env: EnvConfig): AwsClients {
  const cfg = { region: env.region };
  return {
    cfn: new CloudFormationClient(cfg),
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

// ── stack outputs ─────────────────────────────────────────────────────────────
// CONFIRM these output keys against the deployed stacks.
export const OUTPUT_KEYS = {
  persist: {
    dbInstanceId: "DbInstanceIdentifier",
    dbSecretArn: "DbSecretArn",
    listenerArn: "HttpsListenerArn",
  },
  runtime: {
    clusterArn: "ClusterArn",
    taskDefinitionArn: "TaskDefinitionArn",
    privateSubnets: "PrivateSubnets", // comma-separated subnet ids
    securityGroups: "ServiceSecurityGroups", // comma-separated sg ids
  },
} as const;

// CONFIRM: the container name in the task definition that runs the airflow CLI.
export const AIRFLOW_CONTAINER = "airflow";

async function fetchOutputs(cfn: CloudFormationClient, stackName: string): Promise<Map<string, string>> {
  let stack: Stack | undefined;
  try {
    stack = (await cfn.send(new DescribeStacksCommand({ StackName: stackName }))).Stacks?.[0];
  } catch (e) {
    fail(`Could not describe stack "${stackName}": ${(e as Error).message}`);
  }
  if (!stack) fail(`Stack "${stackName}" not found.`);
  const map = new Map<string, string>();
  for (const o of stack.Outputs ?? []) {
    if (o.OutputKey && o.OutputValue != null) map.set(o.OutputKey, o.OutputValue);
  }
  return map;
}

/** Read one output value from a stack. */
export async function stackOutput(cfn: CloudFormationClient, stackName: string, key: string): Promise<string> {
  const v = (await fetchOutputs(cfn, stackName)).get(key);
  if (v == null) fail(`Stack "${stackName}" is missing output "${key}". Check OUTPUT_KEYS in src/aws.ts.`);
  return v;
}

export interface TaskLaunchConfig {
  stackName: string;
  cluster: string;
  taskDefinition: string;
  subnets: string[];
  securityGroups: string[];
  container: string;
}

/** Resolve everything needed to launch a one-off airflow-CLI task (one describe). */
export async function resolveTaskLaunchConfig(
  cfn: CloudFormationClient,
  stackName: string,
): Promise<TaskLaunchConfig> {
  const map = await fetchOutputs(cfn, stackName);
  const K = OUTPUT_KEYS.runtime;
  const need = (key: string): string => {
    const v = map.get(key);
    if (v == null) fail(`Stack "${stackName}" is missing output "${key}". Check OUTPUT_KEYS in src/aws.ts.`);
    return v;
  };
  const list = (s: string) => s.split(",").map((x) => x.trim()).filter(Boolean);
  return {
    stackName,
    cluster: need(K.clusterArn),
    taskDefinition: need(K.taskDefinitionArn),
    subnets: list(need(K.privateSubnets)),
    securityGroups: list(need(K.securityGroups)),
    container: AIRFLOW_CONTAINER,
  };
}

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
