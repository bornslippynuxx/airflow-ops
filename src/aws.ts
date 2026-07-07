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

// AWS clients — created once. Region + credentials come from the ambient AWS
// environment (AWS_REGION + the role the GitLab job assumes).
export const ssm = new SSMClient({});
export const ecs = new ECSClient({});
export const elbv2 = new ElasticLoadBalancingV2Client({});
export const rds = new RDSClient({});
export const secrets = new SecretsManagerClient({});

// ── options ───────────────────────────────────────────────────────────────────
export interface GlobalOpts {
  stack?: string; // runtime stack, e.g. airflow-3_2_1
  yes: boolean;
}

/** The runtime stack a command targets. */
export function runtimeStackName(opts: GlobalOpts): string {
  if (!opts.stack) fail("This command needs a runtime stack: pass --stack airflow-3_2_1.");
  return opts.stack;
}

// ── config from SSM ───────────────────────────────────────────────────────────
// Each stack publishes its config as a single JSON SSM parameter. The interfaces
// below ARE the JSON shape (field name = JSON key). CONFIRM against your stacks.
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
export async function readConfig<T>(name: string): Promise<T> {
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
