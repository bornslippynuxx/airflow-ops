import { DescribeTasksCommand, ECSClient, RunTaskCommand, waitUntilTasksStopped } from "@aws-sdk/client-ecs";
import { Log } from "./log.js";

/** A request to run one Airflow CLI command as a one-off Fargate task. */
export interface TaskRequest {
  cluster: string;
  taskDefinition: string;
  container: string;
  command: string[];
  subnets: string[];
  securityGroups: string[];
  wait: boolean;
}

export interface TaskResult {
  taskArn: string;
  exitCode: number | null;
}

/**
 * Runs a one-off Fargate task with the container command overridden — this is how
 * we run the airflow CLI (there is no shell into a running service). When asked to
 * wait, it waits for the task to stop and throws if the container exits non-zero.
 */
export class EcsTaskRunner {
  constructor(private readonly ecs: ECSClient) {}

  async run(req: TaskRequest): Promise<TaskResult> {
    const started = await this.ecs.send(
      new RunTaskCommand({
        cluster: req.cluster,
        taskDefinition: req.taskDefinition,
        launchType: "FARGATE",
        networkConfiguration: {
          awsvpcConfiguration: {
            subnets: req.subnets,
            securityGroups: req.securityGroups,
            assignPublicIp: "DISABLED",
          },
        },
        overrides: { containerOverrides: [{ name: req.container, command: req.command }] },
      }),
    );

    const taskArn = started.tasks?.[0]?.taskArn;
    if (!taskArn) {
      throw new Error(`RunTask did not start a task: ${started.failures?.[0]?.reason ?? "unknown reason"}`);
    }
    const shortId = taskArn.split("/").pop() ?? taskArn;
    Log.status(`→ started task ${shortId}`);
    if (!req.wait) return { taskArn, exitCode: null };

    await waitUntilTasksStopped({ client: this.ecs, maxWaitTime: 1800 }, { cluster: req.cluster, tasks: [taskArn] });
    const desc = await this.ecs.send(new DescribeTasksCommand({ cluster: req.cluster, tasks: [taskArn] }));
    const container = desc.tasks?.[0]?.containers?.find((c) => c.name === req.container);
    const exitCode = container?.exitCode ?? null;
    if (exitCode !== 0) {
      const reason = container?.reason ?? desc.tasks?.[0]?.stoppedReason ?? "no exit code";
      throw new Error(`task ${shortId} failed (exit ${exitCode ?? "?"}: ${reason})`);
    }
    return { taskArn, exitCode };
  }
}
