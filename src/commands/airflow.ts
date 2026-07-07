import { Command } from "commander";
import {
  runtimeStackName,
  readConfig,
  SSM_PARAM,
  runManualEcsTask,
  AIRFLOW_CONTAINER,
  type RuntimeConfig,
  type GlobalOpts,
} from "../aws.js";
import { requireYes, say } from "../cli.js";

/**
 * `airflow-ops airflow ...` — Airflow *application* operations. Each runs the
 * airflow CLI as a one-off Fargate task against a runtime stack (--stack), waits
 * for it, and fails if the CLI exits non-zero. No REST API / auth needed.
 */
export function registerAirflow(program: Command): void {
  const airflow = program
    .command("airflow")
    .description("Airflow application operations (run the airflow CLI as a one-off ECS task)");

  airflow
    .command("migrate")
    .description("Run `airflow db migrate` on a runtime stack")
    .option("--no-wait", "start the task but don't wait for it to finish")
    .action(async function (this: Command) {
      const opts = this.optsWithGlobals() as GlobalOpts;
      await runCli(opts, ["db", "migrate"], (this.opts() as { wait: boolean }).wait);
    });

  airflow
    .command("metadata-clean")
    .description("Run `airflow db clean` to purge old metadata (destructive)")
    .requiredOption("--before <timestamp>", "clean records before this time, e.g. 2026-01-01")
    .option("--skip-archive", "drop archived rows instead of keeping archive tables", false)
    .option("--no-wait", "start the task but don't wait for it to finish")
    .action(async function (this: Command) {
      const opts = this.optsWithGlobals() as GlobalOpts;
      const o = this.opts() as { before: string; skipArchive: boolean; wait: boolean };
      requireYes(opts.yes, `Permanently purge Airflow metadata before ${o.before} on ${runtimeStackName(opts)}`);
      const args = ["db", "clean", "--clean-before-timestamp", o.before, "--yes"];
      if (o.skipArchive) args.push("--skip-archive");
      await runCli(opts, args, o.wait);
    });

  airflow
    .command("create-task-pool")
    .description("Create/update an Airflow pool (`airflow pools set`)")
    .requiredOption("--name <name>", "pool name")
    .requiredOption("--slots <n>", "pool slots", (v) => parseInt(v, 10))
    .option("--description <text>", "pool description", "")
    .option("--no-wait", "start the task but don't wait for it to finish")
    .action(async function (this: Command) {
      const opts = this.optsWithGlobals() as GlobalOpts;
      const o = this.opts() as { name: string; slots: number; description: string; wait: boolean };
      await runCli(opts, ["pools", "set", o.name, String(o.slots), o.description], o.wait);
    });

  airflow
    .command("create-api-user")
    .description("Create an API service user (`airflow users create`)")
    .requiredOption("--username <name>", "username")
    .option("--role <role>", "role", "Admin")
    .option("--email <email>", "email address")
    .option("--no-wait", "start the task but don't wait for it to finish")
    .action(async function (this: Command) {
      const opts = this.optsWithGlobals() as GlobalOpts;
      const o = this.opts() as { username: string; role: string; email?: string; wait: boolean };
      // TODO: confirm the exact `airflow users create` flags for your Airflow 3 auth
      // manager; prefer sourcing the password from Secrets Manager over a flag.
      await runCli(
        opts,
        [
          "users", "create",
          "--username", o.username,
          "--role", o.role,
          "--email", o.email ?? `${o.username}@example.com`,
          "--firstname", o.username,
          "--lastname", "service",
          "--use-random-password",
        ],
        o.wait,
      );
    });
}

/** Resolve the runtime config and run `airflow <argv>` as a one-off task. */
async function runCli(opts: GlobalOpts, argv: string[], wait: boolean): Promise<void> {
  const stack = runtimeStackName(opts);
  const rc = await readConfig<RuntimeConfig>(SSM_PARAM.runtime(stack));
  const pretty = `airflow ${argv.join(" ")}`;
  say(`→ ${pretty}  (${stack})`);

  const res = await runManualEcsTask({
    cluster: rc.clusterArn,
    taskDefinition: rc.taskDefinitionArn,
    container: AIRFLOW_CONTAINER,
    command: ["airflow", ...argv],
    subnets: rc.privateSubnets,
    securityGroups: rc.serviceSecurityGroups,
  }, { wait });

  say(wait ? `✓ ${pretty} completed (exit ${res.exitCode})` : `✓ ${pretty} started (${res.taskArn.split("/").pop()})`);
}
