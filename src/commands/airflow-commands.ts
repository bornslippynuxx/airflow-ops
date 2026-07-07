import { Command } from "commander";
import { ConfigStore, type RuntimeConfig } from "../config-store.js";
import { EcsTaskRunner } from "../ecs-task-runner.js";
import { Log } from "../log.js";

interface GlobalOpts {
  stack?: string;
  yes: boolean;
}

/**
 * `airflow ...` — Airflow application operations. Each runs the airflow CLI as a
 * one-off ECS task against a runtime stack (--stack), waits for it, and fails if
 * the CLI exits non-zero.
 */
export class AirflowCommands {
  private static readonly CONTAINER = "airflow"; // CONFIRM: container name in the task def

  constructor(
    private readonly config: ConfigStore,
    private readonly runner: EcsTaskRunner,
  ) {}

  register(program: Command): void {
    const airflow = program
      .command("airflow")
      .description("Airflow application operations (run the airflow CLI as a one-off ECS task)");

    const migrate = airflow
      .command("migrate")
      .description("Run `airflow db migrate` on a runtime stack")
      .option("--no-wait", "start the task but don't wait for it to finish");
    migrate.action(() => this.runTask(migrate, ["db", "migrate"]));

    const clean = airflow
      .command("metadata-clean")
      .description("Run `airflow db clean` to purge old metadata (destructive)")
      .requiredOption("--before <timestamp>", "clean records before this time, e.g. 2026-01-01")
      .option("--skip-archive", "drop archived rows instead of keeping archive tables", false)
      .option("--no-wait", "start the task but don't wait for it to finish");
    clean.action(() => this.metadataClean(clean));

    const pool = airflow
      .command("create-task-pool")
      .description("Create/update an Airflow pool (`airflow pools set`)")
      .requiredOption("--name <name>", "pool name")
      .requiredOption("--slots <n>", "pool slots", (v) => parseInt(v, 10))
      .option("--description <text>", "pool description", "")
      .option("--no-wait", "start the task but don't wait for it to finish");
    pool.action(() => {
      const o = pool.opts() as { name: string; slots: number; description: string };
      return this.runTask(pool, ["pools", "set", o.name, String(o.slots), o.description]);
    });

    const user = airflow
      .command("create-api-user")
      .description("Create an API service user (`airflow users create`)")
      .requiredOption("--username <name>", "username")
      .option("--role <role>", "role", "Admin")
      .option("--email <email>", "email address")
      .option("--no-wait", "start the task but don't wait for it to finish");
    user.action(() => {
      const o = user.opts() as { username: string; role: string; email?: string };
      // TODO: confirm the exact `airflow users create` flags for your Airflow 3 auth
      // manager; prefer sourcing the password from Secrets Manager over a flag.
      return this.runTask(user, [
        "users", "create",
        "--username", o.username,
        "--role", o.role,
        "--email", o.email ?? `${o.username}@example.com`,
        "--firstname", o.username,
        "--lastname", "service",
        "--use-random-password",
      ]);
    });
  }

  private metadataClean(cmd: Command): Promise<void> {
    const globals = cmd.optsWithGlobals() as GlobalOpts;
    const o = cmd.opts() as { before: string; skipArchive: boolean };
    if (!globals.yes) {
      throw new Error(`Refusing destructive op without --yes: purge Airflow metadata before ${o.before}`);
    }
    const args = ["db", "clean", "--clean-before-timestamp", o.before, "--yes"];
    if (o.skipArchive) args.push("--skip-archive");
    return this.runTask(cmd, args);
  }

  /** Resolve the runtime config and run `airflow <args>` as a one-off task. */
  private async runTask(cmd: Command, args: string[]): Promise<void> {
    const globals = cmd.optsWithGlobals() as GlobalOpts;
    const stack = this.requireStack(globals);
    const wait = (cmd.opts() as { wait: boolean }).wait;

    const rc = await this.config.read<RuntimeConfig>(ConfigStore.runtimeParam(stack));
    const pretty = `airflow ${args.join(" ")}`;
    Log.status(`→ ${pretty}  (${stack})`);

    const result = await this.runner.run({
      cluster: rc.clusterArn,
      taskDefinition: rc.taskDefinitionArn,
      container: AirflowCommands.CONTAINER,
      command: ["airflow", ...args],
      subnets: rc.privateSubnets,
      securityGroups: rc.serviceSecurityGroups,
      wait,
    });

    Log.status(
      wait
        ? `✓ ${pretty} completed (exit ${result.exitCode})`
        : `✓ ${pretty} started (${result.taskArn.split("/").pop()})`,
    );
  }

  private requireStack(globals: GlobalOpts): string {
    if (!globals.stack) throw new Error("This command needs a runtime stack: pass --stack airflow-3_2_1.");
    return globals.stack;
  }
}
