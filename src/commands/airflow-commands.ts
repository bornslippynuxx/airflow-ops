import { Command } from "commander";
import { ConfigStore, type RuntimeConfig } from "../config-store.js";
import { EcsTaskRunner } from "../ecs-task-runner.js";
import { Log } from "../log.js";

/**
 * `airflow ...` — Airflow application operations, each run as a one-off ECS task
 * (there's no shell into a running service). Two commands:
 *   - `exec`           : pass any airflow CLI command straight through.
 *   - `metadata-clean` : `airflow db clean` with our retention policy on top.
 */
export class AirflowCommands {
  private static readonly CONTAINER = "airflow"; // CONFIRM: container name in the task def

  // Tables cleaned in exclude_dag_version mode — everything except dag_version and dag.
  private static readonly EXCLUDE_DAG_VERSION_TABLES =
    "asset_event,celery_taskmeta,celery_tasksetmeta,dag_run,deadline,import_error,job,log,task_instance_history,task_reschedule,trigger,xcom,session,revoked_token";

  private static readonly MIN_RETENTION_DAYS = 30;

  constructor(
    private readonly config: ConfigStore,
    private readonly runner: EcsTaskRunner,
  ) {}

  register(program: Command): void {
    const airflow = program
      .command("airflow")
      .description("Airflow application operations (run the airflow CLI as a one-off ECS task)");

    airflow
      .command("exec")
      .description("Run an airflow CLI command as a one-off ECS task, e.g. exec --stack X -- db migrate")
      .requiredOption("--stack <name>", "runtime stack, e.g. airflow-3_2_1")
      .option("--no-wait", "start the task but don't wait for it to finish")
      .argument("[args...]", "the airflow command (put it after `--`, e.g. -- db migrate)")
      .action((args: string[], opts: { stack: string; wait: boolean }) => this.run(opts.stack, args, opts.wait));

    airflow
      .command("metadata-clean")
      .description("Purge old Airflow metadata (`airflow db clean`) on a runtime stack")
      .requiredOption("--stack <name>", "runtime stack, e.g. airflow-3_2_1")
      .option("--retention-days <n>", "delete metadata older than N days", (v) => parseInt(v, 10), 60)
      .option("--mode <mode>", "clean_all | exclude_dag_version", "clean_all")
      .option("--dry-run", "show what would be deleted without deleting", false)
      .option("--no-wait", "start the task but don't wait for it to finish")
      .action((opts: { stack: string; retentionDays: number; mode: string; dryRun: boolean; wait: boolean }) =>
        this.metadataClean(opts),
      );
  }

  /** `airflow db clean` with our retention policy (ported from db_metadata_clean.sh). */
  private metadataClean(opts: {
    stack: string;
    retentionDays: number;
    mode: string;
    dryRun: boolean;
    wait: boolean;
  }): Promise<void> {
    const { stack, retentionDays, mode, dryRun, wait } = opts;

    if (!Number.isInteger(retentionDays) || retentionDays <= 0) {
      throw new Error(`--retention-days must be a positive integer (got "${retentionDays}").`);
    }
    if (mode !== "clean_all" && mode !== "exclude_dag_version") {
      throw new Error(`--mode must be clean_all or exclude_dag_version (got "${mode}").`);
    }
    if (retentionDays < AirflowCommands.MIN_RETENTION_DAYS) {
      if (!dryRun) {
        throw new Error(
          `--retention-days cannot be less than ${AirflowCommands.MIN_RETENTION_DAYS} unless --dry-run ` +
            "(protects recent metadata from deletion).",
        );
      }
      Log.status(
        `! retention-days ${retentionDays} < ${AirflowCommands.MIN_RETENTION_DAYS} — allowed only because --dry-run is set`,
      );
    }

    const args = ["db", "clean", "--clean-before-timestamp", cleanBeforeDate(retentionDays), "-v"];
    if (mode === "exclude_dag_version") args.push("--tables", AirflowCommands.EXCLUDE_DAG_VERSION_TABLES);
    args.push("--yes");
    if (dryRun) args.push("--dry-run");

    return this.run(stack, args, wait);
  }

  /** Resolve the runtime config and run `airflow <args>` as a one-off task. */
  private async run(stack: string, args: string[], wait: boolean): Promise<void> {
    const pretty = `airflow ${args.join(" ")}`;
    Log.status(`→ ${pretty}  (${stack})`); // logged before the SSM read, so it's visible offline

    const rc = await this.config.read<RuntimeConfig>(ConfigStore.runtimeParam(stack));
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
}

/** today − `days`, at midnight, as `YYYY-MM-DDT00:00:00` (matches `date -d "-N days"`). */
function cleanBeforeDate(days: number): string {
  const d = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T00:00:00`;
}
