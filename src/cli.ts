import { Command } from "commander";
import { AwsClients } from "./aws-clients.js";
import { ConfigStore } from "./config-store.js";
import { EcsTaskRunner } from "./ecs-task-runner.js";
import { AirflowCommands } from "./commands/airflow-commands.js";
import { AlbCommands } from "./commands/alb-commands.js";
import { BgCommands } from "./commands/bg-commands.js";
import { Log } from "./log.js";

/**
 * The application. Builds the command-line program, wires the services into the
 * command groups (constructor injection), and runs it.
 */
export class Cli {
  private readonly program = new Command();

  constructor() {
    const aws = new AwsClients();
    const config = new ConfigStore(aws.ssm);
    const runner = new EcsTaskRunner(aws.ecs);

    this.program
      .name("airflow-ops")
      .description("Ops CLI for the Airflow ECS/Fargate + RDS deployment.")
      .version("0.1.0")
      .option("--stack <name>", "runtime stack name, e.g. airflow-3_2_1")
      .option("-y, --yes", "confirm destructive ops (required in CI)", false);

    new AirflowCommands(config, runner).register(this.program);
    new AlbCommands(config, aws.elbv2).register(this.program);
    new BgCommands(config, aws.rds, aws.secrets).register(this.program);
  }

  async run(argv: string[]): Promise<void> {
    try {
      await this.program.parseAsync(argv);
    } catch (e) {
      Log.status(`✗ ${e instanceof Error ? e.message : String(e)}`);
      process.exitCode = 1;
    }
  }
}
