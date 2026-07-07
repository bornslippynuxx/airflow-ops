import { Command } from "commander";
import { run } from "./cli.js";
import { registerAirflow } from "./commands/airflow.js";
import { registerAlb } from "./commands/alb.js";
import { registerBg } from "./commands/bg.js";

const program = new Command();
program
  .name("airflow-ops")
  .description("Ops CLI for the Airflow ECS/Fargate + RDS deployment.")
  .version("0.1.0")
  .option("--stack <name>", "runtime stack name, e.g. airflow-3_2_1")
  .option("-y, --yes", "confirm destructive ops (required in CI)", false);

registerAirflow(program);
registerAlb(program);
registerBg(program);

run(() => program.parseAsync(process.argv));
