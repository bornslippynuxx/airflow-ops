import { Command, Option } from "commander";
import { setJson, run } from "./cli.js";
import { registerAirflow } from "./commands/airflow.js";
import { registerAlb } from "./commands/alb.js";
import { registerBg } from "./commands/bg.js";

const program = new Command();
program
  .name("airflow-ops")
  .description("Ops CLI for the Airflow ECS/Fargate + RDS deployment.")
  .version("0.1.0")
  .addOption(new Option("-e, --env <name>", "target environment").choices(["dev", "staging", "prod"]).default("dev"))
  .option("--stack <name>", "runtime stack name, e.g. airflow-3_2_1")
  .option("--to <semver>", "runtime version shorthand, e.g. 3.2.1")
  .option("--json", "emit machine-readable JSON on stdout", false)
  .option("--dry-run", "show what would happen without mutating", false)
  .option("-y, --yes", "confirm destructive ops (required in CI)", false)
  .hook("preAction", (cmd) => setJson(!!cmd.opts().json));

registerAirflow(program);
registerAlb(program);
registerBg(program);

run(() => program.parseAsync(process.argv));
