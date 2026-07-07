import { Command } from "commander";
import {
  DescribeBlueGreenDeploymentsCommand,
  DescribeDBInstancesCommand,
  type BlueGreenDeployment,
} from "@aws-sdk/client-rds";
import { GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { rds, secrets, readConfig, SSM_PARAM, type PersistConfig } from "../aws.js";
import { say, fail } from "../cli.js";

/** `airflow-ops bg ...` — RDS Blue/Green Deployment operations on the persist DB. */
export function registerBg(program: Command): void {
  const bg = program.command("bg").description("RDS Blue/Green Deployment operations (metadata DB)");

  bg
    .command("describe")
    .description("Describe the RDS blue/green deployment(s) for the persist DB (read-only)")
    .action(async () => {
      const persist = await readConfig<PersistConfig>(SSM_PARAM.persist);
      const deployments = await findDeployments(persist.dbInstanceIdentifier);
      const rows = deployments.map((d) => `  ${d.BlueGreenDeploymentName}  status=${d.Status}  ${d.Source} → ${d.Target}`);
      console.log(rows.join("\n") || "  (no active blue/green deployment)");
    });

  bg
    .command("green-db-conn")
    .description("Print the green (target) DB connection string for the active blue/green deployment (read-only)")
    .option("--no-password", "omit the password from the printed connection string")
    .action(async function (this: Command) {
      const withPassword = (this.opts() as { password: boolean }).password;

      const persist = await readConfig<PersistConfig>(SSM_PARAM.persist);
      const dep = (await findDeployments(persist.dbInstanceIdentifier))[0];
      if (!dep?.Target) fail("No active RDS blue/green deployment (or no green target yet).");

      const green = (await rds.send(new DescribeDBInstancesCommand({ DBInstanceIdentifier: dep.Target }))).DBInstances?.[0];
      const endpoint = green?.Endpoint?.Address;
      if (!endpoint) fail("Green DB endpoint not available yet (still provisioning?).");

      const creds = await getDbCreds(persist.dbSecretArn);
      const pw = withPassword ? (creds.password ?? "") : "***";
      const conn = `postgresql://${creds.username ?? "postgres"}:${pw}@${endpoint}:${green?.Endpoint?.Port ?? 5432}/${green?.DBName ?? "airflow"}`;

      if (!withPassword) say("! password omitted (--no-password); for display only");
      console.log(conn);
    });
}

/** Blue/green deployments whose source is the persist DB instance. */
async function findDeployments(dbInstanceId: string): Promise<BlueGreenDeployment[]> {
  const all = (await rds.send(new DescribeBlueGreenDeploymentsCommand({}))).BlueGreenDeployments ?? [];
  const mine = all.filter((d) => (d.Source ?? "").includes(dbInstanceId));
  return mine.length ? mine : all;
}

async function getDbCreds(secretArn: string): Promise<{ username?: string; password?: string }> {
  const res = await secrets.send(new GetSecretValueCommand({ SecretId: secretArn }));
  if (!res.SecretString) return {};
  try {
    return JSON.parse(res.SecretString) as { username?: string; password?: string };
  } catch {
    return {};
  }
}
