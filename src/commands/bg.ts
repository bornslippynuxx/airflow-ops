import { Command } from "commander";
import {
  DescribeBlueGreenDeploymentsCommand,
  DescribeDBInstancesCommand,
  type BlueGreenDeployment,
  type RDSClient,
} from "@aws-sdk/client-rds";
import { GetSecretValueCommand, type SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import type { CloudFormationClient } from "@aws-sdk/client-cloudformation";
import { session, OUTPUT_KEYS, stackOutput } from "../aws.js";
import { out, say, fail } from "../cli.js";

/** `airflow-ops bg ...` — RDS Blue/Green Deployment operations on the persist DB. */
export function registerBg(program: Command): void {
  const bg = program.command("bg").description("RDS Blue/Green Deployment operations (metadata DB)");

  bg
    .command("describe")
    .description("Describe the RDS blue/green deployment(s) for the persist DB (read-only)")
    .action(async function (this: Command) {
      const { env, aws } = session(this);
      const deployments = await findDeployments(aws.cfn, aws.rds, env.persistStackName);
      out(deployments.map(summarize), (ds) =>
        ds.map((x) => `  ${x.name}  status=${x.status}  ${x.source} → ${x.target}`).join("\n") ||
        "  (no active blue/green deployment)",
      );
    });

  bg
    .command("green-db-conn")
    .description("Print the green (target) DB connection string for the active blue/green deployment (read-only)")
    .option("--no-password", "omit the password from the printed connection string")
    .action(async function (this: Command) {
      const { env, aws } = session(this);
      const withPassword = (this.opts() as { password: boolean }).password;

      const dep = (await findDeployments(aws.cfn, aws.rds, env.persistStackName))[0];
      if (!dep?.Target) fail("No active RDS blue/green deployment (or no green target yet).");

      const green = (await aws.rds.send(new DescribeDBInstancesCommand({ DBInstanceIdentifier: dep.Target })))
        .DBInstances?.[0];
      const endpoint = green?.Endpoint?.Address;
      if (!endpoint) fail("Green DB endpoint not available yet (still provisioning?).");

      const secretArn = await stackOutput(aws.cfn, env.persistStackName, OUTPUT_KEYS.persist.dbSecretArn);
      const creds = await getDbCreds(aws.secrets, secretArn);
      const pw = withPassword ? (creds.password ?? "") : "***";
      const conn = `postgresql://${creds.username ?? "postgres"}:${pw}@${endpoint}:${green?.Endpoint?.Port ?? 5432}/${green?.DBName ?? "airflow"}`;

      if (!withPassword) say("! password omitted (--no-password); for display only");
      out({ endpoint, connectionString: conn }, (d) => d.connectionString);
    });
}

/** Blue/green deployments whose source is the persist DB instance. */
async function findDeployments(
  cfn: CloudFormationClient,
  rds: RDSClient,
  persistStack: string,
): Promise<BlueGreenDeployment[]> {
  const dbInstanceId = await stackOutput(cfn, persistStack, OUTPUT_KEYS.persist.dbInstanceId);
  const all = (await rds.send(new DescribeBlueGreenDeploymentsCommand({}))).BlueGreenDeployments ?? [];
  const mine = all.filter((d) => (d.Source ?? "").includes(dbInstanceId));
  return mine.length ? mine : all;
}

function summarize(d: BlueGreenDeployment) {
  return { name: d.BlueGreenDeploymentName, status: d.Status, source: d.Source, target: d.Target };
}

async function getDbCreds(
  secrets: SecretsManagerClient,
  secretArn: string,
): Promise<{ username?: string; password?: string }> {
  const res = await secrets.send(new GetSecretValueCommand({ SecretId: secretArn }));
  if (!res.SecretString) return {};
  try {
    return JSON.parse(res.SecretString) as { username?: string; password?: string };
  } catch {
    return {};
  }
}
