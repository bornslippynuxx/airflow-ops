import { Command } from "commander";
import {
  DescribeBlueGreenDeploymentsCommand,
  DescribeDBInstancesCommand,
  RDSClient,
  type BlueGreenDeployment,
} from "@aws-sdk/client-rds";
import { GetSecretValueCommand, SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { ConfigStore, type PersistConfig } from "../config-store.js";
import { Log } from "../log.js";

interface DbCreds {
  username?: string;
  password?: string;
}

/** `bg ...` — RDS Blue/Green Deployment operations on the persist metadata DB. */
export class BgCommands {
  constructor(
    private readonly config: ConfigStore,
    private readonly rds: RDSClient,
    private readonly secrets: SecretsManagerClient,
  ) {}

  register(program: Command): void {
    const bg = program.command("bg").description("RDS Blue/Green Deployment operations (metadata DB)");

    bg.command("describe")
      .description("Describe the RDS blue/green deployment(s) for the persist DB (read-only)")
      .action(() => this.describe());

    const conn = bg
      .command("green-db-conn")
      .description("Print the green (target) DB connection string (read-only)")
      .option("--no-password", "omit the password from the printed connection string");
    conn.action(() => this.greenDbConn(conn));
  }

  private async describe(): Promise<void> {
    const persist = await this.config.read<PersistConfig>(ConfigStore.PERSIST_PARAM);
    const deployments = await this.findDeployments(persist.dbInstanceIdentifier);
    const rows = deployments.map(
      (d) => `  ${d.BlueGreenDeploymentName}  status=${d.Status}  ${d.Source} → ${d.Target}`,
    );
    Log.result(rows.join("\n") || "  (no active blue/green deployment)");
  }

  private async greenDbConn(cmd: Command): Promise<void> {
    const withPassword = (cmd.opts() as { password: boolean }).password;

    const persist = await this.config.read<PersistConfig>(ConfigStore.PERSIST_PARAM);
    const dep = (await this.findDeployments(persist.dbInstanceIdentifier))[0];
    if (!dep?.Target) throw new Error("No active RDS blue/green deployment (or no green target yet).");

    const green = (await this.rds.send(new DescribeDBInstancesCommand({ DBInstanceIdentifier: dep.Target })))
      .DBInstances?.[0];
    const endpoint = green?.Endpoint?.Address;
    if (!endpoint) throw new Error("Green DB endpoint not available yet (still provisioning?).");

    const creds = await this.dbCreds(persist.dbSecretArn);
    const pw = withPassword ? (creds.password ?? "") : "***";
    const conn = `postgresql://${creds.username ?? "postgres"}:${pw}@${endpoint}:${green?.Endpoint?.Port ?? 5432}/${green?.DBName ?? "airflow"}`;

    if (!withPassword) Log.status("! password omitted (--no-password); for display only");
    Log.result(conn);
  }

  /** Blue/green deployments whose source is the persist DB instance. */
  private async findDeployments(dbInstanceId: string): Promise<BlueGreenDeployment[]> {
    const all = (await this.rds.send(new DescribeBlueGreenDeploymentsCommand({}))).BlueGreenDeployments ?? [];
    const mine = all.filter((d) => (d.Source ?? "").includes(dbInstanceId));
    return mine.length ? mine : all;
  }

  private async dbCreds(secretArn: string): Promise<DbCreds> {
    const res = await this.secrets.send(new GetSecretValueCommand({ SecretId: secretArn }));
    if (!res.SecretString) return {};
    try {
      return JSON.parse(res.SecretString) as DbCreds;
    } catch {
      return {};
    }
  }
}
