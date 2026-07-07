import { Command } from "commander";
import {
  DescribeRulesCommand,
  ElasticLoadBalancingV2Client,
  type Rule,
} from "@aws-sdk/client-elastic-load-balancing-v2";
import { ConfigStore, type PersistConfig } from "../config-store.js";
import { Log } from "../log.js";

/** `alb ...` — ELBv2 listener-rule operations (the listener lives on the persist stack). */
export class AlbCommands {
  constructor(
    private readonly config: ConfigStore,
    private readonly elbv2: ElasticLoadBalancingV2Client,
  ) {}

  register(program: Command): void {
    const alb = program.command("alb").description("ALB listener-rule operations (from airflow-persist)");

    alb
      .command("describe-rules")
      .description("List the listener rules on the persist-stack ALB")
      .action(() => this.describeRules());
  }

  private async describeRules(): Promise<void> {
    const persist = await this.config.read<PersistConfig>(ConfigStore.PERSIST_PARAM);
    const { Rules = [] } = await this.elbv2.send(new DescribeRulesCommand({ ListenerArn: persist.httpsListenerArn }));

    const rows = Rules.map((r) => `  ${(r.Priority ?? "default").padStart(7)}  ${this.action(r)}  ${this.host(r)}`);
    Log.result(rows.join("\n") || "  (no rules)");
  }

  private host(r: Rule): string {
    const c = r.Conditions?.find((x) => x.Field === "host-header");
    const values = c?.HostHeaderConfig?.Values ?? c?.Values ?? [];
    return values.length ? `host=${values.join("|")}` : "(default)";
  }

  private action(r: Rule): string {
    return (
      (r.Actions ?? [])
        .map((a) =>
          a.Type === "forward" ? `forward→${a.TargetGroupArn?.match(/targetgroup\/([^/]+)/)?.[1] ?? "?"}` : a.Type,
        )
        .join(",") || "(none)"
    );
  }
}
