import { Command } from "commander";
import { DescribeRulesCommand, type Rule } from "@aws-sdk/client-elastic-load-balancing-v2";
import { session, persistConfig } from "../aws.js";

/** `airflow-ops alb ...` — ELBv2 listener-rule operations (listener lives on persist). */
export function registerAlb(program: Command): void {
  const alb = program.command("alb").description("ALB listener-rule operations (from airflow-persist)");

  alb
    .command("describe-rules")
    .description("List the listener rules on the persist-stack ALB")
    .action(async function (this: Command) {
      const { aws } = session(this);
      const persist = await persistConfig(aws.ssm);
      const { Rules = [] } = await aws.elbv2.send(new DescribeRulesCommand({ ListenerArn: persist.httpsListenerArn }));

      const rows = Rules.map((r) => `  ${(r.Priority ?? "default").padStart(7)}  ${action(r)}  ${host(r)}`);
      console.log(rows.join("\n") || "  (no rules)");
    });
}

const host = (r: Rule): string => {
  const c = r.Conditions?.find((x) => x.Field === "host-header");
  const v = c?.HostHeaderConfig?.Values ?? c?.Values ?? [];
  return v.length ? `host=${v.join("|")}` : "(default)";
};

const action = (r: Rule): string =>
  (r.Actions ?? [])
    .map((a) => (a.Type === "forward" ? `forward→${a.TargetGroupArn?.match(/targetgroup\/([^/]+)/)?.[1] ?? "?"}` : a.Type))
    .join(",") || "(none)";
