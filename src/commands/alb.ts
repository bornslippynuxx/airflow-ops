import { Command } from "commander";
import { DescribeRulesCommand, type Rule } from "@aws-sdk/client-elastic-load-balancing-v2";
import { session, persistParam, pickStr, FIELDS } from "../aws.js";
import { out } from "../cli.js";

/** `airflow-ops alb ...` — ELBv2 listener-rule operations (listener lives on persist). */
export function registerAlb(program: Command): void {
  const alb = program.command("alb").description("ALB listener-rule operations (from airflow-persist)");

  alb
    .command("describe-rules")
    .description("List the listener rules on the persist-stack ALB (read-only)")
    .action(async function (this: Command) {
      const { aws } = session(this);
      const persist = await persistParam(aws.ssm);
      const listenerArn = pickStr(persist, FIELDS.persist.listenerArn, "persist SSM param");
      const { Rules = [] } = await aws.elbv2.send(new DescribeRulesCommand({ ListenerArn: listenerArn }));

      out(Rules.map(summarizeRule), (rules) =>
        rules
          .map((r) => `  ${String(r.priority).padStart(7)}  ${r.action}  ${r.conditions || "(default)"}`)
          .join("\n") || "  (no rules)",
      );
    });
}

function summarizeRule(r: Rule) {
  const conditions = (r.Conditions ?? [])
    .map((c) =>
      c.Field === "host-header"
        ? `host=${(c.HostHeaderConfig?.Values ?? c.Values ?? []).join("|")}`
        : `${c.Field}=${(c.Values ?? []).join("|")}`,
    )
    .join(", ");
  const action = (r.Actions ?? [])
    .map((a) => (a.Type === "forward" ? `forward→${a.TargetGroupArn?.match(/targetgroup\/([^/]+)/)?.[1] ?? "?"}` : a.Type))
    .join(",");
  return { priority: r.Priority ?? "default", conditions, action: action || "(none)" };
}
