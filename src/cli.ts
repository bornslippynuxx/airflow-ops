// CLI plumbing: output, the destructive guard, and error → exit-code handling.
// This tool only runs in GitLab pipelines, so there's no color/verbose/prompt.

let jsonMode = false;
export const setJson = (v: boolean): void => void (jsonMode = v);

/** Diagnostics/status → stderr, so `--json` stdout stays clean. */
export const say = (msg: string): void => void process.stderr.write(msg + "\n");

/** A command's primary output → stdout: JSON in `--json` mode, else a human string. */
export function out<T>(data: T, human: (d: T) => string): void {
  process.stdout.write((jsonMode ? JSON.stringify(data, null, 2) : human(data)) + "\n");
}

/** An expected failure with a clean message (no stack trace). */
export class OpsError extends Error {}

export function fail(msg: string): never {
  throw new OpsError(msg);
}

/** Destructive ops need explicit `--yes` — that's the whole CI authorization. */
export function requireYes(yes: boolean, what: string): void {
  if (!yes) fail(`Refusing destructive op without --yes: ${what}`);
}

/** Run the CLI; map any error to a clean stderr message + exit 1. */
export async function run(main: () => Promise<unknown>): Promise<void> {
  try {
    await main();
  } catch (e) {
    say(`✗ ${e instanceof OpsError ? e.message : ((e as Error).stack ?? String(e))}`);
    process.exitCode = 1;
  }
}
