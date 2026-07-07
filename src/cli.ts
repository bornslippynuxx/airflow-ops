// CLI plumbing: the destructive guard + error → exit-code handling.
// Runs only in GitLab CI (non-interactive): no color, no prompt, no JSON mode.
// Command results go to stdout via console.log; status lines go to stderr.

/** Status/diagnostics → stderr (keeps stdout clean for results). */
export const say = (msg: string): void => void process.stderr.write(msg + "\n");

/** An expected failure with a clean message (no stack trace). */
export class OpsError extends Error {}

export function fail(msg: string): never {
  throw new OpsError(msg);
}

/** Destructive ops need explicit --yes — the whole CI authorization. */
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
