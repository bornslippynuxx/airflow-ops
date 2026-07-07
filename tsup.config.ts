import { defineConfig } from "tsup";

// Single self-contained CJS file so CI runs `node dist/ops.cjs` with no install step.
export default defineConfig({
  entry: { ops: "src/index.ts" },
  format: ["cjs"],
  outExtension: () => ({ js: ".cjs" }),
  target: "node22",
  platform: "node",
  clean: true,
  banner: { js: "#!/usr/bin/env node" },
});
