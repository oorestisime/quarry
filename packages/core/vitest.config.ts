import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    exclude: ["test/integration/**/*.test.ts"],
    coverage: {
      provider: "v8",
      all: true,
      include: ["src/**/*.ts"],
      exclude: ["src/index.ts"],
      reporter: ["text", "html", "json-summary"],
      reportsDirectory: "coverage/unit",
      skipFull: true,
    },
  },
});
