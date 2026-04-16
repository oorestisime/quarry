import { defineConfig } from "tsdown";

export default defineConfig({
  clean: true,
  dts: true,
  entry: ["src/index.ts"],
  format: ["esm"],
  outDir: "dist",
  platform: "node",
  target: "es2022",
  treeshake: true,
  tsconfig: "./tsconfig.json",
});
