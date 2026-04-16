import { createMDX } from "fumadocs-mdx/next";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const workspaceRoot = resolve(__dirname, "..");

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  // Twoslash bundles the TypeScript compiler at runtime; keep it external.
  serverExternalPackages: ["typescript", "twoslash"],
  // The docs site reads workspace packages and source files during build.
  outputFileTracingRoot: workspaceRoot,
  turbopack: {
    root: workspaceRoot,
  },
};

const withMDX = createMDX();

export default withMDX(config);
