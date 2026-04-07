import { createMDX } from "fumadocs-mdx/next";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  // Twoslash bundles the TypeScript compiler at runtime; keep it external.
  serverExternalPackages: ["typescript", "twoslash"],
  // Pin Turbopack to this directory so Next.js does not try to use the parent
  // library's lockfile as the workspace root.
  turbopack: {
    root: __dirname,
  },
};

const withMDX = createMDX();

export default withMDX(config);
