import { defineConfig, defineDocs } from "fumadocs-mdx/config";
import { rehypeCodeDefaultOptions } from "fumadocs-core/mdx-plugins";
import { transformerTwoslash } from "fumadocs-twoslash";
import {
  createFileSystemGeneratorCache,
  createGenerator,
  remarkAutoTypeTable,
} from "fumadocs-typescript";

import { remarkExamples } from "./lib/remark-examples.mjs";

export const docs = defineDocs({
  dir: "content/docs",
});

const generator = createGenerator({
  cache: createFileSystemGeneratorCache(".next/fumadocs-typescript"),
});

export default defineConfig({
  mdxOptions: {
    remarkPlugins: [remarkExamples, [remarkAutoTypeTable, { generator }]],
    rehypeCodeOptions: {
      themes: {
        light: "github-light",
        dark: "github-dark",
      },
      transformers: [...(rehypeCodeDefaultOptions.transformers ?? []), transformerTwoslash()],
      // Shiki cannot lazy-load languages inside Twoslash popups, so eagerly
      // load the ones we use across the docs.
      langs: ["js", "jsx", "ts", "tsx", "bash", "json", "sql"],
    },
  },
});
