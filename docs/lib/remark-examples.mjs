import { readFileSync } from "node:fs";

const recipeSource = new URL("../../examples/analytics-api/src/recipes.ts", import.meta.url);

// Render the same functions that the analytics example typechecks and executes.
export function remarkExamples() {
  const source = readFileSync(recipeSource, "utf8");
  return function transform(tree) {
    function visit(node) {
      if (node.type === "code" && node.lang === "ts") {
        const region = /\brecipe="(\w+)"/.exec(node.meta ?? "")?.[1];
        if (region) {
          const start = `// #region ${region}\n`;
          const end = `// #endregion ${region}`;
          const startAt = source.indexOf(start);
          const endAt = source.indexOf(end, startAt);
          if (startAt < 0 || endAt < 0) throw new Error(`Missing recipe region: ${region}`);
          node.value = source.slice(startAt + start.length, endAt).trimEnd();
          node.meta = node.meta.replace(/\s*recipe="\w+"/, "");
        }
      }
      if (node.type === "code" && !/\btitle=/.test(node.meta ?? "")) {
        const titles = {
          ts: "TypeScript",
          tsx: "TSX",
          js: "JavaScript",
          bash: "Terminal",
          sh: "Terminal",
          sql: "ClickHouse SQL",
          json: "JSON",
        };
        if (titles[node.lang]) node.meta = `${node.meta ?? ""} title="${titles[node.lang]}"`.trim();
      }
      for (const child of node.children ?? []) visit(child);
    }
    visit(tree);
  };
}
