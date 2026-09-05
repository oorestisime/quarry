import type { CSSProperties } from "react";
import { codeThemes } from "@/lib/code-theme";
import { createHighlighterCoreSync } from "shiki/core";
import { createJavaScriptRegexEngine } from "shiki/engine/javascript";
import typescript from "shiki/langs/typescript.mjs";
import sql from "shiki/langs/sql.mjs";
import json from "shiki/langs/json.mjs";
import light from "shiki/themes/github-light.mjs";
import dark from "shiki/themes/github-dark.mjs";

const highlighter = createHighlighterCoreSync({
  engine: createJavaScriptRegexEngine(),
  langs: [typescript, sql, json],
  themes: [light, dark],
});

export function HighlightedCode({
  code,
  language,
  className = "p-4 text-sm leading-relaxed",
}: {
  code: string;
  language: "typescript" | "sql" | "json";
  className?: string;
}) {
  const { tokens } = highlighter.codeToTokens(code, {
    lang: language,
    themes: codeThemes,
    defaultColor: false,
  });

  return (
    <pre className={`syntax-highlight overflow-x-auto ${className}`}>
      <code className={`language-${language}`}>
        {tokens.map((line, index) => (
          <span key={index}>
            {index > 0 ? "\n" : null}
            {line.map((token, column) => (
              <span key={column} style={token.htmlStyle as CSSProperties}>
                {token.content}
              </span>
            ))}
          </span>
        ))}
      </code>
    </pre>
  );
}
