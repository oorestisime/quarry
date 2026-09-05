import { useMemo, type CSSProperties } from "react";
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
}: {
  code: string;
  language: "typescript" | "sql" | "json";
}) {
  const { tokens } = useMemo(
    () =>
      highlighter.codeToTokens(code, {
        lang: language,
        themes: { light: "github-light", dark: "github-dark" },
        defaultColor: false,
      }),
    [code, language],
  );

  return (
    <pre className="playground-code overflow-x-auto p-4 text-sm leading-relaxed">
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
