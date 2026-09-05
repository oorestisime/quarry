import Link from "next/link";
import { docsRelease } from "@/lib/release";

export function ReleaseNotice({ compact = false }: { compact?: boolean }) {
  const status = docsRelease.published ? "Documentation" : "Not yet published";
  if (compact) {
    return (
      <aside aria-label="Documentation version" className="docs-release-note">
        <span className="docs-release-dot" aria-hidden="true" />
        <span>
          Quarry {docsRelease.version} · {status}
        </span>
        <Link href="/docs/releases">
          Release & upgrade guide <span aria-hidden="true">→</span>
        </Link>
      </aside>
    );
  }
  return (
    <aside
      aria-label="Documentation version"
      className="rounded-lg border border-fd-border bg-fd-muted/40 px-4 py-3 text-sm text-fd-muted-foreground"
    >
      <strong className="text-fd-foreground">Quarry {docsRelease.version} docs.</strong>{" "}
      {!docsRelease.published &&
        "Release preview: the npm packages and release tag are not published yet. "}
      <Link
        href="/docs/releases"
        className="font-medium text-fd-foreground underline underline-offset-4"
      >
        Release & upgrade guide
      </Link>
      .
    </aside>
  );
}

export function ReleaseStatus() {
  if (docsRelease.published) return null;
  return (
    <p>
      <strong>Release preview.</strong> Quarry {docsRelease.version}, its matching CLI, and the v
      {docsRelease.version} Git tag are not published yet. The versioned install and checkout
      commands in these docs become available at publication. For an existing npm installation,
      consult the{" "}
      <a href="https://github.com/oorestisime/quarry/tree/v0.9.1/docs/content/docs">
        0.9.1 documentation
      </a>
      .
    </p>
  );
}
