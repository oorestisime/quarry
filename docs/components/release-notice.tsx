import Link from "next/link";

export function ReleaseNotice({ compact = false }: { compact?: boolean }) {
  if (compact) {
    return (
      <aside aria-label="Documentation version" className="docs-release-note">
        <span className="docs-release-dot" aria-hidden="true" />
        <span>Development docs. npm: 0.9.1.</span>
        <Link href="/docs/releases">
          Check API availability <span aria-hidden="true">→</span>
        </Link>
      </aside>
    );
  }
  return (
    <aside
      aria-label="Documentation version"
      className="rounded-lg border border-fd-border bg-fd-muted/40 px-4 py-3 text-sm text-fd-muted-foreground"
    >
      <strong className="text-fd-foreground">Development docs.</strong> npm currently publishes
      0.9.1. Some APIs here are unreleased.{" "}
      <Link
        href="/docs/releases"
        className="font-medium text-fd-foreground underline underline-offset-4"
      >
        Choose the matching version
      </Link>
      .
    </aside>
  );
}
