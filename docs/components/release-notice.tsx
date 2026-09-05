import Link from "next/link";

export function ReleaseNotice() {
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
