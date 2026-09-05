import Link from "next/link";
import type { ReactNode } from "react";

export function DocCards({ children }: { children: ReactNode }) {
  return <div className="docs-card-grid not-prose">{children}</div>;
}

export function DocCard({
  href,
  title,
  eyebrow,
  featured = false,
  children,
}: {
  href: string;
  title: string;
  eyebrow?: string;
  featured?: boolean;
  children: ReactNode;
}) {
  return (
    <Link href={href} className={`docs-card not-prose${featured ? " docs-card-featured" : ""}`}>
      <div className="docs-card-top">
        {eyebrow && <span className="docs-card-eyebrow">{eyebrow}</span>}
        <span aria-hidden="true" className="docs-card-arrow">
          ↗︎
        </span>
      </div>
      <h3>{title}</h3>
      <div className="docs-card-description">{children}</div>
    </Link>
  );
}
