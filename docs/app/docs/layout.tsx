import "./docs.css";
import Link from "next/link";
import { source } from "@/lib/source";
import { DocsLayout } from "fumadocs-ui/layouts/docs";
import { baseOptions } from "@/lib/layout.shared";
import type { ReactNode } from "react";

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <DocsLayout
      tree={source.getPageTree()}
      {...baseOptions()}
      links={[]}
      containerProps={{ className: "quarry-docs" }}
      sidebar={{
        banner: (
          <Link href="/playground" className="docs-playground-link">
            SQL playground <span aria-hidden="true">↗︎</span>
          </Link>
        ),
      }}
    >
      {children}
    </DocsLayout>
  );
}
