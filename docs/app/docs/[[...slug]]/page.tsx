import Link from "next/link";
import { ReleaseNotice } from "@/components/release-notice";
import { source } from "@/lib/source";
import { DocsBody, DocsDescription, DocsPage, DocsTitle } from "fumadocs-ui/layouts/docs/page";
import { notFound } from "next/navigation";
import { getMDXComponents } from "@/components/mdx";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";

export default async function Page(props: { params: Promise<{ slug?: string[] }> }) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const MDX = page.data.body;
  const section = params.slug?.[0];
  const sections: Record<string, string> = {
    guides: "Guides",
    recipes: "Recipes",
    reference: "API reference",
    concepts: "Concepts",
  };
  const sectionTitle = section ? sections[section] : undefined;

  return (
    <DocsPage
      toc={page.data.toc.filter((item) => item.depth <= 2)}
      full={page.data.full}
      className="docs-article"
      breadcrumb={{ enabled: false }}
      footer={{ className: "docs-pagination" }}
    >
      <header className="docs-article-header">
        <nav aria-label="Breadcrumb" className="docs-breadcrumb">
          <Link href="/docs">Documentation</Link>
          {sectionTitle && (
            <>
              <span aria-hidden="true">/</span>
              <Link href={`/docs/${section}`}>{sectionTitle}</Link>
            </>
          )}
        </nav>
        <DocsTitle className="docs-title">{page.data.title}</DocsTitle>
        <DocsDescription className="docs-description">{page.data.description}</DocsDescription>
        <ReleaseNotice compact />
      </header>
      <DocsBody className="docs-prose">
        <MDX
          components={getMDXComponents({
            a: createRelativeLink(source, page),
          })}
        />
      </DocsBody>
    </DocsPage>
  );
}

export async function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata(props: {
  params: Promise<{ slug?: string[] }>;
}): Promise<Metadata> {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  return {
    title: page.data.title,
    description: page.data.description,
  };
}
