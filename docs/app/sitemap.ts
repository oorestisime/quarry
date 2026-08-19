import { source } from "@/lib/source";
import type { MetadataRoute } from "next";

const siteUrl = "https://ch-quarry.vercel.app";

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    { url: siteUrl },
    ...source.getPages().map((page) => ({
      url: `${siteUrl}${page.url}`,
    })),
  ];
}
