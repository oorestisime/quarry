import "@/app/global.css";
import { RootProvider } from "fumadocs-ui/provider/next";
import type { Metadata } from "next";
import type { ReactNode } from "react";

export const metadata: Metadata = {
  metadataBase: new URL("https://ch-quarry.vercel.app"),
  title: {
    default: "Quarry — Type-safe ClickHouse query builder",
    template: "%s | Quarry",
  },
  description:
    "A ClickHouse-native TypeScript query builder with typed results, native query parameters, and explicit runtime semantics.",
  openGraph: {
    type: "website",
    siteName: "Quarry",
    title: "Quarry — Type-safe ClickHouse query builder",
    description:
      "Build type-safe ClickHouse queries without hiding ClickHouse behind an ORM.",
    images: ["/logo-q.png"],
  },
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        <RootProvider>{children}</RootProvider>
      </body>
    </html>
  );
}
