import { ReleaseNotice } from "@/components/release-notice";
import type { Metadata } from "next";
import { Playground } from "./playground";

export const metadata: Metadata = {
  title: "ClickHouse query playground",
  description:
    "Explore Quarry queries, generated SQL, and bound parameters without connecting a database.",
};

export default function PlaygroundPage() {
  return (
    <>
      <div className="mx-auto w-full max-w-6xl px-6 pt-6">
        <ReleaseNotice />
      </div>
      <Playground />
    </>
  );
}
