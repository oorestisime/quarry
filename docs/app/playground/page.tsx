import type { Metadata } from "next";
import { Playground } from "./playground";

export const metadata: Metadata = {
  title: "ClickHouse query playground",
  description:
    "Explore Quarry queries, generated SQL, and bound parameters without connecting a database.",
};

export default function PlaygroundPage() {
  return <Playground />;
}
