import { source } from "@/lib/source";
import { createFromSource } from "fumadocs-core/search/server";

const search = createFromSource(source, {
  language: "english",
  buildIndex: async (page) => {
    const structuredData = page.data.structuredData;

    return {
      id: page.url,
      title: page.data.title,
      description: page.data.description,
      url: page.url,
      structuredData: {
        headings: structuredData.headings,
        contents: structuredData.contents.filter(
          ({ content }) => !/^(?:<TypeTable\b|typetable\s+id:)/i.test(content.trim()),
        ),
      },
    };
  },
});

export async function GET(request: Request): Promise<Response> {
  const response = await search.GET(request);

  if (!response.ok) {
    return response;
  }

  const results = (await response.json()) as unknown[];
  return Response.json(results.slice(0, 20), {
    headers: response.headers,
    status: response.status,
  });
}
