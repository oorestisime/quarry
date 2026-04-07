import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";

function Logo() {
  return (
    <div className="flex items-center gap-2">
      <img
        src="/logo-q.png"
        alt=""
        width={24}
        height={24}
        className="shrink-0"
      />
      <span className="font-semibold">Quarry</span>
    </div>
  );
}

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: <Logo />,
    },
    githubUrl: "https://github.com/oorestisime/quarry",
    links: [
      {
        text: "Docs",
        url: "/docs",
        active: "nested-url",
      },
    ],
  };
}
