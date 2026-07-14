import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  basePath: "/kats",
  async rewrites() {
    return [
      {
        source: "/api/engine/:path*",
        destination: `${process.env.ENGINE_URL ?? "http://localhost:8000"}/:path*`,
      },
    ];
  },
};

export default nextConfig;
