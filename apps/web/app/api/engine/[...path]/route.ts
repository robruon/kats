import { NextResponse, type NextRequest } from "next/server";

const ENGINE = process.env.ENGINE_URL ?? "http://localhost:8765";

export const dynamic = "force-dynamic";

async function proxy(req: NextRequest, path: string[]) {
  const url = `${ENGINE}/${path.join("/")}`;
  const init: RequestInit = {
    method: req.method,
    headers: { "Content-Type": "application/json" },
  };
  if (req.method !== "GET" && req.method !== "HEAD") {
    init.body = await req.text();
  }
  try {
    const res = await fetch(url, init);
    const body = await res.text();
    return new NextResponse(body, {
      status: res.status,
      headers: { "Content-Type": "application/json" },
    });
  } catch {
    return NextResponse.json({ error: "engine unreachable" }, { status: 503 });
  }
}

export async function GET(req: NextRequest, { params }: { params: { path: string[] } }) {
  return proxy(req, params.path);
}

export async function POST(req: NextRequest, { params }: { params: { path: string[] } }) {
  return proxy(req, params.path);
}
