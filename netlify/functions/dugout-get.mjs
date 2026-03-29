import { getStore } from "@netlify/blobs";

export default async (req) => {
  const url = new URL(req.url);
  const date = url.searchParams.get("date") || new Date().toISOString().slice(0, 10);

  const store = getStore({ name: "dugout-comments", consistency: "strong" });

  try {
    const data = await store.get(date, { type: "json" });
    const comments = data || [];

    return new Response(JSON.stringify({ date, comments, count: comments.length }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=15",
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ date, comments: [], count: 0 }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }
};

export const config = {
  path: "/api/dugout/comments",
  method: ["GET"],
};
