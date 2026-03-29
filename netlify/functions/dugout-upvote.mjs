import { getStore } from "@netlify/blobs";

export default async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("", {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      },
    });
  }

  try {
    const body = await req.json();
    const commentId = String(body.id || "");
    const date = String(body.date || new Date().toISOString().slice(0, 10));

    if (!commentId) {
      return new Response(JSON.stringify({ error: "Comment ID required" }), {
        status: 400,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      });
    }

    const store = getStore({ name: "dugout-comments", consistency: "strong" });

    let comments = [];
    try {
      const existing = await store.get(date, { type: "json" });
      if (existing) comments = existing;
    } catch (e) {}

    const idx = comments.findIndex((c) => c.id === commentId);
    if (idx === -1) {
      return new Response(JSON.stringify({ error: "Comment not found" }), {
        status: 404,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      });
    }

    comments[idx].upvotes = (comments[idx].upvotes || 0) + 1;
    await store.setJSON(date, comments);

    return new Response(JSON.stringify({ success: true, upvotes: comments[idx].upvotes }), {
      status: 200,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: "Failed to upvote" }), {
      status: 500,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  }
};

export const config = {
  path: "/api/dugout/upvote",
  method: ["POST", "OPTIONS"],
};
