import { getStore } from "@netlify/blobs";

// Simple rate limiting: max 5 comments per IP per day
const RATE_LIMIT = 10;

function hashIP(ip) {
  // Simple hash for privacy — don't store raw IPs
  let hash = 0;
  const str = ip + "-upperdecky-salt-2026";
  for (let i = 0; i < str.length; i++) {
    const chr = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + chr;
    hash |= 0;
  }
  return "ip_" + Math.abs(hash).toString(36);
}

function sanitize(str) {
  return str
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
    .trim();
}

export default async (req) => {
  // Handle CORS preflight
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
    const name = sanitize(String(body.name || "").slice(0, 50));
    const comment = sanitize(String(body.comment || "").slice(0, 280));
    const date = new Date().toISOString().slice(0, 10);

    if (!name || name.length < 1) {
      return new Response(JSON.stringify({ error: "Name is required" }), {
        status: 400,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      });
    }
    if (!comment || comment.length < 1) {
      return new Response(JSON.stringify({ error: "Comment is required" }), {
        status: 400,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      });
    }

    const store = getStore({ name: "dugout-comments", consistency: "strong" });

    // Get existing comments for today
    let comments = [];
    try {
      const existing = await store.get(date, { type: "json" });
      if (existing) comments = existing;
    } catch (e) {
      // No comments yet today
    }

    // Rate limit check
    const clientIP = req.headers.get("x-forwarded-for") || req.headers.get("x-nf-client-connection-ip") || "unknown";
    const ipHash = hashIP(clientIP);
    const userComments = comments.filter((c) => c.ipHash === ipHash);
    if (userComments.length >= RATE_LIMIT) {
      return new Response(JSON.stringify({ error: "Easy there, slugger. Max 10 takes per day." }), {
        status: 429,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      });
    }

    // Add the new comment
    const newComment = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      name,
      comment,
      upvotes: 0,
      createdAt: new Date().toISOString(),
      ipHash,
    };

    comments.push(newComment);

    // Save back
    await store.setJSON(date, comments);

    // Return the new comment (without ipHash)
    const { ipHash: _, ...safeComment } = newComment;
    return new Response(JSON.stringify({ success: true, comment: safeComment }), {
      status: 201,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: "Something went wrong. Blame the ump." }), {
      status: 500,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  }
};

export const config = {
  path: "/api/dugout/comments",
  method: ["POST", "OPTIONS"],
};
