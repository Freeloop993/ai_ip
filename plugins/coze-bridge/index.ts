import crypto from "node:crypto";

function verifySignature(body: string, signature: string | undefined, secret: string | undefined): boolean {
  if (!secret) return true;
  if (!signature) return false;
  const expected = crypto.createHmac("sha256", secret).update(body).digest("hex");
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
}

export default function register(api: any) {
  api.registerHttpRoute({
    method: "POST",
    path: "/api/coze-trigger",
    handler: async (req: any, res: any) => {
      const rawBody = await req.text();
      const body = JSON.parse(rawBody || "{}");

      const secret = process.env.COZE_SIGNING_SECRET;
      const signature = req.headers?.["x-coze-signature"] || req.headers?.["X-Coze-Signature"];
      if (!verifySignature(rawBody, signature, secret)) {
        res.status(401).json({ ok: false, error: "coze signature verification failed" });
        return;
      }

      const event = {
        type: "new_video",
        event_id: body.event_id,
        source: body.source || "coze",
        video_url: body.video_url,
        video_id: body.video_id,
        author: body.author,
        platform: body.platform,
        stats: body.stats || {},
        collected_at: body.collected_at,
      };

      await api.injectSystemEvent("ip-host", JSON.stringify(event));
      res.json({ ok: true });
    },
  });
}
