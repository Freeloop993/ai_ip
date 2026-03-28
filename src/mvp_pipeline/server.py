import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .callback_adapters import normalize_review_callback, verify_callback_request
from .config import load_settings
from .security import verify_coze_signature
from .service import PipelineService


class AppHandler(BaseHTTPRequestHandler):
    settings = load_settings()
    service = PipelineService(settings)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    dashboard_root = os.path.join(project_root, "dashboard")

    def _send(self, status: int, data: dict):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_file(self, status: int, path: str, content_type: str):
        if not os.path.exists(path):
            self._send(404, {"ok": False, "error": "not found"})
            return
        with open(path, "rb") as f:
            data = f.read()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        parsed = json.loads(raw.decode("utf-8"))
        return parsed, raw

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query_params = parse_qs(parsed.query)

        if path in ("/dashboard", "/dashboard/"):
            self._send_file(200, os.path.join(self.dashboard_root, "index.html"), "text/html; charset=utf-8")
            return
        if path == "/dashboard/styles.css":
            self._send_file(200, os.path.join(self.dashboard_root, "styles.css"), "text/css; charset=utf-8")
            return
        if path == "/dashboard/app.js":
            self._send_file(200, os.path.join(self.dashboard_root, "app.js"), "application/javascript; charset=utf-8")
            return

        if path == "/health":
            self._send(200, self.service.health())
            return
        if path == "/api/content":
            self._send(200, self.service.list_content())
            return
        if path == "/api/dashboard/summary":
            self._send(200, self.service.dashboard_summary())
            return
        if path == "/api/dashboard/agents":
            self._send(200, self.service.dashboard_agents())
            return
        if path == "/api/dashboard/metrics":
            self._send(200, self.service.dashboard_metrics())
            return
        if path == "/api/dashboard/errors":
            run_id = (query_params.get("run_id") or [None])[0]
            self._send(200, self.service.dashboard_errors(run_id=run_id))
            return
        if path == "/api/coze/graph-parameter":
            data = self.service.coze_graph_parameter()
            self._send(200 if data.get("ok") else 400, data)
            return
        if path == "/api/providers/requirements":
            self._send(200, self.service.provider_requirements())
            return
        if path == "/api/config/ip":
            data = self.service.get_ip_config()
            self._send(200 if data.get("ok") else 404, data)
            return
        if path == "/api/config/soul":
            data = self.service.get_soul()
            self._send(200 if data.get("ok") else 404, data)
            return
        if path == "/api/config/runtime":
            data = self.service.get_runtime_config()
            self._send(200 if data.get("ok") else 404, data)
            return
        if path == "/api/publish/accounts":
            self._send(200, self.service.publish_accounts())
            return
        if path == "/api/publish/accounts/login/status":
            session_id = (query_params.get("session_id") or [""])[0]
            data = self.service.publish_login_status(session_id)
            self._send(200 if data.get("ok") else 400, data)
            return
        if path == "/api/retry/dead":
            self._send(200, self.service.list_dead_jobs())
            return
        if path.startswith("/api/content/") and path.endswith("/timeline"):
            parts = path.split("/")
            try:
                content_id = int(parts[3])
            except Exception:
                self._send(400, {"ok": False, "error": "invalid content id"})
                return
            data = self.service.timeline(content_id)
            self._send(200 if data.get("ok") else 404, data)
            return

        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query_params = parse_qs(parsed.query)
        payload, raw_body = self._read_json()

        if path == "/api/coze-trigger":
            verified = verify_coze_signature(
                headers=dict(self.headers),
                raw_body=raw_body,
                secret=self.settings.coze_signing_secret,
                mode=self.settings.coze_verify_mode,
            )
            if not verified:
                self._send(401, {"ok": False, "error_code": "COZE_SIGNATURE_FAILED", "error": "coze signature verification failed"})
                return
            data = self.service.ingest_coze(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/coze/pull-run":
            data = self.service.coze_pull_run(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/collect/run":
            data = self.service.collect_run(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/analysis-result":
            data = self.service.update_analysis(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/production-result":
            data = self.service.update_production(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/producer/run":
            data = self.service.run_kling_production(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/xhs/generate-draft":
            data = self.service.generate_xhs_draft(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path in ("/api/publish/login", "/api/publish/accounts/login/start"):
            data = self.service.publish_login(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/publish/accounts/remove":
            data = self.service.remove_publish_account(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/config/ip":
            data = self.service.save_ip_config(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/config/soul":
            data = self.service.save_soul(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/config/runtime":
            data = self.service.save_runtime_config(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/collab/feishu/pull":
            data = self.service.sync_feishu_backflow(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path in ("/api/review", "/api/review-callback"):
            data = self.service.review(payload)
            self._send(200 if data.get("ok") else 400, data)
            return

        if path == "/api/recovery/retry-jobs":
            data = self.service.process_retry_jobs(payload)
            self._send(200, data)
            return

        if path == "/api/recovery/reconcile":
            data = self.service.reconcile()
            self._send(200, data)
            return

        if path == "/api/recovery/recover-stuck":
            data = self.service.recover_stuck()
            self._send(200, data)
            return

        if path.startswith("/api/callback/"):
            platform = path.split("/")[-1].strip().lower()
            verification = verify_callback_request(
                platform=platform,
                payload=payload,
                raw_body=raw_body,
                headers=dict(self.headers),
                query_params=query_params,
                verify_mode=self.settings.callback_verify_mode,
                shared_secret=self.settings.callback_shared_secret,
                platform_tokens={
                    "slack": self.settings.slack_signing_secret,
                    "feishu": self.settings.feishu_callback_token,
                    "wecom": self.settings.wecom_callback_token,
                    "qq": self.settings.qq_callback_token,
                    "discord": self.settings.discord_callback_token,
                },
                feishu_encrypt_key=self.settings.feishu_encrypt_key,
            )
            if not verification.get("ok"):
                self._send(401, {"ok": False, "error_code": "CALLBACK_SIGNATURE_FAILED", "error": "signature verification failed", "reason": verification.get("reason")})
                return

            normalized = normalize_review_callback(platform, payload, dict(self.headers))
            if normalized.get("ok") is False:
                self._send(400, normalized)
                return
            if normalized.get("handshake"):
                self._send(200, normalized["body"])
                return
            data = self.service.review(normalized["payload"])
            self._send(200 if data.get("ok") else 400, data)
            return

        self._send(404, {"ok": False, "error": "not found"})


def run() -> None:
    settings = load_settings()
    server = ThreadingHTTPServer((settings.host, settings.port), AppHandler)
    print(f"mvp pipeline running on http://{settings.host}:{settings.port}")
    server.serve_forever()


if __name__ == "__main__":
    run()





