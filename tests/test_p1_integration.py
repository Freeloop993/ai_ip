import json
import os
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from mvp_pipeline.config import Settings
from mvp_pipeline.service import PipelineService


class _PublishHandler(BaseHTTPRequestHandler):
    response = {"publish_url": "https://publish.example/video/1", "external_id": "ext-1"}

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        _ = self.rfile.read(length)
        payload = json.dumps(self.response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class P1IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _settings(self, **kwargs):
        base = Settings(
            db_path=os.path.join(self.tmpdir.name, "mvp.db"),
            collab_provider="none",
            retry_base_delay_seconds=0,
            retry_max_attempts=2,
            stuck_timeout_minutes=0,
        )
        data = base.__dict__.copy()
        data.update(kwargs)
        return Settings(**data)

    def _ingest_payload(self, suffix: str):
        return {
            "event_id": f"evt-{suffix}",
            "source": "coze",
            "video_url": f"https://example.com/video/{suffix}",
            "video_id": suffix,
            "author": "author",
            "platform": "douyin",
            "stats": {},
            "collected_at": "2026-03-28T09:00:00+08:00",
        }

    def _to_pending_review(self, service: PipelineService, suffix: str) -> int:
        ingest = service.ingest_coze(self._ingest_payload(suffix))
        content_id = ingest["content_id"]
        service.update_analysis(
            {
                "content_id": content_id,
                "run_id": f"analysis-{suffix}",
                "result": {
                    "schema_version": "1.0",
                    "topic": "topic",
                    "hook": "hook",
                    "structure": "structure",
                    "hashtags": ["a"],
                    "fit_score": 8,
                    "fit_reason": "good",
                    "replicate": True,
                },
            }
        )
        service.update_production(
            {
                "content_id": content_id,
                "run_id": f"producer-{suffix}",
                "result": {
                    "schema_version": "1.0",
                    "provider": "kling",
                    "task_id": "task1",
                    "status": "completed",
                    "video_url": "https://example.com/v.mp4",
                    "script": "script",
                    "tts_text": "tts",
                    "error": None,
                },
            }
        )
        return content_id

    def test_event_dedupe(self):
        service = PipelineService(self._settings())
        p = self._ingest_payload("dedupe")
        first = service.ingest_coze(p)
        second = service.ingest_coze(p)
        third = service.ingest_coze({**p, "event_id": "evt-dedupe-2"})

        self.assertTrue(first["ok"])
        self.assertFalse(first["dedup"])
        self.assertTrue(second["dedup"])
        self.assertEqual(second["reason"], "event_id")
        self.assertTrue(third["dedup"])
        self.assertEqual(third["reason"], "platform_video_id")

    def test_sessions_spawn_chain(self):
        settings = self._settings(openclaw_enabled=True, openclaw_base_url="http://openclaw.local")
        service = PipelineService(settings)

        spawned = []

        def fake_inject(agent_id, event):
            return {"ok": True, "agent_id": agent_id, "event": event}

        def fake_spawn(parent_agent_id, agent_id, task, run_timeout_seconds):
            spawned.append(agent_id)
            return {"ok": True, "runId": f"run-{agent_id}"}

        service.openclaw.inject_system_event = fake_inject
        service.openclaw.sessions_spawn = fake_spawn

        ingest = service.ingest_coze(self._ingest_payload("spawn"))
        self.assertTrue(ingest["ok"])
        self.assertIn("content-analyst", spawned)

        content_id = ingest["content_id"]
        analysis = service.update_analysis(
            {
                "content_id": content_id,
                "run_id": "analysis-spawn",
                "result": {
                    "schema_version": "1.0",
                    "topic": "topic",
                    "hook": "hook",
                    "structure": "structure",
                    "hashtags": ["a"],
                    "fit_score": 9,
                    "fit_reason": "good",
                    "replicate": True,
                },
            }
        )
        self.assertTrue(analysis["ok"])
        self.assertIn("producer-agent", spawned)

    def test_validation_error_code(self):
        service = PipelineService(self._settings())
        ingest = service.ingest_coze(self._ingest_payload("invalid-analysis"))
        out = service.update_analysis(
            {
                "content_id": ingest["content_id"],
                "run_id": "r1",
                "result": {"topic": "missing schema"},
            }
        )
        self.assertFalse(out["ok"])
        self.assertIn(out["error_code"], {"MISSING_REQUIRED_FIELD", "INVALID_PAYLOAD"})

    def test_publish_failure_enqueues_retry(self):
        service = PipelineService(self._settings())
        content_id = self._to_pending_review(service, "publish-fail")
        out = service.review(
            {
                "content_id": content_id,
                "run_id": "review-1",
                "decision": "approved",
                "review_source": "feishu",
                "platform": "douyin",
            }
        )
        self.assertFalse(out["ok"])
        retry = service.db.list_due_retry_jobs(limit=10)
        self.assertTrue(any(j["job_type"] == "publish_dispatch" for j in retry))

    def test_publish_success_real_webhook(self):
        server = HTTPServer(("127.0.0.1", 0), _PublishHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}/publish"
            service = PipelineService(self._settings(publish_webhook_url=url))
            content_id = self._to_pending_review(service, "publish-ok")

            out = service.review(
                {
                    "content_id": content_id,
                    "run_id": "review-2",
                    "decision": "approved",
                    "review_source": "qq",
                    "platform": "douyin",
                }
            )
            self.assertTrue(out["ok"])
            timeline = service.timeline(content_id)
            self.assertEqual(timeline["content"]["status"], "published")
        finally:
            server.shutdown()
            server.server_close()

    def test_reconcile_enqueues_missing_steps(self):
        service = PipelineService(self._settings())
        content_id = service.ingest_coze(self._ingest_payload("reconcile"))["content_id"]
        # Remove analysis run marker to simulate lost announce/dispatch state.
        with service.db.connect() as conn:
            conn.execute("DELETE FROM task_run WHERE content_id = ?", (content_id,))
            conn.execute("UPDATE content_item SET status = 'collected' WHERE id = ?", (content_id,))
        out = service.reconcile()
        self.assertTrue(out["ok"])
        self.assertTrue(any(x["action"] == "enqueue_spawn_analysis" for x in out["actions"]))

    def test_recover_stuck(self):
        service = PipelineService(self._settings())
        content_id = service.ingest_coze(self._ingest_payload("stuck"))["content_id"]
        with service.db.connect() as conn:
            conn.execute("UPDATE content_item SET status = 'analyzing', updated_at = '2000-01-01T00:00:00+00:00' WHERE id = ?", (content_id,))
        out = service.recover_stuck()
        self.assertTrue(out["ok"])
        self.assertTrue(any(x["action"] == "recover_analysis" for x in out["actions"]))

    def test_retry_dead_letter(self):
        service = PipelineService(self._settings())
        service.db.enqueue_retry_job(
            job_type="unsupported_job",
            dedupe_key="bad:1",
            payload={"x": 1},
            max_attempts=2,
            delay_seconds=0,
        )
        first = service.process_retry_jobs({"limit": 10})
        self.assertTrue(first["ok"])
        second = service.process_retry_jobs({"limit": 10})
        dead = service.db.list_dead_retry_jobs()
        self.assertTrue(len(dead) >= 1)

    def test_dashboard_summary_and_agents(self):
        service = PipelineService(self._settings())
        self._to_pending_review(service, "dash")
        summary = service.dashboard_summary()
        agents = service.dashboard_agents()
        self.assertTrue(summary["ok"])
        self.assertTrue(agents["ok"])
        self.assertGreaterEqual(summary["summary"]["total_content"], 1)
        self.assertEqual(len(agents["items"]), 4)

    def test_ip_config_read_write(self):
        service = PipelineService(self._settings())
        service.ip_config_path = os.path.join(self.tmpdir.name, "ip-config.json")
        service.ip_config_example_path = os.path.join(self.tmpdir.name, "ip-config.example.json")
        with open(service.ip_config_example_path, "w", encoding="utf-8") as f:
            json.dump({"ip": {"name": "demo"}}, f, ensure_ascii=False)

        loaded = service.get_ip_config()
        self.assertTrue(loaded["ok"])
        self.assertEqual(loaded["config"]["ip"]["name"], "demo")

        out = service.save_ip_config({"config": {"ip": {"name": "new-ip"}}})
        self.assertTrue(out["ok"])
        loaded2 = service.get_ip_config()
        self.assertEqual(loaded2["config"]["ip"]["name"], "new-ip")

    def test_ip_config_reject_plaintext_video_api_key(self):
        service = PipelineService(self._settings())
        service.ip_config_path = os.path.join(self.tmpdir.name, "ip-config.json")
        out = service.save_ip_config(
            {
                "config": {
                    "ip": {"name": "new-ip"},
                    "videoApi": {"provider": "kling", "apiKey": "should-not-store"},
                }
            }
        )
        self.assertFalse(out["ok"])
        self.assertEqual(out["error_code"], "INVALID_PAYLOAD")

    def test_ip_config_reject_plaintext_image_api_key(self):
        service = PipelineService(self._settings())
        service.ip_config_path = os.path.join(self.tmpdir.name, "ip-config.json")
        out = service.save_ip_config(
            {
                "config": {
                    "ip": {"name": "new-ip"},
                    "imageApi": {
                        "provider": "keling",
                        "baseUrl": "https://img.example",
                        "apiKey": "should-not-store",
                    },
                }
            }
        )
        self.assertFalse(out["ok"])
        self.assertEqual(out["error_code"], "INVALID_PAYLOAD")

    def test_runtime_config_read_write(self):
        service = PipelineService(self._settings())
        service.runtime_config_path = os.path.join(self.tmpdir.name, "runtime-config.json")

        loaded = service.get_runtime_config()
        self.assertTrue(loaded["ok"])
        self.assertEqual(loaded["config"]["publish_schedule"]["timezone"], "Asia/Shanghai")

        saved = service.save_runtime_config(
            {
                "config": {
                    "publish_schedule": {
                        "enabled": True,
                        "timezone": "Asia/Shanghai",
                        "daily_limit": 2,
                        "slots": ["09:30", "20:00"],
                    },
                    "secret_refs": {
                        "video_api_key_env": "KLING_API_KEY",
                        "publish_webhook_token_env": "PUBLISH_WEBHOOK_TOKEN",
                        "feishu_app_secret_env": "FEISHU_APP_SECRET",
                    },
                }
            }
        )
        self.assertTrue(saved["ok"])

        loaded2 = service.get_runtime_config()
        self.assertTrue(loaded2["config"]["publish_schedule"]["enabled"])
        self.assertEqual(loaded2["config"]["publish_schedule"]["daily_limit"], 2)

    def test_feishu_backflow_updates_manual_fields(self):
        service = PipelineService(
            self._settings(
                collab_provider="feishu",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                feishu_app_token="app-token",
                feishu_table_id="table-id",
            )
        )
        service.feishu.create_record = lambda fields: None
        service.feishu.update_record = lambda record_id, fields: None

        content_id = self._to_pending_review(service, "feishu-backflow")
        service.db.set_feishu_record(content_id, "rec_001")
        service.feishu.list_records = lambda limit=100: [
            {
                "record_id": "rec_001",
                "fields": {
                    "脚本内容": "人工改过的脚本",
                    "配音文本": "人工改过的配音",
                    "人工确认": "❌重做",
                },
            }
        ]

        out = service.sync_feishu_backflow({"limit": 50})
        self.assertTrue(out["ok"])
        self.assertEqual(out["script_updates"], 1)
        self.assertEqual(out["review_updates"], 1)

        timeline = service.timeline(content_id)
        self.assertEqual(timeline["content"]["status"], "pending_rework")
        production = json.loads(timeline["content"]["production_json"] or "{}")
        self.assertEqual(production.get("script"), "人工改过的脚本")
        self.assertEqual(production.get("tts_text"), "人工改过的配音")

    def test_publish_login_unsupported_provider(self):
        service = PipelineService(self._settings())
        out = service.publish_login({"profile": "default"})
        self.assertFalse(out["ok"])
        self.assertEqual(out["error_code"], "INVALID_PAYLOAD")

    def test_publish_login_calls_provider(self):
        service = PipelineService(self._settings())

        class _FakePublisher:
            enabled = True

            def prepare_login(self, payload):
                return {"ok": True, "profile": payload.get("profile", "default")}

        service.publisher = _FakePublisher()
        out = service.publish_login({"profile": "bili-main"})
        self.assertTrue(out["ok"])
        self.assertEqual(out["result"]["profile"], "bili-main")

    def test_publish_account_management_calls_provider(self):
        service = PipelineService(self._settings())

        class _FakePublisher:
            enabled = True

            def list_accounts(self):
                return {"ok": True, "items": [{"profile": "main", "status": "authorized"}]}

            def login_status(self, session_id):
                return {"ok": True, "session_id": session_id, "status": "pending"}

            def remove_account(self, payload):
                return {"ok": True, "profile": payload.get("profile")}

        service.publisher = _FakePublisher()

        listed = service.publish_accounts()
        self.assertTrue(listed["ok"])
        self.assertEqual(listed["items"][0]["profile"], "main")

        status = service.publish_login_status("sid-1")
        self.assertTrue(status["ok"])
        self.assertEqual(status["result"]["session_id"], "sid-1")

        removed = service.remove_publish_account({"profile": "main"})
        self.assertTrue(removed["ok"])
        self.assertEqual(removed["result"]["profile"], "main")

    def test_provider_requirements(self):
        service = PipelineService(self._settings())
        out = service.provider_requirements()
        self.assertTrue(out["ok"])
        providers = {x["provider"]: x for x in out["items"]}
        self.assertIn("kling", providers)
        self.assertIn("keling", providers)
        self.assertFalse(providers["keling"]["configured"])

    def test_generate_xhs_draft_requires_image_api(self):
        service = PipelineService(self._settings())
        ingest = service.ingest_coze(self._ingest_payload("xhs-no-api"))
        content_id = ingest["content_id"]
        service.update_analysis(
            {
                "content_id": content_id,
                "run_id": "analysis-xhs-no-api",
                "result": {
                    "schema_version": "1.0",
                    "topic": "topic",
                    "hook": "hook",
                    "structure": "structure",
                    "hashtags": ["科技"],
                    "fit_score": 8,
                    "fit_reason": "good",
                    "replicate": True,
                },
            }
        )
        out = service.generate_xhs_draft({"content_id": content_id})
        self.assertFalse(out["ok"])
        self.assertEqual(out["error_code"], "IMAGE_API_NOT_CONFIGURED")

    def test_generate_xhs_draft_success(self):
        service = PipelineService(self._settings())
        ingest = service.ingest_coze(self._ingest_payload("xhs-ok"))
        content_id = ingest["content_id"]
        service.update_analysis(
            {
                "content_id": content_id,
                "run_id": "analysis-xhs-ok",
                "result": {
                    "schema_version": "1.0",
                    "topic": "AI 热点",
                    "hook": "hook",
                    "structure": "structure",
                    "hashtags": ["科技", "AI"],
                    "fit_score": 8,
                    "fit_reason": "good",
                    "replicate": True,
                },
            }
        )

        service.image_generator.generate_images = lambda **kwargs: {
            "provider": "keling",
            "images": [
                "https://img.example/1.png",
                "https://img.example/2.png",
                "https://img.example/3.png",
            ],
            "raw": {},
        }

        out = service.generate_xhs_draft(
            {
                "content_id": content_id,
                "title": "图文标题",
                "description": "图文内容",
                "image_count": 3,
            }
        )
        self.assertTrue(out["ok"])
        self.assertEqual(out["status"], "pending_review")
        self.assertEqual(len(out["draft"]["image_urls"]), 3)

        timeline = service.timeline(content_id)
        self.assertEqual(timeline["content"]["status"], "pending_review")
        production = json.loads(timeline["content"]["production_json"] or "{}")
        self.assertEqual(production.get("post_type"), "image_text")

    def test_coze_graph_parameter_proxy(self):
        service = PipelineService(self._settings())

        class _FakeCoze:
            enabled = True

            def graph_parameter(self):
                return {"input_schema": {"type": "object"}, "output_schema": {"type": "object"}}

        service.coze_workflow = _FakeCoze()
        out = service.coze_graph_parameter()
        self.assertTrue(out["ok"])
        self.assertIn("input_schema", out)

    def test_coze_pull_run_dry_run(self):
        service = PipelineService(self._settings())

        class _FakeCoze:
            enabled = True

            def run(self, inputs):
                return {
                    "total_videos": 1,
                    "videos": [
                        {
                            "platform": "douyin",
                            "video_id": "v1",
                            "video_url": "https://example.com/video/v1",
                            "creator_name": "author1",
                            "likes_count": 10,
                            "comments_count": 3,
                        }
                    ],
                }

        service.coze_workflow = _FakeCoze()
        out = service.coze_pull_run({"inputs": {"profile_url": "x"}, "dry_run": True})
        self.assertTrue(out["ok"])
        self.assertEqual(out["event_count"], 1)
        self.assertEqual(service.db.total_content_count(), 0)

    def test_coze_pull_run_ingest(self):
        service = PipelineService(self._settings())

        class _FakeCoze:
            enabled = True

            def run(self, inputs):
                return {
                    "videos": [
                        {
                            "platform": "douyin",
                            "video_id": "v2",
                            "video_url": "https://example.com/video/v2",
                            "creator_name": "author2",
                            "likes_count": 12,
                            "comments_count": 4,
                        }
                    ]
                }

        service.coze_workflow = _FakeCoze()
        out1 = service.coze_pull_run({"inputs": {"profile_url": "x"}})
        self.assertTrue(out1["ok"])
        self.assertEqual(out1["accepted"], 1)
        self.assertEqual(service.db.total_content_count(), 1)

        out2 = service.coze_pull_run({"inputs": {"profile_url": "x"}})
        self.assertTrue(out2["ok"])
        self.assertEqual(out2["deduped"], 1)


if __name__ == "__main__":
    unittest.main()

