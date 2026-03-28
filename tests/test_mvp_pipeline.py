import os
import tempfile
import unittest

from mvp_pipeline.config import Settings
from mvp_pipeline.service import PipelineService


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(self.tmpdir.name, "mvp.db")
        settings = Settings(
            db_path=db_path,
            host="127.0.0.1",
            port=8787,
            fit_threshold=7,
            ingest_provider="coze",
            collab_provider="none",
            video_provider="kling",
            feishu_app_id="",
            feishu_app_secret="",
            feishu_app_token="",
            feishu_table_id="",
            callback_verify_mode="permissive",
            callback_shared_secret="",
            slack_signing_secret="",
            feishu_callback_token="",
            feishu_encrypt_key="",
            wecom_callback_token="",
            qq_callback_token="",
            discord_callback_token="",
        )
        self.service = PipelineService(settings)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_dedupe_ingest(self):
        payload = {
            "event_id": "evt-abc123",
            "source": "coze",
            "video_url": "https://example.com/video/abc123",
            "video_id": "abc123",
            "author": "author",
            "platform": "douyin",
            "stats": {"plays": 100},
            "collected_at": "2026-03-28T09:00:00+08:00",
        }
        first = self.service.ingest_coze(payload)
        second = self.service.ingest_coze(payload)
        self.assertTrue(first["ok"])
        self.assertFalse(first["dedup"])
        self.assertTrue(second["dedup"])

    def test_full_flow_to_pending_review(self):
        ingest = self.service.ingest_coze(
            {
                "event_id": "evt-def456",
                "source": "coze",
                "video_url": "https://example.com/video/def456",
                "video_id": "def456",
                "author": "author",
                "platform": "douyin",
                "stats": {},
                "collected_at": "2026-03-28T09:00:00+08:00",
            }
        )
        content_id = ingest["content_id"]
        analysis = self.service.update_analysis(
            {
                "content_id": content_id,
                "run_id": "run-analysis",
                "result": {
                    "schema_version": "1.0",
                    "topic": "x",
                    "hook": "y",
                    "structure": "z",
                    "hashtags": ["a"],
                    "fit_score": 8,
                    "fit_reason": "good",
                    "replicate": True,
                },
            }
        )
        self.assertTrue(analysis["replicate"])

        production = self.service.update_production(
            {
                "content_id": content_id,
                "run_id": "run-producer",
                "result": {
                    "schema_version": "1.0",
                    "provider": "kling",
                    "task_id": "task1",
                    "status": "completed",
                    "video_url": "https://example.com/v.mp4",
                    "script": "s",
                    "tts_text": "t",
                    "error": None,
                },
            }
        )
        self.assertTrue(production["ok"])
        timeline = self.service.timeline(content_id)
        self.assertEqual(timeline["content"]["status"], "pending_review")

    def test_review_rework(self):
        ingest = self.service.ingest_coze(
            {
                "event_id": "evt-ghi789",
                "source": "coze",
                "video_url": "https://example.com/video/ghi789",
                "video_id": "ghi789",
                "author": "author",
                "platform": "douyin",
                "stats": {},
                "collected_at": "2026-03-28T09:00:00+08:00",
            }
        )
        content_id = ingest["content_id"]
        self.service.update_analysis(
            {
                "content_id": content_id,
                "run_id": "a",
                "result": {
                    "schema_version": "1.0",
                    "topic": "x",
                    "hook": "y",
                    "structure": "z",
                    "hashtags": [],
                    "fit_score": 9,
                    "fit_reason": "good",
                    "replicate": True,
                },
            }
        )
        self.service.update_production(
            {
                "content_id": content_id,
                "run_id": "b",
                "result": {
                    "schema_version": "1.0",
                    "provider": "kling",
                    "task_id": "task1",
                    "status": "completed",
                    "video_url": "https://example.com/v.mp4",
                    "script": "s",
                    "tts_text": "t",
                    "error": None,
                },
            }
        )
        review = self.service.review(
            {
                "content_id": content_id,
                "run_id": "c",
                "decision": "rework",
                "platform": "douyin",
                "review_source": "slack",
                "feedback": "tone mismatch",
            }
        )
        self.assertTrue(review["ok"])
        timeline = self.service.timeline(content_id)
        self.assertEqual(timeline["content"]["status"], "pending_rework")
        self.assertEqual(timeline["timeline"][-1]["source"], "slack")


if __name__ == "__main__":
    unittest.main()
