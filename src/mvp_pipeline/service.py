import json
import hashlib
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .coze_client import CozeWorkflowClient
from .config import Settings
from .db import Database
from .errors import PipelineError
from .feishu import (
    FeishuBitableClient,
    FeishuConfig,
    build_analysis_fields,
    build_collected_fields,
    build_production_fields,
    build_review_fields,
)
from .kling_client import KlingClient
from .image_client import ImageGeneratorClient
from .native_collector import CollectedVideo, NativeCollector
from .openclaw_client import OpenClawClient
from .publish_adapter import create_publisher
from .schemas import (
    ANALYSIS_SCHEMA_VERSION,
    PRODUCTION_SCHEMA_VERSION,
    validate_analysis_result,
    validate_coze_event,
    validate_production_result,
)
from .state_machine import validate_transition
from .tts_mock import build_tts_mock


class PipelineService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.db_path)
        self.db.init()
        self.feishu = FeishuBitableClient(
            FeishuConfig(
                app_id=settings.feishu_app_id,
                app_secret=settings.feishu_app_secret,
                app_token=settings.feishu_app_token,
                table_id=settings.feishu_table_id,
            )
        )
        self.openclaw = OpenClawClient(
            enabled=settings.openclaw_enabled,
            base_url=settings.openclaw_base_url,
            api_key=settings.openclaw_api_key,
            inject_path=settings.openclaw_inject_path,
            spawn_path=settings.openclaw_spawn_path,
        )
        self.coze_workflow = CozeWorkflowClient(
            run_url=settings.coze_workflow_run_url,
            graph_parameter_url=settings.coze_workflow_graph_parameter_url,
            token=settings.coze_workflow_token,
            timeout_seconds=settings.coze_workflow_timeout_seconds,
        )
        self.collector = NativeCollector(timeout_seconds=settings.collector_timeout_seconds)
        self.kling = KlingClient(
            api_key=settings.kling_api_key,
            base_url=settings.kling_api_base_url,
            poll_interval_seconds=settings.kling_poll_interval_seconds,
            timeout_seconds=settings.kling_timeout_seconds,
        )
        self.image_generator = ImageGeneratorClient(
            provider=settings.image_api_provider,
            base_url=settings.image_api_base_url,
            api_key=settings.image_api_key,
            timeout_seconds=settings.image_api_timeout_seconds,
        )
        self.publisher = create_publisher(settings)

        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.ip_config_path = os.path.join(self.project_root, "ip-config.json")
        self.ip_config_example_path = os.path.join(self.project_root, "ip-config.example.json")
        self.soul_path = os.path.join(self.project_root, "agents", "ip-host", "SOUL.md")
        self.runtime_config_path = os.path.join(self.project_root, "runtime-config.json")

    def _ok(self, **kwargs) -> Dict[str, Any]:
        data = {"ok": True}
        data.update(kwargs)
        return data

    def _err(self, err: PipelineError | Exception, code: str | None = None) -> Dict[str, Any]:
        if isinstance(err, PipelineError):
            return err.as_dict()
        return {"ok": False, "error_code": code or "INVALID_PAYLOAD", "error": str(err)}

    def _default_runtime_config(self) -> Dict[str, Any]:
        return {
            "schema_version": "1.0",
            "publish_schedule": {
                "enabled": False,
                "timezone": "Asia/Shanghai",
                "daily_limit": 1,
                "slots": ["09:30"],
            },
            "secret_refs": {
                "video_api_key_env": "KLING_API_KEY",
                "kling_api_key_env": "KLING_API_KEY",
                "image_api_key_env": "IMAGE_API_KEY",
                "publish_webhook_token_env": "PUBLISH_WEBHOOK_TOKEN",
                "feishu_app_secret_env": "FEISHU_APP_SECRET",
            },
        }

    def _extract_token_usage(self, value: Any) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, dict):
            direct = value.get("token_usage") or value.get("tokens")
            if isinstance(direct, (int, float)):
                return int(direct)
            usage = value.get("usage")
            if isinstance(usage, dict):
                total = usage.get("total_tokens")
                if isinstance(total, (int, float)):
                    return int(total)
            total = 0
            for item in value.values():
                total += self._extract_token_usage(item)
            return total
        if isinstance(value, list):
            return sum(self._extract_token_usage(item) for item in value)
        return 0

    def _parse_manual_decision(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        text = value.strip().lower()
        if not text:
            return None
        if any(key in text for key in ["通过", "approved", "approve", "yes"]):
            return "approved"
        if any(key in text for key in ["重做", "驳回", "rework", "reject", "no"]):
            return "rework"
        return None

    def health(self) -> Dict[str, Any]:
        return {
            "ok": True,
            "settings": {
                "ingest_provider": self.settings.ingest_provider,
                "collab_provider": self.settings.collab_provider,
                "video_provider": self.settings.video_provider,
                "fit_threshold": self.settings.fit_threshold,
                "feishu_enabled": self.feishu.enabled,
                "callback_verify_mode": self.settings.callback_verify_mode,
                "coze_verify_mode": self.settings.coze_verify_mode,
                "coze_pull_enabled": self.coze_workflow.enabled,
                "collector_supported_platforms": self.collector.supported_platforms,
                "openclaw_enabled": self.openclaw.enabled,
                "kling_enabled": self.kling.enabled,
                "image_api_enabled": self.image_generator.enabled,
                "publish_enabled": self.publisher.enabled,
                "publish_provider": self.settings.publish_provider,
            },
            "dead_retry_jobs": len(self.db.list_dead_retry_jobs()),
        }

    def _safe_feishu_sync(self, content_id: int, fields: Dict[str, Any], stage: str) -> None:
        if self.settings.collab_provider != "feishu":
            return
        content = self.db.get_content(content_id)
        if not content:
            return
        try:
            if content["feishu_record_id"]:
                self.feishu.update_record(content["feishu_record_id"], fields)
            else:
                record_id = self.feishu.create_record(fields)
                if record_id:
                    self.db.set_feishu_record(content_id, record_id)
        except Exception as exc:
            self.db.enqueue_retry_job(
                job_type="feishu_sync",
                dedupe_key=f"feishu_sync:{stage}:{content_id}",
                payload={"content_id": content_id, "stage": stage, "fields": fields, "error": str(exc)},
                max_attempts=self.settings.retry_max_attempts,
                delay_seconds=self.settings.retry_base_delay_seconds,
            )

    def sync_feishu_backflow(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if self.settings.collab_provider != "feishu":
            return self._err(PipelineError("INVALID_PAYLOAD", "collab provider is not feishu"))
        if not self.feishu.enabled:
            return self._err(PipelineError("INVALID_PAYLOAD", "feishu is not configured"))

        limit = int((payload or {}).get("limit", 100))
        dry_run = bool((payload or {}).get("dry_run", False))
        try:
            records = self.feishu.list_records(limit=limit)
        except Exception as exc:
            self.db.enqueue_retry_job(
                job_type="feishu_pull",
                dedupe_key="feishu_pull:global",
                payload={"limit": limit, "dry_run": dry_run, "error": str(exc)},
                max_attempts=self.settings.retry_max_attempts,
                delay_seconds=self.settings.retry_base_delay_seconds,
            )
            return self._err(exc, "FEISHU_SYNC_FAILED")

        matched = 0
        unmatched = 0
        script_updates = 0
        review_updates = 0
        skipped_decisions = 0
        review_errors = []

        for record in records:
            record_id = record.get("record_id")
            fields = record.get("fields", {})
            if not record_id or not isinstance(fields, dict):
                continue
            content = self.db.get_content_by_feishu_record(str(record_id))
            if not content:
                unmatched += 1
                continue
            matched += 1
            content_id = int(content["id"])

            script_raw = fields.get("脚本内容")
            tts_raw = fields.get("配音文本")
            script = script_raw.strip() if isinstance(script_raw, str) and script_raw.strip() else None
            tts_text = tts_raw.strip() if isinstance(tts_raw, str) and tts_raw.strip() else None

            changed = False
            if script is not None or tts_text is not None:
                if dry_run:
                    changed = True
                else:
                    changed = self.db.override_content_production_text(
                        content_id,
                        script=script,
                        tts_text=tts_text,
                    )
            if changed:
                script_updates += 1

            decision = self._parse_manual_decision(fields.get("人工确认"))
            if decision:
                if str(content["status"]) != "pending_review":
                    skipped_decisions += 1
                    continue
                if dry_run:
                    review_updates += 1
                    continue
                review_data = self.review(
                    {
                        "content_id": content_id,
                        "run_id": str(uuid.uuid4()),
                        "decision": decision,
                        "review_source": "feishu-bitable",
                        "platform": content["platform"],
                        "feedback": str(fields.get("审核备注") or "").strip() or None,
                    }
                )
                if review_data.get("ok"):
                    review_updates += 1
                else:
                    review_errors.append(
                        {
                            "content_id": content_id,
                            "record_id": record_id,
                            "error_code": review_data.get("error_code"),
                            "error": review_data.get("error"),
                        }
                    )

        return self._ok(
            dry_run=dry_run,
            total_records=len(records),
            matched=matched,
            unmatched=unmatched,
            script_updates=script_updates,
            review_updates=review_updates,
            skipped_decisions=skipped_decisions,
            review_errors=review_errors,
        )

    def _inject_ip_host_event(self, event: Dict[str, Any], dedupe_key: str) -> None:
        if not self.openclaw.enabled:
            return
        try:
            self.openclaw.inject_system_event(
                agent_id=self.settings.openclaw_ip_host_agent_id,
                event=event,
            )
        except Exception as exc:
            self.db.enqueue_retry_job(
                job_type="openclaw_inject",
                dedupe_key=dedupe_key,
                payload={"agent_id": self.settings.openclaw_ip_host_agent_id, "event": event, "error": str(exc)},
                max_attempts=self.settings.retry_max_attempts,
                delay_seconds=self.settings.retry_base_delay_seconds,
            )

    def _spawn_analysis(self, content_id: int, *, schedule_retry: bool = True) -> Dict[str, Any]:
        content = self.db.get_content(content_id)
        if not content:
            return {"ok": False, "error_code": "CONTENT_NOT_FOUND", "error": "content not found"}
        if not self.openclaw.enabled:
            return {"ok": False, "error_code": "OPENCLAW_REQUEST_FAILED", "error": "openclaw is disabled"}

        run_id = str(uuid.uuid4())
        task = (
            "分析视频并输出严格 JSON。\n"
            f"content_id: {content_id}\n"
            f"run_id: {run_id}\n"
            f"视频链接: {content['video_url']}\n"
            f"博主: {content['author'] or ''}\n"
            f"输出 schema_version={ANALYSIS_SCHEMA_VERSION}，字段: "
            "topic,hook,structure,hashtags,fit_score,fit_reason,replicate"
        )
        try:
            if content["status"] == "collected":
                validate_transition("collected", "analyzing")
                self.db.update_status(
                    content_id=content_id,
                    to_status="analyzing",
                    source="ip-host",
                    run_id=run_id,
                    note="spawn content-analyst",
                )
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="content-analyst",
                source="sessions_spawn",
                status="running",
                payload={"content_id": content_id, "task": task},
            )
            resp = self.openclaw.sessions_spawn(
                parent_agent_id=self.settings.openclaw_ip_host_agent_id,
                agent_id="content-analyst",
                task=task,
                run_timeout_seconds=600,
            )
            return self._ok(run_id=run_id, spawn_response=resp)
        except Exception as exc:
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="content-analyst",
                source="sessions_spawn",
                status="failed",
                payload={"content_id": content_id, "task": task},
                result={},
                error_code="OPENCLAW_REQUEST_FAILED",
                finished=True,
            )
            if schedule_retry:
                self.db.enqueue_retry_job(
                    job_type="spawn_analysis",
                    dedupe_key=f"spawn_analysis:{content_id}",
                    payload={"content_id": content_id},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=self.settings.retry_base_delay_seconds,
                )
            return self._err(exc, "OPENCLAW_REQUEST_FAILED")

    def _spawn_producer(
        self,
        content_id: int,
        analysis_result: Dict[str, Any],
        *,
        schedule_retry: bool = True,
    ) -> Dict[str, Any]:
        content = self.db.get_content(content_id)
        if not content:
            return {"ok": False, "error_code": "CONTENT_NOT_FOUND", "error": "content not found"}
        if not self.openclaw.enabled:
            return {"ok": False, "error_code": "OPENCLAW_REQUEST_FAILED", "error": "openclaw is disabled"}

        run_id = str(uuid.uuid4())
        task = (
            "基于分析报告生产视频并输出严格 JSON。\n"
            f"content_id: {content_id}\n"
            f"run_id: {run_id}\n"
            f"analysis: {json.dumps(analysis_result, ensure_ascii=False)}\n"
            f"provider: {self.settings.video_provider}\n"
            f"输出 schema_version={PRODUCTION_SCHEMA_VERSION}，字段: provider,task_id,status,video_url,script,tts_text,error"
        )
        try:
            if content["status"] in {"evaluated", "pending_rework"}:
                validate_transition(content["status"], "producing")
                self.db.update_status(
                    content_id=content_id,
                    to_status="producing",
                    source="ip-host",
                    run_id=run_id,
                    note="spawn producer-agent",
                )
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="producer-agent",
                source="sessions_spawn",
                status="running",
                payload={"content_id": content_id, "task": task},
            )
            resp = self.openclaw.sessions_spawn(
                parent_agent_id=self.settings.openclaw_ip_host_agent_id,
                agent_id="producer-agent",
                task=task,
                run_timeout_seconds=900,
            )
            return self._ok(run_id=run_id, spawn_response=resp)
        except Exception as exc:
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="producer-agent",
                source="sessions_spawn",
                status="failed",
                payload={"content_id": content_id, "task": task},
                result={},
                error_code="OPENCLAW_REQUEST_FAILED",
                finished=True,
            )
            if schedule_retry:
                self.db.enqueue_retry_job(
                    job_type="spawn_producer",
                    dedupe_key=f"spawn_producer:{content_id}",
                    payload={"content_id": content_id, "analysis_result": analysis_result},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=self.settings.retry_base_delay_seconds,
                )
            return self._err(exc, "OPENCLAW_REQUEST_FAILED")

    def coze_graph_parameter(self) -> Dict[str, Any]:
        try:
            data = self.coze_workflow.graph_parameter()
            return self._ok(
                graph_parameter=data,
                input_schema=data.get("input_schema"),
                output_schema=data.get("output_schema"),
            )
        except Exception as exc:
            return self._err(exc, "COZE_WORKFLOW_REQUEST_FAILED")

    def _build_native_event_id(self, platform: str, video_id: str, video_url: str) -> str:
        key = f"{platform}|{video_id or video_url}".encode("utf-8")
        digest = hashlib.sha1(key).hexdigest()[:20]
        return f"native:{platform}:{digest}"

    def _native_video_to_event(
        self,
        *,
        video: CollectedVideo,
        source: str,
        author_override: str = "",
    ) -> Dict[str, Any]:
        author = str(author_override or video.author or "unknown").strip() or "unknown"
        return {
            "event_id": self._build_native_event_id(video.platform, video.video_id, video.video_url),
            "source": source,
            "video_url": video.video_url,
            "video_id": video.video_id,
            "author": author,
            "platform": video.platform,
            "stats": {
                "plays": int(video.stats.get("plays", 0)),
                "likes": int(video.stats.get("likes", 0)),
                "comments": int(video.stats.get("comments", 0)),
                "shares": int(video.stats.get("shares", 0)),
            },
            "collected_at": video.collected_at or datetime.now(timezone.utc).isoformat(),
        }

    def _target_cookie(self, target: Dict[str, Any]) -> str:
        cookie = str(
            target.get("platform_cookie")
            or target.get("cookie")
            or target.get("platformCookie")
            or ""
        ).strip()
        if cookie:
            return cookie
        cookie_env = str(target.get("cookie_env") or target.get("cookieEnv") or "").strip()
        if cookie_env:
            return str(os.getenv(cookie_env, "")).strip()
        return ""

    def _load_native_targets(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        targets = payload.get("targets")
        if not isinstance(targets, list):
            targets = []

        if not targets and bool(payload.get("use_ip_config", True)):
            try:
                cfg = self.get_ip_config()
                if cfg.get("ok"):
                    loaded = (cfg.get("config") or {}).get("targets")
                    if isinstance(loaded, list):
                        targets = loaded
            except Exception:
                targets = []

        normalized: List[Dict[str, Any]] = []
        default_platform = str(payload.get("platform") or "").strip().lower()
        default_cookie = str(payload.get("platform_cookie") or payload.get("cookie") or "").strip()
        default_max = int(payload.get("max_videos") or self.settings.collector_default_max_videos or 10)

        for item in targets:
            if not isinstance(item, dict):
                continue
            profile_url = str(item.get("profile_url") or item.get("profileUrl") or item.get("url") or "").strip()
            if not profile_url:
                continue
            platform = str(item.get("platform") or default_platform).strip().lower()
            max_videos_raw = item.get("max_videos") or item.get("maxVideos") or default_max
            try:
                max_videos = int(max_videos_raw)
            except Exception:
                max_videos = default_max
            cookie = default_cookie or self._target_cookie(item)
            normalized.append(
                {
                    "profile_url": profile_url,
                    "platform": platform,
                    "platform_cookie": cookie,
                    "max_videos": max(1, min(max_videos, 30)),
                    "author": str(item.get("name") or item.get("author") or "").strip(),
                }
            )

        if normalized:
            return normalized

        profile_url = str(payload.get("profile_url") or payload.get("profileUrl") or "").strip()
        if profile_url:
            cookie = default_cookie
            cookie_env = str(payload.get("cookie_env") or payload.get("cookieEnv") or "").strip()
            if not cookie and cookie_env:
                cookie = str(os.getenv(cookie_env, "")).strip()
            return [
                {
                    "profile_url": profile_url,
                    "platform": default_platform,
                    "platform_cookie": cookie,
                    "max_videos": max(1, min(default_max, 30)),
                    "author": str(payload.get("author") or "").strip(),
                }
            ]
        return []

    def collect_run(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = payload or {}
        dry_run = bool(payload.get("dry_run", False))
        source = str(payload.get("source") or self.settings.collector_default_source or "native-collector").strip()
        if not source:
            source = "native-collector"

        targets = self._load_native_targets(payload)
        if not targets:
            return self._err(
                PipelineError(
                    "INVALID_PAYLOAD",
                    "no collection targets found; provide targets/profile_url or configure targets in ip-config",
                )
            )

        accepted = 0
        deduped = 0
        failed = 0
        events: List[Dict[str, Any]] = []
        details: List[Dict[str, Any]] = []
        target_errors: List[Dict[str, Any]] = []

        for target in targets:
            profile_url = str(target.get("profile_url") or "").strip()
            platform = str(target.get("platform") or "").strip().lower()
            max_videos = int(target.get("max_videos") or self.settings.collector_default_max_videos or 10)
            cookie = str(target.get("platform_cookie") or "").strip()
            try:
                videos = self.collector.collect(
                    profile_url=profile_url,
                    platform=platform,
                    cookie=cookie,
                    max_videos=max_videos,
                )
            except Exception as exc:
                target_errors.append(
                    {
                        "profile_url": profile_url,
                        "platform": platform or "auto",
                        "error": str(exc),
                    }
                )
                continue

            for video in videos:
                event = self._native_video_to_event(
                    video=video,
                    source=source,
                    author_override=str(target.get("author") or ""),
                )
                events.append(event)

        if dry_run:
            return self._ok(
                dry_run=True,
                source=source,
                target_count=len(targets),
                event_count=len(events),
                target_errors=target_errors,
                events=events,
            )

        for event in events:
            out = self.ingest_coze(event)
            if out.get("ok") and not out.get("dedup"):
                accepted += 1
            elif out.get("ok") and out.get("dedup"):
                deduped += 1
            else:
                failed += 1
            details.append(
                {
                    "event_id": event.get("event_id"),
                    "video_id": event.get("video_id"),
                    "platform": event.get("platform"),
                    "ok": bool(out.get("ok")),
                    "dedup": bool(out.get("dedup", False)),
                    "error_code": out.get("error_code"),
                }
            )

        failed += len(target_errors)

        return self._ok(
            dry_run=False,
            source=source,
            target_count=len(targets),
            event_count=len(events),
            accepted=accepted,
            deduped=deduped,
            failed=failed,
            target_errors=target_errors,
            details=details,
        )

    def _coze_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _extract_coze_events(self, response: Any) -> list[Dict[str, Any]]:
        containers: list[Any] = []
        if isinstance(response, dict):
            containers.append(response)
            for key in ["data", "output", "result", "payload"]:
                value = response.get(key)
                if isinstance(value, dict):
                    containers.append(value)
        elif isinstance(response, list):
            containers.append({"videos": response})

        videos: list[Any] = []
        for container in containers:
            if not isinstance(container, dict):
                continue
            for key in ["videos", "items", "video_list", "list"]:
                value = container.get(key)
                if isinstance(value, list):
                    videos = value
                    break
            if videos:
                break

        events: list[Dict[str, Any]] = []
        for idx, item in enumerate(videos):
            if not isinstance(item, dict):
                continue
            video_url = (
                str(item.get("video_url") or item.get("url") or item.get("origin_url") or "").strip()
            )
            video_id = str(item.get("video_id") or item.get("id") or "").strip()
            if not video_id and video_url:
                video_id = video_url.rstrip("/").split("/")[-1]

            platform = str(item.get("platform") or item.get("source_platform") or "unknown").strip() or "unknown"
            author = str(item.get("creator_name") or item.get("author") or item.get("creator") or "unknown").strip() or "unknown"

            likes = item.get("likes_count")
            comments = item.get("comments_count")
            plays = item.get("plays_count")
            shares = item.get("shares_count")
            try:
                likes_v = int(likes or 0)
            except Exception:
                likes_v = 0
            try:
                comments_v = int(comments or 0)
            except Exception:
                comments_v = 0
            try:
                plays_v = int(plays or 0)
            except Exception:
                plays_v = 0
            try:
                shares_v = int(shares or 0)
            except Exception:
                shares_v = 0

            if not video_url:
                continue

            event = {
                "event_id": f"coze-pull:{platform}:{video_id or idx}:{uuid.uuid4()}",
                "source": "coze-pull",
                "video_url": video_url,
                "video_id": video_id or f"unknown-{idx}",
                "author": author,
                "platform": platform,
                "stats": {
                    "plays": plays_v,
                    "likes": likes_v,
                    "comments": comments_v,
                    "shares": shares_v,
                },
                "collected_at": item.get("publish_time") or self._coze_now_iso(),
            }
            events.append(event)

        return events

    def coze_pull_run(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = payload or {}

        inputs = payload.get("inputs")
        if inputs is None:
            raw = (self.settings.coze_workflow_default_inputs_json or "{}").strip()
            try:
                inputs = json.loads(raw) if raw else {}
            except Exception:
                return self._err(PipelineError("INVALID_PAYLOAD", "COZE_WORKFLOW_DEFAULT_INPUTS_JSON must be valid JSON"))

        if not isinstance(inputs, dict):
            return self._err(PipelineError("INVALID_PAYLOAD", "inputs must be object"))

        dry_run = bool(payload.get("dry_run", False))

        try:
            run_response = self.coze_workflow.run(inputs)
        except Exception as exc:
            return self._err(exc, "COZE_WORKFLOW_REQUEST_FAILED")

        events = self._extract_coze_events(run_response)
        if dry_run:
            return self._ok(
                dry_run=True,
                event_count=len(events),
                events=events,
                run_response=run_response,
            )

        accepted = 0
        deduped = 0
        failed = 0
        details: list[Dict[str, Any]] = []

        for event in events:
            out = self.ingest_coze(event)
            if out.get("ok") and not out.get("dedup"):
                accepted += 1
            elif out.get("ok") and out.get("dedup"):
                deduped += 1
            else:
                failed += 1
            details.append(
                {
                    "event_id": event.get("event_id"),
                    "video_id": event.get("video_id"),
                    "platform": event.get("platform"),
                    "ok": bool(out.get("ok")),
                    "dedup": bool(out.get("dedup", False)),
                    "error_code": out.get("error_code"),
                }
            )

        return self._ok(
            dry_run=False,
            event_count=len(events),
            accepted=accepted,
            deduped=deduped,
            failed=failed,
            details=details,
            run_response=run_response,
        )

    def ingest_coze(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            payload = validate_coze_event(payload)
        except Exception as exc:
            return self._err(exc)

        event_id = str(payload["event_id"])
        source = str(payload.get("source", "coze"))
        platform = str(payload["platform"])
        video_id = str(payload["video_id"])

        existing_event = self.db.get_external_event(event_id)
        if existing_event:
            return self._ok(dedup=True, reason="event_id", event_id=event_id, status=existing_event["status"])

        existing_content = self.db.get_content_by_key(platform, video_id)
        if existing_content:
            self.db.insert_external_event(
                event_id=event_id,
                source=source,
                platform=platform,
                video_id=video_id,
                payload=payload,
                status="dedup_video",
                note="duplicate platform+video_id",
            )
            return self._ok(
                dedup=True,
                reason="platform_video_id",
                event_id=event_id,
                content_id=existing_content["id"],
                status=existing_content["status"],
            )

        run_id = str(uuid.uuid4())
        content_id = self.db.insert_content(
            platform=platform,
            video_id=video_id,
            video_url=payload["video_url"],
            author=payload["author"],
            stats=payload.get("stats", {}),
            source=source,
            status="collected",
            run_id=run_id,
        )
        self.db.insert_external_event(
            event_id=event_id,
            source=source,
            platform=platform,
            video_id=video_id,
            payload=payload,
            status="accepted",
        )
        self.db.upsert_task_run(
            run_id=run_id,
            content_id=content_id,
            agent="ip-host",
            source=source,
            status="succeeded",
            payload=payload,
            result={"action": "ingested"},
            finished=True,
        )

        self._safe_feishu_sync(content_id, build_collected_fields(payload, "采集中"), "collected")
        self._inject_ip_host_event(
            {
                "type": "new_video",
                "event_id": event_id,
                "content_id": content_id,
                "video_url": payload["video_url"],
                "video_id": video_id,
                "author": payload["author"],
                "platform": platform,
                "stats": payload.get("stats", {}),
                "collected_at": payload.get("collected_at"),
            },
            dedupe_key=f"openclaw_inject:{event_id}",
        )

        spawn_data = None
        if self.openclaw.enabled:
            spawn_data = self._spawn_analysis(content_id)

        return self._ok(
            dedup=False,
            content_id=content_id,
            run_id=run_id,
            event_id=event_id,
            spawn=spawn_data,
            next="wait analysis announce",
        )

    def update_analysis(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        content_id = int(payload.get("content_id", 0))
        run_id = payload.get("run_id") or str(uuid.uuid4())
        result = payload.get("result", {})
        content = self.db.get_content(content_id)
        if not content:
            return self._err(PipelineError("CONTENT_NOT_FOUND"))

        try:
            result = validate_analysis_result(result)
        except Exception as exc:
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="content-analyst",
                source="announce",
                status="failed",
                payload=payload,
                result={},
                error_code="INVALID_PAYLOAD",
                finished=True,
            )
            return self._err(exc)

        try:
            if content["status"] == "collected":
                validate_transition("collected", "analyzing")
                self.db.update_status(
                    content_id=content_id,
                    to_status="analyzing",
                    source="content-analyst",
                    run_id=run_id,
                    note="analysis started",
                )
            validate_transition("analyzing", "evaluated")
            self.db.update_status(
                content_id=content_id,
                to_status="evaluated",
                source="content-analyst",
                run_id=run_id,
                note="analysis completed",
            )
        except ValueError as exc:
            return self._err(PipelineError("INVALID_TRANSITION", str(exc)))

        should_replicate = bool(result.get("replicate", False)) and int(result.get("fit_score", 0)) >= self.settings.fit_threshold
        self.db.set_content_analysis(content_id, result, should_replicate)
        self.db.upsert_task_run(
            run_id=run_id,
            content_id=content_id,
            agent="content-analyst",
            source="announce",
            status="succeeded",
            payload=payload,
            result=result,
            finished=True,
        )
        self._safe_feishu_sync(content_id, build_analysis_fields(result, "评估中"), "analysis")

        spawn_data = None
        if should_replicate and self.openclaw.enabled:
            spawn_data = self._spawn_producer(content_id, result)

        return self._ok(
            content_id=content_id,
            run_id=run_id,
            replicate=should_replicate,
            spawn=spawn_data,
            next="wait producer announce" if should_replicate else "skip",
        )

    def update_production(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        content_id = int(payload.get("content_id", 0))
        run_id = payload.get("run_id") or str(uuid.uuid4())
        result = payload.get("result", {})
        content = self.db.get_content(content_id)
        if not content:
            return self._err(PipelineError("CONTENT_NOT_FOUND"))

        try:
            result = validate_production_result(result)
        except Exception as exc:
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="producer-agent",
                source="announce",
                status="failed",
                payload=payload,
                result={},
                error_code="INVALID_PAYLOAD",
                finished=True,
            )
            return self._err(exc)

        if result["status"] != "completed":
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="producer-agent",
                source="announce",
                status="failed" if result["status"] == "failed" else "running",
                payload=payload,
                result=result,
                error_code="KLING_REQUEST_FAILED" if result["status"] == "failed" else None,
                finished=result["status"] == "failed",
            )
            if result["status"] == "failed":
                self.db.update_status(
                    content_id=content_id,
                    to_status="failed",
                    source="producer-agent",
                    run_id=run_id,
                    note="production failed",
                    error_code="KLING_REQUEST_FAILED",
                )
                self.db.enqueue_retry_job(
                    job_type="spawn_producer",
                    dedupe_key=f"spawn_producer:{content_id}",
                    payload={"content_id": content_id, "analysis_result": self.db.latest_task_result(content_id, "content-analyst")},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=self.settings.retry_base_delay_seconds,
                )
            return self._ok(content_id=content_id, run_id=run_id, status=result["status"])

        try:
            if content["status"] in {"evaluated", "pending_rework"}:
                validate_transition(content["status"], "producing")
                self.db.update_status(
                    content_id=content_id,
                    to_status="producing",
                    source="producer-agent",
                    run_id=run_id,
                    note="production started",
                )
            validate_transition("producing", "pending_review")
            self.db.update_status(
                content_id=content_id,
                to_status="pending_review",
                source="producer-agent",
                run_id=run_id,
                note="production completed",
            )
        except ValueError as exc:
            return self._err(PipelineError("INVALID_TRANSITION", str(exc)))

        self.db.set_content_production(content_id, result)
        self.db.upsert_task_run(
            run_id=run_id,
            content_id=content_id,
            agent="producer-agent",
            source="announce",
            status="succeeded",
            payload=payload,
            result=result,
            finished=True,
        )
        self._safe_feishu_sync(content_id, build_production_fields(result, "待审核"), "production")

        return self._ok(content_id=content_id, run_id=run_id, status="pending_review")

    def run_kling_production(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        content_id = int(payload.get("content_id", 0))
        content = self.db.get_content(content_id)
        if not content:
            return self._err(PipelineError("CONTENT_NOT_FOUND"))

        analysis = self.db.latest_task_result(content_id, "content-analyst")
        script = payload.get("script") or analysis.get("topic") or "这是一个自动生成脚本。"
        tts_text = build_tts_mock(script)
        image_url = str(payload.get("image_url") or payload.get("ip_asset_url") or "")
        reference_video_url = str(payload.get("reference_video_url") or content["video_url"])

        run_id = payload.get("run_id") or str(uuid.uuid4())
        try:
            task_id = self.kling.create_motion_transfer(
                image_url=image_url,
                reference_video_url=reference_video_url,
                prompt=script,
            )
            final = self.kling.wait_for_completion(task_id)
            result = {
                "schema_version": PRODUCTION_SCHEMA_VERSION,
                "provider": "kling",
                "task_id": task_id,
                "status": final["status"],
                "video_url": final.get("video_url", ""),
                "script": script,
                "tts_text": tts_text,
                "error": final.get("error") or None,
            }
            return self.update_production({"content_id": content_id, "run_id": run_id, "result": result})
        except Exception as exc:
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="producer-agent",
                source="kling-runtime",
                status="failed",
                payload=payload,
                result={},
                error_code="KLING_REQUEST_FAILED",
                finished=True,
            )
            self.db.enqueue_retry_job(
                job_type="spawn_producer",
                dedupe_key=f"spawn_producer:{content_id}",
                payload={"content_id": content_id, "analysis_result": analysis},
                max_attempts=self.settings.retry_max_attempts,
                delay_seconds=self.settings.retry_base_delay_seconds,
            )
            return self._err(exc, "KLING_REQUEST_FAILED")

    def provider_requirements(self) -> Dict[str, Any]:
        items = [
            {
                "provider": "kling",
                "configured": bool(self.settings.kling_api_key),
                "required_env": ["KLING_API_KEY"],
                "base_url": self.settings.kling_api_base_url,
            },
            {
                "provider": self.settings.image_api_provider or "keling",
                "configured": bool(self.settings.image_api_base_url and self.settings.image_api_key),
                "required_env": ["IMAGE_API_KEY", "IMAGE_API_BASE_URL"],
                "base_url": self.settings.image_api_base_url,
            },
            {
                "provider": "native_collector",
                "configured": True,
                "required_env": [],
                "supported_platforms": self.collector.supported_platforms,
            },
        ]
        return self._ok(items=items)

    def generate_xhs_draft(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        content_id = int(payload.get("content_id", 0))
        content = self.db.get_content(content_id)
        if not content:
            return self._err(PipelineError("CONTENT_NOT_FOUND"))

        status = str(content["status"])
        if status in {"evaluated", "pending_rework"}:
            try:
                validate_transition(status, "producing")
                self.db.update_status(
                    content_id=content_id,
                    to_status="producing",
                    source="producer-agent",
                    run_id=payload.get("run_id") or str(uuid.uuid4()),
                    note="xhs image generation started",
                )
            except ValueError as exc:
                return self._err(PipelineError("INVALID_TRANSITION", str(exc)))
        elif status not in {"producing", "pending_review"}:
            return self._err(
                PipelineError(
                    "INVALID_TRANSITION",
                    f"content {content_id} status={status} is not ready for xhs draft generation",
                )
            )

        analysis = self.db.latest_task_result(content_id, "content-analyst")
        prompt = str(payload.get("prompt") or "").strip()
        if not prompt:
            prompt = str(
                payload.get("title")
                or analysis.get("topic")
                or f"{content['author'] or 'AI IP'} 风格化图文封面"
            )
        title = str(payload.get("title") or analysis.get("topic") or prompt)[:80]
        description = str(payload.get("description") or analysis.get("fit_reason") or prompt)
        tags = payload.get("tags") or analysis.get("hashtags") or []
        if isinstance(tags, str):
            tags = [x.strip() for x in tags.split(",") if x.strip()]

        run_id = payload.get("run_id") or str(uuid.uuid4())
        image_count = int(payload.get("image_count", 3))
        aspect_ratio = str(payload.get("aspect_ratio") or "3:4")
        style = str(payload.get("style") or "xiaohongshu")
        negative_prompt = str(payload.get("negative_prompt") or "")

        try:
            generated = self.image_generator.generate_images(
                prompt=prompt,
                count=image_count,
                aspect_ratio=aspect_ratio,
                style=style,
                negative_prompt=negative_prompt,
            )
        except Exception as exc:
            error_code = exc.code if isinstance(exc, PipelineError) else "IMAGE_API_FAILED"
            self.db.upsert_task_run(
                run_id=run_id,
                content_id=content_id,
                agent="producer-agent",
                source="xhs-image-generator",
                status="failed",
                payload=payload,
                result={},
                error_code=error_code,
                finished=True,
            )
            self.db.update_status(
                content_id=content_id,
                to_status="failed",
                source="producer-agent",
                run_id=run_id,
                note="xhs image generation failed",
                error_code=error_code,
            )
            return self._err(exc, error_code)

        image_urls = generated["images"]
        result = {
            "schema_version": PRODUCTION_SCHEMA_VERSION,
            "provider": generated.get("provider") or self.settings.image_api_provider,
            "task_id": "",
            "status": "completed",
            "video_url": image_urls[0] if image_urls else "",
            "script": description,
            "tts_text": "",
            "error": None,
            "post_type": "image_text",
            "image_urls": image_urls,
            "title": title,
            "description": description,
            "tags": tags,
            "source_prompt": prompt,
        }

        self.db.set_content_production(content_id, result)
        self.db.upsert_task_run(
            run_id=run_id,
            content_id=content_id,
            agent="producer-agent",
            source="xhs-image-generator",
            status="succeeded",
            payload=payload,
            result=result,
            finished=True,
        )

        if status in {"evaluated", "pending_rework", "producing"}:
            try:
                validate_transition("producing", "pending_review")
                self.db.update_status(
                    content_id=content_id,
                    to_status="pending_review",
                    source="producer-agent",
                    run_id=run_id,
                    note="xhs draft generated",
                )
            except ValueError as exc:
                return self._err(PipelineError("INVALID_TRANSITION", str(exc)))

        self._safe_feishu_sync(
            content_id,
            build_production_fields(
                {
                    "script": description,
                    "tts_text": "",
                    "video_url": image_urls[0] if image_urls else "",
                },
                "待审核",
            ),
            "production",
        )

        return self._ok(
            content_id=content_id,
            run_id=run_id,
            status="pending_review",
            draft={
                "post_type": "image_text",
                "title": title,
                "description": description,
                "image_urls": image_urls,
                "tags": tags,
            },
        )

    def publish_login(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = payload or {}
        if not hasattr(self.publisher, "prepare_login"):
            return self._err(
                PipelineError(
                    "INVALID_PAYLOAD",
                    f"publish provider does not support interactive login: {self.settings.publish_provider}",
                )
            )

        try:
            out = self.publisher.prepare_login(payload)
            return self._ok(provider=self.settings.publish_provider, result=out)
        except Exception as exc:
            return self._err(exc, "PUBLISH_DISPATCH_FAILED")

    def publish_login_status(self, session_id: str) -> Dict[str, Any]:
        if not hasattr(self.publisher, "login_status"):
            return self._err(
                PipelineError(
                    "INVALID_PAYLOAD",
                    f"publish provider does not support login status: {self.settings.publish_provider}",
                )
            )

        try:
            out = self.publisher.login_status(session_id)
            return self._ok(provider=self.settings.publish_provider, result=out)
        except Exception as exc:
            return self._err(exc, "PUBLISH_DISPATCH_FAILED")

    def publish_accounts(self) -> Dict[str, Any]:
        if not hasattr(self.publisher, "list_accounts"):
            return self._ok(provider=self.settings.publish_provider, items=[])
        try:
            out = self.publisher.list_accounts()
            return self._ok(provider=self.settings.publish_provider, items=out.get("items", []))
        except Exception as exc:
            return self._err(exc, "PUBLISH_DISPATCH_FAILED")

    def remove_publish_account(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not hasattr(self.publisher, "remove_account"):
            return self._err(
                PipelineError(
                    "INVALID_PAYLOAD",
                    f"publish provider does not support account management: {self.settings.publish_provider}",
                )
            )
        try:
            out = self.publisher.remove_account(payload)
            return self._ok(provider=self.settings.publish_provider, result=out)
        except Exception as exc:
            return self._err(exc, "PUBLISH_DISPATCH_FAILED")

    def _dispatch_publish(
        self,
        *,
        content_id: int,
        platform: str | None,
        feedback: str | None,
        review_source: str,
        schedule_retry: bool,
    ) -> Dict[str, Any]:
        content = self.db.get_content(content_id)
        if not content:
            return self._err(PipelineError("CONTENT_NOT_FOUND"))
        if self.db.has_published_record(content_id):
            return self._ok(content_id=content_id, already_published=True)

        production = json.loads(content["production_json"] or "{}")
        analysis = json.loads(content["analysis_json"] or "{}")
        script_text = str(production.get("script") or "")
        topic = str(analysis.get("topic") or "")
        title = str(production.get("title") or topic or script_text or f"AI IP 内容 {content_id}")[:80]
        post_type = str(production.get("post_type") or "video")
        image_urls = production.get("image_urls") or []
        if not isinstance(image_urls, list):
            image_urls = []
        request_payload = {
            "content_id": content_id,
            "platform": platform or content["platform"],
            "post_type": post_type,
            "video_url": production.get("video_url", ""),
            "video_path": production.get("video_path") or production.get("local_video_path") or "",
            "image_urls": image_urls,
            "script": script_text,
            "title": title,
            "description": str(production.get("description") or script_text),
            "tags": analysis.get("hashtags") or [],
            "tts_text": production.get("tts_text", ""),
            "source": review_source,
        }

        try:
            publish_resp = self.publisher.publish(request_payload)
            publish_url = publish_resp.get("publish_url") or request_payload["video_url"]
            external_id = publish_resp.get("external_id") or ""
            self.db.update_status(
                content_id=content_id,
                to_status="published",
                source="publisher",
                run_id=str(uuid.uuid4()),
                note="publish executed",
            )
            self.db.add_publish_record(
                content_id=content_id,
                decision="approved",
                platform=platform or content["platform"],
                publish_url=publish_url,
                external_id=external_id,
                review_feedback=feedback,
            )
            self._safe_feishu_sync(content_id, build_review_fields("approved", publish_url, "已发布"), "review")
            return self._ok(content_id=content_id, publish_url=publish_url, external_id=external_id)
        except Exception as exc:
            if schedule_retry:
                self.db.enqueue_retry_job(
                    job_type="publish_dispatch",
                    dedupe_key=f"publish_dispatch:{content_id}",
                    payload={
                        "content_id": content_id,
                        "platform": platform,
                        "feedback": feedback,
                        "review_source": review_source,
                    },
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=self.settings.retry_base_delay_seconds,
                )
            self.db.update_status(
                content_id=content_id,
                to_status="failed",
                source="publisher",
                run_id=str(uuid.uuid4()),
                note="publish dispatch failed",
                error_code="PUBLISH_DISPATCH_FAILED",
            )
            return self._err(exc, "PUBLISH_DISPATCH_FAILED")

    def review(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        content_id = int(payload.get("content_id", 0))
        decision = payload.get("decision")
        platform = payload.get("platform")
        feedback = payload.get("feedback")
        review_source = payload.get("review_source", "external-callback")
        run_id = payload.get("run_id") or str(uuid.uuid4())

        content = self.db.get_content(content_id)
        if not content:
            return self._err(PipelineError("CONTENT_NOT_FOUND"))
        if content["status"] != "pending_review":
            return {"ok": False, "error_code": "INVALID_TRANSITION", "error": f"invalid current status: {content['status']}"}

        if decision == "approved":
            self.db.update_status(
                content_id=content_id,
                to_status="publishing",
                source=review_source,
                run_id=run_id,
                note="review approved",
            )
            publish_data = self._dispatch_publish(
                content_id=content_id,
                platform=platform,
                feedback=feedback,
                review_source=review_source,
                schedule_retry=True,
            )
            if not publish_data.get("ok"):
                return publish_data
            return self._ok(content_id=content_id, decision=decision, run_id=run_id, publish=publish_data)

        if decision == "rework":
            self.db.update_status(
                content_id=content_id,
                to_status="pending_rework",
                source=review_source,
                run_id=run_id,
                note="review rejected",
            )
            self.db.add_publish_record(
                content_id=content_id,
                decision="rework",
                platform=platform,
                publish_url=None,
                external_id=None,
                review_feedback=feedback,
            )
            self._safe_feishu_sync(content_id, build_review_fields("rework", None, "待重做"), "review")
            return self._ok(content_id=content_id, decision=decision, run_id=run_id)

        return {"ok": False, "error_code": "INVALID_PAYLOAD", "error": "decision must be approved or rework"}

    def process_retry_jobs(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        limit = int((payload or {}).get("limit", 20))
        jobs = self.db.list_due_retry_jobs(limit=limit)
        processed = []

        for job in jobs:
            job_id = int(job["id"])
            self.db.mark_retry_running(job_id)
            job_type = job["job_type"]
            data = job["payload"]
            try:
                if job_type == "openclaw_inject":
                    self.openclaw.inject_system_event(agent_id=data["agent_id"], event=data["event"])
                elif job_type == "spawn_analysis":
                    out = self._spawn_analysis(int(data["content_id"]), schedule_retry=False)
                    if not out.get("ok"):
                        raise PipelineError("RETRY_JOB_FAILED", out.get("error", "spawn_analysis failed"))
                elif job_type == "spawn_producer":
                    out = self._spawn_producer(
                        int(data["content_id"]),
                        data.get("analysis_result") or self.db.latest_task_result(int(data["content_id"]), "content-analyst"),
                        schedule_retry=False,
                    )
                    if not out.get("ok"):
                        raise PipelineError("RETRY_JOB_FAILED", out.get("error", "spawn_producer failed"))
                elif job_type == "publish_dispatch":
                    out = self._dispatch_publish(
                        content_id=int(data["content_id"]),
                        platform=data.get("platform"),
                        feedback=data.get("feedback"),
                        review_source=data.get("review_source", "retry"),
                        schedule_retry=False,
                    )
                    if not out.get("ok"):
                        raise PipelineError("RETRY_JOB_FAILED", out.get("error", "publish failed"))
                elif job_type == "feishu_sync":
                    self._safe_feishu_sync(int(data["content_id"]), data.get("fields", {}), data.get("stage", "retry"))
                elif job_type == "feishu_pull":
                    out = self.sync_feishu_backflow(
                        {
                            "limit": data.get("limit", 100),
                            "dry_run": data.get("dry_run", False),
                        }
                    )
                    if not out.get("ok"):
                        raise PipelineError("RETRY_JOB_FAILED", out.get("error", "feishu_pull failed"))
                else:
                    raise PipelineError("RETRY_JOB_FAILED", f"unsupported job type: {job_type}")

                self.db.mark_retry_succeeded(job_id)
                processed.append({"job_id": job_id, "job_type": job_type, "status": "succeeded"})
            except Exception as exc:
                self.db.mark_retry_failed(job_id, error=str(exc), base_delay_seconds=self.settings.retry_base_delay_seconds)
                processed.append({"job_id": job_id, "job_type": job_type, "status": "failed", "error": str(exc)})

        return self._ok(processed=processed, dead_jobs=self.db.list_dead_retry_jobs())

    def reconcile(self) -> Dict[str, Any]:
        actions = []

        for item in self.db.list_content_by_status(["collected"]):
            content_id = int(item["id"])
            has_task = self.db.has_task_for_content(content_id, "content-analyst", ["running", "succeeded"])
            if not has_task:
                self.db.enqueue_retry_job(
                    job_type="spawn_analysis",
                    dedupe_key=f"spawn_analysis:{content_id}",
                    payload={"content_id": content_id},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=0,
                )
                actions.append({"content_id": content_id, "action": "enqueue_spawn_analysis"})

        for item in self.db.list_content_by_status(["evaluated", "pending_rework"]):
            content_id = int(item["id"])
            if int(item.get("replicate", 0)) != 1:
                continue
            has_task = self.db.has_task_for_content(content_id, "producer-agent", ["running", "succeeded"])
            if not has_task:
                self.db.enqueue_retry_job(
                    job_type="spawn_producer",
                    dedupe_key=f"spawn_producer:{content_id}",
                    payload={"content_id": content_id, "analysis_result": self.db.latest_task_result(content_id, "content-analyst")},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=0,
                )
                actions.append({"content_id": content_id, "action": "enqueue_spawn_producer"})

        for item in self.db.list_content_by_status(["publishing"]):
            content_id = int(item["id"])
            if self.db.has_published_record(content_id):
                continue
            self.db.enqueue_retry_job(
                job_type="publish_dispatch",
                dedupe_key=f"publish_dispatch:{content_id}",
                payload={"content_id": content_id, "review_source": "reconcile"},
                max_attempts=self.settings.retry_max_attempts,
                delay_seconds=0,
            )
            actions.append({"content_id": content_id, "action": "enqueue_publish_dispatch"})

        return self._ok(actions=actions)

    def recover_stuck(self) -> Dict[str, Any]:
        stuck = self.db.find_stuck_content(["analyzing", "producing", "publishing"], timeout_minutes=self.settings.stuck_timeout_minutes)
        actions = []
        for item in stuck:
            content_id = int(item["id"])
            status = item["status"]
            if status == "analyzing":
                self.db.enqueue_retry_job(
                    job_type="spawn_analysis",
                    dedupe_key=f"spawn_analysis:{content_id}",
                    payload={"content_id": content_id},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=0,
                )
                actions.append({"content_id": content_id, "action": "recover_analysis"})
            elif status == "producing":
                self.db.enqueue_retry_job(
                    job_type="spawn_producer",
                    dedupe_key=f"spawn_producer:{content_id}",
                    payload={"content_id": content_id, "analysis_result": self.db.latest_task_result(content_id, "content-analyst")},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=0,
                )
                actions.append({"content_id": content_id, "action": "recover_production"})
            elif status == "publishing":
                self.db.enqueue_retry_job(
                    job_type="publish_dispatch",
                    dedupe_key=f"publish_dispatch:{content_id}",
                    payload={"content_id": content_id, "review_source": "recover"},
                    max_attempts=self.settings.retry_max_attempts,
                    delay_seconds=0,
                )
                actions.append({"content_id": content_id, "action": "recover_publish"})

        return self._ok(actions=actions)

    def list_content(self) -> Dict[str, Any]:
        return self._ok(items=self.db.list_content())

    def list_dead_jobs(self) -> Dict[str, Any]:
        return self._ok(items=self.db.list_dead_retry_jobs())

    def dashboard_summary(self) -> Dict[str, Any]:
        counts = self.db.count_content_by_status()
        recent_runs = self.db.list_recent_task_runs(limit=100)
        failed_runs = [r for r in recent_runs if r.get("status") == "failed"]
        review_stats = self.db.review_stats()
        total_reviews = int(review_stats.get("total_reviews", 0))
        approved_reviews = int(review_stats.get("approved_reviews", 0))
        pass_rate = (approved_reviews / total_reviews) if total_reviews > 0 else 0.0

        token_total = 0
        for run in self.db.list_recent_task_runs_with_json(limit=300):
            token_total += self._extract_token_usage(run.get("payload"))
            token_total += self._extract_token_usage(run.get("result"))

        summary = {
            "total_content": self.db.total_content_count(),
            "pending_review": int(counts.get("pending_review", 0)),
            "published": int(counts.get("published", 0)),
            "failed": int(counts.get("failed", 0)),
            "dead_jobs": len(self.db.list_dead_retry_jobs()),
            "review_total": total_reviews,
            "pass_rate": round(pass_rate, 4),
            "token_usage_total": token_total,
        }
        return self._ok(summary=summary, status_counts=counts, recent_failed_runs=failed_runs[:20])

    def dashboard_agents(self) -> Dict[str, Any]:
        rows = {r["agent"]: r for r in self.db.list_agent_status()}
        items = []
        for agent_name in ["ip-host", "content-analyst", "producer-agent", "analyst-agent"]:
            row = rows.get(agent_name)
            if not row:
                items.append(
                    {
                        "agent": agent_name,
                        "state": "idle",
                        "last_status": "idle",
                        "running_count": 0,
                        "last_updated_at": None,
                    }
                )
                continue
            running_count = int(row.get("running_count", 0))
            last_status = row.get("last_status") or "idle"
            state = "running" if running_count > 0 else ("error" if last_status == "failed" else "idle")
            items.append(
                {
                    "agent": agent_name,
                    "state": state,
                    "last_status": last_status,
                    "running_count": running_count,
                    "last_updated_at": row.get("last_updated_at"),
                }
            )
        return self._ok(items=items)

    def dashboard_metrics(self) -> Dict[str, Any]:
        items = list(reversed(self.db.list_metrics_snapshots(limit=200)))
        plays = sum(int(x.get("plays") or 0) for x in items)
        likes = sum(int(x.get("likes") or 0) for x in items)
        comments = sum(int(x.get("comments") or 0) for x in items)

        review_stats = self.db.review_stats()
        total_reviews = int(review_stats.get("total_reviews", 0))
        approved_reviews = int(review_stats.get("approved_reviews", 0))
        pass_rate = (approved_reviews / total_reviews) if total_reviews > 0 else 0.0

        token_total = 0
        for run in self.db.list_recent_task_runs_with_json(limit=300):
            token_total += self._extract_token_usage(run.get("payload"))
            token_total += self._extract_token_usage(run.get("result"))

        return self._ok(
            items=items,
            summary={
                "plays": plays,
                "likes": likes,
                "comments": comments,
                "samples": len(items),
                "pass_rate": round(pass_rate, 4),
                "token_usage_total": token_total,
            },
        )

    def dashboard_errors(self, run_id: str | None = None) -> Dict[str, Any]:
        recent = self.db.list_recent_task_runs(limit=300)
        rows = [r for r in recent if r.get("status") == "failed"]
        if run_id:
            rows = [r for r in rows if str(r.get("run_id")) == str(run_id)]
        return self._ok(items=rows[:80], query={"run_id": run_id or ""})

    def get_ip_config(self) -> Dict[str, Any]:
        path = self.ip_config_path if os.path.exists(self.ip_config_path) else self.ip_config_example_path
        if not os.path.exists(path):
            return self._err(PipelineError("CONTENT_NOT_FOUND", "ip-config file not found"))
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        return self._ok(path=path, config=data)

    def save_ip_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        config = payload.get("config")
        if not isinstance(config, dict):
            return self._err(PipelineError("INVALID_PAYLOAD", "config must be object"))

        video_api = config.get("videoApi")
        if isinstance(video_api, dict):
            raw_key = str(video_api.get("apiKey") or "").strip()
            if raw_key:
                return self._err(
                    PipelineError(
                        "INVALID_PAYLOAD",
                        "videoApi.apiKey is not allowed in file, use videoApi.apiKeyEnv",
                    )
                )
            video_api.pop("apiKey", None)
            if not str(video_api.get("apiKeyEnv") or "").strip():
                video_api["apiKeyEnv"] = "KLING_API_KEY"

        image_api = config.get("imageApi")
        if isinstance(image_api, dict):
            raw_key = str(image_api.get("apiKey") or "").strip()
            if raw_key:
                return self._err(
                    PipelineError(
                        "INVALID_PAYLOAD",
                        "imageApi.apiKey is not allowed in file, use imageApi.apiKeyEnv",
                    )
                )
            image_api.pop("apiKey", None)
            if not str(image_api.get("apiKeyEnv") or "").strip():
                image_api["apiKeyEnv"] = "IMAGE_API_KEY"
            if not str(image_api.get("baseUrl") or "").strip():
                image_api["baseUrl"] = "https://your-image-api.example.com"

        with open(self.ip_config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        return self._ok(path=self.ip_config_path)

    def get_soul(self) -> Dict[str, Any]:
        if not os.path.exists(self.soul_path):
            return self._ok(path=self.soul_path, content="")
        with open(self.soul_path, "r", encoding="utf-8-sig") as f:
            content = f.read()
        return self._ok(path=self.soul_path, content=content)

    def save_soul(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        content = payload.get("content")
        if not isinstance(content, str):
            return self._err(PipelineError("INVALID_PAYLOAD", "content must be string"))
        os.makedirs(os.path.dirname(self.soul_path), exist_ok=True)
        with open(self.soul_path, "w", encoding="utf-8") as f:
            f.write(content)
        return self._ok(path=self.soul_path)

    def get_runtime_config(self) -> Dict[str, Any]:
        config = self._default_runtime_config()
        if os.path.exists(self.runtime_config_path):
            with open(self.runtime_config_path, "r", encoding="utf-8-sig") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                for key, value in loaded.items():
                    if isinstance(config.get(key), dict) and isinstance(value, dict):
                        config[key].update(value)
                    else:
                        config[key] = value
        return self._ok(path=self.runtime_config_path, config=config)

    def save_runtime_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        incoming = payload.get("config")
        if not isinstance(incoming, dict):
            return self._err(PipelineError("INVALID_PAYLOAD", "config must be object"))

        config = self._default_runtime_config()
        for key, value in incoming.items():
            if isinstance(config.get(key), dict) and isinstance(value, dict):
                config[key].update(value)
            else:
                config[key] = value

        schedule = config.get("publish_schedule")
        if not isinstance(schedule, dict):
            return self._err(PipelineError("INVALID_PAYLOAD", "publish_schedule must be object"))

        try:
            daily_limit = int(schedule.get("daily_limit", 1))
        except Exception:
            return self._err(PipelineError("INVALID_PAYLOAD", "publish_schedule.daily_limit must be integer"))
        if daily_limit <= 0:
            return self._err(PipelineError("INVALID_PAYLOAD", "publish_schedule.daily_limit must be > 0"))

        raw_slots = schedule.get("slots", [])
        if not isinstance(raw_slots, list):
            return self._err(PipelineError("INVALID_PAYLOAD", "publish_schedule.slots must be list"))
        slots = []
        for raw in raw_slots:
            if not isinstance(raw, str):
                return self._err(PipelineError("INVALID_PAYLOAD", "publish_schedule.slots must be list[str]"))
            text = raw.strip()
            if not re.match(r"^(?:[01]\d|2[0-3]):[0-5]\d$", text):
                return self._err(PipelineError("INVALID_PAYLOAD", f"invalid slot format: {text}"))
            slots.append(text)

        schedule["enabled"] = bool(schedule.get("enabled", False))
        schedule["timezone"] = str(schedule.get("timezone") or "Asia/Shanghai")
        schedule["daily_limit"] = daily_limit
        schedule["slots"] = slots or ["09:30"]

        secret_refs = config.get("secret_refs")
        if not isinstance(secret_refs, dict):
            return self._err(PipelineError("INVALID_PAYLOAD", "secret_refs must be object"))
        env_pattern = re.compile(r"^[A-Z_][A-Z0-9_]*$")
        normalized_refs = {}
        for key, value in secret_refs.items():
            if not isinstance(value, str) or not env_pattern.match(value.strip()):
                return self._err(PipelineError("INVALID_PAYLOAD", f"secret_refs.{key} must be ENV variable name"))
            normalized_refs[str(key)] = value.strip()
        config["secret_refs"] = normalized_refs

        with open(self.runtime_config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        return self._ok(path=self.runtime_config_path)

    def timeline(self, content_id: int) -> Dict[str, Any]:
        content = self.db.get_content(content_id)
        if not content:
            return self._err(PipelineError("CONTENT_NOT_FOUND"))
        return self._ok(content=dict(content), timeline=self.db.get_timeline(content_id))




























