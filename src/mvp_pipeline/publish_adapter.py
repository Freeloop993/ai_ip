import json
import os
import threading
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from .config import Settings
from .errors import PipelineError


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class WebhookPublisher:
    def __init__(self, *, webhook_url: str, token: str) -> None:
        self.webhook_url = webhook_url
        self.token = token

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def publish(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            raise PipelineError("PUBLISH_DISPATCH_FAILED", "PUBLISH_WEBHOOK_URL is not configured")

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(self.webhook_url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8") if exc.fp else str(exc)
            raise PipelineError("PUBLISH_DISPATCH_FAILED", detail) from exc
        except Exception as exc:
            raise PipelineError("PUBLISH_DISPATCH_FAILED", str(exc)) from exc

        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError:
            parsed = {}

        return {
            "ok": True,
            "publish_url": parsed.get("publish_url") or parsed.get("url") or "",
            "external_id": parsed.get("external_id") or parsed.get("id") or "",
            "raw": parsed,
        }


class BilibiliRpaPublisher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.Lock()
        self._login_sessions: Dict[str, Dict[str, Any]] = {}

    @property
    def enabled(self) -> bool:
        return True

    def _build_automator(self, profile_name: str | None = None):
        from .bilibili_rpa import BilibiliRpaAutomator

        return BilibiliRpaAutomator(
            profile_dir=self.settings.bilibili_profile_dir,
            profile_name=profile_name or self.settings.bilibili_profile_name,
            headless=self.settings.bilibili_headless,
            wait_login_seconds=self.settings.bilibili_wait_login_seconds,
            publish_timeout_seconds=self.settings.bilibili_publish_timeout_seconds,
            chromium_executable=self.settings.bilibili_chromium_executable,
            channel=self.settings.bilibili_channel,
        )

    def _session_public(self, session: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "session_id": session["session_id"],
            "profile": session["profile"],
            "status": session["status"],
            "created_at": session["created_at"],
            "updated_at": session["updated_at"],
            "error": session.get("error") or "",
            "storage_state": session.get("storage_state") or "",
            "qr_image_base64": session.get("qr_image_base64") or "",
            "url": session.get("url") or "",
        }

    def _wait_login_worker(self, session_id: str, automator, runtime: Dict[str, Any], timeout_seconds: int) -> None:
        out = automator.wait_for_login_session(runtime, timeout_seconds=timeout_seconds)
        with self._lock:
            session = self._login_sessions.get(session_id)
            if not session:
                return
            session["status"] = out.get("status", "failed")
            session["updated_at"] = _iso_now()
            session["error"] = out.get("error") or ""
            session["url"] = out.get("url") or ""
            if out.get("status") == "success":
                session["storage_state"] = out.get("storage_state") or session.get("storage_state")
            # QR only needed in pending state
            session["qr_image_base64"] = ""

    def prepare_login(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = payload or {}
        profile = str(payload.get("profile") or self.settings.bilibili_profile_name).strip() or "default"
        wait_seconds = int(payload.get("wait_seconds") or self.settings.bilibili_wait_login_seconds)

        automator = self._build_automator(profile)
        runtime = automator.start_login_session()
        if runtime.get("already_logged_in"):
            return {
                "ok": True,
                "session_id": "",
                "profile": profile,
                "status": "success",
                "storage_state": runtime.get("storage_state") or automator.storage_state_path,
                "already_logged_in": True,
                "qr_image_base64": "",
            }

        session_id = str(uuid.uuid4())
        session = {
            "session_id": session_id,
            "profile": profile,
            "status": "pending",
            "created_at": _iso_now(),
            "updated_at": _iso_now(),
            "error": "",
            "storage_state": runtime.get("storage_state") or automator.storage_state_path,
            "qr_image_base64": runtime.get("qr_image_base64") or "",
            "url": "",
        }
        with self._lock:
            self._login_sessions[session_id] = session

        thread = threading.Thread(
            target=self._wait_login_worker,
            args=(session_id, automator, runtime, wait_seconds),
            daemon=True,
        )
        thread.start()

        return {"ok": True, **self._session_public(session)}

    def login_status(self, session_id: str) -> Dict[str, Any]:
        with self._lock:
            session = self._login_sessions.get(session_id)
            if not session:
                raise PipelineError("CONTENT_NOT_FOUND", "login session not found")
            return {"ok": True, **self._session_public(session)}

    def list_accounts(self) -> Dict[str, Any]:
        os.makedirs(self.settings.bilibili_profile_dir, exist_ok=True)
        items = []
        for name in os.listdir(self.settings.bilibili_profile_dir):
            if not name.endswith(".json"):
                continue
            path = os.path.join(self.settings.bilibili_profile_dir, name)
            profile = name[:-5]
            stat = os.stat(path)
            items.append(
                {
                    "profile": profile,
                    "status": "authorized",
                    "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "storage_state": path,
                }
            )

        with self._lock:
            for session in self._login_sessions.values():
                if session.get("status") == "pending":
                    items.append(
                        {
                            "profile": session.get("profile"),
                            "status": "pending",
                            "updated_at": session.get("updated_at"),
                            "storage_state": session.get("storage_state"),
                            "session_id": session.get("session_id"),
                        }
                    )

        items.sort(key=lambda x: (x.get("updated_at") or "", x.get("profile") or ""), reverse=True)
        return {"ok": True, "items": items}

    def remove_account(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        profile = str(payload.get("profile") or "").strip()
        if not profile:
            raise PipelineError("INVALID_PAYLOAD", "profile is required")
        path = os.path.join(self.settings.bilibili_profile_dir, f"{profile}.json")
        if os.path.exists(path):
            os.remove(path)

        with self._lock:
            stale = [sid for sid, s in self._login_sessions.items() if s.get("profile") == profile]
            for sid in stale:
                self._login_sessions.pop(sid, None)

        return {"ok": True, "profile": profile}

    def publish(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        profile = str(payload.get("profile") or self.settings.bilibili_profile_name)
        video_path = str(payload.get("video_path") or payload.get("video_file") or "").strip()
        if not video_path:
            candidate = str(payload.get("local_video_path") or "").strip()
            if candidate and os.path.exists(candidate):
                payload = {**payload, "video_file": candidate}

        automator = self._build_automator(profile)
        return automator.publish(payload)


def create_publisher(settings: Settings):
    provider = (settings.publish_provider or "webhook").strip().lower()
    if provider == "bilibili_rpa":
        return BilibiliRpaPublisher(settings)
    return WebhookPublisher(
        webhook_url=settings.publish_webhook_url,
        token=settings.publish_webhook_token,
    )
