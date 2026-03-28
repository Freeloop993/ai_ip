import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict

from .errors import PipelineError


STATUS_MAP = {
    "queued": "queued",
    "pending": "queued",
    "running": "running",
    "processing": "running",
    "completed": "completed",
    "succeeded": "completed",
    "success": "completed",
    "failed": "failed",
    "error": "failed",
    "canceled": "failed",
}


class KlingClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        poll_interval_seconds: int,
        timeout_seconds: int,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def _request(self, method: str, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if not self.enabled:
            raise PipelineError("KLING_REQUEST_FAILED", "KLING_API_KEY is not configured")
        url = f"{self.base_url}{path}"
        data = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {self.api_key}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8") if exc.fp else str(exc)
            raise PipelineError("KLING_REQUEST_FAILED", detail) from exc
        except Exception as exc:
            raise PipelineError("KLING_REQUEST_FAILED", str(exc)) from exc

        try:
            return json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise PipelineError("KLING_REQUEST_FAILED", f"invalid json response: {body}") from exc

    def create_motion_transfer(self, *, image_url: str, reference_video_url: str, prompt: str = "") -> str:
        payload = {
            "image_url": image_url,
            "reference_video_url": reference_video_url,
            "prompt": prompt,
        }
        data = self._request("POST", "/v1/videos/motion-transfer", payload)
        task_id = (
            data.get("taskId")
            or data.get("task_id")
            or (data.get("data") or {}).get("taskId")
            or (data.get("data") or {}).get("task_id")
        )
        if not task_id:
            raise PipelineError("KLING_REQUEST_FAILED", f"task id missing from response: {data}")
        return str(task_id)

    def get_task(self, task_id: str) -> Dict[str, Any]:
        data = self._request("GET", f"/v1/tasks/{task_id}", None)
        raw_status = (
            data.get("status")
            or (data.get("data") or {}).get("status")
            or "queued"
        )
        status = STATUS_MAP.get(str(raw_status).lower(), "running")
        video_url = (
            data.get("videoUrl")
            or data.get("video_url")
            or (data.get("data") or {}).get("videoUrl")
            or (data.get("data") or {}).get("video_url")
            or ""
        )
        error_msg = (
            data.get("error")
            or (data.get("data") or {}).get("error")
            or ""
        )
        return {
            "status": status,
            "video_url": video_url,
            "error": error_msg,
            "raw": data,
        }

    def wait_for_completion(self, task_id: str) -> Dict[str, Any]:
        deadline = time.time() + self.timeout_seconds
        last = {"status": "queued", "video_url": "", "error": ""}
        while time.time() < deadline:
            last = self.get_task(task_id)
            if last["status"] in {"completed", "failed"}:
                return last
            time.sleep(self.poll_interval_seconds)
        raise PipelineError("KLING_REQUEST_FAILED", f"task timeout: {task_id}")

