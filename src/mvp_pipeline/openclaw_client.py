import json
import urllib.error
import urllib.request
from typing import Any, Dict

from .errors import PipelineError


class OpenClawClient:
    def __init__(
        self,
        *,
        enabled: bool,
        base_url: str,
        api_key: str,
        inject_path: str,
        spawn_path: str,
    ) -> None:
        self.enabled = enabled and bool(base_url)
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.inject_path = inject_path
        self.spawn_path = spawn_path

    def _request(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            raise PipelineError("OPENCLAW_REQUEST_FAILED", "openclaw is disabled")
        url = f"{self.base_url}{path}"
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.api_key:
            req.add_header("Authorization", f"Bearer {self.api_key}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8") if exc.fp else str(exc)
            raise PipelineError("OPENCLAW_REQUEST_FAILED", detail) from exc
        except Exception as exc:
            raise PipelineError("OPENCLAW_REQUEST_FAILED", str(exc)) from exc

        if not body:
            return {"ok": True}
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"ok": True, "raw": body}

    def inject_system_event(self, *, agent_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = {"agentId": agent_id, "event": event}
        return self._request(self.inject_path, payload)

    def sessions_spawn(
        self,
        *,
        parent_agent_id: str,
        agent_id: str,
        task: str,
        run_timeout_seconds: int,
    ) -> Dict[str, Any]:
        payload = {
            "parentAgentId": parent_agent_id,
            "agentId": agent_id,
            "mode": "run",
            "runtime": "subagent",
            "runTimeoutSeconds": run_timeout_seconds,
            "task": task,
        }
        return self._request(self.spawn_path, payload)
