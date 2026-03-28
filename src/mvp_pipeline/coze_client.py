import json
import urllib.error
import urllib.request
from typing import Any, Dict

from .errors import PipelineError


class CozeWorkflowClient:
    def __init__(
        self,
        *,
        run_url: str,
        graph_parameter_url: str,
        token: str,
        timeout_seconds: int,
    ) -> None:
        self.run_url = (run_url or "").strip()
        self.graph_parameter_url = (graph_parameter_url or "").strip()
        self.token = (token or "").strip()
        self.timeout_seconds = timeout_seconds

        if not self.run_url and self.graph_parameter_url.endswith("/graph_parameter"):
            self.run_url = self.graph_parameter_url[: -len("/graph_parameter")] + "/run"

    @property
    def enabled(self) -> bool:
        return bool(self.token and (self.run_url or self.graph_parameter_url))

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, *, method: str, url: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if not self.token:
            raise PipelineError("COZE_WORKFLOW_NOT_CONFIGURED", "COZE_WORKFLOW_TOKEN is required")
        if not url:
            raise PipelineError("COZE_WORKFLOW_NOT_CONFIGURED", "coze workflow url is not configured")

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        for k, v in self._headers().items():
            req.add_header(k, v)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8") if exc.fp else str(exc)
            raise PipelineError("COZE_WORKFLOW_REQUEST_FAILED", detail) from exc
        except Exception as exc:
            raise PipelineError("COZE_WORKFLOW_REQUEST_FAILED", str(exc)) from exc

        if not body:
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise PipelineError("COZE_WORKFLOW_INVALID_OUTPUT", "coze response is not valid json") from exc

    def graph_parameter(self) -> Dict[str, Any]:
        return self._request(method="GET", url=self.graph_parameter_url)

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(method="POST", url=self.run_url, payload=inputs)
