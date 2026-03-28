from typing import Any, Dict

from .errors import PipelineError

ANALYSIS_SCHEMA_VERSION = "1.0"
PRODUCTION_SCHEMA_VERSION = "1.0"
PRODUCTION_STATUSES = {"queued", "running", "completed", "failed"}


def validate_coze_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    required = ["event_id", "source", "video_url", "platform", "author", "collected_at"]
    missing = [k for k in required if not payload.get(k)]
    if missing:
        raise PipelineError("MISSING_REQUIRED_FIELD", f"missing fields: {', '.join(missing)}")

    if not payload.get("video_id"):
        payload = dict(payload)
        payload["video_id"] = payload["video_url"].rstrip("/").split("/")[-1]
    return payload


def validate_analysis_result(result: Dict[str, Any]) -> Dict[str, Any]:
    required = [
        "schema_version",
        "topic",
        "hook",
        "structure",
        "hashtags",
        "fit_score",
        "fit_reason",
        "replicate",
    ]
    missing = [k for k in required if k not in result]
    if missing:
        raise PipelineError("MISSING_REQUIRED_FIELD", f"analysis missing fields: {', '.join(missing)}")

    if str(result.get("schema_version")) != ANALYSIS_SCHEMA_VERSION:
        raise PipelineError("INVALID_SCHEMA_VERSION", "analysis schema_version must be 1.0")

    fit_score = result.get("fit_score")
    if not isinstance(fit_score, int) or fit_score < 0 or fit_score > 10:
        raise PipelineError("INVALID_FIT_SCORE")

    hashtags = result.get("hashtags")
    if not isinstance(hashtags, list):
        raise PipelineError("INVALID_PAYLOAD", "hashtags must be list")

    replicate = result.get("replicate")
    if not isinstance(replicate, bool):
        raise PipelineError("INVALID_PAYLOAD", "replicate must be boolean")

    return result


def validate_production_result(result: Dict[str, Any]) -> Dict[str, Any]:
    required = [
        "schema_version",
        "provider",
        "task_id",
        "status",
        "video_url",
        "script",
        "tts_text",
    ]
    missing = [k for k in required if k not in result]
    if missing:
        raise PipelineError("MISSING_REQUIRED_FIELD", f"production missing fields: {', '.join(missing)}")

    if str(result.get("schema_version")) != PRODUCTION_SCHEMA_VERSION:
        raise PipelineError("INVALID_SCHEMA_VERSION", "production schema_version must be 1.0")

    status = str(result.get("status"))
    if status not in PRODUCTION_STATUSES:
        raise PipelineError("INVALID_PRODUCTION_STATUS")

    return result
