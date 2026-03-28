ERRORS = {
    "INVALID_PAYLOAD": "Payload structure is invalid.",
    "MISSING_REQUIRED_FIELD": "Required field is missing.",
    "INVALID_SCHEMA_VERSION": "Unsupported schema version.",
    "INVALID_FIT_SCORE": "fit_score must be an integer between 0 and 10.",
    "INVALID_PRODUCTION_STATUS": "Production status must be queued/running/completed/failed.",
    "CONTENT_NOT_FOUND": "Content item not found.",
    "INVALID_TRANSITION": "State transition is invalid.",
    "COZE_SIGNATURE_FAILED": "Coze signature verification failed.",
    "COZE_WORKFLOW_NOT_CONFIGURED": "Coze workflow pull configuration is missing.",
    "COZE_WORKFLOW_REQUEST_FAILED": "Coze workflow request failed.",
    "COZE_WORKFLOW_INVALID_OUTPUT": "Coze workflow output is invalid.",
    "CALLBACK_SIGNATURE_FAILED": "Callback signature verification failed.",
    "OPENCLAW_REQUEST_FAILED": "OpenClaw request failed.",
    "KLING_REQUEST_FAILED": "Kling API request failed.",
    "IMAGE_API_NOT_CONFIGURED": "Image API is not configured.",
    "IMAGE_API_FAILED": "Image generation API request failed.",
    "PUBLISH_DISPATCH_FAILED": "Publish webhook dispatch failed.",
    "RETRY_JOB_FAILED": "Retry job execution failed.",
}


class PipelineError(Exception):
    def __init__(self, code: str, message: str | None = None):
        self.code = code
        self.message = message or ERRORS.get(code, code)
        super().__init__(self.message)

    def as_dict(self) -> dict:
        return {"ok": False, "error_code": self.code, "error": self.message}

