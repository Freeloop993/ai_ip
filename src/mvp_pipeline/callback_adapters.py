import hashlib
import hmac
import time
from typing import Any, Dict

APPROVED_WORDS = {"approved", "approve", "pass", "ok", "yes", "true", "1", "✅"}
REWORK_WORDS = {"rework", "reject", "rejected", "fail", "no", "false", "0", "❌"}


def _to_decision(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in APPROVED_WORDS:
        return "approved"
    if text in REWORK_WORDS:
        return "rework"
    return None


def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    return None


def _normalize_common(platform: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    decision = _to_decision(
        _pick(payload, "decision", "result", "status", "action", "approve", "approved")
    )
    content_id = _pick(payload, "content_id", "contentId", "id")
    run_id = _pick(payload, "run_id", "runId")
    publish_platform = _pick(payload, "platform", "publish_platform", "publishPlatform")
    publish_url = _pick(payload, "publish_url", "publishUrl", "url", "link")
    feedback = _pick(payload, "feedback", "reason", "comment", "message")

    if not decision:
        return {"ok": False, "error": "cannot parse decision"}
    if content_id is None:
        return {"ok": False, "error": "cannot parse content_id"}

    return {
        "ok": True,
        "payload": {
            "content_id": int(content_id),
            "run_id": run_id,
            "decision": decision,
            "platform": publish_platform,
            "publish_url": publish_url,
            "feedback": feedback,
            "review_source": platform,
        },
    }


def _normalize_feishu(payload: Dict[str, Any]) -> Dict[str, Any]:
    if "challenge" in payload:
        return {"ok": True, "handshake": True, "body": {"challenge": payload["challenge"]}}

    event = payload.get("event", {}) if isinstance(payload.get("event"), dict) else {}
    action = event.get("action", {}) if isinstance(event.get("action"), dict) else {}
    value = action.get("value", {}) if isinstance(action.get("value"), dict) else {}

    merged = {}
    merged.update(payload)
    merged.update(event)
    merged.update(action)
    merged.update(value)
    return _normalize_common("feishu", merged)


def _normalize_slack(payload: Dict[str, Any]) -> Dict[str, Any]:
    merged = {}
    merged.update(payload)
    if isinstance(payload.get("actions"), list) and payload["actions"]:
        first = payload["actions"][0]
        if isinstance(first, dict):
            merged.update(first)
            if isinstance(first.get("value"), dict):
                merged.update(first["value"])
    return _normalize_common("slack", merged)


def _normalize_discord(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
    merged = {}
    merged.update(payload)
    merged.update(data)
    return _normalize_common("discord", merged)


def _normalize_qq(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
    merged = {}
    merged.update(payload)
    merged.update(data)
    return _normalize_common("qq", merged)


def _normalize_wecom(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
    merged = {}
    merged.update(payload)
    merged.update(data)
    return _normalize_common("wecom", merged)


def normalize_review_callback(
    platform: str,
    payload: Dict[str, Any],
    headers: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    _ = headers
    p = (platform or "").lower()
    if p == "feishu":
        return _normalize_feishu(payload)
    if p == "slack":
        return _normalize_slack(payload)
    if p == "discord":
        return _normalize_discord(payload)
    if p == "qq":
        return _normalize_qq(payload)
    if p == "wecom":
        return _normalize_wecom(payload)
    return _normalize_common(p or "external-callback", payload)


def _header(headers: Dict[str, Any], name: str) -> str:
    for k, v in headers.items():
        if k.lower() == name.lower():
            return str(v)
    return ""


def _q(query_params: Dict[str, Any], key: str) -> str:
    value = query_params.get(key)
    if isinstance(value, list):
        return str(value[0]) if value else ""
    if value is None:
        return ""
    return str(value)


def _verify_shared_hmac(headers: Dict[str, Any], body: bytes, secret: str) -> bool:
    sig = _header(headers, "X-Callback-Signature")
    if not sig or not secret:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, expected)


def _verify_slack(headers: Dict[str, Any], body: bytes, secret: str) -> bool:
    ts = _header(headers, "X-Slack-Request-Timestamp")
    sig = _header(headers, "X-Slack-Signature")
    if not ts or not sig or not secret:
        return False
    try:
        age = abs(int(time.time()) - int(ts))
    except Exception:
        return False
    if age > 300:
        return False
    base = f"v0:{ts}:{body.decode('utf-8')}"
    expected = "v0=" + hmac.new(
        secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(sig, expected)


def _verify_feishu_signature(
    headers: Dict[str, Any], body: bytes, encrypt_key: str
) -> bool:
    ts = _header(headers, "X-Lark-Request-Timestamp")
    nonce = _header(headers, "X-Lark-Request-Nonce")
    sig = _header(headers, "X-Lark-Signature")
    if not ts or not nonce or not sig or not encrypt_key:
        return False
    plain = f"{ts}{nonce}{encrypt_key}{body.decode('utf-8')}"
    expected = hashlib.sha256(plain.encode("utf-8")).hexdigest()
    return hmac.compare_digest(sig, expected)


def _verify_wecom_signature(
    payload: Dict[str, Any], query_params: Dict[str, Any], token: str
) -> bool:
    if not token:
        return False
    msg_signature = _q(query_params, "msg_signature")
    timestamp = _q(query_params, "timestamp")
    nonce = _q(query_params, "nonce")
    encrypt = _pick(payload, "encrypt", "Encrypt")
    if not msg_signature or not timestamp or not nonce or not encrypt:
        return False
    pieces = [token, timestamp, nonce, str(encrypt)]
    pieces.sort()
    expected = hashlib.sha1("".join(pieces).encode("utf-8")).hexdigest()
    return hmac.compare_digest(msg_signature, expected)


def _verify_token_field(payload: Dict[str, Any], token: str) -> bool:
    if not token:
        return False
    candidates = [
        payload.get("token"),
        payload.get("verification_token"),
        (payload.get("header") or {}).get("token")
        if isinstance(payload.get("header"), dict)
        else None,
        (payload.get("event") or {}).get("token")
        if isinstance(payload.get("event"), dict)
        else None,
    ]
    return any(str(x) == token for x in candidates if x is not None)


def verify_callback_request(
    *,
    platform: str,
    payload: Dict[str, Any],
    raw_body: bytes,
    headers: Dict[str, Any],
    query_params: Dict[str, Any],
    verify_mode: str,
    shared_secret: str,
    platform_tokens: Dict[str, str],
    feishu_encrypt_key: str,
) -> Dict[str, Any]:
    p = (platform or "").lower()
    mode = (verify_mode or "permissive").lower()
    if mode not in {"permissive", "strict"}:
        mode = "permissive"

    if p == "slack" and platform_tokens.get("slack"):
        ok = _verify_slack(headers, raw_body, platform_tokens["slack"])
        return {"ok": ok, "reason": "slack-signature"}

    if p == "feishu" and feishu_encrypt_key:
        ok = _verify_feishu_signature(headers, raw_body, feishu_encrypt_key)
        return {"ok": ok, "reason": "feishu-signature"}

    if p == "wecom" and platform_tokens.get("wecom"):
        ok = _verify_wecom_signature(payload, query_params, platform_tokens["wecom"])
        return {"ok": ok, "reason": "wecom-msg-signature"}

    token = platform_tokens.get(p, "")
    if token:
        ok = _verify_token_field(payload, token)
        return {"ok": ok, "reason": f"{p}-token"}

    if shared_secret:
        ok = _verify_shared_hmac(headers, raw_body, shared_secret)
        return {"ok": ok, "reason": "shared-hmac"}

    if mode == "strict":
        return {"ok": False, "reason": "no-verification-configured"}
    return {"ok": True, "reason": "permissive-no-secret"}
