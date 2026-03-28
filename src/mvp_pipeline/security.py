import hashlib
import hmac
from typing import Any, Dict


def _header(headers: Dict[str, Any], name: str) -> str:
    for k, v in headers.items():
        if k.lower() == name.lower():
            return str(v)
    return ""


def verify_coze_signature(
    *,
    headers: Dict[str, Any],
    raw_body: bytes,
    secret: str,
    mode: str,
) -> bool:
    normalized_mode = (mode or "permissive").lower()
    if not secret:
        return normalized_mode != "strict"

    signature = _header(headers, "X-Coze-Signature")
    if not signature:
        return False

    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected)
