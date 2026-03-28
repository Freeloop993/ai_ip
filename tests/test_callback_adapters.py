import hashlib
import hmac
import json
import time
import unittest

from mvp_pipeline.callback_adapters import normalize_review_callback, verify_callback_request


class CallbackAdapterTests(unittest.TestCase):
    def test_feishu_handshake(self):
        out = normalize_review_callback("feishu", {"challenge": "abc"}, {})
        self.assertTrue(out["handshake"])
        self.assertEqual(out["body"]["challenge"], "abc")

    def test_feishu_card_action(self):
        payload = {
            "event": {
                "action": {
                    "value": {
                        "content_id": 10,
                        "decision": "approved",
                        "platform": "douyin",
                        "publish_url": "https://p",
                    }
                }
            }
        }
        out = normalize_review_callback("feishu", payload, {})
        self.assertTrue(out["ok"])
        self.assertEqual(out["payload"]["review_source"], "feishu")
        self.assertEqual(out["payload"]["decision"], "approved")

    def test_slack_action(self):
        payload = {
            "actions": [
                {
                    "value": {
                        "content_id": 9,
                        "decision": "rework",
                        "feedback": "retry",
                    }
                }
            ]
        }
        out = normalize_review_callback("slack", payload, {})
        self.assertTrue(out["ok"])
        self.assertEqual(out["payload"]["decision"], "rework")
        self.assertEqual(out["payload"]["review_source"], "slack")

    def test_qq_data(self):
        payload = {
            "data": {
                "content_id": 8,
                "decision": "approved",
                "platform": "douyin",
            }
        }
        out = normalize_review_callback("qq", payload, {})
        self.assertTrue(out["ok"])
        self.assertEqual(out["payload"]["review_source"], "qq")

    def test_verify_slack_signature(self):
        body_dict = {"content_id": 1, "decision": "approved"}
        raw = json.dumps(body_dict).encode("utf-8")
        ts = str(int(time.time()))
        base = f"v0:{ts}:{raw.decode('utf-8')}"
        secret = "slack-secret"
        sig = "v0=" + hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {
            "X-Slack-Request-Timestamp": ts,
            "X-Slack-Signature": sig,
        }
        out = verify_callback_request(
            platform="slack",
            payload=body_dict,
            raw_body=raw,
            headers=headers,
            query_params={},
            verify_mode="strict",
            shared_secret="",
            platform_tokens={"slack": secret},
            feishu_encrypt_key="",
        )
        self.assertTrue(out["ok"])

    def test_verify_feishu_signature(self):
        body_dict = {"event": {"action": {"value": {"content_id": 1, "decision": "approved"}}}}
        raw = json.dumps(body_dict).encode("utf-8")
        ts = str(int(time.time()))
        nonce = "nonce123"
        encrypt_key = "lark_encrypt_key"
        plain = f"{ts}{nonce}{encrypt_key}{raw.decode('utf-8')}"
        sig = hashlib.sha256(plain.encode("utf-8")).hexdigest()
        headers = {
            "X-Lark-Request-Timestamp": ts,
            "X-Lark-Request-Nonce": nonce,
            "X-Lark-Signature": sig,
        }
        out = verify_callback_request(
            platform="feishu",
            payload=body_dict,
            raw_body=raw,
            headers=headers,
            query_params={},
            verify_mode="strict",
            shared_secret="",
            platform_tokens={},
            feishu_encrypt_key=encrypt_key,
        )
        self.assertTrue(out["ok"])

    def test_verify_wecom_signature(self):
        payload = {"encrypt": "encrypted_msg"}
        token = "wecom_token"
        timestamp = "1711617600"
        nonce = "abc123"
        pieces = [token, timestamp, nonce, payload["encrypt"]]
        pieces.sort()
        msg_signature = hashlib.sha1("".join(pieces).encode("utf-8")).hexdigest()
        out = verify_callback_request(
            platform="wecom",
            payload=payload,
            raw_body=json.dumps(payload).encode("utf-8"),
            headers={},
            query_params={
                "msg_signature": msg_signature,
                "timestamp": timestamp,
                "nonce": nonce,
            },
            verify_mode="strict",
            shared_secret="",
            platform_tokens={"wecom": token},
            feishu_encrypt_key="",
        )
        self.assertTrue(out["ok"])

    def test_verify_token_field(self):
        payload = {"token": "abc", "content_id": 1, "decision": "approved"}
        out = verify_callback_request(
            platform="feishu",
            payload=payload,
            raw_body=json.dumps(payload).encode("utf-8"),
            headers={},
            query_params={},
            verify_mode="strict",
            shared_secret="",
            platform_tokens={"feishu": "abc"},
            feishu_encrypt_key="",
        )
        self.assertTrue(out["ok"])

    def test_verify_strict_fail_without_secret(self):
        payload = {"content_id": 1, "decision": "approved"}
        out = verify_callback_request(
            platform="discord",
            payload=payload,
            raw_body=json.dumps(payload).encode("utf-8"),
            headers={},
            query_params={},
            verify_mode="strict",
            shared_secret="",
            platform_tokens={},
            feishu_encrypt_key="",
        )
        self.assertFalse(out["ok"])


if __name__ == "__main__":
    unittest.main()
