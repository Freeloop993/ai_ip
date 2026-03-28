import hashlib
import hmac
import unittest

from mvp_pipeline.security import verify_coze_signature


class SecurityTests(unittest.TestCase):
    def test_permissive_without_secret(self):
        ok = verify_coze_signature(headers={}, raw_body=b"{}", secret="", mode="permissive")
        self.assertTrue(ok)

    def test_strict_without_secret(self):
        ok = verify_coze_signature(headers={}, raw_body=b"{}", secret="", mode="strict")
        self.assertFalse(ok)

    def test_valid_signature(self):
        body = b'{"event_id":"evt"}'
        secret = "coze_secret"
        sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        ok = verify_coze_signature(
            headers={"X-Coze-Signature": sig},
            raw_body=body,
            secret=secret,
            mode="strict",
        )
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
