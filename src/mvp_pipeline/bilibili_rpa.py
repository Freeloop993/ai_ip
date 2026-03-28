import argparse
import base64
import os
import re
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .errors import PipelineError

BILIBILI_UPLOAD_URL = "https://member.bilibili.com/platform/upload/video/frame"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class BilibiliRpaAutomator:
    def __init__(
        self,
        *,
        profile_dir: str,
        profile_name: str,
        headless: bool,
        wait_login_seconds: int,
        publish_timeout_seconds: int,
        chromium_executable: str,
        channel: str,
    ) -> None:
        self.profile_dir = os.path.abspath(profile_dir)
        self.profile_name = profile_name.strip() or "default"
        self.headless = headless
        self.wait_login_seconds = wait_login_seconds
        self.publish_timeout_seconds = publish_timeout_seconds
        self.chromium_executable = chromium_executable.strip()
        self.channel = channel.strip() or "chromium"

        os.makedirs(self.profile_dir, exist_ok=True)
        self.storage_state_path = os.path.join(self.profile_dir, f"{self.profile_name}.json")

    def _load_playwright(self):
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # pragma: no cover
            raise PipelineError(
                "PUBLISH_DISPATCH_FAILED",
                "playwright is required for bilibili_rpa (pip install playwright && playwright install chromium)",
            ) from exc
        return sync_playwright

    def _launch_browser(self, playwright):
        launch_kwargs: Dict[str, Any] = {
            "headless": self.headless,
            "channel": self.channel,
        }
        if self.chromium_executable:
            launch_kwargs["executable_path"] = self.chromium_executable
        return playwright.chromium.launch(**launch_kwargs)

    def _new_context(self, browser):
        kwargs: Dict[str, Any] = {
            "viewport": {"width": 1440, "height": 960},
            "locale": "zh-CN",
            "timezone_id": "Asia/Shanghai",
        }
        if os.path.exists(self.storage_state_path):
            kwargs["storage_state"] = self.storage_state_path
        return browser.new_context(**kwargs)

    def _is_login_required(self, page) -> bool:
        url = page.url or ""
        if "passport.bilibili.com" in url or "passport.bilibili" in url:
            return True
        markers = [
            "text=扫码登录",
            "text=登录",
            "input[placeholder*='手机号']",
            "input[placeholder*='账号']",
        ]
        for selector in markers:
            try:
                if page.locator(selector).count() > 0:
                    return True
            except Exception:
                continue
        return False

    def _capture_qr_base64(self, page) -> str:
        selectors = [
            ".qrcode img",
            "img[src*='qrcode']",
            "canvas",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() > 0:
                    data = locator.screenshot(type="png")
                    return base64.b64encode(data).decode("utf-8")
            except Exception:
                continue

        # fallback: full page screenshot
        data = page.screenshot(full_page=True, type="png")
        return base64.b64encode(data).decode("utf-8")

    def start_login_session(self) -> Dict[str, Any]:
        sync_playwright = self._load_playwright()
        playwright = sync_playwright().start()
        browser = self._launch_browser(playwright)
        context = self._new_context(browser)
        page = context.new_page()
        page.goto(BILIBILI_UPLOAD_URL, wait_until="domcontentloaded", timeout=60000)

        if not self._is_login_required(page):
            context.storage_state(path=self.storage_state_path)
            browser.close()
            playwright.stop()
            return {
                "already_logged_in": True,
                "storage_state": self.storage_state_path,
            }

        qr_base64 = self._capture_qr_base64(page)
        return {
            "already_logged_in": False,
            "qr_image_base64": qr_base64,
            "playwright": playwright,
            "browser": browser,
            "context": context,
            "page": page,
            "storage_state": self.storage_state_path,
        }

    def wait_for_login_session(self, runtime: Dict[str, Any], *, timeout_seconds: int) -> Dict[str, Any]:
        playwright = runtime["playwright"]
        browser = runtime["browser"]
        context = runtime["context"]
        page = runtime["page"]
        deadline = time.time() + timeout_seconds

        try:
            while time.time() < deadline:
                page.goto(BILIBILI_UPLOAD_URL, wait_until="domcontentloaded", timeout=60000)
                if not self._is_login_required(page):
                    context.storage_state(path=self.storage_state_path)
                    return {
                        "status": "success",
                        "storage_state": self.storage_state_path,
                        "url": page.url,
                        "updated_at": _iso_now(),
                    }
                time.sleep(2)
            return {
                "status": "timeout",
                "error": f"QR login timeout after {timeout_seconds}s",
                "updated_at": _iso_now(),
            }
        finally:
            try:
                browser.close()
            except Exception:
                pass
            try:
                playwright.stop()
            except Exception:
                pass

    def _choose_locator(self, page, selectors: list[str]):
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if locator.count() > 0:
                    return locator
            except Exception:
                continue
        return None

    def _set_input_files(self, page, video_path: str) -> None:
        selectors = [
            "input[type='file']",
            "input[name='file']",
            "input.accept-upload",
        ]
        locator = self._choose_locator(page, selectors)
        if not locator:
            raise PipelineError("PUBLISH_DISPATCH_FAILED", "cannot find upload file input")
        locator.set_input_files(video_path)

    def _fill_text(self, page, selectors: list[str], value: str) -> bool:
        if not value:
            return False
        locator = self._choose_locator(page, selectors)
        if not locator:
            return False
        locator.click()
        locator.fill(value)
        return True

    def _click_publish(self, page) -> None:
        selectors = [
            "button:has-text('立即投稿')",
            "button:has-text('发布')",
            "button:has-text('投稿')",
            "button:has-text('确认投稿')",
        ]
        locator = self._choose_locator(page, selectors)
        if not locator:
            raise PipelineError("PUBLISH_DISPATCH_FAILED", "cannot find publish button")
        locator.click()

    def _wait_publish_result(self, page):
        deadline = time.time() + self.publish_timeout_seconds
        success_markers = ["投稿成功", "发布成功", "已投稿"]
        while time.time() < deadline:
            current = page.url or ""
            for text in success_markers:
                try:
                    if page.locator(f"text={text}").count() > 0:
                        return current
                except Exception:
                    continue
            if any(key in current for key in ["success", "archive", "manager"]):
                return current
            time.sleep(2)
        raise PipelineError(
            "PUBLISH_DISPATCH_FAILED",
            f"publish did not reach success state in {self.publish_timeout_seconds}s",
        )

    def _download_remote_video(self, url: str) -> tuple[str, bool]:
        fd, path = tempfile.mkstemp(prefix="bili_upload_", suffix=".mp4")
        os.close(fd)
        try:
            with urllib.request.urlopen(url, timeout=120) as resp, open(path, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
        except Exception as exc:
            if os.path.exists(path):
                os.remove(path)
            raise PipelineError("PUBLISH_DISPATCH_FAILED", f"failed to download video_url: {exc}") from exc
        return path, True

    def publish(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        video_path = str(payload.get("video_file") or payload.get("video_path") or "").strip()
        remote_url = str(payload.get("video_url") or "").strip()
        cleanup_tmp = False

        if not video_path and remote_url.startswith("http"):
            video_path, cleanup_tmp = self._download_remote_video(remote_url)

        if not video_path:
            raise PipelineError("PUBLISH_DISPATCH_FAILED", "video_file/video_path or downloadable video_url is required")
        if not os.path.exists(video_path):
            raise PipelineError("PUBLISH_DISPATCH_FAILED", f"video file not found: {video_path}")

        title = str(payload.get("title") or payload.get("script") or "AI IP 自动投稿").strip()
        if len(title) > 80:
            title = title[:80]
        description = str(payload.get("description") or payload.get("script") or "").strip()
        tags = payload.get("tags") or []
        if isinstance(tags, str):
            tags = [x.strip() for x in tags.split(",") if x.strip()]

        sync_playwright = self._load_playwright()
        try:
            with sync_playwright() as p:
                browser = self._launch_browser(p)
                context = self._new_context(browser)
                page = context.new_page()
                page.goto(BILIBILI_UPLOAD_URL, wait_until="domcontentloaded", timeout=60000)

                if self._is_login_required(page):
                    browser.close()
                    raise PipelineError(
                        "PUBLISH_DISPATCH_FAILED",
                        "bilibili login is required, call /api/publish/accounts/login/start first",
                    )

                self._set_input_files(page, video_path)
                page.wait_for_timeout(2000)

                self._fill_text(
                    page,
                    [
                        "input[placeholder*='标题']",
                        "textarea[placeholder*='标题']",
                        "input[maxlength='80']",
                    ],
                    title,
                )
                self._fill_text(
                    page,
                    [
                        "textarea[placeholder*='简介']",
                        "textarea[placeholder*='描述']",
                        "div[contenteditable='true']",
                    ],
                    description,
                )

                if tags:
                    tag_locator = self._choose_locator(
                        page,
                        [
                            "input[placeholder*='标签']",
                            "input[placeholder*='按回车']",
                        ],
                    )
                    if tag_locator:
                        for tag in tags[:10]:
                            tag_locator.fill(tag)
                            page.keyboard.press("Enter")
                            page.wait_for_timeout(200)

                self._click_publish(page)
                publish_url = self._wait_publish_result(page)
                context.storage_state(path=self.storage_state_path)
                browser.close()

                match = re.search(r"(BV[0-9A-Za-z]+)", publish_url)
                external_id = match.group(1) if match else ""
                return {
                    "ok": True,
                    "publish_url": publish_url,
                    "external_id": external_id,
                    "raw": {"profile": self.profile_name},
                }
        finally:
            if cleanup_tmp and os.path.exists(video_path):
                try:
                    os.remove(video_path)
                except OSError:
                    pass


def _build_automator_from_env(profile: Optional[str]) -> BilibiliRpaAutomator:
    return BilibiliRpaAutomator(
        profile_dir=os.getenv("BILIBILI_PROFILE_DIR", "./data/publish_profiles/bilibili"),
        profile_name=profile or os.getenv("BILIBILI_PROFILE_NAME", "default"),
        headless=os.getenv("BILIBILI_HEADLESS", "false").strip().lower() in {"1", "true", "yes", "on"},
        wait_login_seconds=int(os.getenv("BILIBILI_WAIT_LOGIN_SECONDS", "300")),
        publish_timeout_seconds=int(os.getenv("BILIBILI_PUBLISH_TIMEOUT_SECONDS", "900")),
        chromium_executable=os.getenv("BILIBILI_CHROMIUM_EXECUTABLE", ""),
        channel=os.getenv("BILIBILI_CHANNEL", "chromium"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Bilibili RPA helper")
    sub = parser.add_subparsers(dest="cmd", required=True)

    login_cmd = sub.add_parser("login", help="QR login and persist session")
    login_cmd.add_argument("--profile", default=None)
    login_cmd.add_argument("--wait", type=int, default=None)

    publish_cmd = sub.add_parser("publish", help="Publish with existing session")
    publish_cmd.add_argument("--profile", default=None)
    publish_cmd.add_argument("--video", required=True)
    publish_cmd.add_argument("--title", default="AI IP 自动投稿")
    publish_cmd.add_argument("--description", default="")
    publish_cmd.add_argument("--tags", default="")

    args = parser.parse_args()
    automator = _build_automator_from_env(getattr(args, "profile", None))

    if args.cmd == "login":
        runtime = automator.start_login_session()
        if runtime.get("already_logged_in"):
            print({"ok": True, "status": "already_logged_in", "storage_state": runtime.get("storage_state")})
            return
        out = automator.wait_for_login_session(runtime, timeout_seconds=args.wait or automator.wait_login_seconds)
        print({"ok": out.get("status") == "success", **out})
        return

    if args.cmd == "publish":
        out = automator.publish(
            {
                "video_file": args.video,
                "title": args.title,
                "description": args.description,
                "tags": args.tags,
            }
        )
        print(out)
        return


if __name__ == "__main__":
    main()
