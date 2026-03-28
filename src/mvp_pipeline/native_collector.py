import json
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

from .errors import PipelineError


def _to_count(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if value is None:
        return 0
    text = str(value).strip().lower().replace(",", "")
    if not text:
        return 0
    try:
        return int(float(text))
    except Exception:
        pass
    if text.endswith("\u4e07"):
        try:
            return int(float(text[:-1]) * 10000)
        except Exception:
            return 0
    if text.endswith("\u4ebf"):
        try:
            return int(float(text[:-1]) * 100000000)
        except Exception:
            return 0
    return 0


@dataclass
class CollectedVideo:
    platform: str
    video_id: str
    video_url: str
    author: str
    title: str
    stats: Dict[str, int]
    collected_at: str
    raw: Dict[str, Any]


class NativeCollector:
    def __init__(self, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
        self.supported_platforms = ["bilibili", "xiaohongshu", "douyin", "youtube"]

    def detect_platform(self, profile_url: str) -> str:
        url = str(profile_url or "").strip().lower()
        if "space.bilibili.com/" in url or "bilibili.com/space/" in url:
            return "bilibili"
        if "xiaohongshu.com/user/profile/" in url or "xhslink.com/" in url:
            return "xiaohongshu"
        if "douyin.com/" in url or "iesdouyin.com/" in url:
            return "douyin"
        if "youtube.com/" in url or "youtu.be/" in url:
            return "youtube"
        return "unknown"

    def collect(
        self,
        *,
        profile_url: str,
        platform: str = "",
        cookie: str = "",
        max_videos: int = 10,
    ) -> List[CollectedVideo]:
        resolved = str(platform or "").strip().lower()
        if not resolved:
            resolved = self.detect_platform(profile_url)
        if resolved == "bilibili":
            return self._collect_bilibili(profile_url=profile_url, cookie=cookie, max_videos=max_videos)
        if resolved == "xiaohongshu":
            return self._collect_xiaohongshu(profile_url=profile_url, cookie=cookie, max_videos=max_videos)
        if resolved == "douyin":
            return self._collect_douyin(profile_url=profile_url, cookie=cookie, max_videos=max_videos)
        if resolved == "youtube":
            return self._collect_youtube(profile_url=profile_url, cookie=cookie, max_videos=max_videos)
        raise PipelineError(
            "COLLECTOR_PLATFORM_UNSUPPORTED",
            f"native collector does not support platform: {resolved or 'unknown'}",
        )

    def _request_text(
        self,
        *,
        url: str,
        query: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
    ) -> str:
        full_url = url
        if query:
            full_url = f"{url}?{urllib.parse.urlencode(query)}"
        req = urllib.request.Request(full_url, method="GET")
        req.add_header("User-Agent", self.user_agent)
        req.add_header("Accept", "application/json, text/plain, */*")
        if headers:
            for key, value in headers.items():
                if value:
                    req.add_header(key, value)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                body = resp.read().decode("utf-8", errors="ignore")
            return body
        except Exception as exc:
            raise PipelineError("COLLECTOR_REQUEST_FAILED", f"request failed: {full_url}, error={exc}") from exc

    def _request_json(
        self,
        *,
        url: str,
        query: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        body = self._request_text(url=url, query=query, headers=headers)
        try:
            return json.loads(body)
        except Exception as exc:
            raise PipelineError("COLLECTOR_REQUEST_FAILED", f"response is not json: url={url}") from exc

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _extract_bilibili_uid(self, profile_url: str) -> str:
        text = str(profile_url or "").strip()
        patterns = [
            r"space\.bilibili\.com/(\d+)",
            r"bilibili\.com/space/(\d+)",
        ]
        for pattern in patterns:
            matched = re.search(pattern, text, flags=re.IGNORECASE)
            if matched:
                return matched.group(1)
        raise PipelineError(
            "COLLECTOR_INVALID_PROFILE_URL",
            "bilibili profile_url must look like https://space.bilibili.com/<uid>",
        )

    def _extract_xhs_user_id(self, profile_url: str) -> str:
        text = str(profile_url or "").strip()
        patterns = [
            r"xiaohongshu\.com/user/profile/([^/?#]+)",
            r"xhslink\.com/([^/?#]+)",
        ]
        for pattern in patterns:
            matched = re.search(pattern, text, flags=re.IGNORECASE)
            if matched:
                return matched.group(1)
        raise PipelineError(
            "COLLECTOR_INVALID_PROFILE_URL",
            "xiaohongshu profile_url must look like https://www.xiaohongshu.com/user/profile/<user_id>",
        )

    def _extract_douyin_sec_user_id(self, profile_url: str) -> str:
        text = str(profile_url or "").strip()
        patterns = [
            r"douyin\.com/user/([^/?#]+)",
            r"iesdouyin\.com/share/user/([^/?#]+)",
        ]
        for pattern in patterns:
            matched = re.search(pattern, text, flags=re.IGNORECASE)
            if matched:
                return matched.group(1)
        raise PipelineError(
            "COLLECTOR_INVALID_PROFILE_URL",
            "douyin profile_url must look like https://www.douyin.com/user/<sec_user_id>",
        )

    def _extract_youtube_video_id(self, url: str) -> str:
        text = str(url or "").strip()
        patterns = [
            r"youtube\.com/watch\?v=([^&#]+)",
            r"youtu\.be/([^/?#]+)",
            r"youtube\.com/shorts/([^/?#]+)",
        ]
        for pattern in patterns:
            matched = re.search(pattern, text, flags=re.IGNORECASE)
            if matched:
                return matched.group(1)
        return ""

    def _is_youtube_video_url(self, url: str) -> bool:
        return bool(self._extract_youtube_video_id(url))

    def _extract_youtube_channel_id_from_url(self, profile_url: str) -> str:
        text = str(profile_url or "").strip()
        matched = re.search(r"youtube\.com/channel/([^/?#]+)", text, flags=re.IGNORECASE)
        if matched:
            return matched.group(1)
        return ""

    def _resolve_youtube_channel_id(self, profile_url: str, cookie: str) -> str:
        direct = self._extract_youtube_channel_id_from_url(profile_url)
        if direct:
            return direct

        html = self._request_text(
            url=profile_url,
            headers={
                "Cookie": cookie,
                "Referer": "https://www.youtube.com/",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        patterns = [
            r'"channelId":"(UC[0-9A-Za-z_-]+)"',
            r"https://www\.youtube\.com/channel/(UC[0-9A-Za-z_-]+)",
            r'"externalId":"(UC[0-9A-Za-z_-]+)"',
        ]
        for pattern in patterns:
            matched = re.search(pattern, html)
            if matched:
                return matched.group(1)
        raise PipelineError(
            "COLLECTOR_INVALID_PROFILE_URL",
            "youtube profile_url must resolve to a channel (channel/@handle/c/user)",
        )

    def _collect_bilibili(self, *, profile_url: str, cookie: str, max_videos: int) -> List[CollectedVideo]:
        uid = self._extract_bilibili_uid(profile_url)
        size = max(1, min(int(max_videos or 10), 30))
        data = self._request_json(
            url="https://api.bilibili.com/x/space/arc/search",
            query={"mid": uid, "ps": size, "tid": 0, "pn": 1, "order": "pubdate"},
            headers={"Cookie": cookie, "Referer": "https://www.bilibili.com/"},
        )
        if int(data.get("code", -1)) != 0:
            raise PipelineError(
                "COLLECTOR_REQUEST_FAILED",
                f"bilibili api error code={data.get('code')} message={data.get('message')}",
            )

        items = data.get("data", {}).get("list", {}).get("vlist", []) or []
        now_iso = self._now_iso()
        out: List[CollectedVideo] = []
        for item in items[:size]:
            bvid = str(item.get("bvid") or "").strip()
            aid = str(item.get("aid") or "").strip()
            video_id = bvid or aid
            if not video_id:
                continue
            if bvid:
                video_url = f"https://www.bilibili.com/video/{bvid}"
            else:
                video_url = f"https://www.bilibili.com/video/av{aid}"
            out.append(
                CollectedVideo(
                    platform="bilibili",
                    video_id=video_id,
                    video_url=video_url,
                    author=str(item.get("author") or uid),
                    title=str(item.get("title") or ""),
                    stats={
                        "plays": _to_count(item.get("play")),
                        "likes": _to_count(item.get("like")),
                        "comments": _to_count(item.get("comment")),
                        "shares": _to_count(item.get("share")),
                    },
                    collected_at=now_iso,
                    raw=dict(item),
                )
            )
        return out

    def _collect_xiaohongshu(self, *, profile_url: str, cookie: str, max_videos: int) -> List[CollectedVideo]:
        user_id = self._extract_xhs_user_id(profile_url)
        size = max(1, min(int(max_videos or 10), 30))
        data = self._request_json(
            url="https://edith.xiaohongshu.com/api/sns/web/v1/user_posted",
            query={"user_id": user_id, "cursor_score": "", "page_size": size, "sort": "time_descending"},
            headers={"Cookie": cookie, "Referer": profile_url},
        )
        if not bool(data.get("success")):
            raise PipelineError("COLLECTOR_REQUEST_FAILED", f"xiaohongshu api failed: {data}")

        notes = data.get("data", {}).get("notes", []) or []
        now_iso = self._now_iso()
        out: List[CollectedVideo] = []
        for item in notes[:size]:
            card = item.get("noteCard", {}) or {}
            note_id = str(card.get("noteId") or "").strip()
            if not note_id:
                continue
            interact = card.get("interactInfo", {}) or {}
            out.append(
                CollectedVideo(
                    platform="xiaohongshu",
                    video_id=note_id,
                    video_url=f"https://www.xiaohongshu.com/explore/{note_id}",
                    author=str((card.get("user", {}) or {}).get("nickname") or user_id),
                    title=str(card.get("displayTitle") or ""),
                    stats={
                        "plays": 0,
                        "likes": _to_count(interact.get("likedCount")),
                        "comments": _to_count(interact.get("commentCount")),
                        "shares": _to_count(interact.get("shareCount")),
                    },
                    collected_at=now_iso,
                    raw=dict(item),
                )
            )
        return out

    def _map_douyin_aweme_list(self, items: List[Dict[str, Any]], size: int, fallback_author: str) -> List[CollectedVideo]:
        now_iso = self._now_iso()
        out: List[CollectedVideo] = []
        for item in items[:size]:
            if not isinstance(item, dict):
                continue
            aweme_id = str(item.get("aweme_id") or item.get("awemeId") or item.get("item_id") or "").strip()
            if not aweme_id:
                continue
            author_data = item.get("author", {}) or {}
            stats_data = item.get("statistics", {}) or item.get("stats", {}) or {}
            share_url = str(item.get("share_url") or item.get("shareUrl") or "").strip()
            if not share_url:
                share_url = f"https://www.douyin.com/video/{aweme_id}"
            title = str(item.get("desc") or item.get("title") or "").strip()
            out.append(
                CollectedVideo(
                    platform="douyin",
                    video_id=aweme_id,
                    video_url=share_url,
                    author=str(author_data.get("nickname") or author_data.get("unique_id") or fallback_author),
                    title=title,
                    stats={
                        "plays": _to_count(stats_data.get("play_count")),
                        "likes": _to_count(stats_data.get("digg_count") or stats_data.get("like_count")),
                        "comments": _to_count(stats_data.get("comment_count")),
                        "shares": _to_count(stats_data.get("share_count")),
                    },
                    collected_at=now_iso,
                    raw=dict(item),
                )
            )
        return out

    def _extract_douyin_render_data(self, html: str) -> Dict[str, Any]:
        matched = re.search(
            r'<script[^>]+id="RENDER_DATA"[^>]*>(.*?)</script>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not matched:
            raise PipelineError("COLLECTOR_REQUEST_FAILED", "douyin render data script not found")
        raw = matched.group(1).strip()
        decoded = urllib.parse.unquote(raw)
        try:
            return json.loads(decoded)
        except Exception:
            try:
                return json.loads(raw)
            except Exception as exc:
                raise PipelineError("COLLECTOR_REQUEST_FAILED", "douyin render data is not valid json") from exc

    def _find_douyin_aweme_list(self, obj: Any) -> List[Dict[str, Any]]:
        if isinstance(obj, dict):
            for key in ["aweme_list", "awemeList", "itemList", "post"]:
                value = obj.get(key)
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    return value
                if isinstance(value, dict):
                    values = [x for x in value.values() if isinstance(x, dict)]
                    if values and ("aweme_id" in values[0] or "awemeId" in values[0]):
                        return values
            for value in obj.values():
                found = self._find_douyin_aweme_list(value)
                if found:
                    return found
        if isinstance(obj, list):
            for item in obj:
                found = self._find_douyin_aweme_list(item)
                if found:
                    return found
        return []

    def _collect_douyin(self, *, profile_url: str, cookie: str, max_videos: int) -> List[CollectedVideo]:
        sec_user_id = self._extract_douyin_sec_user_id(profile_url)
        size = max(1, min(int(max_videos or 10), 30))
        errors: List[str] = []

        try:
            data = self._request_json(
                url="https://www.douyin.com/aweme/v1/web/aweme/post/",
                query={
                    "device_platform": "webapp",
                    "aid": "6383",
                    "channel": "channel_pc_web",
                    "sec_user_id": sec_user_id,
                    "max_cursor": "0",
                    "count": str(size),
                    "publish_video_strategy_type": "2",
                },
                headers={"Cookie": cookie, "Referer": profile_url},
            )
            status_code = int(data.get("status_code", -1))
            aweme_list = data.get("aweme_list", []) or []
            if status_code == 0 and isinstance(aweme_list, list) and aweme_list:
                return self._map_douyin_aweme_list(aweme_list, size, fallback_author=sec_user_id)
            errors.append(f"web_api_empty_or_failed(status_code={status_code})")
        except Exception as exc:
            errors.append(f"web_api_error({exc})")

        try:
            html = self._request_text(
                url=profile_url,
                headers={"Cookie": cookie, "Referer": "https://www.douyin.com/"},
            )
            render = self._extract_douyin_render_data(html)
            aweme_list = self._find_douyin_aweme_list(render)
            if aweme_list:
                return self._map_douyin_aweme_list(aweme_list, size, fallback_author=sec_user_id)
            errors.append("render_data_has_no_aweme_list")
        except Exception as exc:
            errors.append(f"render_data_error({exc})")

        raise PipelineError("COLLECTOR_REQUEST_FAILED", "douyin collector failed: " + "; ".join(errors))

    def _parse_youtube_feed(self, xml_text: str, size: int) -> List[CollectedVideo]:
        try:
            root = ET.fromstring(xml_text)
        except Exception as exc:
            raise PipelineError("COLLECTOR_REQUEST_FAILED", "youtube feed xml parse failed") from exc

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "yt": "http://www.youtube.com/xml/schemas/2015",
        }
        out: List[CollectedVideo] = []
        for entry in root.findall("atom:entry", ns)[:size]:
            video_id = entry.findtext("yt:videoId", default="", namespaces=ns).strip()
            title = entry.findtext("atom:title", default="", namespaces=ns).strip()
            author = entry.findtext("atom:author/atom:name", default="", namespaces=ns).strip()
            published = entry.findtext("atom:published", default="", namespaces=ns).strip()
            link = entry.find("atom:link", ns)
            video_url = ""
            if link is not None:
                video_url = str(link.attrib.get("href") or "").strip()
            if not video_url and video_id:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
            if not video_id and video_url:
                video_id = self._extract_youtube_video_id(video_url)
            if not video_id:
                continue
            out.append(
                CollectedVideo(
                    platform="youtube",
                    video_id=video_id,
                    video_url=video_url,
                    author=author,
                    title=title,
                    stats={"plays": 0, "likes": 0, "comments": 0, "shares": 0},
                    collected_at=published or self._now_iso(),
                    raw={},
                )
            )
        return out

    def _collect_youtube(self, *, profile_url: str, cookie: str, max_videos: int) -> List[CollectedVideo]:
        size = max(1, min(int(max_videos or 10), 30))

        if self._is_youtube_video_url(profile_url):
            video_id = self._extract_youtube_video_id(profile_url)
            if not video_id:
                raise PipelineError("COLLECTOR_INVALID_PROFILE_URL", "invalid youtube video url")
            title = ""
            author = ""
            try:
                oembed = self._request_json(
                    url="https://www.youtube.com/oembed",
                    query={"url": f"https://www.youtube.com/watch?v={video_id}", "format": "json"},
                )
                title = str(oembed.get("title") or "")
                author = str(oembed.get("author_name") or "")
            except Exception:
                pass
            return [
                CollectedVideo(
                    platform="youtube",
                    video_id=video_id,
                    video_url=f"https://www.youtube.com/watch?v={video_id}",
                    author=author,
                    title=title,
                    stats={"plays": 0, "likes": 0, "comments": 0, "shares": 0},
                    collected_at=self._now_iso(),
                    raw={},
                )
            ]

        channel_id = self._resolve_youtube_channel_id(profile_url, cookie)
        xml_text = self._request_text(
            url="https://www.youtube.com/feeds/videos.xml",
            query={"channel_id": channel_id},
            headers={"Cookie": cookie},
        )
        videos = self._parse_youtube_feed(xml_text, size)
        if videos:
            return videos
        raise PipelineError("COLLECTOR_REQUEST_FAILED", "youtube feed has no videos")
