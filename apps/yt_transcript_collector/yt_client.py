from __future__ import annotations

import json
import random
import subprocess
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


class RateLimitError(Exception):
    pass


@dataclass
class VideoInfo:
    video_id: str
    channel_id: str
    channel_name: str
    title: str
    video_url: str
    publication_datetime: datetime
    duration_seconds: int | None = None
    language_hint: str | None = None
    is_short: int = 0
    metadata_json: str | None = None


class AdaptiveRateLimiter:
    def __init__(self, config: dict, logger):
        yt_cfg = config.get("yt", {})
        self.logger = logger

        self.base_interval_seconds = float(yt_cfg.get("base_interval_seconds", 4.0))
        self.max_interval_seconds = float(yt_cfg.get("max_interval_seconds", 240.0))
        self.success_decay = float(yt_cfg.get("success_decay", 0.90))
        self.min_penalty_seconds = float(yt_cfg.get("min_penalty_seconds", 0.0))
        self.penalty_step_seconds = float(yt_cfg.get("penalty_step_seconds", 15.0))
        self.max_penalty_seconds = float(yt_cfg.get("max_penalty_seconds", 360.0))
        self.multiplier_on_429 = float(yt_cfg.get("multiplier_on_429", 2.0))
        self.max_multiplier = float(yt_cfg.get("max_multiplier", 12.0))
        self.jitter_ratio = float(yt_cfg.get("jitter_ratio", 0.25))
        self.recovery_success_threshold = int(yt_cfg.get("recovery_success_threshold", 4))
        self.post_429_cooldown_seconds = float(yt_cfg.get("post_429_cooldown_seconds", 30.0))

        self.light_request_weight = float(yt_cfg.get("light_request_weight", 1.0))
        self.heavy_request_weight = float(yt_cfg.get("heavy_request_weight", 3.0))

        self.penalty_seconds = 0.0
        self.multiplier = 1.0
        self.successes_since_429 = 0
        self.rate_limits_seen = 0
        self.last_request_finished_at = 0.0

    def _current_delay(self, weight: float) -> float:
        delay = (self.base_interval_seconds * self.multiplier * weight) + self.penalty_seconds
        delay = min(delay, self.max_interval_seconds)
        jitter = delay * self.jitter_ratio
        if jitter > 0:
            delay += random.uniform(-jitter, jitter)
        return max(0.0, delay)

    def before_request(self, weight: float, label: str, attempt: int) -> None:
        now_ts = time.time()
        delay = self._current_delay(weight)
        ready_at = self.last_request_finished_at + delay
        sleep_seconds = max(0.0, ready_at - now_ts)

        if sleep_seconds > 0:
            self.logger.info(
                "Adaptive limiter sleeping %.2f seconds before %s request (attempt %s)",
                sleep_seconds,
                label,
                attempt,
            )
            time.sleep(sleep_seconds)

    def on_success(self) -> None:
        self.last_request_finished_at = time.time()
        self.successes_since_429 += 1
        self.penalty_seconds = max(
            self.min_penalty_seconds,
            self.penalty_seconds * self.success_decay,
        )

        if self.successes_since_429 >= self.recovery_success_threshold:
            self.multiplier = max(1.0, self.multiplier * self.success_decay)

        self.logger.info(
            "Limiter state after success: base=%.2fs penalty=%.2fs multiplier=%.2f successes=%s rate_limits=%s",
            self.base_interval_seconds,
            self.penalty_seconds,
            self.multiplier,
            self.successes_since_429,
            0,
        )

    def on_429(self) -> None:
        self.last_request_finished_at = time.time()
        self.rate_limits_seen += 1
        self.successes_since_429 = 0
        self.penalty_seconds = min(
            self.max_penalty_seconds,
            self.penalty_seconds + self.penalty_step_seconds,
        )
        self.multiplier = min(
            self.max_multiplier,
            max(1.0, self.multiplier * self.multiplier_on_429),
        )

        self.logger.warning(
            "Limiter state after 429: base=%.2fs penalty=%.2fs multiplier=%.2f successes=%s rate_limits=%s",
            self.base_interval_seconds,
            self.penalty_seconds,
            self.multiplier,
            self.successes_since_429,
            self.rate_limits_seen,
        )

        if self.post_429_cooldown_seconds > 0:
            self.logger.warning(
                "Extra cooldown after 429: sleeping %.2f seconds",
                self.post_429_cooldown_seconds,
            )
            time.sleep(self.post_429_cooldown_seconds)


class YTClient:
    def __init__(self, config: dict, logger):
        self.config = config
        self.logger = logger
        self.limiter = AdaptiveRateLimiter(config, logger)

        yt_cfg = config.get("yt", {})
        self.max_429_retries = int(yt_cfg.get("max_429_retries", 3))
        self.use_cookies_if_present = bool(yt_cfg.get("use_cookies_if_present", True))
        self.cookies_path = yt_cfg.get("cookies_path")
        self.cookies_from_browser = str(yt_cfg.get("cookies_from_browser", "")).strip()
        self.cookies_preference = str(yt_cfg.get("cookies_preference", "auto")).strip().lower()
        self.user_agent = yt_cfg.get("user_agent", "Mozilla/5.0")
        self.referer = yt_cfg.get("referer", "https://www.youtube.com/")
        self.ytdlp_remote_components = str(yt_cfg.get("ytdlp_remote_components", "")).strip()
        self.ytdlp_js_runtime = str(yt_cfg.get("ytdlp_js_runtime", "")).strip()
        self.rss_max_items = int(yt_cfg.get("rss_max_items", 2))
        self.rss_retry_count = int(yt_cfg.get("rss_retry_count", 2))
        self.rss_retry_base_seconds = float(yt_cfg.get("rss_retry_base_seconds", 2.0))
        self.rss_max_404_warnings = int(yt_cfg.get("rss_max_404_warnings", 3))
        self.discovery_fallback_to_ytdlp = bool(yt_cfg.get("discovery_fallback_to_ytdlp", True))
        self.discovery_request_weight = float(yt_cfg.get("discovery_request_weight", 1.5))
        self.discovery_max_429_retries = int(yt_cfg.get("discovery_max_429_retries", 1))
        self.discovery_rate_limit_pause_seconds = float(yt_cfg.get("discovery_rate_limit_pause_seconds", 1800.0))
        self.discovery_rate_limited_until = 0.0
        self.python_executable = sys.executable or "python"

    def _base_ytdlp_args(self, *, allow_playlist: bool = False) -> list[str]:
        args = [self.python_executable, "-m", "yt_dlp"]

        cookies_file_available = bool(
            self.use_cookies_if_present and self.cookies_path and Path(self.cookies_path).exists()
        )
        browser_cookies_available = bool(self.cookies_from_browser)

        if self.cookies_preference == "browser":
            if browser_cookies_available:
                args += ["--cookies-from-browser", self.cookies_from_browser]
            elif cookies_file_available:
                args += ["--cookies", self.cookies_path]
        elif self.cookies_preference == "file":
            if cookies_file_available:
                args += ["--cookies", self.cookies_path]
            elif browser_cookies_available:
                args += ["--cookies-from-browser", self.cookies_from_browser]
        else:
            if cookies_file_available:
                args += ["--cookies", self.cookies_path]
            elif browser_cookies_available:
                args += ["--cookies-from-browser", self.cookies_from_browser]

        if self.ytdlp_remote_components:
            args += ["--remote-components", self.ytdlp_remote_components]
        if self.ytdlp_js_runtime:
            args += ["--js-runtimes", self.ytdlp_js_runtime]

        args += [
            "--user-agent", self.user_agent,
            "--referer", self.referer,
        ]
        if not allow_playlist:
            args.append("--no-playlist")
        return args

    def _run(
        self,
        args: list[str],
        *,
        weight: float,
        label: str,
        max_429_retries: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        last_error: Exception | None = None
        retries = self.max_429_retries if max_429_retries is None else max_429_retries

        for attempt in range(1, retries + 2):
            self.limiter.before_request(weight=weight, label=label, attempt=attempt)

            self.logger.info("Running: %s", " ".join(args))
            cp = subprocess.run(
                args,
                text=True,
                capture_output=True,
                encoding="utf-8",
                errors="replace",
            )

            combined = ((cp.stdout or "") + "\n" + (cp.stderr or "")).strip()
            lowered = combined.lower()

            if "429" in combined or "too many requests" in lowered:
                self.limiter.on_429()
                last_error = RateLimitError("YouTube rate limit detected")
                if attempt <= retries:
                    continue
                raise last_error

            self.limiter.on_success()
            return cp

        assert last_error is not None
        raise last_error

    def _log_rss_retry(self, channel_name: str, ex: Exception, attempt: int, total_attempts: int, delay: float) -> None:
        if isinstance(ex, urllib.error.HTTPError) and ex.code == 404:
            if attempt <= self.rss_max_404_warnings:
                self.logger.warning(
                    "RSS fetch failed for %s with HTTP 404 on attempt %s/%s. Retrying in %.2f seconds.",
                    channel_name,
                    attempt,
                    total_attempts,
                    delay,
                )
            elif attempt == self.rss_max_404_warnings + 1:
                self.logger.info(
                    "RSS fetch for %s is still returning HTTP 404. Further 404 retry warnings are suppressed after %s attempts; continuing retries up to %s total attempts.",
                    channel_name,
                    self.rss_max_404_warnings,
                    total_attempts,
                )
            return

        if isinstance(ex, urllib.error.HTTPError):
            self.logger.warning(
                "RSS fetch failed for %s with HTTP %s on attempt %s/%s. Retrying in %.2f seconds.",
                channel_name,
                ex.code,
                attempt,
                total_attempts,
                delay,
            )
            return

        self.logger.warning(
            "RSS fetch failed for %s on attempt %s/%s: %s. Retrying in %.2f seconds.",
            channel_name,
            attempt,
            total_attempts,
            ex,
            delay,
        )

    def _uploads_playlist_url(self, channel_id: str) -> str:
        if channel_id.startswith("UC") and len(channel_id) > 2:
            return f"https://www.youtube.com/playlist?list=UU{channel_id[2:]}"
        return f"https://www.youtube.com/channel/{channel_id}/videos"

    def _parse_ytdlp_publication_datetime(self, entry: dict[str, Any]) -> datetime | None:
        timestamp = entry.get("timestamp") or entry.get("release_timestamp")
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp)

        for key in ("upload_date", "release_date"):
            raw = entry.get(key)
            if not raw:
                continue
            raw_str = str(raw).strip()
            for fmt in ("%Y%m%d", "%Y-%m-%d"):
                try:
                    return datetime.strptime(raw_str, fmt)
                except ValueError:
                    continue
        return None

    def _fetch_recent_videos_via_ytdlp(self, channel: dict) -> list[VideoInfo]:
        channel_id = channel["id"]
        channel_name = channel["name"]
        source_url = self._uploads_playlist_url(channel_id)

        now_ts = time.time()
        if now_ts < self.discovery_rate_limited_until:
            resume_at = datetime.fromtimestamp(self.discovery_rate_limited_until).isoformat(timespec="seconds")
            raise RateLimitError(
                f"Discovery fallback temporarily paused until {resume_at} after repeated YouTube rate limits"
            )

        args = self._base_ytdlp_args(allow_playlist=True) + [
            "--dump-single-json",
            "--playlist-end", str(self.rss_max_items),
            source_url,
        ]

        try:
            cp = self._run(
                args,
                weight=self.discovery_request_weight,
                label="discovery fallback",
                max_429_retries=self.discovery_max_429_retries,
            )
        except RateLimitError:
            self.discovery_rate_limited_until = time.time() + self.discovery_rate_limit_pause_seconds
            resume_at = datetime.fromtimestamp(self.discovery_rate_limited_until).isoformat(timespec="seconds")
            self.logger.warning(
                "Pausing yt-dlp discovery fallback until %s after repeated 429 responses.",
                resume_at,
            )
            raise

        if cp.returncode != 0:
            raise RuntimeError(cp.stderr or cp.stdout or "yt-dlp discovery fallback failed")

        payload = (cp.stdout or "").strip()
        if not payload:
            raise RuntimeError("yt-dlp discovery fallback returned empty output")

        data = json.loads(payload)
        entries = data.get("entries") or []
        videos: list[VideoInfo] = []

        for entry in entries:
            if not isinstance(entry, dict):
                continue

            video_id = str(entry.get("id") or "").strip()
            title = str(entry.get("title") or "").strip()
            if not video_id or not title:
                continue

            publication_datetime = self._parse_ytdlp_publication_datetime(entry)
            if publication_datetime is None:
                self.logger.warning(
                    "Skipping yt-dlp discovery item for %s because publication date could not be parsed: id=%s title=%s",
                    channel_name,
                    video_id,
                    title,
                )
                continue

            video_url = str(entry.get("webpage_url") or "").strip()
            if not video_url:
                raw_url = str(entry.get("url") or "").strip()
                if raw_url.startswith("http"):
                    video_url = raw_url
                elif raw_url.startswith("/shorts/"):
                    video_url = f"https://www.youtube.com{raw_url}"
                else:
                    video_url = f"https://www.youtube.com/watch?v={video_id}"

            metadata = {
                "discovery_source": "yt_dlp_fallback",
                "channel_name": channel_name,
                "source_url": source_url,
                "upload_date": entry.get("upload_date"),
                "timestamp": entry.get("timestamp"),
            }

            videos.append(
                VideoInfo(
                    video_id=video_id,
                    channel_id=channel_id,
                    channel_name=channel_name,
                    title=title,
                    video_url=video_url,
                    publication_datetime=publication_datetime,
                    duration_seconds=entry.get("duration"),
                    language_hint=None,
                    is_short=1 if "/shorts/" in video_url else 0,
                    metadata_json=json.dumps(metadata, ensure_ascii=False),
                )
            )

        videos = videos[: self.rss_max_items]
        self.logger.info("yt-dlp fallback worked for %s with %s IDs", channel_name, len(videos))
        return videos

    def fetch_recent_videos(self, channel: dict) -> list[VideoInfo]:
        """
        Prefer RSS discovery. If YouTube's RSS endpoint keeps failing, fall back to yt-dlp
        against the channel uploads playlist.
        """
        channel_id = channel["id"]
        channel_name = channel["name"]
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

        req = urllib.request.Request(
            rss_url,
            headers={
                "User-Agent": self.user_agent,
                "Referer": self.referer,
            },
        )

        last_error: Exception | None = None
        xml_bytes: bytes | None = None
        total_attempts = self.rss_retry_count + 1

        for attempt in range(1, total_attempts + 1):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    xml_bytes = resp.read()
                break
            except urllib.error.HTTPError as ex:
                last_error = ex
                retryable = ex.code in {404, 429, 500, 502, 503, 504}
                if retryable and attempt <= self.rss_retry_count:
                    delay = self.rss_retry_base_seconds * attempt
                    self._log_rss_retry(channel_name, ex, attempt, total_attempts, delay)
                    time.sleep(delay)
                    continue
                break
            except Exception as ex:
                last_error = ex
                if attempt <= self.rss_retry_count:
                    delay = self.rss_retry_base_seconds * attempt
                    self._log_rss_retry(channel_name, ex, attempt, total_attempts, delay)
                    time.sleep(delay)
                    continue
                break

        if xml_bytes is None:
            if self.discovery_fallback_to_ytdlp:
                self.logger.warning(
                    "RSS discovery exhausted for %s after %s attempt(s): %s. Falling back to yt-dlp uploads discovery.",
                    channel_name,
                    total_attempts,
                    last_error,
                )
                return self._fetch_recent_videos_via_ytdlp(channel)

            assert last_error is not None
            raise last_error

        root = ET.fromstring(xml_bytes)

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "yt": "http://www.youtube.com/xml/schemas/2015",
        }

        videos: list[VideoInfo] = []

        for entry in root.findall("atom:entry", ns):
            video_id = entry.findtext("yt:videoId", default="", namespaces=ns).strip()
            title = entry.findtext("atom:title", default="", namespaces=ns).strip()
            published = entry.findtext("atom:published", default="", namespaces=ns).strip()
            link_el = entry.find("atom:link", ns)

            if not video_id or not title or not published:
                continue

            video_url = f"https://www.youtube.com/watch?v={video_id}"
            if link_el is not None:
                href = link_el.attrib.get("href", "").strip()
                if href:
                    video_url = href

            try:
                publication_datetime = datetime.fromisoformat(published.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                self.logger.warning(
                    "Skipping RSS item for %s because published datetime could not be parsed: %s",
                    channel_name,
                    published,
                )
                continue

            metadata = {
                "discovery_source": "rss",
                "rss_published": published,
                "channel_name": channel_name,
            }

            videos.append(
                VideoInfo(
                    video_id=video_id,
                    channel_id=channel_id,
                    channel_name=channel_name,
                    title=title,
                    video_url=video_url,
                    publication_datetime=publication_datetime,
                    duration_seconds=None,
                    language_hint=None,
                    is_short=1 if "/shorts/" in video_url else 0,
                    metadata_json=json.dumps(metadata, ensure_ascii=False),
                )
            )

        videos = videos[: self.rss_max_items]
        self.logger.info("RSS worked for %s with %s IDs", channel_name, len(videos))
        return videos

    def download_subtitles(
        self,
        video_url: str,
        out_base: str,
        languages: list[str],
        source_type: str,
    ) -> Path | None:
        base_args = self._base_ytdlp_args()
        args = base_args + [
            "--skip-download",
            "--sub-langs", ",".join(languages),
            "--sub-format", "vtt/best",
            "--convert-subs", "vtt",
            "-o", f"{out_base}.%(ext)s",
            video_url,
        ]

        insert_at = len(base_args)
        if source_type == "manual":
            args.insert(insert_at, "--write-subs")
        elif source_type == "auto":
            args.insert(insert_at, "--write-auto-subs")
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")

        cp = self._run(
            args,
            weight=self.limiter.heavy_request_weight,
            label="heavy",
        )

        if cp.returncode != 0:
            self.logger.warning("Subtitle download failed: %s", cp.stderr or cp.stdout)
            return None

        parent = Path(out_base).parent
        stem = Path(out_base).name
        vtt_files = sorted(parent.glob(f"{stem}*.vtt"))
        if vtt_files:
            return vtt_files[0]

        fallback_patterns = ["*.srv1", "*.srv2", "*.srv3", "*.ttml", "*.json3"]
        fallback_files: list[Path] = []
        for suffix_pattern in fallback_patterns:
            fallback_files.extend(sorted(parent.glob(f"{stem}{suffix_pattern}")))

        combined_output = ((cp.stdout or "") + "\n" + (cp.stderr or "")).strip()
        if fallback_files:
            self.logger.warning(
                "Subtitle download produced non-VTT files for %s but no .vtt after conversion: %s | output=%s",
                video_url,
                ", ".join(str(p) for p in fallback_files),
                combined_output,
            )
        else:
            self.logger.warning(
                "Subtitle download completed without creating subtitle files for %s | output=%s",
                video_url,
                combined_output,
            )
        return None
