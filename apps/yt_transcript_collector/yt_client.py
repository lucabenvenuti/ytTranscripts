from __future__ import annotations

import json
import random
import subprocess
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
        self.user_agent = yt_cfg.get("user_agent", "Mozilla/5.0")
        self.referer = yt_cfg.get("referer", "https://www.youtube.com/")
        self.rss_max_items = int(yt_cfg.get("rss_max_items", 2))
        self.rss_retry_count = int(yt_cfg.get("rss_retry_count", 2))
        self.rss_retry_base_seconds = float(yt_cfg.get("rss_retry_base_seconds", 2.0))

    def _base_ytdlp_args(self) -> list[str]:
        args = ["python", "-m", "yt_dlp"]

        if self.use_cookies_if_present and self.cookies_path and Path(self.cookies_path).exists():
            args += ["--cookies", self.cookies_path]

        args += [
            "--remote-components", "ejs:github",
            "--js-runtimes", "deno",
            "--user-agent", self.user_agent,
            "--referer", self.referer,
            "--no-playlist",
        ]
        return args

    def _run(self, args: list[str], *, weight: float, label: str) -> subprocess.CompletedProcess[str]:
        last_error: Exception | None = None

        for attempt in range(1, self.max_429_retries + 2):
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
                if attempt <= self.max_429_retries:
                    continue
                raise last_error

            self.limiter.on_success()
            return cp

        assert last_error is not None
        raise last_error

    def fetch_recent_videos(self, channel: dict) -> list[VideoInfo]:
        """
        RSS-only discovery to reduce yt-dlp metadata calls and avoid 429s.
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

        for attempt in range(1, self.rss_retry_count + 2):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    xml_bytes = resp.read()
                break
            except urllib.error.HTTPError as ex:
                last_error = ex
                retryable = ex.code in {404, 429, 500, 502, 503, 504}
                if not retryable or attempt > self.rss_retry_count:
                    raise
                delay = self.rss_retry_base_seconds * attempt
                self.logger.warning(
                    "RSS fetch failed for %s with HTTP %s on attempt %s/%s. Retrying in %.2f seconds.",
                    channel_name,
                    ex.code,
                    attempt,
                    self.rss_retry_count + 1,
                    delay,
                )
                time.sleep(delay)
            except Exception as ex:
                last_error = ex
                if attempt > self.rss_retry_count:
                    raise
                delay = self.rss_retry_base_seconds * attempt
                self.logger.warning(
                    "RSS fetch failed for %s on attempt %s/%s: %s. Retrying in %.2f seconds.",
                    channel_name,
                    attempt,
                    self.rss_retry_count + 1,
                    ex,
                    delay,
                )
                time.sleep(delay)

        if xml_bytes is None:
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
                    is_short=0,
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
        args = self._base_ytdlp_args() + [
            "--skip-download",
            "--sub-langs", ",".join(languages),
            "--sub-format", "vtt",
            "-o", f"{out_base}.%(ext)s",
            video_url,
        ]

        insert_at = len(self._base_ytdlp_args())
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
        files = sorted(parent.glob(f"{stem}*.vtt"))
        return files[0] if files else None