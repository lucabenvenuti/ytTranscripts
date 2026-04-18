from __future__ import annotations

import hashlib
import platform
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from config_loader import load_config, load_channels
from db import (
    open_db,
    initialize_schema_if_needed,
    get_last_successful_run_end,
    insert_app_run,
    finalize_app_run,
    upsert_channel,
    upsert_video,
    transcript_success_exists,
    mark_transcript_in_progress,
    mark_transcript_success,
    mark_transcript_failed,
)
from logging_setup import setup_logger
from naming import build_transcript_filename, sanitize_windows_filename
from plot_writer import write_plot
from report_writer import write_json_report
from yt_client import YTClient


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_config_path() -> Path:
    config_dir = resolve_repo_root() / "config"
    if platform.system() == "Darwin":
        return config_dir / "config.mac.yaml"
    return config_dir / "config.yaml"


def resolve_channels_path() -> Path:
    return resolve_repo_root() / "config" / "channels.yaml"


CONFIG_PATH = resolve_config_path()
CHANNELS_PATH = resolve_channels_path()
# TEST_MAX_CANDIDATES = 1


def local_now() -> datetime:
    return datetime.now()


def ensure_dirs(config: dict) -> None:
    for key, path in config["paths"].items():
        if key.endswith("_path") or key.endswith("_lock"):
            continue
        Path(path).mkdir(parents=True, exist_ok=True)


def lock_exists(lock_path: str) -> bool:
    return Path(lock_path).exists()


def create_lock(lock_path: str) -> None:
    Path(lock_path).parent.mkdir(parents=True, exist_ok=True)
    Path(lock_path).write_text(str(datetime.now()), encoding="utf-8")


def delete_lock(lock_path: str) -> None:
    p = Path(lock_path)
    if p.exists():
        p.unlink()


def cleanup_old_generated_files(config: dict, logger, now: datetime) -> None:
    retention_cfg = config.get("retention", {})
    if not bool(retention_cfg.get("enabled", True)):
        return

    max_age_days = int(retention_cfg.get("max_age_days", 7))
    if max_age_days < 0:
        return

    cutoff = now - timedelta(days=max_age_days)
    paths_cfg = config.get("paths", {})
    protected_files = {
        str(Path(paths_cfg.get("collector_lock", ""))).lower(),
        str(Path(paths_cfg.get("summary_lock", ""))).lower(),
    }

    directory_keys = [
        "collector_reports_dir",
        "summary_reports_dir",
        "collector_plots_dir",
        "summary_plots_dir",
        "collector_logs_dir",
        "summary_logs_dir",
        "pdf_dir",
        "temp_dir",
    ]

    if not bool(config.get("transcript_collector", {}).get("keep_audio_cache", True)):
        directory_keys.append("audio_cache_dir")

    removed_count = 0

    for key in directory_keys:
        raw_dir = paths_cfg.get(key)
        if not raw_dir:
            continue

        directory = Path(raw_dir)
        if not directory.exists() or not directory.is_dir():
            continue

        for file_path in directory.rglob("*"):
            if not file_path.is_file():
                continue
            if str(file_path).lower() in protected_files:
                continue

            try:
                modified_at = datetime.fromtimestamp(file_path.stat().st_mtime)
            except OSError:
                continue

            if modified_at >= cutoff:
                continue

            try:
                file_path.unlink()
                removed_count += 1
                logger.info("Deleted old generated file: %s", file_path)
            except Exception as ex:
                logger.warning("Failed deleting old generated file %s: %s", file_path, ex)

    if removed_count:
        logger.info("Cleanup removed %s generated file(s) older than %s days", removed_count, max_age_days)


def parse_iso_maybe(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def derive_status(stats: dict) -> str:
    if stats["failed_items"] == 0 and stats["remaining_items"] == 0:
        return "success"
    if stats["completed_items"] > 0 and stats["remaining_items"] > 0:
        return "partial_success"
    if stats["completed_items"] > 0:
        return "partial_success"
    if stats["remaining_items"] > 0:
        return "incomplete"
    return "failed"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def save_transcript_file(transcripts_dir: str, filename: str, text: str, video_id: str) -> str:
    out_dir = Path(transcripts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    candidate = out_dir / filename
    if candidate.exists():
        stem = candidate.stem
        suffix = candidate.suffix
        candidate = out_dir / f"{stem}-{video_id[:8]}{suffix}"

    candidate.write_text(text, encoding="utf-8")
    return str(candidate)


def sleep_with_jitter(min_seconds: float, max_seconds: float, logger) -> None:
    delay = random.uniform(min_seconds, max_seconds)
    logger.info("Sleeping %.2f seconds before next YouTube request", delay)
    time.sleep(delay)


def try_download_subtitles_for_channel_language(
    yt: YTClient,
    logger,
    video_url: str,
    temp_base: str,
    channel_language: str,
) -> tuple[Path | None, str | None, str | None]:
    """
    Download exactly one transcript in the channel's language preference.

    Rules:
    - italian channel -> manual it, then auto it
    - english channel -> manual en, then auto en

    Returns:
        (vtt_path, language_code, source_type)
    """
    normalized = (channel_language or "").strip().lower()

    if normalized == "italian":
        attempts = [
            (["it"], "manual"),
            (["it"], "auto"),
        ]
    elif normalized == "english":
        attempts = [
            (["en"], "manual"),
            (["en"], "auto"),
        ]
    else:
        logger.warning(
            "Unknown channel language '%s'. Falling back to english manual/auto.",
            channel_language,
        )
        attempts = [
            (["en"], "manual"),
            (["en"], "auto"),
        ]

    for langs, source_type in attempts:
        lang_label = ",".join(langs)
        logger.info(
            "Trying subtitle download: channel_language=%s langs=%s source_type=%s",
            normalized,
            lang_label,
            source_type,
        )

        try:
            vtt_path = yt.download_subtitles(video_url, temp_base, langs, source_type)
        except Exception as ex:
            logger.warning(
                "Subtitle attempt failed for channel_language=%s langs=%s source_type=%s: %s",
                normalized,
                lang_label,
                source_type,
                ex,
            )
            vtt_path = None

        if vtt_path and vtt_path.exists():
            logger.info(
                "Subtitle download succeeded: channel_language=%s langs=%s source_type=%s path=%s",
                normalized,
                lang_label,
                source_type,
                vtt_path,
            )
            return vtt_path, langs[0], source_type

    return None, None, None


def extract_text_from_vtt_if_possible(vtt_path: Path | None, logger) -> str:
    if not vtt_path or not vtt_path.exists():
        return ""

    try:
        raw = vtt_path.read_text(encoding="utf-8", errors="replace")
    except Exception as ex:
        logger.warning("Failed reading VTT file %s: %s", vtt_path, ex)
        return ""

    lines: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        if s == "WEBVTT":
            continue
        if s.startswith("Kind:"):
            continue
        if s.startswith("Language:"):
            continue
        if "-->" in s:
            continue
        if s.isdigit():
            continue
        if s.startswith("NOTE "):
            continue

        s = s.replace("<c>", "").replace("</c>", "")
        s = s.replace("<i>", "").replace("</i>", "")
        s = s.replace("<b>", "").replace("</b>", "")
        s = s.replace("<u>", "").replace("</u>", "")

        lines.append(s)

    if not lines:
        return ""

    cleaned_lines: list[str] = []
    previous = None
    for line in lines:
        if line != previous:
            cleaned_lines.append(line)
        previous = line

    text = "\n".join(cleaned_lines).strip()
    logger.info("Extracted %s characters from subtitle file", len(text))
    return text


def main() -> int:
    config = load_config(CONFIG_PATH)
    channels = load_channels(CHANNELS_PATH)
    logger = setup_logger(
        config["paths"]["collector_logs_dir"],
        "yt_transcript_collector",
        config.get("logging", {}).get("level", "INFO"),
    )

    lock_path = config["paths"]["collector_lock"]
    if lock_exists(lock_path):
        logger.info("Collector already running, exiting.")
        return 0

    create_lock(lock_path)

    conn = None
    run_id = None

    try:
        ensure_dirs(config)
        cleanup_old_generated_files(config, logger, local_now())
        conn = open_db(config["paths"]["db_path"])
        initialize_schema_if_needed(conn, config["paths"]["schema_path"])

        now = local_now()
        today = now.date()

        start_time = datetime.strptime(config["transcript_collector"]["start_time"], "%H:%M").time()
        end_time = datetime.strptime(config["transcript_collector"]["end_time"], "%H:%M").time()
        no_new_work_after_time = datetime.strptime(config["transcript_collector"]["no_new_work_after"], "%H:%M").time()

        window_start = datetime.combine(today, start_time)
        window_end = datetime.combine(today, end_time)
        no_new_work_after = datetime.combine(today, no_new_work_after_time)

        run_id = insert_app_run(
            conn=conn,
            app_name="yt_transcript_collector",
            started_at=now.isoformat(),
            time_window_start=window_start.isoformat(),
            time_window_end=window_end.isoformat(),
            status="incomplete",
        )

        last_success_str = get_last_successful_run_end(conn, "yt_transcript_collector")
        last_success_dt = parse_iso_maybe(last_success_str)
        seven_days_ago = now - timedelta(days=int(config["transcript_collector"].get("discovery_max_age_days", 7)))
        lower_bound = max([d for d in [last_success_dt, seven_days_ago] if d is not None])

        yt = YTClient(config, logger)

        candidates: list[dict] = []
        for channel in channels:
            if not channel.get("enabled", True):
                continue

            upsert_channel(conn, channel["id"], channel["name"], now.isoformat(), 1)

            try:
                videos = yt.fetch_recent_videos(channel)
            except KeyboardInterrupt:
                raise
            except Exception as ex:
                logger.warning("Failed fetching videos for %s: %s", channel["name"], ex)
                continue

            for video in videos:
                upsert_video(
                    conn,
                    video_id=video.video_id,
                    channel_id=video.channel_id,
                    title=video.title,
                    video_url=video.video_url,
                    publication_datetime=video.publication_datetime.isoformat(),
                    discovered_at=now.isoformat(),
                    duration_seconds=video.duration_seconds,
                    language_hint=video.language_hint,
                    is_short=video.is_short,
                    metadata_json=video.metadata_json,
                )

                logger.info(
                    "Discovered video: id=%s | title=%s | published=%s | lower_bound=%s",
                    video.video_id,
                    video.title,
                    video.publication_datetime.isoformat(),
                    lower_bound.isoformat(),
                )

                if video.publication_datetime < lower_bound:
                    logger.info("Skipping %s because it is older than lower bound", video.video_id)
                    continue

                if transcript_success_exists(conn, video.video_id):
                    logger.info("Skipping %s because transcript already exists", video.video_id)
                    continue

                logger.info("Candidate accepted: %s", video.video_id)
                candidates.append(
                    {
                        "video": video,
                        "channel_language": str(channel.get("language", "")).strip().lower(),
                    }
                )

        unique: dict[str, dict] = {}
        for item in candidates:
            video = item["video"]
            unique[video.video_id] = item

        candidates = sorted(
            unique.values(),
            key=lambda x: x["video"].publication_datetime,
        )
        logger.info("Total accepted candidates after filtering: %s", len(candidates))
        # candidates = candidates[:TEST_MAX_CANDIDATES]
        # logger.info("Candidates after TEST_MAX_CANDIDATES limit: %s", len(candidates))

        stats = {
            "total_candidates": len(candidates),
            "completed_items": 0,
            "skipped_items": 0,
            "failed_items": 0,
            "interrupted_items": 0,
            "remaining_items": 0,
        }

        min_chars = int(config["transcript_collector"]["min_transcript_length_chars"])
        request_pause_min = float(config["transcript_collector"].get("request_pause_min_seconds", 2.0))
        request_pause_max = float(config["transcript_collector"].get("request_pause_max_seconds", 5.0))

        for idx, item in enumerate(candidates):
            video = item["video"]
            channel_language = item["channel_language"]

            current = local_now()
            if current >= window_end or current >= no_new_work_after:
                stats["remaining_items"] = len(candidates) - idx
                break

            sleep_with_jitter(request_pause_min, request_pause_max, logger)

            logger.info(
                "Processing: %s | %s | channel_language=%s",
                video.video_id,
                video.title,
                channel_language,
            )
            mark_transcript_in_progress(conn, video.video_id, current.isoformat())

            try:
                transcript_text = ""
                transcript_source = ""
                transcript_language = None

                safe_base = sanitize_windows_filename(f"{video.video_id}-{video.title}", 100)
                temp_base = str(Path(config["paths"]["audio_cache_dir"]) / safe_base)

                vtt_path, subtitle_lang, subtitle_source_type = try_download_subtitles_for_channel_language(
                    yt=yt,
                    logger=logger,
                    video_url=video.video_url,
                    temp_base=temp_base,
                    channel_language=channel_language,
                )

                if vtt_path and vtt_path.exists():
                    cleaned = extract_text_from_vtt_if_possible(vtt_path, logger)
                    if len(cleaned) >= min_chars:
                        transcript_text = cleaned
                        transcript_language = subtitle_lang
                        transcript_source = (
                            "youtube_subtitles_manual"
                            if subtitle_source_type == "manual"
                            else "youtube_subtitles_auto"
                        )
                    else:
                        logger.info(
                            "Subtitle text too short for %s (%s chars)",
                            video.video_id,
                            len(cleaned),
                        )

                if not transcript_text:
                    mark_transcript_failed(
                        conn,
                        video.video_id,
                        f"No usable subtitles found for channel language '{channel_language}'",
                        local_now().isoformat(),
                    )
                    stats["failed_items"] += 1
                    continue

                filename = build_transcript_filename(video.publication_datetime, video.title, ".txt")
                transcript_path = save_transcript_file(
                    config["paths"]["transcripts_dir"],
                    filename,
                    transcript_text,
                    video.video_id,
                )

                mark_transcript_success(
                    conn=conn,
                    video_id=video.video_id,
                    transcript_path=transcript_path,
                    transcript_source=transcript_source,
                    transcript_language=transcript_language,
                    transcript_length_chars=len(transcript_text),
                    transcript_hash=sha256_text(transcript_text),
                    completed_at=local_now().isoformat(),
                )

                stats["completed_items"] += 1
                logger.info("Saved transcript: %s", transcript_path)

            except Exception as ex:
                logger.exception("Processing failed for %s: %s", video.video_id, ex)
                mark_transcript_failed(conn, video.video_id, str(ex), local_now().isoformat())
                stats["failed_items"] += 1

        status = derive_status(stats)

        report_payload = {
            "app_name": "yt_transcript_collector",
            "started_at": now.isoformat(),
            "completed_at": local_now().isoformat(),
            "status": status,
            **stats,
        }
        report_path = write_json_report(
            config["paths"]["collector_reports_dir"],
            "yt_transcript_collector",
            report_payload,
        )

        plot_path = None
        if stats["remaining_items"] > 0 and bool(config["transcript_collector"].get("plot_on_incomplete", True)):
            plot_path = write_plot(
                config["paths"]["collector_plots_dir"],
                "yt_transcript_collector",
                stats,
            )

        finalize_app_run(
            conn=conn,
            run_id=run_id,
            status=status,
            completed_at=local_now().isoformat(),
            total_candidates=stats["total_candidates"],
            completed_items=stats["completed_items"],
            skipped_items=stats["skipped_items"],
            failed_items=stats["failed_items"],
            interrupted_items=stats["interrupted_items"],
            remaining_items=stats["remaining_items"],
            report_path=report_path,
            plot_path=plot_path,
            error_message=None,
        )

        logger.info("Collector finished with status: %s", status)
        return 0

    except Exception as ex:
        if conn is not None and run_id is not None:
            finalize_app_run(
                conn=conn,
                run_id=run_id,
                status="failed",
                completed_at=local_now().isoformat(),
                total_candidates=0,
                completed_items=0,
                skipped_items=0,
                failed_items=1,
                interrupted_items=0,
                remaining_items=0,
                report_path=None,
                plot_path=None,
                error_message=str(ex),
            )
        print(f"FATAL: {ex}", file=sys.stderr)
        return 1

    finally:
        delete_lock(lock_path)


if __name__ == "__main__":
    raise SystemExit(main())