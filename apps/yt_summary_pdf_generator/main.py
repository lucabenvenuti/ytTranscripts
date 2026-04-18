from __future__ import annotations

import hashlib
import platform
import sys
from datetime import datetime, timedelta
from pathlib import Path

from config_loader import load_config
from db import (
    open_db,
    initialize_schema_if_needed,
    insert_app_run,
    finalize_app_run,
    select_eligible_unsummarized_transcripts,
    mark_summary_in_progress,
    mark_summary_success,
    mark_summary_failed,
    insert_pdf_batch,
    mark_pdf_batch_success,
    mark_pdf_batch_incomplete,
    mark_pdf_batch_copy_failed,
    insert_pdf_batch_item,
)
from distributor import copy_file_to_share, copy_pdf_to_share
from logging_setup import setup_logger
from html_builder import build_html
from pdf_builder import build_pdf
from plot_writer import write_plot
from report_writer import write_json_report
from summarizer import GeminiSummarizer


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_config_path() -> Path:
    config_dir = resolve_repo_root() / "config"
    if platform.system() == "Darwin":
        return config_dir / "config.mac.yaml"
    return config_dir / "config.yaml"


CONFIG_PATH = resolve_config_path()


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


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def derive_status(stats: dict) -> str:
    if stats["failed_items"] == 0 and stats["remaining_items"] == 0 and stats["completed_items"] > 0:
        return "success"
    if stats["completed_items"] > 0 and (stats["failed_items"] > 0 or stats["remaining_items"] > 0):
        return "partial_success"
    if stats["remaining_items"] > 0:
        return "incomplete"
    if stats["completed_items"] == 0 and stats["failed_items"] == 0:
        return "no_work"
    return "failed"


def sort_batch_items_for_pdf(items: list[dict]) -> list[dict]:
    def key_fn(item: dict) -> tuple[str, str, str]:
        channel = str(item.get("channel_name") or "").casefold()
        published = str(item.get("publication_datetime") or "")
        title = str(item.get("title") or "").casefold()
        return (channel, published, title)

    return sorted(items, key=key_fn)


def main() -> int:
    config = load_config(CONFIG_PATH)
    logger = setup_logger(
        config["paths"]["summary_logs_dir"],
        "yt_summary_pdf_generator",
        config.get("logging", {}).get("level", "INFO"),
    )

    lock_path = config["paths"]["summary_lock"]
    if lock_exists(lock_path):
        logger.info("Summary generator already running, exiting.")
        return 0

    create_lock(lock_path)

    conn = None
    run_id = None
    batch_id = None

    try:
        ensure_dirs(config)
        cleanup_old_generated_files(config, logger, local_now())
        conn = open_db(config["paths"]["db_path"])
        initialize_schema_if_needed(conn, config["paths"]["schema_path"])

        now = local_now()

        summary_phase_minutes = int(config["summary_pdf_generator"].get("summary_phase_minutes", 50))
        total_run_minutes = int(config["summary_pdf_generator"].get("total_run_minutes", 60))
        deadline_safety_seconds = int(config["summary_pdf_generator"].get("deadline_safety_seconds", 20))

        summary_deadline = now + timedelta(minutes=summary_phase_minutes)
        hard_stop_deadline = now + timedelta(minutes=total_run_minutes)

        logger.info("Run started at: %s", now.isoformat())
        logger.info("Summary deadline: %s", summary_deadline.isoformat())
        logger.info("Hard stop deadline: %s", hard_stop_deadline.isoformat())

        run_id = insert_app_run(
            conn=conn,
            app_name="yt_summary_pdf_generator",
            started_at=now.isoformat(),
            time_window_start=now.isoformat(),
            time_window_end=hard_stop_deadline.isoformat(),
            status="incomplete",
        )

        candidates = select_eligible_unsummarized_transcripts(conn)

        stats = {
            "total_candidates": len(candidates),
            "completed_items": 0,
            "skipped_items": 0,
            "failed_items": 0,
            "interrupted_items": 0,
            "remaining_items": 0,
        }

        logger.info("Eligible transcripts for summarization: %s", len(candidates))

        if not candidates:
            status = "no_work"
            report_payload = {
                "app_name": "yt_summary_pdf_generator",
                "started_at": now.isoformat(),
                "completed_at": local_now().isoformat(),
                "status": status,
                **stats,
            }
            report_path = write_json_report(
                config["paths"]["summary_reports_dir"],
                "yt_summary_pdf_generator",
                report_payload,
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
                plot_path=None,
                error_message=None,
            )
            logger.info("Summary generator finished with status: %s", status)
            return 0

        batch_id = insert_pdf_batch(
            conn=conn,
            batch_date=now.strftime("%Y-%m-%d"),
            started_at=now.isoformat(),
            status="in_progress",
        )

        summarizer = GeminiSummarizer(config, logger)
        if not summarizer.healthcheck():
            logger.warning(
                "Gemini healthcheck did not pass, but the run will continue and rely on per-request retries. "
                "Check summarization.base_url and GEMINI_API_KEY if request failures persist."
            )

        completed_batch_items: list[dict] = []
        summary_phase_stopped_early = False

        for idx, row in enumerate(candidates):
            current = local_now()

            if current >= hard_stop_deadline:
                logger.warning("Hard stop deadline reached before next summary.")
                stats["remaining_items"] = len(candidates) - idx
                summary_phase_stopped_early = True
                break

            if current >= summary_deadline:
                logger.info("Summary phase deadline reached. Switching to PDF phase.")
                stats["remaining_items"] = len(candidates) - idx
                summary_phase_stopped_early = True
                break

            video_id = row["video_id"]
            transcript_id = int(row["transcript_id"])
            transcript_path = row["transcript_path"]
            transcript_language = row["transcript_language"]
            title = row["title"]
            video_url = row["video_url"]
            publication_datetime = row["publication_datetime"]
            channel_name = row["channel_name"]

            logger.info("Summarizing: %s | %s", video_id, title)
            mark_summary_in_progress(conn, video_id, transcript_id, current.isoformat())

            try:
                transcript_text = Path(transcript_path).read_text(encoding="utf-8", errors="ignore").strip()
                if not transcript_text:
                    raise RuntimeError("Transcript file is empty")

                summary_text = summarizer.summarize(
                    transcript_text=transcript_text,
                    title=title,
                    channel_name=channel_name,
                    transcript_language=transcript_language,
                    hard_deadline=summary_deadline,
                )

                if not summary_text.strip():
                    raise RuntimeError("Gemini returned empty summary")

                summary_path = summarizer.save_summary_file(
                    config["paths"]["summaries_dir"],
                    Path(transcript_path).stem,
                    summary_text,
                )

                summary_id = mark_summary_success(
                    conn=conn,
                    video_id=video_id,
                    transcript_id=transcript_id,
                    summary_path=summary_path,
                    summary_language=transcript_language,
                    model_name=config["summarization"]["model_name"],
                    prompt_version=config["summarization"].get("prompt_version", "v1"),
                    summary_hash=sha256_text(summary_text),
                    completed_at=local_now().isoformat(),
                )

                completed_batch_items.append({
                    "video_id": video_id,
                    "summary_id": summary_id,
                    "summary_path": summary_path,
                    "channel_name": channel_name,
                    "publication_datetime": publication_datetime,
                    "title": title,
                    "video_url": video_url,
                    "summary_text": summary_text,
                })

                stats["completed_items"] += 1
                logger.info("Summary saved: %s", summary_path)

            except TimeoutError as ex:
                logger.warning("Summary phase deadline hit during %s: %s", video_id, ex)
                mark_summary_failed(conn, video_id, str(ex), local_now().isoformat())
                stats["failed_items"] += 1
                stats["remaining_items"] = len(candidates) - idx - 1
                summary_phase_stopped_early = True
                break

            except Exception as ex:
                logger.exception("Summary failed for %s: %s", video_id, ex)
                mark_summary_failed(conn, video_id, str(ex), local_now().isoformat())
                stats["failed_items"] += 1

        status = derive_status(stats)
        local_pdf_path = None
        local_html_path = None
        pdf_name = None
        html_name = None

        if completed_batch_items:
            current = local_now()

            if current >= hard_stop_deadline - timedelta(seconds=deadline_safety_seconds):
                logger.warning("Not enough time left to safely start PDF generation.")
                mark_pdf_batch_incomplete(
                    conn=conn,
                    batch_id=batch_id,
                    completed_at=local_now().isoformat(),
                    error_message="Skipped PDF generation because hard stop deadline was too close",
                )
                status = derive_status(stats)

            else:
                timestamp_stem = local_now().strftime("%Y-%m-%d-%H-%M")
                pdf_name = timestamp_stem + ".pdf"
                html_name = timestamp_stem + ".html"
                local_pdf_path = str(Path(config["paths"]["pdf_dir"]) / pdf_name)
                local_html_path = str(Path(config["paths"]["pdf_dir"]) / html_name)

                ordered_batch_items = sort_batch_items_for_pdf(completed_batch_items)

                logger.info(
                    "Building PDF with %s completed summaries ordered by channel name",
                    len(ordered_batch_items),
                )

                document_title = config["pdf"].get("document_title", "YT Daily Summaries")

                build_pdf(
                    output_path=local_pdf_path,
                    batch_items=ordered_batch_items,
                    document_title=document_title,
                )
                build_html(
                    output_path=local_html_path,
                    batch_items=ordered_batch_items,
                    document_title=document_title,
                )

                try:
                    target_pdf_path = None
                    target_html_path = None
                    copied_at = None

                    if bool(config["distribution"].get("copy_enabled", True)):
                        current = local_now()
                        if current >= hard_stop_deadline - timedelta(seconds=deadline_safety_seconds):
                            raise RuntimeError("Skipping copy because hard stop deadline is too close")

                        target_pdf_path = copy_pdf_to_share(
                            local_pdf_path=local_pdf_path,
                            destination_share=config["distribution"]["destination_share"],
                            verify=bool(config["distribution"].get("verify_after_copy", True)),
                        )
                        target_html_path = copy_file_to_share(
                            local_file_path=local_html_path,
                            destination_share=config["distribution"]["destination_share"],
                            verify=bool(config["distribution"].get("verify_after_copy", True)),
                        )
                        copied_at = local_now().isoformat()
                        logger.info("HTML report copied to: %s", target_html_path)

                    mark_pdf_batch_success(
                        conn=conn,
                        batch_id=batch_id,
                        local_pdf_path=local_pdf_path,
                        target_pdf_path=target_pdf_path,
                        file_name=pdf_name,
                        copied_at=copied_at,
                        completed_at=local_now().isoformat(),
                    )

                    for item in ordered_batch_items:
                        insert_pdf_batch_item(
                            conn=conn,
                            batch_id=batch_id,
                            video_id=item["video_id"],
                            summary_id=item["summary_id"],
                            included_at=local_now().isoformat(),
                        )

                    status = derive_status(stats)

                except Exception as ex:
                    logger.exception("PDF/HTML publish/copy failed: %s", ex)
                    mark_pdf_batch_copy_failed(
                        conn=conn,
                        batch_id=batch_id,
                        local_pdf_path=local_pdf_path,
                        file_name=pdf_name,
                        completed_at=local_now().isoformat(),
                        error_message=str(ex),
                    )
                    status = "failed" if stats["completed_items"] == 0 else "partial_success"

        else:
            mark_pdf_batch_incomplete(
                conn=conn,
                batch_id=batch_id,
                completed_at=local_now().isoformat(),
                error_message="No completed summaries available for PDF generation",
            )
            status = derive_status(stats)

        report_payload = {
            "app_name": "yt_summary_pdf_generator",
            "started_at": now.isoformat(),
            "completed_at": local_now().isoformat(),
            "status": status,
            "batch_id": batch_id,
            "summary_deadline": summary_deadline.isoformat(),
            "hard_stop_deadline": hard_stop_deadline.isoformat(),
            "summary_phase_stopped_early": summary_phase_stopped_early,
            **stats,
        }

        report_path = write_json_report(
            config["paths"]["summary_reports_dir"],
            "yt_summary_pdf_generator",
            report_payload,
        )

        plot_path = None
        if status != "success" and bool(config["summary_pdf_generator"].get("plot_on_incomplete", True)):
            plot_path = write_plot(
                config["paths"]["summary_plots_dir"],
                "yt_summary_pdf_generator",
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

        logger.info("Summary generator finished with status: %s", status)
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