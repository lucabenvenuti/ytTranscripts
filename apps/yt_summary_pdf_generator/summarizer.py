from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class GeminiSettings:
    base_url: str
    model_name: str
    fallback_model_name: str
    fallback_cooldown_seconds: float
    api_key: str
    timeout: float | None
    prompt_version: str
    temperature: float
    max_output_tokens: int
    max_chunk_chars: int
    overlap_chars: int
    max_chunks_per_transcript: int
    combine_batch_size: int
    retries_per_call: int
    deadline_safety_seconds: int
    requests_per_minute_limit: int
    min_request_interval_seconds: float
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    retry_backoff_max_seconds: float


def _now() -> datetime:
    return datetime.now()


def _seconds_until(deadline: datetime | None) -> float | None:
    if deadline is None:
        return None
    return (deadline - _now()).total_seconds()


def _clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _parse_retry_after_seconds(error_body: str) -> float | None:
    match = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", error_body, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _extract_quota_metric(error_body: str) -> str | None:
    match = re.search(r"Quota exceeded for metric:\s*([^,\n]+)", error_body, flags=re.IGNORECASE)
    if not match:
        return None
    metric = match.group(1).strip()
    return metric or None


def _is_quota_exceeded(error_body: str) -> bool:
    normalized = error_body.casefold()
    return "quota exceeded" in normalized or "billing details" in normalized


def _is_high_demand_unavailable(error_body: str) -> bool:
    normalized = error_body.casefold()
    return "currently experiencing high demand" in normalized or "spikes in demand are usually temporary" in normalized


class HighDemandModelError(RuntimeError):
    def __init__(self, model_name: str, stage_label: str):
        super().__init__(f"Model {model_name} is experiencing high demand at stage {stage_label}")
        self.model_name = model_name
        self.stage_label = stage_label


def _split_text_into_chunks(
    text: str,
    max_chunk_chars: int,
    overlap_chars: int,
    max_chunks: int,
) -> list[str]:
    text = _clean_text(text)
    if not text:
        return []

    if len(text) <= max_chunk_chars:
        return [text]

    chunks: list[str] = []
    start = 0
    text_len = len(text)

    while start < text_len and len(chunks) < max_chunks:
        remaining_chunks = max_chunks - len(chunks)
        remaining_text = text_len - start

        if remaining_chunks == 1:
            end = text_len
        else:
            dynamic_target = min(
                max_chunk_chars,
                max(max_chunk_chars, remaining_text // remaining_chunks),
            )
            end = min(start + dynamic_target, text_len)

            if end < text_len:
                split_candidates = [
                    text.rfind("\n\n", start, end),
                    text.rfind("\n", start, end),
                    text.rfind(". ", start, end),
                    text.rfind(" ", start, end),
                ]
                split_at = max(split_candidates)
                if split_at > start + dynamic_target // 2:
                    end = split_at

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= text_len:
            break

        start = max(0, end - overlap_chars)

    return chunks


class RequestRateLimiter:
    def __init__(self, requests_per_minute: int, min_interval_seconds: float, logger):
        self.requests_per_minute = max(1, int(requests_per_minute))
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.logger = logger
        self.request_timestamps: list[float] = []
        self.last_request_time: float | None = None

    def _prune_old(self, now_ts: float) -> None:
        cutoff = now_ts - 60.0
        self.request_timestamps = [ts for ts in self.request_timestamps if ts > cutoff]

    def wait_for_slot(self) -> None:
        while True:
            now_ts = time.time()
            self._prune_old(now_ts)

            wait_due_to_interval = 0.0
            if self.last_request_time is not None:
                elapsed = now_ts - self.last_request_time
                wait_due_to_interval = max(0.0, self.min_interval_seconds - elapsed)

            wait_due_to_rpm = 0.0
            if len(self.request_timestamps) >= self.requests_per_minute:
                oldest = min(self.request_timestamps)
                wait_due_to_rpm = max(0.0, 60.0 - (now_ts - oldest) + 0.05)

            wait_time = max(wait_due_to_interval, wait_due_to_rpm)

            if wait_time <= 0:
                now_mark = time.time()
                self.request_timestamps.append(now_mark)
                self.last_request_time = now_mark
                return

            self.logger.info(
                "Gemini rate limiter sleeping %.2f seconds (limit=%s req/min, last_minute_requests=%s)",
                wait_time,
                self.requests_per_minute,
                len(self.request_timestamps),
            )
            time.sleep(wait_time)


class GeminiSummarizer:
    def __init__(self, config: dict, logger):
        self.config = config
        self.logger = logger

        gen_cfg = config.get("summary_pdf_generator", {})
        sum_cfg = config["summarization"]

        api_key = ""
        env_var_name = str(sum_cfg.get("api_key_env_var", "GEMINI_API_KEY")).strip()
        if env_var_name:
            api_key = os.environ.get(env_var_name, "").strip()

        if not api_key:
            api_key = str(sum_cfg.get("api_key", "")).strip()

        timeout_value = sum_cfg.get("request_timeout_seconds", None)
        if timeout_value in ("", 0, "0", None):
            timeout_value = None
        else:
            timeout_value = float(timeout_value)

        self.settings = GeminiSettings(
            base_url=str(sum_cfg.get("base_url", "https://generativelanguage.googleapis.com/v1beta")).rstrip("/"),
            model_name=str(sum_cfg.get("model_name", "gemini-3.1-flash-lite-preview")),
            fallback_model_name=str(sum_cfg.get("fallback_model_name", "gemini-2.5-flash")).strip(),
            fallback_cooldown_seconds=float(sum_cfg.get("fallback_cooldown_seconds", 60.0)),
            api_key=api_key,
            timeout=timeout_value,
            prompt_version=str(sum_cfg.get("prompt_version", "v3")),
            temperature=float(sum_cfg.get("temperature", 0.1)),
            max_output_tokens=int(sum_cfg.get("max_output_tokens", 3072)),
            max_chunk_chars=int(sum_cfg.get("max_chunk_chars", 28000)),
            overlap_chars=int(sum_cfg.get("overlap_chars", 400)),
            max_chunks_per_transcript=int(sum_cfg.get("max_chunks_per_transcript", 5)),
            combine_batch_size=int(sum_cfg.get("combine_batch_size", 10)),
            retries_per_call=int(sum_cfg.get("retries_per_call", 2)),
            deadline_safety_seconds=int(gen_cfg.get("deadline_safety_seconds", 20)),
            requests_per_minute_limit=int(sum_cfg.get("requests_per_minute_limit", 14)),
            min_request_interval_seconds=float(sum_cfg.get("min_request_interval_seconds", 4.5)),
            retry_backoff_seconds=float(sum_cfg.get("retry_backoff_seconds", 6.0)),
            retry_backoff_multiplier=float(sum_cfg.get("retry_backoff_multiplier", 1.8)),
            retry_backoff_max_seconds=float(sum_cfg.get("retry_backoff_max_seconds", 45.0)),
        )

        self.rate_limiter = RequestRateLimiter(
            requests_per_minute=self.settings.requests_per_minute_limit,
            min_interval_seconds=self.settings.min_request_interval_seconds,
            logger=logger,
        )

    def healthcheck(self) -> bool:
        if not self.settings.api_key:
            self.logger.warning("Gemini API key is missing")
            return False

        url = self._model_url(self.settings.model_name)
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": "Reply with exactly: ok"}],
                }
            ],
            "generationConfig": {
                "temperature": 0,
                "maxOutputTokens": 8,
            },
        }

        attempts = 3
        for attempt in range(attempts):
            try:
                text = self._post_json(url=url, payload=payload, timeout=20)
                return bool(text)
            except Exception as ex:
                self.logger.warning(
                    "Gemini healthcheck failed on attempt %s/%s: %s",
                    attempt + 1,
                    attempts,
                    ex,
                )
                if attempt < attempts - 1:
                    delay = min(3.0 * (attempt + 1), 10.0)
                    self.logger.info("Retrying Gemini healthcheck in %.2f seconds", delay)
                    time.sleep(delay)

        return False

    def _target_language_instruction(self, transcript_language: str | None) -> str:
        normalized = (transcript_language or "").strip().lower()

        if normalized.startswith("it"):
            return "Write the entire output in Italian."
        if normalized.startswith("en"):
            return "Write the entire output in English."
        return "Write the entire output in the same language as the transcript."

    def _effective_timeout(self, hard_deadline: datetime | None) -> float | None:
        base_timeout = self.settings.timeout
        remaining = _seconds_until(hard_deadline)

        if remaining is None:
            return base_timeout

        allowed = remaining - self.settings.deadline_safety_seconds
        if allowed <= 5:
            raise TimeoutError("Not enough time remaining before summary deadline")

        if base_timeout is None:
            return allowed

        return min(base_timeout, allowed)

    def _model_url(self, model_name: str) -> str:
        return f"{self.settings.base_url}/models/{model_name}:generateContent"

    def _post_json(self, *, url: str, payload: dict, timeout: float | None) -> str:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": self.settings.api_key,
            },
            method="POST",
        )

        if timeout is None:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        else:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")

        data = json.loads(raw)

        candidates = data.get("candidates", [])
        if not candidates:
            raise RuntimeError(f"Gemini returned no candidates: {raw[:1000]}")

        content = candidates[0].get("content", {})
        parts = content.get("parts", [])
        texts = [str(part.get("text", "")) for part in parts if part.get("text")]
        response_text = "\n".join(t.strip() for t in texts if t.strip()).strip()

        if not response_text:
            raise RuntimeError(f"Gemini returned empty text: {raw[:1000]}")

        return response_text

    def _sleep_backoff(self, attempt: int) -> None:
        delay = min(
            self.settings.retry_backoff_seconds * (self.settings.retry_backoff_multiplier ** attempt),
            self.settings.retry_backoff_max_seconds,
        )
        self.logger.info("Gemini retry backoff sleeping %.2f seconds", delay)
        time.sleep(delay)

    def _sleep_retry_delay(
        self,
        *,
        attempt: int,
        hard_deadline: datetime | None,
        retry_after_seconds: float | None = None,
        quota_metric: str | None = None,
    ) -> bool:
        delay = min(
            self.settings.retry_backoff_seconds * (self.settings.retry_backoff_multiplier ** attempt),
            self.settings.retry_backoff_max_seconds,
        )
        if retry_after_seconds is not None:
            delay = max(delay, retry_after_seconds + 0.5)

        remaining = _seconds_until(hard_deadline)
        if remaining is not None and delay >= max(0.0, remaining - self.settings.deadline_safety_seconds):
            self.logger.warning(
                "Skipping Gemini retry because waiting %.2fs would exceed the summary deadline%s",
                delay,
                f" (quota_metric={quota_metric})" if quota_metric else "",
            )
            return False

        if retry_after_seconds is not None:
            self.logger.info(
                "Gemini retry sleeping %.2f seconds using server hint%s",
                delay,
                f" (quota_metric={quota_metric})" if quota_metric else "",
            )
        else:
            self.logger.info(
                "Gemini retry backoff sleeping %.2f seconds%s",
                delay,
                f" (quota_metric={quota_metric})" if quota_metric else "",
            )

        time.sleep(delay)
        return True

    def _call_gemini(
        self,
        *,
        prompt: str,
        stage_label: str,
        hard_deadline: datetime | None = None,
        model_name: str | None = None,
    ) -> str:
        last_error = None
        selected_model = (model_name or self.settings.model_name).strip() or self.settings.model_name
        url = self._model_url(selected_model)

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": {
                "temperature": self.settings.temperature,
                "maxOutputTokens": self.settings.max_output_tokens,
            },
        }

        for attempt in range(self.settings.retries_per_call + 1):
            try:
                timeout = self._effective_timeout(hard_deadline)
                self.rate_limiter.wait_for_slot()

                self.logger.info(
                    "Sending transcript to Gemini model=%s timeout=%s prompt_version=%s stage=%s attempt=%s/%s",
                    selected_model,
                    timeout,
                    self.settings.prompt_version,
                    stage_label,
                    attempt + 1,
                    self.settings.retries_per_call + 1,
                )

                return self._post_json(url=url, payload=payload, timeout=timeout)

            except urllib.error.HTTPError as ex:
                last_error = ex
                body = ""
                try:
                    body = ex.read().decode("utf-8", errors="replace")
                except Exception:
                    body = ""
                retry_after_seconds = _parse_retry_after_seconds(body)
                quota_metric = _extract_quota_metric(body)
                quota_exceeded = ex.code == 429 and _is_quota_exceeded(body)
                high_demand_unavailable = ex.code == 503 and _is_high_demand_unavailable(body)
                self.logger.warning(
                    "Gemini HTTP error at stage=%s attempt=%s/%s: %s %s",
                    stage_label,
                    attempt + 1,
                    self.settings.retries_per_call + 1,
                    ex,
                    body[:500],
                )
                if high_demand_unavailable and selected_model == self.settings.model_name and self.settings.fallback_model_name:
                    raise HighDemandModelError(selected_model, stage_label)
                if attempt < self.settings.retries_per_call:
                    if quota_exceeded and retry_after_seconds is None:
                        self.logger.warning(
                            "Gemini quota error at stage=%s has no retry hint; aborting retries%s",
                            stage_label,
                            f" (quota_metric={quota_metric})" if quota_metric else "",
                        )
                        break
                    if not self._sleep_retry_delay(
                        attempt=attempt,
                        hard_deadline=hard_deadline,
                        retry_after_seconds=retry_after_seconds,
                        quota_metric=quota_metric,
                    ):
                        break

            except Exception as ex:
                last_error = ex
                self.logger.warning(
                    "Gemini failed at stage=%s attempt=%s/%s: %s",
                    stage_label,
                    attempt + 1,
                    self.settings.retries_per_call + 1,
                    ex,
                )
                if attempt < self.settings.retries_per_call:
                    self._sleep_backoff(attempt)

        assert last_error is not None
        raise last_error

    def _call_with_summary_model_fallback(
        self,
        *,
        prompt: str,
        stage_label: str,
        hard_deadline: datetime | None,
        active_model: str,
    ) -> tuple[str, str, bool]:
        try:
            return (
                self._call_gemini(
                    prompt=prompt,
                    stage_label=stage_label,
                    hard_deadline=hard_deadline,
                    model_name=active_model,
                ),
                active_model,
                False,
            )
        except HighDemandModelError:
            fallback_model = self.settings.fallback_model_name.strip()
            if not fallback_model or active_model == fallback_model:
                raise
            self.logger.warning(
                "Primary Gemini model %s is under high demand at stage=%s. Falling back to %s for the rest of this summary.",
                active_model,
                stage_label,
                fallback_model,
            )
            return (
                self._call_gemini(
                    prompt=prompt,
                    stage_label=stage_label,
                    hard_deadline=hard_deadline,
                    model_name=fallback_model,
                ),
                fallback_model,
                True,
            )

    def _build_chunk_prompt(
        self,
        *,
        title: str | None,
        channel_name: str | None,
        transcript_language: str | None,
        chunk_index: int,
        total_chunks: int,
        chunk_text: str,
    ) -> str:
        language_rule = self._target_language_instruction(transcript_language)

        return f"""
You are summarizing one chunk of a YouTube transcript.

Rules:
- Be faithful to the transcript.
- Do not invent facts.
- Keep names, places, dates, and claims accurate.
- Keep the summary dense and useful.
- Keep the structure consistent so later merging is easy.
- {language_rule}

Output exactly in this format:

## Chunk Summary
A concise but informative summary of this chunk.

## Key Points
- bullet
- bullet
- bullet
- bullet

Metadata:
- Video title: {title or ""}
- Channel: {channel_name or ""}
- Transcript language hint: {transcript_language or ""}
- Chunk: {chunk_index} of {total_chunks}

Transcript chunk:
\"\"\"
{chunk_text}
\"\"\"
""".strip()

    def _build_final_prompt(
        self,
        *,
        title: str | None,
        channel_name: str | None,
        transcript_language: str | None,
        partial_summaries: list[str],
    ) -> str:
        language_rule = self._target_language_instruction(transcript_language)
        joined = "\n\n".join(
            f"### Partial Summary {idx + 1}\n{item}" for idx, item in enumerate(partial_summaries)
        )

        return f"""
You are combining partial summaries of a YouTube transcript into one final, normalized output.

Rules:
- Be faithful to the transcript.
- Merge overlaps and duplicates.
- Preserve chronology when useful.
- Do not invent details.
- Make the output complete and consistent.
- Do NOT return a chunk-style answer.
- Always return the same normalized structure.
- {language_rule}

Output exactly in this format:

# Summary
Write 2 to 4 solid paragraphs.

# Key Points
- bullet
- bullet
- bullet
- bullet
- bullet

# Main Themes
- bullet
- bullet
- bullet

# Notable Details
- bullet
- bullet
- bullet

Metadata:
- Video title: {title or ""}
- Channel: {channel_name or ""}
- Transcript language hint: {transcript_language or ""}

Partial summaries:
{joined}
""".strip()

    def _build_repair_prompt(
        self,
        *,
        title: str | None,
        channel_name: str | None,
        transcript_language: str | None,
        draft_summary: str,
    ) -> str:
        language_rule = self._target_language_instruction(transcript_language)

        return f"""
Rewrite the following draft into the required final normalized format.

Rules:
- Keep the meaning and facts.
- Do not invent details.
- Do NOT return a chunk-style answer.
- Always return the full normalized structure.
- {language_rule}

Output exactly in this format:

# Summary
Write 2 to 4 solid paragraphs.

# Key Points
- bullet
- bullet
- bullet
- bullet
- bullet

# Main Themes
- bullet
- bullet
- bullet

# Notable Details
- bullet
- bullet
- bullet

Metadata:
- Video title: {title or ""}
- Channel: {channel_name or ""}
- Transcript language hint: {transcript_language or ""}

Draft:
\"\"\"
{draft_summary}
\"\"\"
""".strip()

    def _is_normalized_final_format(self, text: str) -> bool:
        required_headers = [
            "# Summary",
            "# Key Points",
            "# Main Themes",
            "# Notable Details",
        ]
        return all(header in text for header in required_headers)

    def summarize(
        self,
        *,
        transcript_text: str,
        title: str | None = None,
        channel_name: str | None = None,
        transcript_language: str | None = None,
        hard_deadline: datetime | None = None,
    ) -> str:
        cleaned = _clean_text(transcript_text)
        if not cleaned:
            raise RuntimeError("Transcript text is empty")

        chunks = _split_text_into_chunks(
            cleaned,
            max_chunk_chars=self.settings.max_chunk_chars,
            overlap_chars=self.settings.overlap_chars,
            max_chunks=self.settings.max_chunks_per_transcript,
        )

        self.logger.info(
            "Prepared %s chunk(s) for title=%s primary_model=%s fallback_model=%s deadline=%s rpm_limit=%s min_interval=%.2fs transcript_language=%s",
            len(chunks),
            title,
            self.settings.model_name,
            self.settings.fallback_model_name,
            hard_deadline.isoformat() if hard_deadline else None,
            self.settings.requests_per_minute_limit,
            self.settings.min_request_interval_seconds,
            transcript_language,
        )

        partial_summaries: list[str] = []
        active_model = self.settings.model_name
        used_fallback_for_summary = False

        for idx, chunk in enumerate(chunks, start=1):
            remaining = _seconds_until(hard_deadline)
            if remaining is not None and remaining <= self.settings.deadline_safety_seconds + 5:
                raise TimeoutError("Summary deadline reached before chunk completion")

            prompt = self._build_chunk_prompt(
                title=title,
                channel_name=channel_name,
                transcript_language=transcript_language,
                chunk_index=idx,
                total_chunks=len(chunks),
                chunk_text=chunk,
            )
            partial, active_model, used_fallback_now = self._call_with_summary_model_fallback(
                prompt=prompt,
                stage_label=f"chunk-{idx}-of-{len(chunks)}",
                hard_deadline=hard_deadline,
                active_model=active_model,
            )
            used_fallback_for_summary = used_fallback_for_summary or used_fallback_now
            partial_summaries.append(partial.strip())

        remaining = _seconds_until(hard_deadline)
        if remaining is not None and remaining <= self.settings.deadline_safety_seconds + 5:
            raise TimeoutError("Summary deadline reached before final combine")

        final_prompt = self._build_final_prompt(
            title=title,
            channel_name=channel_name,
            transcript_language=transcript_language,
            partial_summaries=partial_summaries,
        )

        final_text, active_model, used_fallback_now = self._call_with_summary_model_fallback(
            prompt=final_prompt,
            stage_label="final-combine",
            hard_deadline=hard_deadline,
            active_model=active_model,
        )
        used_fallback_for_summary = used_fallback_for_summary or used_fallback_now
        final_text = final_text.strip()

        if not self._is_normalized_final_format(final_text):
            self.logger.info("Final summary format was inconsistent, running repair pass")
            repair_prompt = self._build_repair_prompt(
                title=title,
                channel_name=channel_name,
                transcript_language=transcript_language,
                draft_summary=final_text,
            )
            final_text, active_model, used_fallback_now = self._call_with_summary_model_fallback(
                prompt=repair_prompt,
                stage_label="repair-format",
                hard_deadline=hard_deadline,
                active_model=active_model,
            )
            used_fallback_for_summary = used_fallback_for_summary or used_fallback_now
            final_text = final_text.strip()

        if not self._is_normalized_final_format(final_text):
            raise RuntimeError("Gemini returned a non-normalized final summary format")

        if used_fallback_for_summary and self.settings.fallback_cooldown_seconds > 0:
            cooldown = self.settings.fallback_cooldown_seconds
            remaining = _seconds_until(hard_deadline)
            if remaining is None:
                effective_cooldown = cooldown
            else:
                effective_cooldown = min(cooldown, max(0.0, remaining - self.settings.deadline_safety_seconds))
            if effective_cooldown > 0:
                self.logger.info(
                    "Summary completed with fallback model %s. Cooling down %.2f seconds before the next summary retries %s.",
                    active_model,
                    effective_cooldown,
                    self.settings.model_name,
                )
                time.sleep(effective_cooldown)

        return final_text

    def save_summary_file(self, summaries_dir: str, transcript_stem: str, summary_text: str) -> str:
        out_dir = Path(summaries_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{transcript_stem}.md"
        out_path.write_text(summary_text, encoding="utf-8")
        return str(out_path)