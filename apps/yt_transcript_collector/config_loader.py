from __future__ import annotations

import os
import re
from pathlib import Path

try:
    import yaml  # type: ignore
except ModuleNotFoundError:
    yaml = None


def _parse_scalar(value: str):
    value = value.strip()
    if value == "":
        return ""
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1]
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        return value[1:-1]

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None

    try:
        if any(ch in value for ch in [".", "e", "E"]):
            return float(value)
        return int(value)
    except ValueError:
        return value


def _prepare_lines(text: str) -> list[tuple[int, str]]:
    prepared: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        if not raw_line.strip():
            continue
        stripped = raw_line.lstrip(" ")
        if stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(stripped)
        prepared.append((indent, stripped.rstrip()))
    return prepared


def _parse_block(lines: list[tuple[int, str]], index: int, indent: int):
    if index >= len(lines):
        return {}, index

    current_indent, current_content = lines[index]
    if current_indent != indent:
        return {}, index

    if current_content.startswith("- "):
        return _parse_list(lines, index, indent)
    return _parse_dict(lines, index, indent)


def _parse_dict(lines: list[tuple[int, str]], index: int, indent: int):
    result: dict = {}

    while index < len(lines):
        current_indent, content = lines[index]
        if current_indent < indent:
            break
        if current_indent != indent or content.startswith("- "):
            break

        key, sep, remainder = content.partition(":")
        if not sep:
            raise ValueError(f"Invalid YAML line: {content}")

        key = key.strip()
        remainder = remainder.strip()
        index += 1

        if remainder:
            result[key] = _parse_scalar(remainder)
            continue

        if index >= len(lines) or lines[index][0] <= current_indent:
            result[key] = {}
            continue

        nested, index = _parse_block(lines, index, lines[index][0])
        result[key] = nested

    return result, index


def _parse_list(lines: list[tuple[int, str]], index: int, indent: int):
    items: list = []

    while index < len(lines):
        current_indent, content = lines[index]
        if current_indent < indent:
            break
        if current_indent != indent or not content.startswith("- "):
            break

        payload = content[2:].strip()
        index += 1

        if payload == "":
            if index < len(lines) and lines[index][0] > current_indent:
                nested, index = _parse_block(lines, index, lines[index][0])
                items.append(nested)
            else:
                items.append(None)
            continue

        if ":" in payload:
            key, sep, remainder = payload.partition(":")
            item = {key.strip(): _parse_scalar(remainder.strip()) if remainder.strip() else {}}
            if index < len(lines) and lines[index][0] > current_indent:
                nested, index = _parse_block(lines, index, lines[index][0])
                if isinstance(nested, dict):
                    item.update(nested)
            items.append(item)
            continue

        items.append(_parse_scalar(payload))

    return items, index


def _fallback_safe_load(text: str) -> dict:
    lines = _prepare_lines(text)
    if not lines:
        return {}
    data, index = _parse_block(lines, 0, lines[0][0])
    if index < len(lines):
        raise ValueError("Could not parse full YAML content")
    if not isinstance(data, dict):
        raise ValueError("Top-level YAML must be a mapping")
    return data


def _expand_env_placeholders(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        return os.environ.get(key, match.group(0))

    return re.sub(r"\$\{([^}]+)\}", repl, value)


def _expand_value(value):
    if isinstance(value, str):
        expanded = _expand_env_placeholders(value)
        return os.path.expanduser(os.path.expandvars(expanded))
    if isinstance(value, list):
        return [_expand_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_value(item) for key, item in value.items()}
    return value


def load_yaml(path: str | Path) -> dict:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(text) or {}
    else:
        data = _fallback_safe_load(text)
    return _expand_value(data)


def load_config(config_path: str | Path) -> dict:
    return load_yaml(config_path)


def load_channels(channels_path: str | Path) -> list[dict]:
    data = load_yaml(channels_path)
    return data.get("channels", [])
