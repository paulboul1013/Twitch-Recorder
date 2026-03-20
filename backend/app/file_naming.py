from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path


def build_recording_output_filename(
    *,
    channel: str,
    started_at: datetime,
    extension: str,
) -> str:
    normalized_channel = str(channel or "recording").strip().lower() or "recording"
    normalized_extension = str(extension or "").strip().lstrip(".").lower() or "ts"
    started = _normalize_timestamp(started_at)
    return f"{normalized_channel}_{started}.{normalized_extension}"


def normalize_channel_directory_name(channel: str) -> str:
    return str(channel or "recording").strip().lower() or "recording"


def build_channel_recording_directory(*, recordings_path: Path, channel: str) -> Path:
    return recordings_path / normalize_channel_directory_name(channel)


def parse_recording_timestamp(value: str | None) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        utc_value = value.replace(tzinfo=UTC)
    else:
        utc_value = value.astimezone(UTC)
    return utc_value.strftime("%Y%m%d_%H%M%S")
