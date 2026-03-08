from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


class StreamerStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> list[str]:
        if not self.path.exists():
            return []

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("streamers store must contain a JSON list")
        return sorted({str(item).strip().lower() for item in payload if str(item).strip()})

    def save(self, streamers: list[str]) -> None:
        normalized = sorted({name.strip().lower() for name in streamers if name.strip()})
        self.path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")


@dataclass(slots=True)
class TrackedRecording:
    channel: str
    source_file_path: str
    metadata_path: str | None = None
    watchable_file_path: str | None = None
    watchable_state: str = "pending"
    ad_break_count: int = 0
    source_mode: str = "unauthenticated"
    started_at: str | None = None
    ended_at: str | None = None
    state: str | None = None
    clean_output_error: str | None = None

    @property
    def file_path(self) -> str:
        # Backward compatibility for old call sites.
        return self.source_file_path


class RecordingHistoryStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> list[TrackedRecording]:
        if not self.path.exists():
            return []

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("recordings store must contain a JSON list")

        entries: list[TrackedRecording] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            channel = str(item.get("channel", "")).strip().lower()
            source_file_path = str(
                item.get("source_file_path") or item.get("file_path") or ""
            ).strip()
            if not (channel and source_file_path):
                continue

            has_watchable_fields = "watchable_state" in item or "watchable_file_path" in item
            watchable_state = str(item.get("watchable_state", "pending")).strip() or "pending"
            try:
                ad_break_count = int(item.get("ad_break_count", 0))
            except (TypeError, ValueError):
                ad_break_count = 0

            metadata_value = item.get("metadata_path")
            watchable_value = item.get("watchable_file_path")
            if not has_watchable_fields:
                watchable_value = source_file_path
                watchable_state = "ready"
            started_at = item.get("started_at")
            ended_at = item.get("ended_at")
            state = item.get("state")
            clean_output_error = item.get("clean_output_error")

            entries.append(
                TrackedRecording(
                    channel=channel,
                    source_file_path=source_file_path,
                    metadata_path=str(metadata_value).strip() if metadata_value else None,
                    watchable_file_path=str(watchable_value).strip() if watchable_value else None,
                    watchable_state=watchable_state,
                    ad_break_count=max(0, ad_break_count),
                    source_mode=str(item.get("source_mode", "unauthenticated")).strip()
                    or "unauthenticated",
                    started_at=str(started_at).strip() if started_at else None,
                    ended_at=str(ended_at).strip() if ended_at else None,
                    state=str(state).strip() if state else None,
                    clean_output_error=str(clean_output_error).strip() if clean_output_error else None,
                )
            )
        return entries

    def save(self, recordings: list[TrackedRecording]) -> None:
        self.path.write_text(
            json.dumps([asdict(recording) for recording in recordings], indent=2),
            encoding="utf-8",
        )

    def upsert(self, recording: TrackedRecording) -> None:
        recordings = [
            item for item in self.load() if item.source_file_path != recording.source_file_path
        ]
        recordings.insert(0, recording)
        self.save(recordings)
