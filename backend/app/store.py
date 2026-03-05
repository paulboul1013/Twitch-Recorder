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
    file_path: str


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
            file_path = str(item.get("file_path", "")).strip()
            if channel and file_path:
                entries.append(TrackedRecording(channel=channel, file_path=file_path))
        return entries

    def save(self, recordings: list[TrackedRecording]) -> None:
        self.path.write_text(
            json.dumps([asdict(recording) for recording in recordings], indent=2),
            encoding="utf-8",
        )

    def upsert(self, recording: TrackedRecording) -> None:
        recordings = [item for item in self.load() if item.file_path != recording.file_path]
        recordings.insert(0, recording)
        self.save(recordings)
