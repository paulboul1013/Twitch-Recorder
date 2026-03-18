from __future__ import annotations

import subprocess
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(slots=True)
class RecordingEvent:
    type: str
    at: datetime
    message: str | None = None

    def as_dict(self) -> dict[str, str]:
        payload: dict[str, str] = {
            "type": self.type,
            "at": self.at.isoformat(),
        }
        if self.message:
            payload["message"] = self.message
        return payload


@dataclass(slots=True)
class ActiveRecording:
    recording_id: str
    artifact_mode: str
    channel: str
    process: subprocess.Popen
    segmenter_process: subprocess.Popen | None
    file_path: Path
    metadata_path: Path
    started_at: datetime
    source_mode: str
    recording_root: Path | None = None
    full_artifact_path: Path | None = None
    clean_artifact_path: Path | None = None
    playlist_url: str | None = None
    playlist_markers_seen: bool = False
    playlist_ad_windows: list[tuple[datetime, datetime]] = field(default_factory=list)
    playlist_poll_error: str | None = None
    playlist_thread: threading.Thread | None = None
    playlist_stop_event: threading.Event = field(default_factory=threading.Event)
    events: list[RecordingEvent] = field(default_factory=list)
    stderr_tail: list[str] = field(default_factory=list)
    ad_break_active: bool = False
    stderr_thread: threading.Thread | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(slots=True)
class RecordingResult:
    recording_id: str
    artifact_mode: str
    channel: str
    file_path: Path
    metadata_path: Path
    started_at: datetime
    ended_at: datetime
    exit_code: int
    state: str
    source_mode: str
    full_artifact_path: str | None
    clean_artifact_path: str | None
    full_segment_count: int
    clean_segment_count: int
    clean_export_state: str
    clean_export_path: str | None
    clean_export_error: str | None
    clean_compact_state: str
    clean_compact_path: str | None
    clean_compact_error: str | None
    unknown_ad_confidence: bool
    clean_output_path: str | None
    clean_output_state: str
    clean_output_error: str | None
    ad_break_count: int
    source_available: bool = True
    source_deleted_on_success: bool = False
    source_delete_error: str | None = None


@dataclass(slots=True)
class WatchableMetadataContext:
    watchable_strategy: str | None = None
    ad_detection_sources: list[str] = field(default_factory=list)
    prepare_mitigation: list[str] = field(default_factory=list)
