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
    channel: str
    process: subprocess.Popen[str]
    file_path: Path
    metadata_path: Path
    started_at: datetime
    source_mode: str
    events: list[RecordingEvent] = field(default_factory=list)
    stderr_tail: list[str] = field(default_factory=list)
    ad_break_active: bool = False
    stderr_thread: threading.Thread | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(slots=True)
class RecordingResult:
    channel: str
    file_path: Path
    metadata_path: Path
    started_at: datetime
    ended_at: datetime
    exit_code: int
    state: str
    source_mode: str
    clean_output_path: str | None
    clean_output_state: str
    clean_output_error: str | None
    ad_break_count: int


@dataclass(slots=True)
class WatchableMetadataContext:
    watchable_strategy: str | None = None
    ad_detection_sources: list[str] = field(default_factory=list)
    prepare_mitigation: list[str] = field(default_factory=list)
