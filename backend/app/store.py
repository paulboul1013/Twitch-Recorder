from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(slots=True, frozen=True)
class StreamerConfig:
    name: str
    enabled_for_recording: bool = True


class StreamerStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def load_configs(self) -> list[StreamerConfig]:
        with self._lock:
            if not self.path.exists():
                return []

            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(payload, list):
                raise ValueError("streamers store must contain a JSON list")
            configs_by_name: dict[str, bool] = {}
            for item in payload:
                if isinstance(item, str):
                    name = item.strip().lower()
                    if name:
                        configs_by_name[name] = True
                    continue
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip().lower()
                if not name:
                    continue
                enabled_for_recording = item.get("enabled_for_recording")
                if not isinstance(enabled_for_recording, bool):
                    enabled_for_recording = True
                configs_by_name[name] = enabled_for_recording
            return [
                StreamerConfig(name=name, enabled_for_recording=enabled_for_recording)
                for name, enabled_for_recording in sorted(configs_by_name.items())
            ]

    def load(self) -> list[str]:
        return [item.name for item in self.load_configs()]

    def save(self, streamers: list[str]) -> None:
        self.save_configs(
            [
                StreamerConfig(name=name, enabled_for_recording=True)
                for name in streamers
            ]
        )

    def save_configs(self, streamers: list[StreamerConfig]) -> None:
        normalized: list[dict[str, object]] = []
        seen: set[str] = set()
        for item in sorted(streamers, key=lambda value: value.name):
            name = item.name.strip().lower()
            if not name or name in seen:
                continue
            seen.add(name)
            normalized.append(
                {
                    "name": name,
                    "enabled_for_recording": bool(item.enabled_for_recording),
                }
            )
        with self._lock:
            _write_text_atomic(
                self.path,
                json.dumps(normalized, indent=2),
            )


@dataclass(slots=True)
class TrackedRecording:
    channel: str
    source_file_path: str
    recording_id: str = ""
    artifact_mode: str = "legacy"
    metadata_path: str | None = None
    watchable_file_path: str | None = None
    watchable_state: str = "pending"
    ad_break_count: int = 0
    source_mode: str = "unauthenticated"
    started_at: str | None = None
    ended_at: str | None = None
    state: str | None = None
    clean_output_error: str | None = None
    source_available: bool = True
    source_deleted_on_success: bool = False
    source_delete_error: str | None = None
    full_artifact_path: str | None = None
    clean_artifact_path: str | None = None
    clean_compact_state: str = "none"
    clean_compact_path: str | None = None
    clean_compact_error: str | None = None
    full_segment_count: int = 0
    clean_segment_count: int = 0
    clean_export_state: str = "none"
    clean_export_path: str | None = None
    clean_export_error: str | None = None
    unknown_ad_confidence: bool = False

    def __post_init__(self) -> None:
        if not self.recording_id:
            self.recording_id = _derive_recording_id(
                channel=self.channel,
                source_file_path=self.source_file_path,
            )
        normalized_mode = str(self.artifact_mode or "legacy").strip().lower()
        self.artifact_mode = normalized_mode if normalized_mode in {"legacy", "segment_native"} else "legacy"
        if not self.full_artifact_path:
            self.full_artifact_path = self.source_file_path
        if not self.clean_artifact_path and self.watchable_file_path:
            self.clean_artifact_path = self.watchable_file_path
        try:
            self.full_segment_count = max(0, int(self.full_segment_count))
        except (TypeError, ValueError):
            self.full_segment_count = 0
        try:
            self.clean_segment_count = max(0, int(self.clean_segment_count))
        except (TypeError, ValueError):
            self.clean_segment_count = 0
        normalized_compact_state = str(self.clean_compact_state or "none").strip().lower()
        if normalized_compact_state not in {"none", "queued", "processing", "ready", "failed"}:
            normalized_compact_state = "none"
        self.clean_compact_state = normalized_compact_state
        normalized_export_state = str(self.clean_export_state or "none").strip().lower()
        if normalized_export_state not in {"none", "queued", "processing", "ready", "failed"}:
            normalized_export_state = "none"
        self.clean_export_state = normalized_export_state
        self.clean_compact_error = str(self.clean_compact_error).strip() if self.clean_compact_error else None
        self.unknown_ad_confidence = bool(self.unknown_ad_confidence)

    @property
    def file_path(self) -> str:
        # Backward compatibility for old call sites.
        return self.source_file_path

    @property
    def effective_full_artifact_path(self) -> str:
        return self.full_artifact_path or self.source_file_path

    @property
    def effective_clean_artifact_path(self) -> str | None:
        return self.clean_artifact_path or self.watchable_file_path


def _derive_recording_id(*, channel: str, source_file_path: str) -> str:
    normalized_channel = str(channel or "recording").strip().lower() or "recording"
    normalized_source = str(source_file_path or "").strip()
    digest = hashlib.sha1(f"{normalized_channel}:{normalized_source}".encode("utf-8")).hexdigest()[:12]
    return f"{normalized_channel}-{digest}"


class RecordingHistoryStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def _load_payload(self) -> list[object]:
        if not self.path.exists():
            return []

        content = self.path.read_text(encoding="utf-8")
        try:
            payload = json.loads(content)
        except json.JSONDecodeError:
            # Recover from trailing garbage caused by interrupted/overlapped writes.
            decoder = json.JSONDecoder()
            stripped = content.lstrip()
            payload, end_index = decoder.raw_decode(stripped)
            trailing = stripped[end_index:].strip()
            if trailing:
                _write_text_atomic(self.path, json.dumps(payload, indent=2))
        if not isinstance(payload, list):
            raise ValueError("recordings store must contain a JSON list")
        return payload

    def load(self) -> list[TrackedRecording]:
        with self._lock:
            payload = self._load_payload()

        entries: list[TrackedRecording] = []
        used_recording_ids: set[str] = set()
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
            recording_id = str(item.get("recording_id", "")).strip()
            if not recording_id:
                recording_id = _derive_recording_id(
                    channel=channel,
                    source_file_path=source_file_path,
                )
            unique_recording_id = recording_id
            suffix = 1
            while unique_recording_id in used_recording_ids:
                suffix += 1
                unique_recording_id = f"{recording_id}-{suffix}"
            used_recording_ids.add(unique_recording_id)

            artifact_mode = str(item.get("artifact_mode", "legacy")).strip().lower() or "legacy"
            if artifact_mode not in {"legacy", "segment_native"}:
                artifact_mode = "legacy"

            full_artifact_value = item.get("full_artifact_path")
            clean_artifact_value = item.get("clean_artifact_path")
            clean_compact_state = str(item.get("clean_compact_state", "none")).strip().lower() or "none"
            if clean_compact_state not in {"none", "queued", "processing", "ready", "failed"}:
                clean_compact_state = "none"
            clean_compact_path = item.get("clean_compact_path")
            clean_compact_error = item.get("clean_compact_error")
            if not full_artifact_value:
                full_artifact_value = source_file_path
            if not clean_artifact_value and watchable_value:
                clean_artifact_value = watchable_value

            full_segment_value = item.get("full_segment_count", 0)
            clean_segment_value = item.get("clean_segment_count", 0)
            try:
                full_segment_count = max(0, int(full_segment_value))
            except (TypeError, ValueError):
                full_segment_count = 0
            try:
                clean_segment_count = max(0, int(clean_segment_value))
            except (TypeError, ValueError):
                clean_segment_count = 0

            clean_export_state = str(item.get("clean_export_state", "none")).strip().lower() or "none"
            if clean_export_state not in {"none", "queued", "processing", "ready", "failed"}:
                clean_export_state = "none"

            clean_export_path = item.get("clean_export_path")
            clean_export_error = item.get("clean_export_error")
            unknown_ad_confidence = bool(item.get("unknown_ad_confidence", False))
            source_available_value = item.get("source_available")
            source_deleted_value = item.get("source_deleted_on_success")
            source_delete_error = item.get("source_delete_error")

            entries.append(
                TrackedRecording(
                    channel=channel,
                    source_file_path=source_file_path,
                    recording_id=unique_recording_id,
                    artifact_mode=artifact_mode,
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
                    source_available=(
                        source_available_value
                        if isinstance(source_available_value, bool)
                        else Path(source_file_path).exists()
                    ),
                    source_deleted_on_success=(
                        source_deleted_value if isinstance(source_deleted_value, bool) else False
                    ),
                    source_delete_error=(
                        str(source_delete_error).strip() if source_delete_error else None
                    ),
                    full_artifact_path=(
                        str(full_artifact_value).strip() if full_artifact_value else None
                    ),
                    clean_artifact_path=(
                        str(clean_artifact_value).strip() if clean_artifact_value else None
                    ),
                    clean_compact_state=clean_compact_state,
                    clean_compact_path=(
                        str(clean_compact_path).strip() if clean_compact_path else None
                    ),
                    clean_compact_error=(
                        str(clean_compact_error).strip() if clean_compact_error else None
                    ),
                    full_segment_count=full_segment_count,
                    clean_segment_count=clean_segment_count,
                    clean_export_state=clean_export_state,
                    clean_export_path=(
                        str(clean_export_path).strip() if clean_export_path else None
                    ),
                    clean_export_error=(
                        str(clean_export_error).strip() if clean_export_error else None
                    ),
                    unknown_ad_confidence=unknown_ad_confidence,
                )
            )
        return entries

    def save(self, recordings: list[TrackedRecording]) -> None:
        with self._lock:
            _write_text_atomic(
                self.path,
                json.dumps([asdict(recording) for recording in recordings], indent=2),
            )

    def upsert(self, recording: TrackedRecording) -> None:
        with self._lock:
            recordings = [
                item
                for item in self.load()
                if item.recording_id != recording.recording_id
                and item.source_file_path != recording.source_file_path
            ]
            recordings.insert(0, recording)
            self.save(recordings)


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        try:
            os.chmod(path, 0o644)
        except OSError:
            pass
    finally:
        tmp_path = Path(tmp_name)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
