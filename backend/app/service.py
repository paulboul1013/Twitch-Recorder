from __future__ import annotations

import asyncio
import json
import shutil
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import httpx

from .clean_export import CleanExportJob, CleanExportManager
from .config import Settings
from .file_naming import build_recording_output_filename, parse_recording_timestamp
from .models import (
    CleanExportStatusResponse,
    RecordingDirectoryDeleteResponse,
    RecordingDirectoryInfo,
    RecordingInfo,
    StartRecordingResponse,
    StopRecordingResponse,
    StreamStatus,
    StreamerInfo,
)
from .recorder import RecorderManager, RecordingResult
from .store import RecordingHistoryStore, StreamerConfig, StreamerStore, TrackedRecording
from .twitch import TwitchAuthError, TwitchClient


class MonitorService:
    def __init__(
        self,
        settings: Settings,
        store: StreamerStore,
        recording_store: RecordingHistoryStore,
        twitch_client: TwitchClient,
        recorder: RecorderManager,
    ) -> None:
        self.settings = settings
        self.store = store
        self.recording_store = recording_store
        self.twitch_client = twitch_client
        self.recorder = recorder
        streamer_configs = self.store.load_configs()
        self._streamers = [item.name for item in streamer_configs]
        self._recording_enabled_by_streamer: dict[str, bool] = {
            item.name: item.enabled_for_recording for item in streamer_configs
        }
        self._statuses: dict[str, StreamStatus] = {
            name: StreamStatus(name=name) for name in self._streamers
        }
        migrated_recordings = self.recording_store.load()
        self.recording_store.save(migrated_recordings)
        self._manually_stopped: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._recorder_lock = asyncio.Lock()
        self._clean_export_manager = CleanExportManager(
            max_concurrency=self.settings.clean_export_max_concurrency,
            on_state_change=self._on_clean_export_state_change,
        )

    def _is_recording_enabled(self, name: str) -> bool:
        return self._recording_enabled_by_streamer.get(name, True)

    def _save_streamers(self) -> None:
        self.store.save_configs(
            [
                StreamerConfig(
                    name=name,
                    enabled_for_recording=self._is_recording_enabled(name),
                )
                for name in self._streamers
            ]
        )

    def _watchable_state_from_tracked_recording(self, tracked: TrackedRecording) -> str:
        if tracked.artifact_mode != "segment_native":
            return tracked.watchable_state
        if tracked.clean_export_state == "failed":
            return "failed"
        if tracked.clean_export_state in {"queued", "processing"}:
            return "processing"
        if tracked.clean_export_state == "ready":
            return "ready"
        if tracked.clean_compact_state == "ready":
            return "ready"
        return tracked.watchable_state

    def _clean_compact_path_if_ready(self, tracked: TrackedRecording) -> Path | None:
        if tracked.clean_compact_state != "ready" or not tracked.clean_compact_path:
            return None
        compact_path = Path(tracked.clean_compact_path)
        if compact_path.exists() and compact_path.is_file():
            return compact_path
        return None

    def _segment_native_watchable_path(self, tracked: TrackedRecording) -> Path | None:
        if tracked.clean_export_state == "ready" and tracked.clean_export_path:
            export_path = Path(tracked.clean_export_path)
            if export_path.exists() and export_path.is_file():
                return export_path
        compact_path = self._clean_compact_path_if_ready(tracked)
        if compact_path is not None:
            return compact_path
        clean_artifact_value = tracked.effective_clean_artifact_path
        if clean_artifact_value:
            clean_artifact_path = Path(clean_artifact_value)
            if clean_artifact_path.exists() and clean_artifact_path.is_file():
                return clean_artifact_path
        source_path = Path(tracked.source_file_path)
        if source_path.exists() and source_path.is_file():
            return source_path
        return None

    def _segment_native_stat_path(self, tracked: TrackedRecording) -> Path | None:
        if tracked.clean_export_state == "ready" and tracked.clean_export_path:
            export_path = Path(tracked.clean_export_path)
            if export_path.exists() and export_path.is_file():
                return export_path
        compact_path = self._clean_compact_path_if_ready(tracked)
        if compact_path is not None:
            return compact_path
        clean_artifact_value = tracked.effective_clean_artifact_path
        if clean_artifact_value:
            clean_artifact_path = Path(clean_artifact_value)
            if clean_artifact_path.exists() and clean_artifact_path.is_file():
                return clean_artifact_path
        source_path = Path(tracked.source_file_path)
        if source_path.exists() and source_path.is_file():
            return source_path
        return None

    def _on_clean_export_state_change(self, job: CleanExportJob) -> None:
        tracked_recordings = self.recording_store.load()
        updated = False
        metadata_path: Path | None = None
        for tracked in tracked_recordings:
            if tracked.recording_id != job.recording_id:
                continue
            tracked.clean_export_state = job.state
            tracked.clean_export_path = job.output_path if job.state == "ready" else None
            tracked.clean_export_error = job.error
            tracked.watchable_state = self._watchable_state_from_tracked_recording(tracked)
            if job.state == "ready":
                tracked.watchable_file_path = job.output_path
            metadata_path = Path(tracked.metadata_path) if tracked.metadata_path else None
            updated = True
            break
        if updated:
            self.recording_store.save(tracked_recordings)
            self._update_recording_metadata_export_fields(
                metadata_path=metadata_path,
                state=job.state,
                output_path=job.output_path if job.state == "ready" else None,
                error=job.error,
            )

    def _update_recording_metadata_export_fields(
        self,
        *,
        metadata_path: Path | None,
        state: str,
        output_path: str | None,
        error: str | None,
    ) -> None:
        if metadata_path is None or not metadata_path.exists() or not metadata_path.is_file():
            return
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(payload, dict):
            return
        payload["clean_export_state"] = state
        payload["clean_export_path"] = output_path
        payload["clean_export_dir_path"] = str(Path(output_path).parent) if output_path else None
        payload["clean_export_error"] = error
        try:
            metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except OSError:
            return

    def _clean_export_dir_path(
        self,
        *,
        artifact_mode: str,
        source_file_path: str,
        full_artifact_path: str | None,
        clean_artifact_path: str | None,
        clean_export_path: str | None,
        clean_compact_path: str | None = None,
    ) -> str | None:
        if artifact_mode != "segment_native":
            return None
        candidate_path: Path | None = None
        for candidate in (
            clean_export_path,
            clean_compact_path,
            clean_artifact_path,
            full_artifact_path,
            source_file_path,
        ):
            if candidate:
                candidate_path = Path(candidate)
                break
        if candidate_path is None:
            return None
        if candidate_path.parent.name in {"exports", "manifests", "segments"}:
            return str((candidate_path.parent.parent / "exports").resolve())
        return str((candidate_path.parent / "exports").resolve())

    @staticmethod
    def _optional_text(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.lower() == "none":
            return None
        return text

    def _should_auto_create_clean_export(self, tracked: TrackedRecording) -> bool:
        if tracked.artifact_mode != "segment_native":
            return False
        state = str(tracked.state or "").strip().lower()
        if state not in {"completed", "stopped"}:
            return False
        if tracked.clean_export_state in {"queued", "processing", "ready"}:
            if tracked.clean_export_state != "ready":
                return False
            if tracked.clean_export_path and Path(tracked.clean_export_path).exists():
                return False
        compact_path = self._clean_compact_path_if_ready(tracked)
        if compact_path is not None:
            return True
        return tracked.clean_segment_count > 0

    def _auto_create_clean_export_if_needed(self, recording_id: str) -> None:
        tracked = self._find_recording_by_id(recording_id)
        if tracked is None or not self._should_auto_create_clean_export(tracked):
            return
        try:
            self.create_clean_export(recording_id, mode="auto")
        except (ValueError, RuntimeError, FileNotFoundError):
            return

    def _apply_recording_result(self, result: RecordingResult) -> None:
        status = self._statuses.get(result.channel)
        if status is not None:
            current_output_path = self.recorder.current_output_path(result.channel)
            result_output_path = result.full_artifact_path or str(result.file_path)
            is_stale_for_active_status = (
                self.recorder.is_recording(result.channel)
                and current_output_path is not None
                and current_output_path != result_output_path
            )
            if not is_stale_for_active_status:
                status.is_recording = False
                status.output_path = result_output_path
                status.recording_state = result.state
                status.recording_started_at = result.started_at
                status.recording_ended_at = result.ended_at
                status.recording_exit_code = result.exit_code
                status.stop_after_at = None
                if result.state == "failed":
                    status.last_error = f"recording process exited with code {result.exit_code}"
                elif result.clean_output_state == "failed" and result.clean_output_error:
                    status.last_error = result.clean_output_error
                else:
                    status.last_error = None
        tracked = TrackedRecording(
            channel=result.channel,
            source_file_path=str(result.file_path),
            recording_id=result.recording_id,
            artifact_mode=result.artifact_mode,
            metadata_path=str(result.metadata_path),
            watchable_file_path=(
                result.clean_export_path
                if result.clean_export_path
                else result.clean_compact_path
                if result.clean_compact_state == "ready" and result.clean_compact_path
                else result.clean_artifact_path or result.clean_output_path
            ),
            watchable_state=result.clean_output_state,
            ad_break_count=result.ad_break_count,
            source_mode=result.source_mode,
            started_at=result.started_at.isoformat(),
            ended_at=result.ended_at.isoformat(),
            state=result.state,
            clean_output_error=result.clean_output_error,
            source_available=result.source_available,
            source_deleted_on_success=result.source_deleted_on_success,
            source_delete_error=result.source_delete_error,
            full_artifact_path=result.full_artifact_path,
            clean_artifact_path=result.clean_artifact_path,
            clean_compact_state=result.clean_compact_state,
            clean_compact_path=result.clean_compact_path,
            clean_compact_error=result.clean_compact_error,
            full_segment_count=result.full_segment_count,
            clean_segment_count=result.clean_segment_count,
            clean_export_state=result.clean_export_state,
            clean_export_path=result.clean_export_path,
            clean_export_error=result.clean_export_error,
            unknown_ad_confidence=result.unknown_ad_confidence,
        )
        self.recording_store.upsert(tracked)
        self._auto_create_clean_export_if_needed(result.recording_id)

    def _apply_finished_recordings(self, results: list[RecordingResult]) -> None:
        for result in results:
            self._apply_recording_result(result)

    async def _sync_finished_recordings(self) -> None:
        async with self._recorder_lock:
            results = self.recorder.poll()
        self._apply_finished_recordings(results)

    def _sync_active_recording_fields(self, status: StreamStatus) -> None:
        status.is_recording = self.recorder.is_recording(status.name)
        current_output_path = self.recorder.current_output_path(status.name)
        if current_output_path is not None:
            status.output_path = current_output_path
        if status.is_recording and status.recording_state != "grace_period":
            status.recording_state = (
                "ad_break" if self.recorder.is_in_ad_break(status.name) else "recording"
            )

    async def _handle_offline_recording(self, name: str, status: StreamStatus, now: datetime) -> None:
        if not self.recorder.has_session(name):
            status.is_recording = False
            status.stop_after_at = None
            return

        if status.offline_since is None:
            status.offline_since = now

        status.stop_after_at = status.offline_since + timedelta(
            seconds=self.settings.offline_grace_period_seconds
        )
        status.recording_state = "grace_period"

        if now >= status.stop_after_at:
            async with self._recorder_lock:
                result = self.recorder.stop_recording(name)
            if result is not None:
                self._apply_recording_result(result)
                status.recording_state = "stopped"
                status.last_error = None
        else:
            status.is_recording = True
            status.output_path = self.recorder.current_output_path(name)

    def _start_delay_remaining_seconds(
        self, stream_started_at: datetime | None, *, now: datetime
    ) -> int:
        delay_seconds = self.settings.recording_start_delay_seconds
        if delay_seconds <= 0 or stream_started_at is None:
            return 0
        start_deadline = stream_started_at + timedelta(seconds=delay_seconds)
        remaining = (start_deadline - now).total_seconds()
        if remaining <= 0:
            return 0
        return int(remaining + 0.999)

    async def start(self) -> None:
        if self._task is None:
            self._recover_pending_clean_export_jobs()
            self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        async with self._recorder_lock:
            results = self.recorder.stop_all(wait_for_finalize=True)
        self._apply_finished_recordings(results)
        self._clean_export_manager.shutdown()

    def list_streamers(self) -> list[StreamerInfo]:
        return [
            StreamerInfo(name=name, enabled_for_recording=self._is_recording_enabled(name))
            for name in self._streamers
        ]

    async def add_streamer(self, name: str, *, enabled_for_recording: bool = True) -> StreamerInfo:
        normalized = name.strip().lower()
        async with self._lock:
            if normalized not in self._streamers:
                self._streamers.append(normalized)
                self._streamers.sort()
                self._recording_enabled_by_streamer[normalized] = enabled_for_recording
                self._save_streamers()
            self._statuses.setdefault(
                normalized,
                StreamStatus(
                    name=normalized,
                    enabled_for_recording=self._is_recording_enabled(normalized),
                ),
            )
        return StreamerInfo(
            name=normalized,
            enabled_for_recording=self._is_recording_enabled(normalized),
        )

    async def remove_streamer(self, name: str) -> None:
        normalized = name.strip().lower()
        async with self._lock:
            async with self._recorder_lock:
                result = self.recorder.stop_recording(normalized)
            if result is not None:
                self._apply_recording_result(result)
            self._streamers = [streamer for streamer in self._streamers if streamer != normalized]
            self._recording_enabled_by_streamer.pop(normalized, None)
            self._save_streamers()
            self._statuses.pop(normalized, None)
            self._manually_stopped.discard(normalized)

    async def set_streamer_recording_enabled(self, name: str, enabled_for_recording: bool) -> StreamerInfo:
        normalized = name.strip().lower()
        async with self._lock:
            if normalized not in self._streamers:
                raise ValueError("streamer not found")
            self._recording_enabled_by_streamer[normalized] = enabled_for_recording
            self._save_streamers()
            status = self._statuses.setdefault(normalized, StreamStatus(name=normalized))
            status.enabled_for_recording = enabled_for_recording
            if not enabled_for_recording:
                async with self._recorder_lock:
                    result = self.recorder.stop_recording(normalized)
                if result is not None:
                    self._apply_recording_result(result)
                status.is_recording = False
                status.recording_state = "disabled"
                status.last_error = None
                status.stop_after_at = None
                self._manually_stopped.discard(normalized)
        return StreamerInfo(
            name=normalized,
            enabled_for_recording=enabled_for_recording,
        )

    def _resolve_streamer_recording_root(self, tracked: TrackedRecording) -> Path:
        source_path = Path(tracked.source_file_path)
        return source_path.parent.parent

    def _is_streamer_recording_deletable(
        self,
        tracked: TrackedRecording,
        *,
        channel: str,
        active_recording_ids: set[str],
    ) -> bool:
        if tracked.channel != channel or tracked.artifact_mode != "segment_native":
            return False
        if tracked.recording_id in active_recording_ids:
            return False
        if str(tracked.state or "").strip().lower() == "recording":
            return False
        if tracked.clean_compact_state in {"queued", "processing"}:
            return False
        if tracked.clean_export_state in {"queued", "processing"}:
            return False
        recording_root = self._resolve_streamer_recording_root(tracked)
        return recording_root.exists() and recording_root.is_dir()

    async def list_streamer_recording_directories(self, name: str) -> list[RecordingDirectoryInfo]:
        normalized = name.strip().lower()
        await self._sync_finished_recordings()
        tracked_recordings = self.recording_store.load()
        async with self._recorder_lock:
            active_recording_ids = {
                str(snapshot.get("recording_id", "")).strip()
                for snapshot in self.recorder.list_active_recordings()
                if str(snapshot.get("recording_id", "")).strip()
            }

        directories: list[RecordingDirectoryInfo] = []
        for tracked in tracked_recordings:
            if not self._is_streamer_recording_deletable(
                tracked,
                channel=normalized,
                active_recording_ids=active_recording_ids,
            ):
                continue
            recording_root = self._resolve_streamer_recording_root(tracked)
            started_at = parse_recording_timestamp(tracked.started_at)
            ended_at = parse_recording_timestamp(tracked.ended_at)
            try:
                modified_at = datetime.fromtimestamp(recording_root.stat().st_mtime, tz=UTC)
            except OSError:
                continue
            directories.append(
                RecordingDirectoryInfo(
                    recording_id=tracked.recording_id,
                    channel=normalized,
                    directory_name=recording_root.name,
                    started_at=started_at,
                    ended_at=ended_at,
                    modified_at=modified_at,
                )
            )

        directories.sort(key=lambda item: item.modified_at, reverse=True)
        return directories

    async def delete_streamer_recording_directories(
        self,
        name: str,
        recording_ids: list[str],
    ) -> RecordingDirectoryDeleteResponse:
        normalized = name.strip().lower()
        unique_recording_ids = list(dict.fromkeys(str(value).strip() for value in recording_ids if str(value).strip()))
        if not unique_recording_ids:
            raise ValueError("recording_ids must not be empty")

        await self._sync_finished_recordings()
        async with self._lock:
            tracked_recordings = self.recording_store.load()
            tracked_by_id = {tracked.recording_id: tracked for tracked in tracked_recordings}
            async with self._recorder_lock:
                active_recording_ids = {
                    str(snapshot.get("recording_id", "")).strip()
                    for snapshot in self.recorder.list_active_recordings()
                    if str(snapshot.get("recording_id", "")).strip()
                }

            selected: list[tuple[TrackedRecording, Path]] = []
            channel_root = (self.settings.recordings_path / normalized).resolve()
            for recording_id in unique_recording_ids:
                tracked = tracked_by_id.get(recording_id)
                if tracked is None or tracked.channel != normalized:
                    raise ValueError("one or more recording directories are invalid")
                if tracked.artifact_mode != "segment_native":
                    raise ValueError("legacy recordings are not supported by directory deletion")
                if tracked.recording_id in active_recording_ids:
                    raise ValueError("one or more recording directories are still recording")
                if tracked.clean_compact_state in {"queued", "processing"}:
                    raise ValueError("one or more recording directories are still processing")
                if tracked.clean_export_state in {"queued", "processing"}:
                    raise ValueError("one or more recording directories are still exporting")

                recording_root = self._resolve_streamer_recording_root(tracked)
                resolved_root = recording_root.resolve()
                if not resolved_root.is_relative_to(channel_root):
                    raise ValueError("one or more recording directories are outside the channel directory")
                selected.append((tracked, recording_root))

            deleted_directory_names: list[str] = []
            selected_ids = {tracked.recording_id for tracked, _ in selected}
            for _tracked, recording_root in selected:
                deleted_directory_names.append(recording_root.name)
                if recording_root.exists():
                    shutil.rmtree(recording_root)

            remaining_recordings = [
                tracked for tracked in tracked_recordings if tracked.recording_id not in selected_ids
            ]
            self.recording_store.save(remaining_recordings)

            if channel_root.exists() and channel_root.is_dir():
                try:
                    next(channel_root.iterdir())
                except StopIteration:
                    channel_root.rmdir()

        return RecordingDirectoryDeleteResponse(
            channel=normalized,
            deleted_recording_ids=unique_recording_ids,
            deleted_directory_names=deleted_directory_names,
        )

    async def stop_streamer_recording(self, name: str) -> StopRecordingResponse:
        normalized = name.strip().lower()
        async with self._lock:
            async with self._recorder_lock:
                result = self.recorder.stop_recording(normalized)
            if result is None:
                return StopRecordingResponse(name=normalized, stopped=False)
            self._apply_recording_result(result)
            self._manually_stopped.add(normalized)
            return StopRecordingResponse(name=normalized, stopped=True)

    async def start_streamer_recording(self, name: str) -> StartRecordingResponse:
        normalized = name.strip().lower()
        async with self._lock:
            await self._sync_finished_recordings()
            status = self._statuses.setdefault(normalized, StreamStatus(name=normalized))
            status.enabled_for_recording = self._is_recording_enabled(normalized)

            if self.recorder.is_recording(normalized):
                return StartRecordingResponse(name=normalized, started=False)

            if not self._is_recording_enabled(normalized):
                status.recording_state = "disabled"
                status.last_error = "recording is disabled for this streamer"
                return StartRecordingResponse(name=normalized, started=False)

            if not status.is_live:
                status.last_error = "stream is not live"
                return StartRecordingResponse(name=normalized, started=False)

            remaining_start_delay = self._start_delay_remaining_seconds(
                status.started_at,
                now=datetime.now(UTC),
            )
            if remaining_start_delay > 0:
                status.recording_state = "start_delay"
                status.last_error = (
                    f"recording start delay active ({remaining_start_delay}s remaining)"
                )
                return StartRecordingResponse(name=normalized, started=False)

            if self.recorder.active_count() >= self.settings.max_concurrent_streamers:
                status.last_error = "max concurrent recordings reached"
                return StartRecordingResponse(name=normalized, started=False)

            try:
                output_path = self.recorder.start_recording(normalized)
            except OSError as exc:
                status.last_error = str(exc)
                return StartRecordingResponse(name=normalized, started=False)

            self._manually_stopped.discard(normalized)
            status.output_path = output_path
            status.recording_state = "recording"
            status.recording_started_at = datetime.now(UTC)
            status.recording_ended_at = None
            status.recording_exit_code = None
            status.offline_since = None
            status.stop_after_at = None
            status.last_error = None
            status.is_recording = True
            return StartRecordingResponse(name=normalized, started=True)

    async def list_statuses(self) -> list[StreamStatus]:
        await self._sync_finished_recordings()
        statuses = []
        for name in sorted(self._statuses):
            status = self._statuses[name]
            status.enabled_for_recording = self._is_recording_enabled(name)
            self._sync_active_recording_fields(status)
            statuses.append(status)
        return statuses

    async def list_recordings(self) -> list[RecordingInfo]:
        await self._sync_finished_recordings()
        tracked_recordings = self.recording_store.load()
        existing_files: list[
            tuple[
                float,
                int,
                Path,
                TrackedRecording,
                bool,
                bool,
                str | None,
                str,
            ]
        ] = []
        for tracked in tracked_recordings:
            source_path = Path(tracked.source_file_path)
            source_exists = source_path.exists() and source_path.is_file()
            full_artifact_path = Path(tracked.effective_full_artifact_path)
            full_artifact_exists = full_artifact_path.exists() and full_artifact_path.is_file()
            clean_artifact_value = tracked.effective_clean_artifact_path
            clean_artifact_path = Path(clean_artifact_value) if clean_artifact_value else None
            clean_artifact_exists = (
                clean_artifact_path.exists() and clean_artifact_path.is_file()
                if clean_artifact_path is not None
                else False
            )
            compact_artifact_path = self._clean_compact_path_if_ready(tracked)
            clean_export_path = Path(tracked.clean_export_path) if tracked.clean_export_path else None
            clean_export_exists = (
                clean_export_path.exists() and clean_export_path.is_file()
                if clean_export_path is not None
                else False
            )

            watchable_name: str | None = None
            watchable_exists = False
            watchable_stat_path: Path | None = None
            if tracked.artifact_mode == "segment_native":
                watchable_stat_path = self._segment_native_watchable_path(tracked)
                if watchable_stat_path is not None:
                    watchable_exists = True
                    watchable_name = watchable_stat_path.name
                watchable_state = self._watchable_state_from_tracked_recording(tracked)
                watchable_path = str(watchable_stat_path) if watchable_stat_path is not None else None
            else:
                watchable_path = tracked.watchable_file_path
                watchable_state = tracked.watchable_state
                if watchable_path:
                    watchable_file = Path(watchable_path)
                    if watchable_file.exists() and watchable_file.is_file():
                        watchable_exists = True
                        watchable_name = watchable_file.name
                        watchable_stat_path = watchable_file
                    elif watchable_file == source_path and source_exists:
                        watchable_exists = True
                        watchable_name = source_path.name
                        watchable_stat_path = source_path

            stat_path: Path | None = None
            if clean_export_exists and clean_export_path is not None:
                stat_path = clean_export_path
            elif compact_artifact_path is not None:
                stat_path = compact_artifact_path
            elif tracked.artifact_mode == "legacy" and watchable_stat_path is not None:
                stat_path = watchable_stat_path
            elif source_exists:
                stat_path = source_path
            elif full_artifact_exists:
                stat_path = full_artifact_path
            elif clean_artifact_exists and clean_artifact_path is not None:
                stat_path = clean_artifact_path

            if stat_path is None:
                continue

            stat = stat_path.stat()
            existing_files.append(
                (
                    stat.st_mtime,
                    stat.st_size,
                    stat_path,
                    tracked,
                    source_exists,
                    watchable_exists,
                    watchable_name,
                    watchable_state,
                )
            )

        existing_files.sort(key=lambda item: item[0], reverse=True)
        recordings_by_id: dict[str, RecordingInfo] = {}
        for (
            modified_ts,
            size_bytes,
            _stat_path,
            tracked,
            source_exists,
            watchable_available,
            watchable_name,
            watchable_state,
        ) in existing_files:
            source_path = Path(tracked.source_file_path)
            recordings_by_id[tracked.recording_id] = (
                RecordingInfo(
                    recording_id=tracked.recording_id,
                    artifact_mode=tracked.artifact_mode,
                    is_recording=False,
                    channel=tracked.channel,
                    file_path=str(source_path),
                    file_name=source_path.name,
                    source_file_path=str(source_path),
                    source_file_name=source_path.name,
                    source_available=source_exists,
                    watchable_file_path=(
                        tracked.clean_export_path
                        if tracked.artifact_mode == "segment_native" and tracked.clean_export_path
                        else tracked.effective_clean_artifact_path
                    ),
                    watchable_file_name=watchable_name,
                    watchable_available=watchable_available,
                    watchable_state=watchable_state,
                    ad_break_count=tracked.ad_break_count,
                    source_mode=tracked.source_mode,
                    full_artifact_path=tracked.effective_full_artifact_path,
                    clean_artifact_path=tracked.effective_clean_artifact_path,
                    clean_compact_state=tracked.clean_compact_state,
                    clean_compact_path=tracked.clean_compact_path,
                    clean_compact_error=tracked.clean_compact_error,
                    full_segment_count=tracked.full_segment_count,
                    clean_segment_count=tracked.clean_segment_count,
                    clean_export_state=tracked.clean_export_state,
                    clean_export_path=tracked.clean_export_path,
                    clean_export_dir_path=self._clean_export_dir_path(
                        artifact_mode=tracked.artifact_mode,
                        source_file_path=tracked.source_file_path,
                        full_artifact_path=tracked.effective_full_artifact_path,
                        clean_artifact_path=tracked.effective_clean_artifact_path,
                        clean_export_path=tracked.clean_export_path,
                        clean_compact_path=tracked.clean_compact_path,
                    ),
                    clean_export_error=tracked.clean_export_error,
                    unknown_ad_confidence=tracked.unknown_ad_confidence,
                    size_bytes=size_bytes,
                    modified_at=datetime.fromtimestamp(modified_ts, tz=UTC),
                )
            )

        async with self._recorder_lock:
            active_snapshots = self.recorder.list_active_recordings()

        for snapshot in active_snapshots:
            recording_id = self._optional_text(snapshot.get("recording_id")) or ""
            source_path_value = self._optional_text(snapshot.get("source_path")) or ""
            if not (recording_id and source_path_value):
                continue

            source_path = Path(source_path_value)
            channel = (self._optional_text(snapshot.get("channel")) or "").lower()
            if not channel:
                channel = recording_id

            modified_at_raw = snapshot.get("modified_at")
            if isinstance(modified_at_raw, datetime):
                modified_at = modified_at_raw
            else:
                modified_at = datetime.now(UTC)
            if modified_at.tzinfo is None:
                modified_at = modified_at.replace(tzinfo=UTC)

            artifact_mode = str(snapshot.get("artifact_mode", "legacy")).strip().lower()
            if artifact_mode not in {"legacy", "segment_native"}:
                artifact_mode = "legacy"

            try:
                size_bytes = max(0, int(snapshot.get("size_bytes", 0)))
            except (TypeError, ValueError):
                size_bytes = 0

            source_available = bool(
                snapshot.get(
                    "source_available",
                    source_path.exists() and source_path.is_file(),
                )
            )
            source_mode = self._optional_text(snapshot.get("source_mode")) or "unauthenticated"
            full_artifact_path = self._optional_text(snapshot.get("full_artifact_path"))
            clean_artifact_path = self._optional_text(snapshot.get("clean_artifact_path"))

            try:
                full_segment_count = max(0, int(snapshot.get("full_segment_count", 0)))
            except (TypeError, ValueError):
                full_segment_count = 0
            try:
                clean_segment_count = max(0, int(snapshot.get("clean_segment_count", 0)))
            except (TypeError, ValueError):
                clean_segment_count = 0

            clean_export_state = (
                self._optional_text(snapshot.get("clean_export_state")) or "none"
            ).lower()
            if clean_export_state not in {"none", "queued", "processing", "ready", "failed"}:
                clean_export_state = "none"
            clean_export_path = self._optional_text(snapshot.get("clean_export_path"))
            clean_export_error = self._optional_text(snapshot.get("clean_export_error"))
            clean_compact_state = (
                self._optional_text(snapshot.get("clean_compact_state")) or "none"
            ).lower()
            if clean_compact_state not in {"none", "queued", "processing", "ready", "failed"}:
                clean_compact_state = "none"
            clean_compact_path = self._optional_text(snapshot.get("clean_compact_path"))
            clean_compact_error = self._optional_text(snapshot.get("clean_compact_error"))

            try:
                ad_break_count = max(0, int(snapshot.get("ad_break_count", 0)))
            except (TypeError, ValueError):
                ad_break_count = 0

            unknown_ad_confidence = bool(snapshot.get("unknown_ad_confidence", False))

            watchable_file_path = None
            watchable_file_name = None
            watchable_available = False
            if clean_export_state == "ready" and clean_export_path:
                clean_export_candidate = Path(clean_export_path)
                if clean_export_candidate.exists() and clean_export_candidate.is_file():
                    watchable_file_path = str(clean_export_candidate)
                    watchable_file_name = clean_export_candidate.name
                    watchable_available = True
            elif clean_compact_state == "ready" and clean_compact_path:
                clean_compact_candidate = Path(clean_compact_path)
                if clean_compact_candidate.exists() and clean_compact_candidate.is_file():
                    watchable_file_path = str(clean_compact_candidate)
                    watchable_file_name = clean_compact_candidate.name
                    watchable_available = True
            elif clean_artifact_path:
                clean_artifact_candidate = Path(clean_artifact_path)
                if clean_artifact_candidate.exists() and clean_artifact_candidate.is_file():
                    watchable_file_path = str(clean_artifact_candidate)
                    watchable_file_name = clean_artifact_candidate.name
                    watchable_available = True

            recordings_by_id[recording_id] = RecordingInfo(
                recording_id=recording_id,
                artifact_mode=artifact_mode,
                is_recording=True,
                channel=channel,
                file_path=str(source_path),
                file_name=source_path.name,
                source_file_path=str(source_path),
                source_file_name=source_path.name,
                source_available=source_available,
                watchable_file_path=watchable_file_path,
                watchable_file_name=watchable_file_name,
                watchable_available=watchable_available,
                watchable_state="processing",
                ad_break_count=ad_break_count,
                source_mode=source_mode,
                full_artifact_path=full_artifact_path,
                clean_artifact_path=clean_artifact_path,
                clean_compact_state=clean_compact_state,
                clean_compact_path=clean_compact_path,
                clean_compact_error=clean_compact_error,
                full_segment_count=full_segment_count,
                clean_segment_count=clean_segment_count,
                clean_export_state=clean_export_state,
                clean_export_path=clean_export_path,
                clean_export_dir_path=self._clean_export_dir_path(
                    artifact_mode=artifact_mode,
                    source_file_path=str(source_path),
                    full_artifact_path=full_artifact_path,
                    clean_artifact_path=clean_artifact_path,
                    clean_export_path=clean_export_path,
                    clean_compact_path=clean_compact_path,
                ),
                clean_export_error=clean_export_error,
                unknown_ad_confidence=unknown_ad_confidence,
                size_bytes=size_bytes,
                modified_at=modified_at,
            )

        return sorted(recordings_by_id.values(), key=lambda item: item.modified_at, reverse=True)

    def _find_recording_by_id(self, recording_id: str) -> TrackedRecording | None:
        for tracked in self.recording_store.load():
            if tracked.recording_id == recording_id:
                return tracked
        return None

    def get_download_full_path(self, recording_id: str) -> Path:
        tracked = self._find_recording_by_id(recording_id)
        if tracked is None:
            raise ValueError("recording not found")
        if tracked.artifact_mode == "segment_native":
            target_path = Path(tracked.effective_full_artifact_path)
        else:
            target_path = Path(tracked.source_file_path)
        if not (target_path.exists() and target_path.is_file()):
            raise FileNotFoundError("full artifact not found")
        return target_path

    def get_download_clean_manifest_path(self, recording_id: str) -> Path:
        tracked = self._find_recording_by_id(recording_id)
        if tracked is None:
            raise ValueError("recording not found")
        if tracked.artifact_mode != "segment_native":
            raise RuntimeError("clean manifest is not available for legacy recordings")
        clean_path_value = tracked.effective_clean_artifact_path
        if not clean_path_value:
            raise FileNotFoundError("clean manifest not found")
        clean_manifest_path = Path(clean_path_value)
        if not (clean_manifest_path.exists() and clean_manifest_path.is_file()):
            raise FileNotFoundError("clean manifest not found")
        return clean_manifest_path

    def get_download_clean_export_path(self, recording_id: str) -> Path:
        tracked = self._find_recording_by_id(recording_id)
        if tracked is None:
            raise ValueError("recording not found")
        if tracked.artifact_mode != "segment_native":
            raise RuntimeError("clean export is not available for legacy recordings")
        if tracked.clean_export_state != "ready" or not tracked.clean_export_path:
            raise FileNotFoundError("clean export is not ready")
        export_path = Path(tracked.clean_export_path)
        if not (export_path.exists() and export_path.is_file()):
            raise FileNotFoundError("clean export file not found")
        return export_path

    def _resolve_clean_export_input_path(self, tracked: TrackedRecording) -> Path:
        compact_path = self._clean_compact_path_if_ready(tracked)
        if compact_path is not None:
            return compact_path
        clean_manifest_value = tracked.effective_clean_artifact_path
        if not clean_manifest_value:
            raise FileNotFoundError("clean manifest not found")
        clean_manifest_path = Path(clean_manifest_value)
        if not (clean_manifest_path.exists() and clean_manifest_path.is_file()):
            raise FileNotFoundError("clean manifest not found")
        return clean_manifest_path

    def _recover_pending_clean_export_jobs(self) -> None:
        for tracked in self.recording_store.load():
            if tracked.artifact_mode != "segment_native":
                continue
            if tracked.clean_export_state not in {"queued", "processing"}:
                continue
            try:
                self.create_clean_export(tracked.recording_id, mode="force")
            except (ValueError, RuntimeError, FileNotFoundError) as exc:
                self._on_clean_export_state_change(
                    CleanExportJob(
                        job_id="startup-recovery",
                        recording_id=tracked.recording_id,
                        state="failed",
                        manifest_path="",
                        output_path="",
                        error=str(exc) or "failed to recover clean export job",
                    )
                )

    def create_clean_export(
        self,
        recording_id: str,
        *,
        mode: Literal["auto", "retry", "force"] = "retry",
    ) -> CleanExportStatusResponse:
        tracked = self._find_recording_by_id(recording_id)
        if tracked is None:
            raise ValueError("recording not found")
        if tracked.artifact_mode != "segment_native":
            raise RuntimeError("clean export is not available for legacy recordings")
        compact_path = self._clean_compact_path_if_ready(tracked)
        if tracked.clean_segment_count <= 0 and compact_path is None:
            raise RuntimeError("clean recording has no playable segments")

        clean_input_path = self._resolve_clean_export_input_path(tracked)
        recording_root = clean_input_path.parent.parent
        started_at = parse_recording_timestamp(tracked.started_at)
        if started_at is not None:
            output_name = build_recording_output_filename(
                channel=tracked.channel,
                started_at=started_at,
                extension="mp4",
            )
        else:
            output_name = "clean.mp4"
        output_path = recording_root / "exports" / output_name

        if mode in {"auto", "retry"}:
            in_progress_job = self._clean_export_manager.get(recording_id)
            if in_progress_job is not None and in_progress_job.state in {"queued", "processing"}:
                return CleanExportStatusResponse(
                    recording_id=recording_id,
                    job_id=in_progress_job.job_id,
                    state=in_progress_job.state,
                    output_path=in_progress_job.output_path if in_progress_job.state == "ready" else None,
                    error=in_progress_job.error,
                )
        if mode == "retry":
            if tracked.clean_export_state == "ready" and tracked.clean_export_path:
                export_path = Path(tracked.clean_export_path)
                if export_path.exists() and export_path.is_file():
                    raise RuntimeError("clean export is already ready; use mode=force to rebuild")
            if tracked.clean_export_state != "failed":
                raise RuntimeError("clean export retry is only available after a failed export")
        if mode == "auto" and tracked.clean_export_state == "ready" and tracked.clean_export_path:
            export_path = Path(tracked.clean_export_path)
            if export_path.exists() and export_path.is_file():
                return CleanExportStatusResponse(
                    recording_id=recording_id,
                    state="ready",
                    output_path=str(export_path),
                    error=None,
                )

        job = self._clean_export_manager.enqueue(
            recording_id=recording_id,
            manifest_path=clean_input_path,
            output_path=output_path,
            force=(mode == "force"),
        )
        return CleanExportStatusResponse(
            recording_id=recording_id,
            job_id=job.job_id,
            state=job.state,
            output_path=job.output_path if job.state == "ready" else None,
            error=job.error,
        )

    def get_clean_export_status(self, recording_id: str) -> CleanExportStatusResponse:
        tracked = self._find_recording_by_id(recording_id)
        if tracked is None:
            raise ValueError("recording not found")
        job = self._clean_export_manager.get(recording_id)
        if job is not None:
            return CleanExportStatusResponse(
                recording_id=recording_id,
                job_id=job.job_id,
                state=job.state,
                output_path=job.output_path if job.state == "ready" else None,
                error=job.error,
            )
        return CleanExportStatusResponse(
            recording_id=recording_id,
            state=tracked.clean_export_state,
            output_path=tracked.clean_export_path,
            error=tracked.clean_export_error,
        )

    async def refresh_once(self) -> None:
        now = datetime.now(UTC)
        async with self._lock:
            streamers = list(self._streamers)

        await self._sync_finished_recordings()
        if not streamers:
            return

        shared_error = None
        live_streams: dict[str, object] | None = None
        try:
            live_streams = await self.twitch_client.get_live_streams(streamers)
        except (TwitchAuthError, httpx.HTTPError) as exc:
            shared_error = str(exc)

        try:
            users = await self.twitch_client.get_users(streamers)
        except (TwitchAuthError, httpx.HTTPError) as exc:
            users = {}
            if shared_error is None:
                shared_error = str(exc)

        for name in streamers:
            user = users.get(name)
            status = self._statuses.setdefault(name, StreamStatus(name=name))
            status.enabled_for_recording = self._is_recording_enabled(name)
            if user is not None:
                status.profile_image_url = user.profile_image_url
            status.last_checked_at = now
            self._sync_active_recording_fields(status)
            status.last_error = shared_error

            if live_streams is None:
                continue

            live = live_streams.get(name)

            status.is_live = live is not None
            status.title = live.title if live else None
            status.game_name = live.game_name if live else None
            status.viewer_count = live.viewer_count if live else None
            status.started_at = live.started_at if live else None

            if not live:
                self._manually_stopped.discard(name)
                await self._handle_offline_recording(name, status, now)
                continue

            status.offline_since = None
            status.stop_after_at = None

            if name in self._manually_stopped:
                status.recording_state = "stopped"
                status.is_recording = False
                continue

            if not self._is_recording_enabled(name):
                if self.recorder.has_session(name):
                    async with self._recorder_lock:
                        result = self.recorder.stop_recording(name)
                    if result is not None:
                        self._apply_recording_result(result)
                status.recording_state = "disabled"
                status.is_recording = self.recorder.is_recording(name)
                status.last_error = None
                continue

            if not self.recorder.has_session(name):
                remaining_start_delay = self._start_delay_remaining_seconds(
                    live.started_at,
                    now=now,
                )
                if remaining_start_delay > 0:
                    status.recording_state = "start_delay"
                    status.is_recording = False
                    status.last_error = None
                    continue

            can_start = self.recorder.active_count() < self.settings.max_concurrent_streamers
            if self.recorder.has_session(name) and not self.recorder.is_recording(name):
                try:
                    output_path = self.recorder.resume_recording(name)
                    if output_path is not None:
                        status.output_path = output_path
                        status.recording_state = "recording"
                        status.recording_ended_at = None
                        status.recording_exit_code = None
                        status.last_error = None
                except OSError as exc:
                    status.last_error = str(exc)
            elif not self.recorder.has_session(name) and can_start:
                try:
                    output_path = self.recorder.start_recording(name)
                    status.output_path = output_path
                    status.recording_state = "recording"
                    status.recording_started_at = datetime.now(UTC)
                    status.recording_ended_at = None
                    status.recording_exit_code = None
                    status.offline_since = None
                    status.stop_after_at = None
                    status.last_error = None
                    self._manually_stopped.discard(name)
                except OSError as exc:
                    status.last_error = str(exc)

            self._sync_active_recording_fields(status)
            if status.is_recording:
                status.recording_state = (
                    "ad_break" if self.recorder.is_in_ad_break(name) else "recording"
                )

    async def _poll_loop(self) -> None:
        while True:
            await self.refresh_once()
            await asyncio.sleep(self.settings.poll_interval_seconds)
