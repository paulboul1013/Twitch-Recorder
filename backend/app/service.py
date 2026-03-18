from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from .clean_export import CleanExportJob, CleanExportManager
from .config import Settings
from .models import (
    CleanExportStatusResponse,
    RecordingInfo,
    StartRecordingResponse,
    StopRecordingResponse,
    StreamStatus,
    StreamerInfo,
)
from .recorder import RecorderManager, RecordingResult
from .store import RecordingHistoryStore, StreamerStore, TrackedRecording
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
        self._streamers = self.store.load()
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
        for tracked in tracked_recordings:
            if tracked.recording_id != job.recording_id:
                continue
            tracked.clean_export_state = job.state
            tracked.clean_export_path = job.output_path if job.state == "ready" else None
            tracked.clean_export_error = job.error
            tracked.watchable_state = self._watchable_state_from_tracked_recording(tracked)
            if job.state == "ready":
                tracked.watchable_file_path = job.output_path
            updated = True
            break
        if updated:
            self.recording_store.save(tracked_recordings)

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
        self.recording_store.upsert(
            TrackedRecording(
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
        )

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
        if not self.recorder.is_recording(name):
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
        return [StreamerInfo(name=name) for name in self._streamers]

    async def add_streamer(self, name: str) -> StreamerInfo:
        normalized = name.strip().lower()
        async with self._lock:
            if normalized not in self._streamers:
                self._streamers.append(normalized)
                self._streamers.sort()
                self.store.save(self._streamers)
            self._statuses.setdefault(normalized, StreamStatus(name=normalized))
        return StreamerInfo(name=normalized)

    async def remove_streamer(self, name: str) -> None:
        normalized = name.strip().lower()
        async with self._lock:
            async with self._recorder_lock:
                result = self.recorder.stop_recording(normalized)
            if result is not None:
                self._apply_recording_result(result)
            self._streamers = [streamer for streamer in self._streamers if streamer != normalized]
            self.store.save(self._streamers)
            self._statuses.pop(normalized, None)
            self._manually_stopped.discard(normalized)

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

            if self.recorder.is_recording(normalized):
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
                    clean_export_error=tracked.clean_export_error,
                    unknown_ad_confidence=tracked.unknown_ad_confidence,
                    size_bytes=size_bytes,
                    modified_at=datetime.fromtimestamp(modified_ts, tz=UTC),
                )
            )

        async with self._recorder_lock:
            active_snapshots = self.recorder.list_active_recordings()

        for snapshot in active_snapshots:
            recording_id = str(snapshot.get("recording_id", "")).strip()
            source_path_value = str(snapshot.get("source_path", "")).strip()
            if not (recording_id and source_path_value):
                continue

            source_path = Path(source_path_value)
            channel = str(snapshot.get("channel", "")).strip().lower()
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
            source_mode = str(snapshot.get("source_mode", "unauthenticated")).strip() or "unauthenticated"
            full_artifact_path = str(snapshot.get("full_artifact_path", "")).strip() or None
            clean_artifact_path = str(snapshot.get("clean_artifact_path", "")).strip() or None

            try:
                full_segment_count = max(0, int(snapshot.get("full_segment_count", 0)))
            except (TypeError, ValueError):
                full_segment_count = 0
            try:
                clean_segment_count = max(0, int(snapshot.get("clean_segment_count", 0)))
            except (TypeError, ValueError):
                clean_segment_count = 0

            clean_export_state = str(snapshot.get("clean_export_state", "none")).strip().lower() or "none"
            if clean_export_state not in {"none", "queued", "processing", "ready", "failed"}:
                clean_export_state = "none"
            clean_export_path = str(snapshot.get("clean_export_path", "")).strip() or None
            clean_export_error = str(snapshot.get("clean_export_error", "")).strip() or None
            clean_compact_state = str(snapshot.get("clean_compact_state", "none")).strip().lower() or "none"
            if clean_compact_state not in {"none", "queued", "processing", "ready", "failed"}:
                clean_compact_state = "none"
            clean_compact_path = str(snapshot.get("clean_compact_path", "")).strip() or None
            clean_compact_error = str(snapshot.get("clean_compact_error", "")).strip() or None

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

    def create_clean_export(self, recording_id: str) -> CleanExportStatusResponse:
        tracked = self._find_recording_by_id(recording_id)
        if tracked is None:
            raise ValueError("recording not found")
        if tracked.artifact_mode != "segment_native":
            raise RuntimeError("clean export is not available for legacy recordings")

        clean_input_path = self._resolve_clean_export_input_path(tracked)
        recording_root = clean_input_path.parent.parent
        output_path = recording_root / "exports" / "clean.mp4"

        job = self._clean_export_manager.enqueue(
            recording_id=recording_id,
            manifest_path=clean_input_path,
            output_path=output_path,
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

            if not self.recorder.is_recording(name):
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
            if not self.recorder.is_recording(name) and can_start:
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
