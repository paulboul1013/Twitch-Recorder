from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from .config import Settings
from .models import RecordingInfo, StartRecordingResponse, StopRecordingResponse, StreamStatus, StreamerInfo
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
        self._manually_stopped: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    def _apply_recording_result(self, result: RecordingResult) -> None:
        status = self._statuses.setdefault(result.channel, StreamStatus(name=result.channel))
        status.is_recording = False
        status.output_path = str(result.file_path)
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
                metadata_path=str(result.metadata_path),
                watchable_file_path=result.clean_output_path,
                watchable_state=result.clean_output_state,
                ad_break_count=result.ad_break_count,
                source_mode=result.source_mode,
                started_at=result.started_at.isoformat(),
                ended_at=result.ended_at.isoformat(),
                state=result.state,
                clean_output_error=result.clean_output_error,
            )
        )

    def _sync_finished_recordings(self) -> None:
        for result in self.recorder.poll():
            self._apply_recording_result(result)

    def _sync_active_recording_fields(self, status: StreamStatus) -> None:
        status.is_recording = self.recorder.is_recording(status.name)
        current_output_path = self.recorder.current_output_path(status.name)
        if current_output_path is not None:
            status.output_path = current_output_path
        if status.is_recording and status.recording_state != "grace_period":
            status.recording_state = (
                "ad_break" if self.recorder.is_in_ad_break(status.name) else "recording"
            )

    def _handle_offline_recording(self, name: str, status: StreamStatus, now: datetime) -> None:
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
        self.recorder.stop_all()

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
            result = self.recorder.stop_recording(normalized)
            if result is None:
                return StopRecordingResponse(name=normalized, stopped=False)
            self._apply_recording_result(result)
            self._manually_stopped.add(normalized)
            return StopRecordingResponse(name=normalized, stopped=True)

    async def start_streamer_recording(self, name: str) -> StartRecordingResponse:
        normalized = name.strip().lower()
        async with self._lock:
            self._sync_finished_recordings()
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

    def list_statuses(self) -> list[StreamStatus]:
        self._sync_finished_recordings()
        statuses = []
        for name in sorted(self._statuses):
            status = self._statuses[name]
            self._sync_active_recording_fields(status)
            statuses.append(status)
        return statuses

    def list_recordings(self) -> list[RecordingInfo]:
        tracked_recordings = self.recording_store.load()
        existing_files: list[tuple[Path, TrackedRecording]] = []
        for tracked in tracked_recordings:
            file_path = Path(tracked.source_file_path)
            if file_path.exists() and file_path.is_file():
                existing_files.append((file_path, tracked))

        existing_files.sort(key=lambda item: item[0].stat().st_mtime, reverse=True)
        recordings: list[RecordingInfo] = []
        for file_path, tracked in existing_files:
            stat = file_path.stat()
            watchable_path = tracked.watchable_file_path
            watchable_available = False
            watchable_name: str | None = None
            if watchable_path:
                watchable_file = Path(watchable_path)
                if watchable_file.exists() and watchable_file.is_file():
                    watchable_available = True
                    watchable_name = watchable_file.name
                elif watchable_file == file_path and file_path.exists():
                    watchable_available = True
                    watchable_name = file_path.name
            recordings.append(
                RecordingInfo(
                    channel=tracked.channel,
                    file_path=str(file_path),
                    file_name=file_path.name,
                    source_file_path=str(file_path),
                    source_file_name=file_path.name,
                    watchable_file_path=watchable_path,
                    watchable_file_name=watchable_name,
                    watchable_available=watchable_available,
                    watchable_state=tracked.watchable_state,
                    ad_break_count=tracked.ad_break_count,
                    source_mode=tracked.source_mode,
                    size_bytes=stat.st_size,
                    modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                )
            )
        return recordings

    async def refresh_once(self) -> None:
        now = datetime.now(UTC)
        async with self._lock:
            streamers = list(self._streamers)

        self._sync_finished_recordings()
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
                self._handle_offline_recording(name, status, now)
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
