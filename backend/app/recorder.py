from __future__ import annotations

import json
import math
import subprocess
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from .ad_detection import (
    collect_ad_windows,
    count_ad_breaks,
    detect_ad_transition,
    infer_ad_detection_sources_from_events,
    merge_offset_ranges,
    normalize_timed_id3_marker_times,
    ocr_text_matches_ad_overlay,
    ocr_text_matches_prepare_overlay,
    ocr_text_matches_twitch_overlay,
    parse_twitch_daterange_ad_windows,
    ranges_intersect,
)
from .api_integration import build_streamlink_command, build_streamlink_stream_url_command
from .finalizer import RecordingFinalizer
from .metadata import RecordingMetadataWriter
from .recording_types import ActiveRecording, RecordingEvent, RecordingResult, WatchableMetadataContext


class RecorderManager:
    STDERR_TAIL_MAX_LINES = 40
    OCR_SCAN_INTERVAL_SECONDS = 4.0
    OCR_VERIFY_INTERVAL_SECONDS = 6.0
    OCR_HIT_PADDING_SECONDS = 4.0
    OCR_MERGE_GAP_SECONDS = 8.0
    OCR_LOCAL_VALIDATION_PADDING_SECONDS = 10.0
    TIMED_ID3_DISCONTINUITY_MIN_SECONDS = 30.0
    TIMED_ID3_DISCONTINUITY_FACTOR = 8.0
    MAX_WATCHABLE_REPAIR_PASSES = 2
    MAX_CONCURRENT_FINALIZERS = 1
    TRIM_PREPARE_VERIFY_SECONDS = 30.0
    SEGMENT_VERIFY_NEIGHBOR_SECONDS = 10.0
    SEGMENT_DURATION_SECONDS = 10.0
    PLAYLIST_POLL_INTERVAL_SECONDS = 5.0

    def __init__(
        self,
        recordings_path: Path,
        preferred_qualities: tuple[str, ...],
        twitch_user_oauth_token: str = "",
        twitch_user_login: str = "",
        watchable_trim_start_seconds: int = 0,
        recording_start_delay_seconds: int = 0,
        recording_mode: str = "legacy",
        segment_ad_padding_seconds: float = 2.0,
        recording_raw_container: str = "ts",
        delete_raw_on_success: bool = True,
    ) -> None:
        self.recordings_path = recordings_path
        self.preferred_qualities = preferred_qualities
        self.twitch_user_oauth_token = twitch_user_oauth_token
        self.twitch_user_login = twitch_user_login
        self.watchable_trim_start_seconds = max(0, int(watchable_trim_start_seconds))
        self.recording_start_delay_seconds = max(0, int(recording_start_delay_seconds))
        normalized_recording_mode = str(recording_mode or "legacy").strip().lower()
        if normalized_recording_mode not in {"legacy", "segment_native"}:
            normalized_recording_mode = "legacy"
        self.recording_mode = normalized_recording_mode
        self.segment_ad_padding_seconds = max(0.0, float(segment_ad_padding_seconds))
        normalized_container = recording_raw_container.strip().lower().lstrip(".")
        self.recording_raw_container = normalized_container or "ts"
        self.delete_raw_on_success = bool(delete_raw_on_success)
        self._active: dict[str, ActiveRecording] = {}
        self._completed_results: list[RecordingResult] = []
        self._pending_finalizers: dict[str, threading.Thread] = {}
        self._state_lock = threading.Lock()
        self._finalize_slots = threading.Semaphore(self.MAX_CONCURRENT_FINALIZERS)
        self._last_watchable_context = WatchableMetadataContext()

        self._metadata_writer = RecordingMetadataWriter(
            recording_start_delay_seconds=self.recording_start_delay_seconds,
        )
        self._finalizer = RecordingFinalizer(
            recordings_path=self.recordings_path,
            watchable_trim_start_seconds=self.watchable_trim_start_seconds,
            ocr_verify_interval_seconds=self.OCR_VERIFY_INTERVAL_SECONDS,
            ocr_hit_padding_seconds=self.OCR_HIT_PADDING_SECONDS,
            ocr_merge_gap_seconds=self.OCR_MERGE_GAP_SECONDS,
            ocr_local_validation_padding_seconds=self.OCR_LOCAL_VALIDATION_PADDING_SECONDS,
            timed_id3_discontinuity_min_seconds=self.TIMED_ID3_DISCONTINUITY_MIN_SECONDS,
            timed_id3_discontinuity_factor=self.TIMED_ID3_DISCONTINUITY_FACTOR,
            max_watchable_repair_passes=self.MAX_WATCHABLE_REPAIR_PASSES,
            trim_prepare_verify_seconds=self.TRIM_PREPARE_VERIFY_SECONDS,
            segment_verify_neighbor_seconds=self.SEGMENT_VERIFY_NEIGHBOR_SECONDS,
        )

    def is_recording(self, channel: str) -> bool:
        with self._state_lock:
            recording = self._active.get(channel)
        return bool(recording and self._is_recording_running(recording))

    def is_in_ad_break(self, channel: str) -> bool:
        recording = self._active.get(channel)
        if not recording:
            return False
        with recording.lock:
            return recording.ad_break_active

    def current_output_path(self, channel: str) -> str | None:
        with self._state_lock:
            recording = self._active.get(channel)
        if not recording:
            return None
        if recording.artifact_mode == "segment_native" and recording.full_artifact_path is not None:
            return str(recording.full_artifact_path)
        return str(recording.file_path)

    def active_count(self) -> int:
        self._reap_finished_processes()
        with self._state_lock:
            return len(self._active)

    def _is_recording_running(self, recording: ActiveRecording) -> bool:
        if recording.artifact_mode != "segment_native":
            return recording.process.poll() is None
        streamlink_alive = recording.process.poll() is None
        if recording.segmenter_process is None:
            return streamlink_alive
        segmenter_alive = recording.segmenter_process.poll() is None
        return streamlink_alive and segmenter_alive

    def _is_recording_finished(self, recording: ActiveRecording) -> bool:
        if recording.artifact_mode != "segment_native":
            return recording.process.poll() is not None
        streamlink_done = recording.process.poll() is not None
        if recording.segmenter_process is None:
            return streamlink_done
        segmenter_done = recording.segmenter_process.poll() is not None
        return streamlink_done and segmenter_done

    def _resolve_recording_exit_code(self, recording: ActiveRecording) -> int:
        primary_exit = recording.process.poll()
        if recording.segmenter_process is None:
            return primary_exit if primary_exit is not None else recording.process.wait()
        segmenter_exit = recording.segmenter_process.poll()
        if primary_exit is None:
            primary_exit = recording.process.wait()
        if segmenter_exit is None:
            segmenter_exit = recording.segmenter_process.wait()
        if primary_exit not in (None, 0):
            return primary_exit
        if segmenter_exit not in (None, 0):
            return segmenter_exit
        return primary_exit if primary_exit is not None else 0

    def poll(self) -> list[RecordingResult]:
        self._reap_finished_processes()
        return self._drain_completed_results()

    def list_active_recordings(self) -> list[dict[str, object]]:
        self._reap_finished_processes()
        with self._state_lock:
            active_recordings = list(self._active.values())

        snapshots: list[dict[str, object]] = []
        for recording in active_recordings:
            if not self._is_recording_running(recording):
                continue

            source_path = recording.file_path
            size_bytes = 0
            modified_at = recording.started_at
            full_segment_count = 0
            unknown_ad_confidence = False

            with recording.lock:
                ad_break_count = self._count_ad_breaks(list(recording.events))
                if recording.artifact_mode == "segment_native":
                    unknown_ad_confidence = not recording.playlist_markers_seen

            if recording.artifact_mode == "segment_native" and recording.recording_root is not None:
                segments_dir = recording.recording_root / "segments"
                segments = sorted(segments_dir.glob("segment_*.ts"))
                if segments:
                    source_path = segments[-1]
                    full_segment_count = len(segments)
                    latest_mtime = 0.0
                    for segment_file in segments:
                        try:
                            stat = segment_file.stat()
                        except OSError:
                            continue
                        size_bytes += stat.st_size
                        latest_mtime = max(latest_mtime, stat.st_mtime)
                    if latest_mtime > 0:
                        modified_at = datetime.fromtimestamp(latest_mtime, tz=UTC)
            else:
                try:
                    stat = source_path.stat()
                    size_bytes = stat.st_size
                    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
                except OSError:
                    size_bytes = 0

            snapshots.append(
                {
                    "recording_id": recording.recording_id,
                    "artifact_mode": recording.artifact_mode,
                    "channel": recording.channel,
                    "source_path": str(source_path),
                    "source_available": source_path.exists() and source_path.is_file(),
                    "source_mode": recording.source_mode,
                    "full_artifact_path": (
                        str(recording.full_artifact_path) if recording.full_artifact_path else str(source_path)
                    ),
                    "clean_artifact_path": (
                        str(recording.clean_artifact_path) if recording.clean_artifact_path else None
                    ),
                    "full_segment_count": full_segment_count,
                    "clean_segment_count": 0,
                    "clean_export_state": "none",
                    "clean_export_path": None,
                    "clean_export_error": None,
                    "ad_break_count": ad_break_count,
                    "unknown_ad_confidence": unknown_ad_confidence,
                    "size_bytes": size_bytes,
                    "modified_at": modified_at,
                }
            )
        return snapshots

    def start_recording(self, channel: str) -> str:
        self._reap_finished_processes()
        with self._state_lock:
            existing = self._active.get(channel)
            if existing is not None:
                return str(existing.file_path)

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
        recording_id = f"{channel}_{timestamp}"
        recording_root: Path | None = None
        full_artifact_path: Path | None = None
        clean_artifact_path: Path | None = None
        if self.recording_mode == "segment_native":
            recording_root = self.recordings_path / recording_id
            segments_dir = recording_root / "segments"
            manifests_dir = recording_root / "manifests"
            exports_dir = recording_root / "exports"
            segments_dir.mkdir(parents=True, exist_ok=True)
            manifests_dir.mkdir(parents=True, exist_ok=True)
            exports_dir.mkdir(parents=True, exist_ok=True)
            output_path = segments_dir / f"segment_000000.{self.recording_raw_container}"
            metadata_path = recording_root / "recording.meta.json"
            full_artifact_path = manifests_dir / "full.m3u8"
            clean_artifact_path = manifests_dir / "clean.m3u8"
        else:
            output_path = self.recordings_path / f"{channel}_{timestamp}.{self.recording_raw_container}"
            metadata_path = output_path.with_suffix(".meta.json")
            full_artifact_path = output_path

        segmenter_process: subprocess.Popen | None = None
        if self.recording_mode == "segment_native":
            cmd, source_mode = build_streamlink_command(
                channel=channel,
                output_path=None,
                preferred_qualities=self.preferred_qualities,
                twitch_user_oauth_token=self.twitch_user_oauth_token,
                raw_container=self.recording_raw_container,
                output_to_stdout=True,
            )
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            if process.stdout is None:
                process.kill()
                raise OSError("failed to initialize streamlink stdout pipe for segment pipeline")

            segment_pattern = str((recording_root / "segments" / "segment_%06d.ts").resolve())
            segment_cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                "pipe:0",
                "-c",
                "copy",
                "-f",
                "segment",
                "-segment_format",
                "mpegts",
                "-segment_time",
                f"{self.SEGMENT_DURATION_SECONDS}",
                "-reset_timestamps",
                "1",
                segment_pattern,
            ]
            segmenter_process = subprocess.Popen(
                segment_cmd,
                stdin=process.stdout,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            process.stdout.close()
        else:
            cmd, source_mode = build_streamlink_command(
                channel=channel,
                output_path=output_path,
                preferred_qualities=self.preferred_qualities,
                twitch_user_oauth_token=self.twitch_user_oauth_token,
                raw_container=self.recording_raw_container,
            )

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )

        started_at = datetime.now(UTC)
        recording = ActiveRecording(
            recording_id=recording_id,
            artifact_mode=self.recording_mode,
            channel=channel,
            process=process,
            segmenter_process=segmenter_process,
            file_path=output_path,
            metadata_path=metadata_path,
            started_at=started_at,
            source_mode=source_mode,
            recording_root=recording_root,
            full_artifact_path=full_artifact_path,
            clean_artifact_path=clean_artifact_path,
        )
        if recording.artifact_mode == "segment_native":
            self._start_playlist_ad_window_tracking(recording)
        self._append_event(recording, "recording_started", at=started_at)
        self._write_metadata(
            recording=recording,
            ended_at=None,
            exit_code=None,
            state="recording",
            full_artifact_path=str(full_artifact_path) if full_artifact_path else None,
            clean_artifact_path=str(clean_artifact_path) if clean_artifact_path else None,
            full_segment_count=0,
            clean_segment_count=0,
            clean_export_state="none",
            clean_export_path=None,
            clean_export_error=None,
            unknown_ad_confidence=False,
            clean_output_path=None,
            clean_output_state="pending",
            clean_output_error=None,
            watchable_processing_seconds=None,
            source_available=True,
        )
        recording.stderr_thread = self._start_stderr_thread(recording)
        with self._state_lock:
            self._active[channel] = recording
        return str(output_path)

    def stop_recording(self, channel: str, *, wait_for_finalize: bool = False) -> RecordingResult | None:
        self._reap_finished_processes()
        with self._state_lock:
            recording = self._active.pop(channel, None)
        if recording is None:
            return None

        self._stop_playlist_ad_window_tracking(recording)
        if recording.process.poll() is None:
            recording.process.terminate()
            try:
                recording.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                recording.process.kill()
                recording.process.wait(timeout=5)

        if recording.segmenter_process is not None and recording.segmenter_process.poll() is None:
            try:
                recording.segmenter_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                recording.segmenter_process.terminate()
                try:
                    recording.segmenter_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    recording.segmenter_process.kill()
                    recording.segmenter_process.wait(timeout=3)

        exit_code = self._resolve_recording_exit_code(recording)

        self._join_stderr_thread(recording)
        processing_result = self._start_recording_finalization(
            recording=recording,
            ended_at=datetime.now(UTC),
            exit_code=exit_code,
            state="stopped",
            run_inline=wait_for_finalize,
        )
        if wait_for_finalize:
            completed_results = self.poll()
            for result in completed_results:
                if result.file_path == processing_result.file_path:
                    return result
        return processing_result

    def stop_all(self, *, wait_for_finalize: bool = False) -> list[RecordingResult]:
        self._reap_finished_processes()
        with self._state_lock:
            active_channels = list(self._active)

        results = [
            result
            for channel in active_channels
            if (result := self.stop_recording(channel, wait_for_finalize=wait_for_finalize)) is not None
        ]
        if wait_for_finalize:
            self.wait_for_pending_finalizations()
            results.extend(self.poll())
        return results

    def wait_for_pending_finalizations(self) -> None:
        while True:
            with self._state_lock:
                pending_threads = list(self._pending_finalizers.values())
            if not pending_threads:
                return
            for thread in pending_threads:
                thread.join()

    def _reap_finished_processes(self) -> None:
        with self._state_lock:
            finished_channels = [
                channel
                for channel, recording in self._active.items()
                if self._is_recording_finished(recording)
            ]
            broken_segmenter_channels = [
                channel
                for channel, recording in self._active.items()
                if recording.artifact_mode == "segment_native"
                and recording.segmenter_process is not None
                and recording.segmenter_process.poll() is not None
                and recording.process.poll() is None
            ]
            finished_recordings = [
                self._active.pop(channel)
                for channel in finished_channels
                if channel in self._active
            ]
            for channel in broken_segmenter_channels:
                recording = self._active.get(channel)
                if recording is not None and recording.process.poll() is None:
                    recording.process.terminate()

        for recording in finished_recordings:
            self._stop_playlist_ad_window_tracking(recording)
            exit_code = self._resolve_recording_exit_code(recording)
            self._join_stderr_thread(recording)
            processing_result = self._start_recording_finalization(
                recording=recording,
                ended_at=datetime.now(UTC),
                exit_code=exit_code,
                state="completed" if exit_code == 0 else "failed",
            )
            self._append_completed_result(processing_result)

    def _append_completed_result(self, result: RecordingResult) -> None:
        with self._state_lock:
            self._completed_results.append(result)

    def _drain_completed_results(self) -> list[RecordingResult]:
        with self._state_lock:
            if not self._completed_results:
                return []
            results = list(self._completed_results)
            self._completed_results.clear()
        return results

    def _start_stderr_thread(self, recording: ActiveRecording) -> threading.Thread | None:
        if recording.process.stderr is None:
            return None
        thread = threading.Thread(
            target=self._consume_stderr,
            args=(recording,),
            daemon=True,
            name=f"streamlink-stderr-{recording.channel}",
        )
        thread.start()
        return thread

    def _join_stderr_thread(self, recording: ActiveRecording) -> None:
        if recording.stderr_thread is not None:
            recording.stderr_thread.join(timeout=2)
        if recording.process.stderr is not None:
            recording.process.stderr.close()

    def _start_playlist_ad_window_tracking(self, recording: ActiveRecording) -> None:
        recording.playlist_stop_event.clear()
        thread = threading.Thread(
            target=self._poll_playlist_ad_windows,
            args=(recording,),
            daemon=True,
            name=f"playlist-ad-tracker-{recording.channel}",
        )
        recording.playlist_thread = thread
        thread.start()

    def _stop_playlist_ad_window_tracking(self, recording: ActiveRecording) -> None:
        recording.playlist_stop_event.set()
        if recording.playlist_thread is not None:
            recording.playlist_thread.join(timeout=2)

    def _poll_playlist_ad_windows(self, recording: ActiveRecording) -> None:
        playlist_url = self._resolve_stream_playlist_url(recording.channel)
        with recording.lock:
            recording.playlist_url = playlist_url
            if not playlist_url:
                recording.playlist_poll_error = "failed to resolve stream playlist url"

        if not playlist_url:
            return

        with httpx.Client(timeout=10.0, follow_redirects=True) as client:
            while not recording.playlist_stop_event.is_set():
                try:
                    response = client.get(playlist_url)
                    response.raise_for_status()
                    windows, markers_seen = parse_twitch_daterange_ad_windows(response.text)
                    with recording.lock:
                        if markers_seen:
                            recording.playlist_markers_seen = True
                        if windows:
                            recording.playlist_ad_windows = self._merge_datetime_windows(
                                recording.playlist_ad_windows + windows
                            )
                        recording.playlist_poll_error = None
                except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                    with recording.lock:
                        recording.playlist_poll_error = str(exc) or "playlist poll failed"
                recording.playlist_stop_event.wait(self.PLAYLIST_POLL_INTERVAL_SECONDS)

    def _resolve_stream_playlist_url(self, channel: str) -> str | None:
        cmd, _source_mode = build_streamlink_stream_url_command(
            channel=channel,
            preferred_qualities=self.preferred_qualities,
            twitch_user_oauth_token=self.twitch_user_oauth_token,
        )
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=20,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        stream_url = result.stdout.strip().splitlines()
        if not stream_url:
            return None
        candidate = stream_url[0].strip()
        return candidate or None

    def _merge_datetime_windows(
        self,
        windows: list[tuple[datetime, datetime]],
    ) -> list[tuple[datetime, datetime]]:
        merged: list[tuple[datetime, datetime]] = []
        for start, end in sorted(windows, key=lambda item: item[0]):
            if end <= start:
                continue
            if not merged:
                merged.append((start, end))
                continue
            prev_start, prev_end = merged[-1]
            if start <= prev_end:
                merged[-1] = (prev_start, max(prev_end, end))
            else:
                merged.append((start, end))
        return merged

    def _consume_stderr(self, recording: ActiveRecording) -> None:
        assert recording.process.stderr is not None
        for raw_line in recording.process.stderr:
            if isinstance(raw_line, bytes):
                line = raw_line.decode("utf-8", errors="replace").strip()
            else:
                line = raw_line.strip()
            if not line:
                continue
            with recording.lock:
                recording.stderr_tail.append(line)
                if len(recording.stderr_tail) > self.STDERR_TAIL_MAX_LINES:
                    del recording.stderr_tail[:-self.STDERR_TAIL_MAX_LINES]
                event_type = self._detect_ad_transition(line, recording.ad_break_active)
            if event_type == "ad_break_started":
                self._append_event(recording, "ad_break_started", message=line)
            elif event_type == "ad_break_ended":
                self._append_event(recording, "ad_break_ended", message=line)

    def _detect_ad_transition(self, line: str, ad_break_active: bool) -> str | None:
        return detect_ad_transition(line, ad_break_active)

    def _append_event(
        self,
        recording: ActiveRecording,
        event_type: str,
        *,
        at: datetime | None = None,
        message: str | None = None,
    ) -> None:
        with recording.lock:
            if event_type == "ad_break_started":
                if recording.ad_break_active:
                    return
                recording.ad_break_active = True
            elif event_type == "ad_break_ended":
                if not recording.ad_break_active:
                    return
                recording.ad_break_active = False
            recording.events.append(
                RecordingEvent(
                    type=event_type,
                    at=at or datetime.now(UTC),
                    message=message,
                )
            )

    def _start_recording_finalization(
        self,
        *,
        recording: ActiveRecording,
        ended_at: datetime,
        exit_code: int,
        state: str,
        run_inline: bool = False,
    ) -> RecordingResult:
        events_snapshot = self._snapshot_terminal_events(
            recording=recording,
            ended_at=ended_at,
            state=state,
        )
        processing_result = self._build_processing_result(
            recording=recording,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            events_snapshot=events_snapshot,
        )
        self._write_metadata(
            recording=recording,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            full_artifact_path=processing_result.full_artifact_path,
            clean_artifact_path=processing_result.clean_artifact_path,
            full_segment_count=processing_result.full_segment_count,
            clean_segment_count=processing_result.clean_segment_count,
            clean_export_state=processing_result.clean_export_state,
            clean_export_path=processing_result.clean_export_path,
            clean_export_error=processing_result.clean_export_error,
            unknown_ad_confidence=processing_result.unknown_ad_confidence,
            clean_output_path=processing_result.clean_output_path,
            clean_output_state=processing_result.clean_output_state,
            clean_output_error=processing_result.clean_output_error,
            watchable_processing_seconds=None,
            ad_break_count_override=processing_result.ad_break_count,
            source_available=recording.file_path.exists(),
        )
        if run_inline:
            final_result = self._run_finalization_job(
                recording=recording,
                ended_at=ended_at,
                exit_code=exit_code,
                state=state,
                events_snapshot=events_snapshot,
            )
            self._append_completed_result(final_result)
            return processing_result

        finalize_key = str(recording.file_path)
        finalization_started_at = time.perf_counter()

        def worker() -> None:
            try:
                result = self._run_finalization_job(
                    recording=recording,
                    ended_at=ended_at,
                    exit_code=exit_code,
                    state=state,
                    events_snapshot=events_snapshot,
                )
            except Exception as exc:  # pragma: no cover - defensive fallback
                error_message = str(exc) or "watchable finalization failed unexpectedly"
                result = RecordingResult(
                    recording_id=processing_result.recording_id,
                    artifact_mode=processing_result.artifact_mode,
                    channel=recording.channel,
                    file_path=recording.file_path,
                    metadata_path=recording.metadata_path,
                    started_at=recording.started_at,
                    ended_at=ended_at,
                    exit_code=exit_code,
                    state=state,
                    source_mode=recording.source_mode,
                    full_artifact_path=processing_result.full_artifact_path,
                    clean_artifact_path=processing_result.clean_artifact_path,
                    full_segment_count=processing_result.full_segment_count,
                    clean_segment_count=processing_result.clean_segment_count,
                    clean_export_state=processing_result.clean_export_state,
                    clean_export_path=processing_result.clean_export_path,
                    clean_export_error=processing_result.clean_export_error,
                    unknown_ad_confidence=processing_result.unknown_ad_confidence,
                    clean_output_path=None,
                    clean_output_state="failed",
                    clean_output_error=error_message,
                    ad_break_count=processing_result.ad_break_count,
                )
                self._write_metadata(
                    recording=recording,
                    ended_at=ended_at,
                    exit_code=exit_code,
                    state=state,
                    full_artifact_path=processing_result.full_artifact_path,
                    clean_artifact_path=processing_result.clean_artifact_path,
                    full_segment_count=processing_result.full_segment_count,
                    clean_segment_count=processing_result.clean_segment_count,
                    clean_export_state=processing_result.clean_export_state,
                    clean_export_path=processing_result.clean_export_path,
                    clean_export_error=processing_result.clean_export_error,
                    unknown_ad_confidence=processing_result.unknown_ad_confidence,
                    clean_output_path=None,
                    clean_output_state="failed",
                    clean_output_error=error_message,
                    watchable_processing_seconds=round(
                        max(0.0, time.perf_counter() - finalization_started_at),
                        3,
                    ),
                    ad_break_count_override=result.ad_break_count,
                    source_available=recording.file_path.exists(),
                )
            self._append_completed_result(result)
            with self._state_lock:
                self._pending_finalizers.pop(finalize_key, None)

        thread = threading.Thread(
            target=worker,
            daemon=True,
            name=f"recording-finalize-{recording.channel}",
        )
        with self._state_lock:
            self._pending_finalizers[finalize_key] = thread
        thread.start()
        return processing_result

    def _run_finalization_job(
        self,
        *,
        recording: ActiveRecording,
        ended_at: datetime,
        exit_code: int,
        state: str,
        events_snapshot: list[RecordingEvent],
    ) -> RecordingResult:
        self._finalize_slots.acquire()
        try:
            return self._finalize_recording(
                recording=recording,
                ended_at=ended_at,
                exit_code=exit_code,
                state=state,
                events_snapshot=events_snapshot,
            )
        finally:
            self._finalize_slots.release()

    def _snapshot_terminal_events(
        self,
        *,
        recording: ActiveRecording,
        ended_at: datetime,
        state: str,
    ) -> list[RecordingEvent]:
        with recording.lock:
            if recording.ad_break_active:
                recording.events.append(
                    RecordingEvent(
                        type="ad_break_ended",
                        at=ended_at,
                        message="ad break auto-closed at recording end",
                    )
                )
                recording.ad_break_active = False

            terminal_event = {
                "stopped": "recording_stopped",
                "completed": "recording_completed",
                "failed": "recording_failed",
            }[state]
            recording.events.append(RecordingEvent(type=terminal_event, at=ended_at))
            return list(recording.events)

    def _build_processing_result(
        self,
        *,
        recording: ActiveRecording,
        ended_at: datetime,
        exit_code: int,
        state: str,
        events_snapshot: list[RecordingEvent],
    ) -> RecordingResult:
        ad_break_count = self._count_ad_breaks(events_snapshot)
        full_artifact_path = recording.full_artifact_path or recording.file_path
        clean_artifact_path = recording.clean_artifact_path
        return RecordingResult(
            recording_id=recording.recording_id,
            artifact_mode=recording.artifact_mode,
            channel=recording.channel,
            file_path=recording.file_path,
            metadata_path=recording.metadata_path,
            started_at=recording.started_at,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            source_mode=recording.source_mode,
            full_artifact_path=str(full_artifact_path) if full_artifact_path else None,
            clean_artifact_path=str(clean_artifact_path) if clean_artifact_path else None,
            full_segment_count=0,
            clean_segment_count=0,
            clean_export_state="none",
            clean_export_path=None,
            clean_export_error=None,
            unknown_ad_confidence=False,
            clean_output_path=None,
            clean_output_state="processing",
            clean_output_error=None,
            ad_break_count=ad_break_count,
        )

    def _finalize_recording(
        self,
        recording: ActiveRecording,
        ended_at: datetime,
        exit_code: int,
        state: str,
        events_snapshot: list[RecordingEvent] | None = None,
    ) -> RecordingResult:
        if events_snapshot is None:
            events_snapshot = self._snapshot_terminal_events(
                recording=recording,
                ended_at=ended_at,
                state=state,
            )

        processing_started_at = time.perf_counter()
        full_artifact_path = recording.full_artifact_path or recording.file_path
        clean_artifact_path = recording.clean_artifact_path
        full_segment_count = 0
        clean_segment_count = 0
        clean_export_state = "none"
        clean_export_path: str | None = None
        clean_export_error: str | None = None
        unknown_ad_confidence = False
        if self.recording_mode == "segment_native":
            (
                full_artifact_path,
                clean_artifact_path,
                full_segment_count,
                clean_segment_count,
                unknown_ad_confidence,
                clean_output_state,
                clean_output_error,
                resolved_ad_break_count,
            ) = self._build_segment_native_artifacts(
                recording=recording,
                started_at=recording.started_at,
                ended_at=ended_at,
                events=events_snapshot,
            )
            clean_output_path = clean_artifact_path
            source_available = recording.file_path.exists()
            source_deleted_on_success = False
            source_delete_error = None
            ad_detection_sources = (
                ["playlist_daterange"]
                if not unknown_ad_confidence
                else ["playlist_daterange_missing"]
            )
            self._set_last_watchable_context(
                watchable_strategy="segment_native_manifest",
                ad_detection_sources=ad_detection_sources,
                prepare_mitigation=self._base_prepare_mitigation(),
            )
        else:
            (
                clean_output_path,
                clean_output_state,
                clean_output_error,
                resolved_ad_break_count,
            ) = self._build_watchable_output(
                source_path=recording.file_path,
                started_at=recording.started_at,
                ended_at=ended_at,
                events=events_snapshot,
            )
            (
                source_available,
                source_deleted_on_success,
                source_delete_error,
            ) = self._apply_source_retention_policy(
                source_path=recording.file_path,
                clean_output_path=clean_output_path,
                clean_output_state=clean_output_state,
            )
        watchable_context = self._consume_last_watchable_context()
        watchable_processing_seconds = round(max(0.0, time.perf_counter() - processing_started_at), 3)
        self._write_metadata(
            recording=recording,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            full_artifact_path=str(full_artifact_path) if full_artifact_path else None,
            clean_artifact_path=str(clean_artifact_path) if clean_artifact_path else None,
            full_segment_count=full_segment_count,
            clean_segment_count=clean_segment_count,
            clean_export_state=clean_export_state,
            clean_export_path=clean_export_path,
            clean_export_error=clean_export_error,
            unknown_ad_confidence=unknown_ad_confidence,
            clean_output_path=clean_output_path,
            clean_output_state=clean_output_state,
            clean_output_error=clean_output_error,
            watchable_processing_seconds=watchable_processing_seconds,
            ad_break_count_override=resolved_ad_break_count,
            watchable_strategy=watchable_context.watchable_strategy,
            ad_detection_sources=watchable_context.ad_detection_sources,
            prepare_mitigation=watchable_context.prepare_mitigation,
            source_available=source_available,
            source_deleted_on_success=source_deleted_on_success,
            source_delete_error=source_delete_error,
        )
        return RecordingResult(
            recording_id=recording.recording_id,
            artifact_mode=recording.artifact_mode,
            channel=recording.channel,
            file_path=recording.file_path,
            metadata_path=recording.metadata_path,
            started_at=recording.started_at,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            source_mode=recording.source_mode,
            full_artifact_path=str(full_artifact_path) if full_artifact_path else None,
            clean_artifact_path=str(clean_artifact_path) if clean_artifact_path else None,
            full_segment_count=full_segment_count,
            clean_segment_count=clean_segment_count,
            clean_export_state=clean_export_state,
            clean_export_path=clean_export_path,
            clean_export_error=clean_export_error,
            unknown_ad_confidence=unknown_ad_confidence,
            clean_output_path=clean_output_path,
            clean_output_state=clean_output_state,
            clean_output_error=clean_output_error,
            ad_break_count=resolved_ad_break_count,
            source_available=source_available,
            source_deleted_on_success=source_deleted_on_success,
            source_delete_error=source_delete_error,
        )

    def _build_segment_native_artifacts(
        self,
        *,
        recording: ActiveRecording,
        started_at: datetime,
        ended_at: datetime,
        events: list[RecordingEvent],
    ) -> tuple[Path, Path, int, int, bool, str, str | None, int]:
        if recording.recording_root is None:
            raise RuntimeError("segment-native recording root is missing")
        if recording.full_artifact_path is None or recording.clean_artifact_path is None:
            raise RuntimeError("segment-native manifest path is missing")
        segments_dir = recording.recording_root / "segments"
        segment_files = sorted(segments_dir.glob("segment_*.ts"))
        if not segment_files and recording.file_path.exists():
            segment_files = [recording.file_path]

        if not segment_files:
            return (
                recording.full_artifact_path,
                recording.clean_artifact_path,
                0,
                0,
                True,
                "failed",
                "no segment files were produced",
                0,
            )

        segment_offsets: list[tuple[Path, float, float, float]] = []
        cursor = 0.0
        for segment_file in segment_files:
            duration_seconds = self._probe_media_duration(segment_file)
            if duration_seconds is None or duration_seconds <= 0:
                duration_seconds = self.SEGMENT_DURATION_SECONDS
            segment_offsets.append(
                (segment_file, duration_seconds, cursor, cursor + duration_seconds)
            )
            cursor += duration_seconds

        total_duration = cursor
        if total_duration <= 0:
            return (
                recording.full_artifact_path,
                recording.clean_artifact_path,
                0,
                0,
                True,
                "failed",
                "recording duration too short to build segment manifests",
                0,
            )

        with recording.lock:
            daterange_ad_windows = list(recording.playlist_ad_windows)
            has_daterange_markers = bool(recording.playlist_markers_seen)

        unknown_ad_confidence = not has_daterange_markers
        ad_ranges = self._merge_offset_ranges(
            [
                (
                    max(0.0, (ad_start - started_at).total_seconds() - self.segment_ad_padding_seconds),
                    min(total_duration, (ad_end - started_at).total_seconds() + self.segment_ad_padding_seconds),
                )
                for ad_start, ad_end in daterange_ad_windows
                if ad_end > ad_start
            ]
        ) if has_daterange_markers else []

        segment_index_payload: list[dict[str, object]] = []
        for seq, (segment_file, duration_seconds, start_offset, end_offset) in enumerate(segment_offsets):
            relative_segment_path = str(Path("../segments") / segment_file.name)
            segment_range = (start_offset, end_offset)
            is_ad = (
                has_daterange_markers
                and any(self._ranges_intersect(segment_range, ad_range) for ad_range in ad_ranges)
            )
            segment_index_payload.append(
                {
                    "seq": seq,
                    "start_ts": (started_at + timedelta(seconds=start_offset)).isoformat(),
                    "duration": round(duration_seconds, 3),
                    "is_ad": is_ad,
                    "file_path": relative_segment_path,
                }
            )

        segment_index_path = recording.recording_root / "segment_index.json"
        segment_index_path.write_text(
            json.dumps(
                {
                    "recording_id": recording.recording_id,
                    "unknown_ad_confidence": unknown_ad_confidence,
                    "segments": segment_index_payload,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        self._write_hls_manifest(
            manifest_path=recording.full_artifact_path,
            segments=segment_index_payload,
        )
        clean_segments = (
            [segment for segment in segment_index_payload if not segment["is_ad"]]
            if has_daterange_markers
            else list(segment_index_payload)
        )
        self._write_hls_manifest(
            manifest_path=recording.clean_artifact_path,
            segments=clean_segments,
        )
        ad_break_count = len(daterange_ad_windows) if has_daterange_markers else 0
        return (
            recording.full_artifact_path,
            recording.clean_artifact_path,
            len(segment_index_payload),
            len(clean_segments),
            unknown_ad_confidence,
            "ready",
            None,
            ad_break_count,
        )

    def _write_hls_manifest(
        self,
        *,
        manifest_path: Path,
        segments: list[dict[str, object]],
    ) -> None:
        max_duration = 1
        if segments:
            max_duration = max(
                1,
                math.ceil(
                    max(float(segment.get("duration", 0.0)) for segment in segments)
                ),
            )
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{max_duration}",
            "#EXT-X-MEDIA-SEQUENCE:0",
        ]
        for segment in segments:
            duration = max(0.001, float(segment.get("duration", 0.0)))
            file_path = str(segment.get("file_path", "")).strip()
            if not file_path:
                continue
            lines.append(f"#EXTINF:{duration:.3f},")
            lines.append(file_path)
        lines.append("#EXT-X-ENDLIST")
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _apply_source_retention_policy(
        self,
        *,
        source_path: Path,
        clean_output_path: str | None,
        clean_output_state: str,
    ) -> tuple[bool, bool, str | None]:
        source_available = source_path.exists()
        source_deleted_on_success = False
        source_delete_error: str | None = None

        if self.recording_mode != "legacy":
            return source_available, source_deleted_on_success, source_delete_error

        if not self.delete_raw_on_success:
            return source_available, source_deleted_on_success, source_delete_error

        if clean_output_state != "ready" or not clean_output_path:
            return source_available, source_deleted_on_success, source_delete_error

        watchable_path = Path(clean_output_path)
        if not (watchable_path.exists() and watchable_path.is_file()):
            return source_available, source_deleted_on_success, source_delete_error

        same_path = source_path == watchable_path
        if not same_path:
            try:
                same_path = source_path.resolve() == watchable_path.resolve()
            except OSError:
                same_path = source_path == watchable_path
        if same_path:
            return source_available, source_deleted_on_success, source_delete_error

        try:
            source_path.unlink()
            source_available = False
            source_deleted_on_success = True
        except FileNotFoundError:
            source_available = False
        except OSError as exc:
            source_available = source_path.exists()
            source_delete_error = str(exc) or "failed to delete source recording file"
        return source_available, source_deleted_on_success, source_delete_error

    def _count_ad_breaks(self, events: list[RecordingEvent]) -> int:
        return count_ad_breaks(events)

    def _base_prepare_mitigation(self) -> list[str]:
        return self._metadata_writer.base_prepare_mitigation()

    def _infer_ad_detection_sources_from_events(self, events: list[RecordingEvent]) -> list[str]:
        return infer_ad_detection_sources_from_events(events)

    def _set_last_watchable_context(
        self,
        *,
        watchable_strategy: str | None,
        ad_detection_sources: list[str],
        prepare_mitigation: list[str],
    ) -> None:
        self._last_watchable_context = WatchableMetadataContext(
            watchable_strategy=watchable_strategy,
            ad_detection_sources=list(dict.fromkeys(ad_detection_sources)),
            prepare_mitigation=list(dict.fromkeys(prepare_mitigation)),
        )

    def _consume_last_watchable_context(self) -> WatchableMetadataContext:
        context = self._last_watchable_context
        self._last_watchable_context = WatchableMetadataContext()
        return context

    def _write_metadata(
        self,
        *,
        recording: ActiveRecording,
        ended_at: datetime | None,
        exit_code: int | None,
        state: str,
        full_artifact_path: str | None,
        clean_artifact_path: str | None,
        full_segment_count: int,
        clean_segment_count: int,
        clean_export_state: str,
        clean_export_path: str | None,
        clean_export_error: str | None,
        unknown_ad_confidence: bool,
        clean_output_path: str | None,
        clean_output_state: str,
        clean_output_error: str | None,
        watchable_processing_seconds: float | None,
        ad_break_count_override: int | None = None,
        watchable_strategy: str | None = None,
        ad_detection_sources: list[str] | None = None,
        prepare_mitigation: list[str] | None = None,
        source_available: bool | None = None,
        source_deleted_on_success: bool = False,
        source_delete_error: str | None = None,
    ) -> None:
        self._metadata_writer.write(
            recording=recording,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            full_artifact_path=full_artifact_path,
            clean_artifact_path=clean_artifact_path,
            full_segment_count=full_segment_count,
            clean_segment_count=clean_segment_count,
            clean_export_state=clean_export_state,
            clean_export_path=clean_export_path,
            clean_export_error=clean_export_error,
            unknown_ad_confidence=unknown_ad_confidence,
            clean_output_path=clean_output_path,
            clean_output_state=clean_output_state,
            clean_output_error=clean_output_error,
            watchable_processing_seconds=watchable_processing_seconds,
            ad_break_count_override=ad_break_count_override,
            watchable_strategy=watchable_strategy,
            ad_detection_sources=ad_detection_sources,
            prepare_mitigation=prepare_mitigation,
            source_available=source_available,
            source_deleted_on_success=source_deleted_on_success,
            source_delete_error=source_delete_error,
        )

    def _build_watchable_output(
        self,
        *,
        source_path: Path,
        started_at: datetime,
        ended_at: datetime,
        events: list[RecordingEvent],
    ) -> tuple[str | None, str, str | None, int]:
        base_prepare_mitigation = self._base_prepare_mitigation()
        ad_detection_sources = self._infer_ad_detection_sources_from_events(events)
        self._set_last_watchable_context(
            watchable_strategy=None,
            ad_detection_sources=ad_detection_sources,
            prepare_mitigation=base_prepare_mitigation,
        )
        if not source_path.exists():
            return None, "failed", "source recording file does not exist", 0

        duration_seconds = max(0.0, (ended_at - started_at).total_seconds())
        if duration_seconds <= 0:
            return None, "failed", "recording duration too short to produce watchable output", 0

        trim_start_seconds = float(self.watchable_trim_start_seconds)
        watchable_path = source_path.with_name(f"{source_path.stem}.watchable.mp4")

        stderr_windows = self._collect_ad_windows(events, ended_at=ended_at)
        stderr_offsets = self._merge_offset_ranges(
            [
                (
                    max(0.0, (ad_start - started_at).total_seconds()),
                    min(duration_seconds, (ad_end - started_at).total_seconds()),
                )
                for ad_start, ad_end in stderr_windows
                if ad_end > ad_start
            ]
        )

        timed_id3_offsets = self._confirm_timed_id3_ad_offsets_with_ocr(
            source_path=source_path,
            duration_seconds=duration_seconds,
        )
        if timed_id3_offsets:
            ad_detection_sources.append("timed_id3_confirmed_by_ocr")

        ad_offsets = self._merge_offset_ranges(stderr_offsets + timed_id3_offsets)
        ad_break_count = len(ad_offsets)

        try:
            if self._can_remux_watchable(ad_offsets=ad_offsets):
                try:
                    self._remux_watchable(source_path=source_path, watchable_path=watchable_path)
                    self._set_last_watchable_context(
                        watchable_strategy="remux",
                        ad_detection_sources=ad_detection_sources,
                        prepare_mitigation=base_prepare_mitigation,
                    )
                    return str(watchable_path), "ready", None, ad_break_count
                except (OSError, RuntimeError, subprocess.SubprocessError):
                    watchable_path.unlink(missing_ok=True)
                    fallback_ranges = self._build_keep_ranges_from_offsets(
                        duration_seconds,
                        [],
                        trim_start_seconds=0.0,
                    )
                    if not fallback_ranges:
                        return (
                            None,
                            "failed",
                            "recording duration too short to produce watchable output",
                            ad_break_count,
                        )
                    self._render_watchable(
                        source_path=source_path,
                        watchable_path=watchable_path,
                        keep_ranges=fallback_ranges,
                    )
                    self._set_last_watchable_context(
                        watchable_strategy="fallback_reencode",
                        ad_detection_sources=ad_detection_sources,
                        prepare_mitigation=base_prepare_mitigation + ["reencode_fallback"],
                    )
                    return str(watchable_path), "ready", None, ad_break_count

            if ad_break_count == 0 and trim_start_seconds > 0:
                self._trim_copy_watchable(
                    source_path=source_path,
                    watchable_path=watchable_path,
                    trim_start_seconds=trim_start_seconds,
                )
                if self._has_prepare_overlay_in_prefix(
                    watchable_path,
                    verify_seconds=self.TRIM_PREPARE_VERIFY_SECONDS,
                ):
                    fallback_ranges = self._build_keep_ranges_from_offsets(
                        duration_seconds,
                        [],
                        trim_start_seconds=trim_start_seconds,
                    )
                    if not fallback_ranges:
                        return (
                            None,
                            "failed",
                            "recording duration too short to produce watchable output",
                            ad_break_count,
                        )
                    self._render_watchable(
                        source_path=source_path,
                        watchable_path=watchable_path,
                        keep_ranges=fallback_ranges,
                    )
                    self._set_last_watchable_context(
                        watchable_strategy="fallback_reencode",
                        ad_detection_sources=ad_detection_sources,
                        prepare_mitigation=base_prepare_mitigation
                        + ["trim_copy_fallback", "reencode_fallback"],
                    )
                    return str(watchable_path), "ready", None, ad_break_count

                self._set_last_watchable_context(
                    watchable_strategy="trim_copy",
                    ad_detection_sources=ad_detection_sources,
                    prepare_mitigation=base_prepare_mitigation + ["trim_copy_fallback"],
                )
                return str(watchable_path), "ready", None, ad_break_count

            keep_ranges = self._build_keep_ranges_from_offsets(
                duration_seconds,
                ad_offsets,
                trim_start_seconds=trim_start_seconds,
            )
            if not keep_ranges:
                return None, "failed", "recording duration too short to produce watchable output", ad_break_count

            self._render_watchable(
                source_path=source_path,
                watchable_path=watchable_path,
                keep_ranges=keep_ranges,
            )

            verification_ranges = self._build_segment_verification_ranges(keep_ranges)
            if verification_ranges and self._contains_overlay_in_ranges(
                watchable_path,
                sample_ranges=verification_ranges,
            ):
                self._reencode_existing_watchable_output(watchable_path)
                if self._contains_overlay_in_ranges(
                    watchable_path,
                    sample_ranges=verification_ranges,
                ):
                    watchable_path.unlink(missing_ok=True)
                    self._set_last_watchable_context(
                        watchable_strategy="fallback_reencode",
                        ad_detection_sources=ad_detection_sources,
                        prepare_mitigation=base_prepare_mitigation + ["reencode_fallback"],
                    )
                    return (
                        None,
                        "failed",
                        "watchable verification still detected Twitch playback overlay",
                        ad_break_count,
                    )
                self._set_last_watchable_context(
                    watchable_strategy="fallback_reencode",
                    ad_detection_sources=ad_detection_sources,
                    prepare_mitigation=base_prepare_mitigation + ["reencode_fallback"],
                )
                return str(watchable_path), "ready", None, ad_break_count

            self._set_last_watchable_context(
                watchable_strategy="segment_transcode",
                ad_detection_sources=ad_detection_sources,
                prepare_mitigation=base_prepare_mitigation,
            )
            return str(watchable_path), "ready", None, ad_break_count
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            return None, "failed", str(exc), ad_break_count

    def _can_remux_watchable(self, *, ad_offsets: list[tuple[float, float]]) -> bool:
        return self._finalizer.can_remux_watchable(ad_offsets=ad_offsets)

    def _trim_copy_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        trim_start_seconds: float,
    ) -> None:
        self._finalizer.trim_copy_watchable(
            source_path=source_path,
            watchable_path=watchable_path,
            trim_start_seconds=trim_start_seconds,
        )

    def _confirm_timed_id3_ad_offsets_with_ocr(
        self,
        *,
        source_path: Path,
        duration_seconds: float,
    ) -> list[tuple[float, float]]:
        return self._finalizer.confirm_timed_id3_ad_offsets_with_ocr(
            source_path=source_path,
            duration_seconds=duration_seconds,
            extract_offsets=lambda path, expected_duration: self._extract_timed_id3_ad_offsets(
                path,
                expected_duration_seconds=expected_duration,
            ),
            collect_ocr_windows=lambda path, duration, interval, sample_ranges, matcher: self._collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges,
                matcher=matcher,
            ),
        )

    def _has_prepare_overlay_in_prefix(
        self,
        source_path: Path,
        *,
        verify_seconds: float,
    ) -> bool:
        return self._finalizer.has_prepare_overlay_in_prefix(
            source_path,
            verify_seconds=verify_seconds,
            probe_duration=self._probe_media_duration,
            collect_ocr_windows=lambda path, duration, interval, sample_ranges, matcher: self._collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges,
                matcher=matcher,
            ),
        )

    def _contains_overlay_in_ranges(
        self,
        source_path: Path,
        *,
        sample_ranges: list[tuple[float, float]],
    ) -> bool:
        return self._finalizer.contains_overlay_in_ranges(
            source_path,
            sample_ranges=sample_ranges,
            probe_duration=self._probe_media_duration,
            collect_ocr_windows=lambda path, duration, interval, sample_ranges_arg, matcher: self._collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges_arg,
                matcher=matcher,
            ),
        )

    def _build_segment_verification_ranges(
        self,
        keep_ranges: list[tuple[float, float]],
    ) -> list[tuple[float, float]]:
        return self._finalizer.build_segment_verification_ranges(keep_ranges)

    def _reencode_existing_watchable_output(self, watchable_path: Path) -> None:
        self._finalizer.reencode_existing_watchable_output(
            watchable_path,
            probe_duration=self._probe_media_duration,
            build_keep_ranges_from_offsets=lambda duration, remove_ranges, trim_start: self._build_keep_ranges_from_offsets(
                duration,
                remove_ranges,
                trim_start_seconds=trim_start,
            ),
            render_watchable=lambda source, output, keep_ranges: self._render_watchable(
                source_path=source,
                watchable_path=output,
                keep_ranges=keep_ranges,
            ),
        )

    def _ranges_intersect(
        self,
        left: tuple[float, float],
        right: tuple[float, float],
    ) -> bool:
        return ranges_intersect(left, right)

    def _remux_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
    ) -> None:
        self._finalizer.remux_watchable(
            source_path=source_path,
            watchable_path=watchable_path,
        )

    def _collect_timed_id3_ad_windows(
        self,
        *,
        source_path: Path,
        started_at: datetime,
        duration_seconds: float,
    ) -> list[tuple[datetime, datetime]]:
        return self._finalizer.collect_timed_id3_ad_windows(
            source_path=source_path,
            started_at=started_at,
            duration_seconds=duration_seconds,
            run_cmd=subprocess.run,
        )

    def _extract_timed_id3_ad_offsets(
        self,
        source_path: Path,
        *,
        expected_duration_seconds: float | None = None,
    ) -> list[tuple[float, float]]:
        return self._finalizer.extract_timed_id3_ad_offsets(
            source_path,
            expected_duration_seconds=expected_duration_seconds,
            run_cmd=subprocess.run,
        )

    def _normalize_timed_id3_marker_times(
        self,
        *,
        packet_pts: list[float],
        marker_times: list[float],
        expected_duration_seconds: float | None = None,
    ) -> list[float]:
        return normalize_timed_id3_marker_times(
            packet_pts=packet_pts,
            marker_times=marker_times,
            expected_duration_seconds=expected_duration_seconds,
            discontinuity_min_seconds=self.TIMED_ID3_DISCONTINUITY_MIN_SECONDS,
            discontinuity_factor=self.TIMED_ID3_DISCONTINUITY_FACTOR,
        )

    def _collect_ad_windows(
        self,
        events: list[RecordingEvent],
        *,
        ended_at: datetime,
    ) -> list[tuple[datetime, datetime]]:
        return collect_ad_windows(events, ended_at=ended_at)

    def _build_keep_ranges(
        self,
        started_at: datetime,
        ended_at: datetime,
        ad_windows: list[tuple[datetime, datetime]],
        trim_start_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        return self._finalizer.build_keep_ranges(
            started_at,
            ended_at,
            ad_windows,
            trim_start_seconds=trim_start_seconds,
        )

    def _build_keep_ranges_from_offsets(
        self,
        duration_seconds: float,
        remove_ranges: list[tuple[float, float]],
        *,
        trim_start_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        return self._finalizer.build_keep_ranges_from_offsets(
            duration_seconds,
            remove_ranges,
            trim_start_seconds=trim_start_seconds,
        )

    def _repair_watchable_output(self, watchable_path: Path) -> tuple[str | None, int]:
        return self._finalizer.repair_watchable_output(
            watchable_path,
            probe_duration=self._probe_media_duration,
            collect_ocr_windows=lambda path, duration, interval, sample_ranges, matcher: self._collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges,
                matcher=matcher,
            ),
            build_keep_ranges_from_offsets=lambda duration, remove_ranges, trim_start: self._build_keep_ranges_from_offsets(
                duration,
                remove_ranges,
                trim_start_seconds=trim_start,
            ),
            render_watchable=lambda source, output, keep_ranges: self._render_watchable(
                source_path=source,
                watchable_path=output,
                keep_ranges=keep_ranges,
            ),
        )

    def _probe_media_duration(self, source_path: Path) -> float | None:
        return self._finalizer.probe_media_duration(source_path)

    def _collect_ocr_ad_windows(
        self,
        source_path: Path,
        *,
        duration_seconds: float,
        sample_interval_seconds: float,
        sample_ranges: list[tuple[float, float]] | None = None,
        matcher=None,
    ) -> list[tuple[float, float]]:
        return self._finalizer.collect_ocr_ad_windows(
            source_path,
            duration_seconds=duration_seconds,
            sample_interval_seconds=sample_interval_seconds,
            sample_ranges=sample_ranges,
            matcher=matcher,
        )

    def _run_tesseract(self, frame_path: Path) -> str:
        return self._finalizer.run_tesseract(frame_path)

    def _ocr_text_matches_ad_overlay(self, text: str) -> bool:
        return ocr_text_matches_ad_overlay(text)

    def _ocr_text_matches_prepare_overlay(self, text: str) -> bool:
        return ocr_text_matches_prepare_overlay(text)

    def _ocr_text_matches_twitch_overlay(self, text: str) -> bool:
        return ocr_text_matches_twitch_overlay(text)

    def _merge_offset_ranges(
        self,
        ranges: list[tuple[float, float]],
        *,
        merge_gap_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        return merge_offset_ranges(ranges, merge_gap_seconds=merge_gap_seconds)

    def _render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        self._finalizer.render_watchable(
            source_path=source_path,
            watchable_path=watchable_path,
            keep_ranges=keep_ranges,
        )
