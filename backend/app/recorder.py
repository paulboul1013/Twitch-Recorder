from __future__ import annotations

import json
import shutil
import statistics
import subprocess
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
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


class RecorderManager:
    STDERR_TAIL_MAX_LINES = 40
    OCR_SCAN_INTERVAL_SECONDS = 4.0
    OCR_VERIFY_INTERVAL_SECONDS = 2.0
    OCR_HIT_PADDING_SECONDS = 4.0
    OCR_MERGE_GAP_SECONDS = 8.0
    TIMED_ID3_DISCONTINUITY_MIN_SECONDS = 30.0
    TIMED_ID3_DISCONTINUITY_FACTOR = 8.0
    MAX_WATCHABLE_REPAIR_PASSES = 2

    def __init__(
        self,
        recordings_path: Path,
        preferred_qualities: tuple[str, ...],
        twitch_user_oauth_token: str = "",
        twitch_user_login: str = "",
        watchable_trim_start_seconds: int = 0,
    ) -> None:
        self.recordings_path = recordings_path
        self.preferred_qualities = preferred_qualities
        self.twitch_user_oauth_token = twitch_user_oauth_token
        self.twitch_user_login = twitch_user_login
        self.watchable_trim_start_seconds = max(0, int(watchable_trim_start_seconds))
        self._active: dict[str, ActiveRecording] = {}
        self._completed_results: list[RecordingResult] = []
        self._pending_finalizers: dict[str, threading.Thread] = {}
        self._state_lock = threading.Lock()

    def is_recording(self, channel: str) -> bool:
        with self._state_lock:
            recording = self._active.get(channel)
        return bool(recording and recording.process.poll() is None)

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
        return str(recording.file_path)

    def active_count(self) -> int:
        self._reap_finished_processes()
        with self._state_lock:
            return len(self._active)

    def poll(self) -> list[RecordingResult]:
        self._reap_finished_processes()
        return self._drain_completed_results()

    def start_recording(self, channel: str) -> str:
        self._reap_finished_processes()
        with self._state_lock:
            existing = self._active.get(channel)
            if existing is not None:
                return str(existing.file_path)

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        output_path = self.recordings_path / f"{channel}_{timestamp}.mp4"
        metadata_path = output_path.with_suffix(".meta.json")
        quality = ",".join(self.preferred_qualities)

        cmd = [
            "streamlink",
            f"https://twitch.tv/{channel}",
            quality,
            "-o",
            str(output_path),
        ]
        source_mode = "unauthenticated"
        if self.twitch_user_oauth_token:
            cmd.extend(
                [
                    "--twitch-api-header",
                    f"Authorization=OAuth {self.twitch_user_oauth_token}",
                ]
            )
            source_mode = "authenticated"

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
            channel=channel,
            process=process,
            file_path=output_path,
            metadata_path=metadata_path,
            started_at=started_at,
            source_mode=source_mode,
        )
        self._append_event(recording, "recording_started", at=started_at)
        self._write_metadata(
            recording=recording,
            ended_at=None,
            exit_code=None,
            state="recording",
            clean_output_path=None,
            clean_output_state="pending",
            clean_output_error=None,
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

        if recording.process.poll() is None:
            recording.process.terminate()
            try:
                exit_code = recording.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                recording.process.kill()
                exit_code = recording.process.wait(timeout=5)
        else:
            exit_code = recording.process.wait()

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
                if recording.process.poll() is not None
            ]
            finished_recordings = [
                self._active.pop(channel)
                for channel in finished_channels
                if channel in self._active
            ]

        for recording in finished_recordings:
            exit_code = recording.process.wait()
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

    def _consume_stderr(self, recording: ActiveRecording) -> None:
        assert recording.process.stderr is not None
        for raw_line in recording.process.stderr:
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
        lower = line.lower()
        if any(
            token in lower
            for token in (
                "ad_break_ended",
                "ad break ended",
                "commercial break ended",
                "ads ended",
                "ads complete",
                "ad finished",
            )
        ):
            return "ad_break_ended"

        if any(
            token in lower
            for token in (
                "ad_break_started",
                "ad break started",
                "commercial break started",
                "commercial break",
                "midroll",
                "mid-roll",
                "ad break",
            )
        ):
            if any(token in lower for token in ("ended", "finished", "resume", "resuming", "back")):
                return "ad_break_ended"
            return "ad_break_started"

        if "discontinuity" in lower and not ad_break_active:
            return "ad_break_started"

        if ad_break_active and any(
            token in lower for token in ("resuming stream", "stream resumed", "back to stream", "playback resumed")
        ):
            return "ad_break_ended"

        return None

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
            clean_output_path=processing_result.clean_output_path,
            clean_output_state=processing_result.clean_output_state,
            clean_output_error=processing_result.clean_output_error,
            ad_break_count_override=processing_result.ad_break_count,
        )
        if run_inline:
            final_result = self._finalize_recording(
                recording=recording,
                ended_at=ended_at,
                exit_code=exit_code,
                state=state,
                events_snapshot=events_snapshot,
            )
            self._append_completed_result(final_result)
            return processing_result

        finalize_key = str(recording.file_path)

        def worker() -> None:
            try:
                result = self._finalize_recording(
                    recording=recording,
                    ended_at=ended_at,
                    exit_code=exit_code,
                    state=state,
                    events_snapshot=events_snapshot,
                )
            except Exception as exc:  # pragma: no cover - defensive fallback
                error_message = str(exc) or "watchable finalization failed unexpectedly"
                result = RecordingResult(
                    channel=recording.channel,
                    file_path=recording.file_path,
                    metadata_path=recording.metadata_path,
                    started_at=recording.started_at,
                    ended_at=ended_at,
                    exit_code=exit_code,
                    state=state,
                    source_mode=recording.source_mode,
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
                    clean_output_path=None,
                    clean_output_state="failed",
                    clean_output_error=error_message,
                    ad_break_count_override=result.ad_break_count,
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
        return RecordingResult(
            channel=recording.channel,
            file_path=recording.file_path,
            metadata_path=recording.metadata_path,
            started_at=recording.started_at,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            source_mode=recording.source_mode,
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
        self._write_metadata(
            recording=recording,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            clean_output_path=clean_output_path,
            clean_output_state=clean_output_state,
            clean_output_error=clean_output_error,
            ad_break_count_override=resolved_ad_break_count,
        )
        return RecordingResult(
            channel=recording.channel,
            file_path=recording.file_path,
            metadata_path=recording.metadata_path,
            started_at=recording.started_at,
            ended_at=ended_at,
            exit_code=exit_code,
            state=state,
            source_mode=recording.source_mode,
            clean_output_path=clean_output_path,
            clean_output_state=clean_output_state,
            clean_output_error=clean_output_error,
            ad_break_count=resolved_ad_break_count,
        )

    def _count_ad_breaks(self, events: list[RecordingEvent]) -> int:
        return sum(1 for event in events if event.type == "ad_break_started")

    def _write_metadata(
        self,
        *,
        recording: ActiveRecording,
        ended_at: datetime | None,
        exit_code: int | None,
        state: str,
        clean_output_path: str | None,
        clean_output_state: str,
        clean_output_error: str | None,
        ad_break_count_override: int | None = None,
    ) -> None:
        with recording.lock:
            events_payload = [event.as_dict() for event in recording.events]
            stderr_tail = list(recording.stderr_tail)
        if ad_break_count_override is None:
            ad_break_count = sum(1 for event in events_payload if event["type"] == "ad_break_started")
        else:
            ad_break_count = max(0, int(ad_break_count_override))

        payload = {
            "channel": recording.channel,
            "file_path": str(recording.file_path),
            "started_at": recording.started_at.isoformat(),
            "ended_at": ended_at.isoformat() if ended_at else None,
            "exit_code": exit_code,
            "state": state,
            "events": events_payload,
            "streamlink_stderr_tail": stderr_tail,
            "clean_output_path": clean_output_path,
            "clean_output_state": clean_output_state,
            "clean_output_error": clean_output_error,
            "source_mode": recording.source_mode,
            "ad_break_count": ad_break_count,
        }
        recording.metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _build_watchable_output(
        self,
        *,
        source_path: Path,
        started_at: datetime,
        ended_at: datetime,
        events: list[RecordingEvent],
    ) -> tuple[str | None, str, str | None, int]:
        if not source_path.exists():
            return None, "failed", "source recording file does not exist", 0

        duration_seconds = max(0.0, (ended_at - started_at).total_seconds())
        ad_windows = self._collect_ad_windows(events, ended_at=ended_at)
        ad_windows.extend(
            self._collect_timed_id3_ad_windows(
                source_path=source_path,
                started_at=started_at,
                duration_seconds=duration_seconds,
            )
        )
        ad_offsets = self._merge_offset_ranges(
            [
                (
                    max(0.0, (ad_start - started_at).total_seconds()),
                    min(duration_seconds, (ad_end - started_at).total_seconds()),
                )
                for ad_start, ad_end in ad_windows
                if ad_end > ad_start
            ]
        )
        ad_break_count = len(ad_offsets)
        keep_ranges = self._build_keep_ranges_from_offsets(
            duration_seconds,
            ad_offsets,
            trim_start_seconds=float(self.watchable_trim_start_seconds),
        )
        if not keep_ranges:
            return None, "failed", "recording duration too short to produce watchable output", ad_break_count

        watchable_path = source_path.with_name(f"{source_path.stem}.watchable{source_path.suffix}")
        try:
            self._render_watchable(source_path=source_path, watchable_path=watchable_path, keep_ranges=keep_ranges)
            repair_error, repaired_ad_break_count = self._repair_watchable_output(watchable_path)
            resolved_ad_break_count = max(ad_break_count, repaired_ad_break_count)
            if repair_error is not None:
                watchable_path.unlink(missing_ok=True)
                return None, "failed", repair_error, resolved_ad_break_count
            return str(watchable_path), "ready", None, resolved_ad_break_count
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            return None, "failed", str(exc), ad_break_count

    def _collect_timed_id3_ad_windows(
        self,
        *,
        source_path: Path,
        started_at: datetime,
        duration_seconds: float,
    ) -> list[tuple[datetime, datetime]]:
        offsets = self._extract_timed_id3_ad_offsets(
            source_path,
            expected_duration_seconds=duration_seconds,
        )
        if not offsets:
            return []

        windows: list[tuple[datetime, datetime]] = []
        for start_offset, end_offset in offsets:
            start = max(0.0, min(duration_seconds, start_offset))
            end = max(0.0, min(duration_seconds, end_offset))
            if end <= start:
                continue
            windows.append(
                (
                    started_at + timedelta(seconds=start),
                    started_at + timedelta(seconds=end),
                )
            )
        return windows

    def _extract_timed_id3_ad_offsets(
        self,
        source_path: Path,
        *,
        expected_duration_seconds: float | None = None,
    ) -> list[tuple[float, float]]:
        cmd = [
            "ffprobe",
            "-hide_banner",
            "-loglevel",
            "error",
            "-select_streams",
            "d:0",
            "-show_packets",
            "-show_entries",
            "packet=pts_time,data",
            "-show_data",
            str(source_path),
        ]
        result = subprocess.run(
            cmd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            return []

        current_pts: float | None = None
        packet_has_ad = False
        packet_pts: list[float] = []
        marker_times: list[float] = []
        ad_tokens = (
            "content.ad",
            "stitched-ad",
            "twitch-ads",
            "ad_signifier",
            "amazon-adsystem",
        )

        def flush_packet() -> None:
            if current_pts is None:
                return
            packet_pts.append(current_pts)
            if packet_has_ad:
                marker_times.append(current_pts)

        for raw_line in result.stdout.splitlines():
            line = raw_line.strip()
            if line.startswith("pts_time="):
                flush_packet()
                try:
                    current_pts = float(line.split("=", 1)[1])
                except ValueError:
                    current_pts = None
                    packet_has_ad = False
                    continue
                packet_has_ad = False
                continue

            if line == "[/PACKET]":
                flush_packet()
                current_pts = None
                packet_has_ad = False
                continue

            lower = line.lower()
            if any(token in lower for token in ad_tokens):
                packet_has_ad = True

        flush_packet()
        if not marker_times or not packet_pts:
            return []

        relative_times = self._normalize_timed_id3_marker_times(
            packet_pts=packet_pts,
            marker_times=marker_times,
            expected_duration_seconds=expected_duration_seconds,
        )
        if not relative_times:
            return []

        max_gap_seconds = 6.0
        tail_padding_seconds = 2.5
        windows: list[tuple[float, float]] = []
        group_start = relative_times[0]
        previous = relative_times[0]

        for current in relative_times[1:]:
            if current - previous <= max_gap_seconds:
                previous = current
                continue
            windows.append((group_start, previous + tail_padding_seconds))
            group_start = current
            previous = current
        windows.append((group_start, previous + tail_padding_seconds))
        return windows

    def _normalize_timed_id3_marker_times(
        self,
        *,
        packet_pts: list[float],
        marker_times: list[float],
        expected_duration_seconds: float | None = None,
    ) -> list[float]:
        if not packet_pts or not marker_times:
            return []

        deltas = [
            current - previous
            for previous, current in zip(packet_pts, packet_pts[1:])
            if 0.0 < current - previous <= self.TIMED_ID3_DISCONTINUITY_MIN_SECONDS
        ]
        typical_gap = statistics.median(deltas) if deltas else 2.0
        discontinuity_threshold = max(
            self.TIMED_ID3_DISCONTINUITY_MIN_SECONDS,
            typical_gap * self.TIMED_ID3_DISCONTINUITY_FACTOR,
        )

        normalized_pts: list[float] = [packet_pts[0]]
        for previous_raw, current_raw in zip(packet_pts, packet_pts[1:]):
            delta = current_raw - previous_raw
            if delta <= 0.0:
                delta = typical_gap
            elif expected_duration_seconds is not None and delta > discontinuity_threshold:
                delta = typical_gap
            normalized_pts.append(normalized_pts[-1] + delta)

        marker_counts: dict[float, int] = {}
        for marker in marker_times:
            marker_counts[marker] = marker_counts.get(marker, 0) + 1

        normalized_markers: list[float] = []
        for raw_pts, normalized_pts_value in zip(packet_pts, normalized_pts):
            count = marker_counts.get(raw_pts, 0)
            if count <= 0:
                continue
            normalized_markers.extend([normalized_pts_value] * count)

        if not normalized_markers:
            return []

        base_pts = normalized_pts[0]
        relative_times = sorted({max(0.0, pts - base_pts) for pts in normalized_markers})
        if expected_duration_seconds is None:
            return relative_times
        return [
            offset
            for offset in relative_times
            if offset <= expected_duration_seconds + self.TIMED_ID3_DISCONTINUITY_MIN_SECONDS
        ]

    def _collect_ad_windows(
        self, events: list[RecordingEvent], *, ended_at: datetime
    ) -> list[tuple[datetime, datetime]]:
        windows: list[tuple[datetime, datetime]] = []
        ad_started_at: datetime | None = None

        for event in events:
            if event.type == "ad_break_started":
                if ad_started_at is None:
                    ad_started_at = event.at
            elif event.type == "ad_break_ended":
                if ad_started_at is None:
                    continue
                if event.at > ad_started_at:
                    windows.append((ad_started_at, event.at))
                ad_started_at = None

        if ad_started_at is not None and ended_at > ad_started_at:
            windows.append((ad_started_at, ended_at))

        return windows

    def _build_keep_ranges(
        self,
        started_at: datetime,
        ended_at: datetime,
        ad_windows: list[tuple[datetime, datetime]],
        trim_start_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        duration = max(0.0, (ended_at - started_at).total_seconds())
        if duration <= 0:
            return []

        clipped = [
            (
                max(0.0, (ad_start - started_at).total_seconds()),
                min(duration, (ad_end - started_at).total_seconds()),
            )
            for ad_start, ad_end in sorted(ad_windows)
            if ad_end > ad_start
        ]
        return self._build_keep_ranges_from_offsets(
            duration,
            clipped,
            trim_start_seconds=trim_start_seconds,
        )

    def _build_keep_ranges_from_offsets(
        self,
        duration_seconds: float,
        remove_ranges: list[tuple[float, float]],
        *,
        trim_start_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        if duration_seconds <= 0:
            return []
        trim_start = min(duration_seconds, max(0.0, trim_start_seconds))

        merged: list[tuple[float, float]] = []
        for start, end in sorted(remove_ranges):
            start = max(trim_start, start)
            end = min(duration_seconds, end)
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

        keep_ranges: list[tuple[float, float]] = []
        cursor = trim_start
        for ad_start, ad_end in merged:
            if ad_start > cursor:
                keep_ranges.append((cursor, ad_start))
            cursor = max(cursor, ad_end)
        if cursor < duration_seconds:
            keep_ranges.append((cursor, duration_seconds))

        return [(start, end) for start, end in keep_ranges if end - start >= 0.25]

    def _repair_watchable_output(self, watchable_path: Path) -> tuple[str | None, int]:
        detected_ad_break_count = 0
        for _ in range(self.MAX_WATCHABLE_REPAIR_PASSES):
            duration_seconds = self._probe_media_duration(watchable_path)
            if duration_seconds is None:
                return "failed to probe watchable output duration", detected_ad_break_count

            overlay_windows = self._collect_ocr_ad_windows(
                watchable_path,
                duration_seconds=duration_seconds,
                sample_interval_seconds=self.OCR_VERIFY_INTERVAL_SECONDS,
            )
            if not overlay_windows:
                return None, detected_ad_break_count

            detected_ad_break_count = max(detected_ad_break_count, len(overlay_windows))
            keep_ranges = self._build_keep_ranges_from_offsets(
                duration_seconds,
                overlay_windows,
                trim_start_seconds=0.0,
            )
            if not keep_ranges:
                return "Twitch playback overlay covered the entire watchable output", detected_ad_break_count

            repaired_path = watchable_path.with_name(f"{watchable_path.stem}.repair{watchable_path.suffix}")
            self._render_watchable(
                source_path=watchable_path,
                watchable_path=repaired_path,
                keep_ranges=keep_ranges,
            )
            repaired_path.replace(watchable_path)

        residual_duration = self._probe_media_duration(watchable_path)
        residual_windows = (
            self._collect_ocr_ad_windows(
                watchable_path,
                duration_seconds=residual_duration,
                sample_interval_seconds=self.OCR_VERIFY_INTERVAL_SECONDS,
            )
            if residual_duration is not None
            else []
        )
        if residual_windows:
            detected_ad_break_count = max(detected_ad_break_count, len(residual_windows))
            return "watchable verification still detected Twitch playback overlay", detected_ad_break_count
        return None, detected_ad_break_count

    def _probe_media_duration(self, source_path: Path) -> float | None:
        cmd = [
            "ffprobe",
            "-hide_banner",
            "-loglevel",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(source_path),
        ]
        try:
            result = subprocess.run(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        try:
            duration = float(result.stdout.strip())
        except ValueError:
            return None
        if duration <= 0:
            return None
        return duration

    def _collect_ocr_ad_windows(
        self,
        source_path: Path,
        *,
        duration_seconds: float,
        sample_interval_seconds: float,
    ) -> list[tuple[float, float]]:
        if duration_seconds <= 0 or sample_interval_seconds <= 0:
            return []

        frame_pattern = "frame_%06d.png"
        filter_chain = (
            "fps=1/"
            f"{sample_interval_seconds},"
            "crop=iw*0.78:ih*0.30:iw*0.11:ih*0.24,"
            "scale=1400:-1,"
            "format=gray"
        )
        with tempfile.TemporaryDirectory(prefix="ocrscan_") as temp_dir:
            temp_root = Path(temp_dir)
            cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(source_path),
                "-vf",
                filter_chain,
                "-start_number",
                "0",
                str(temp_root / frame_pattern),
            ]
            try:
                result = subprocess.run(
                    cmd,
                    text=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
            except (OSError, subprocess.SubprocessError):
                return []
            if result.returncode != 0:
                return []

            hit_windows: list[tuple[float, float]] = []
            for frame_path in sorted(temp_root.glob("frame_*.png")):
                try:
                    frame_index = int(frame_path.stem.rsplit("_", 1)[1])
                except (IndexError, ValueError):
                    continue
                ocr_text = self._run_tesseract(frame_path)
                if not self._ocr_text_matches_twitch_overlay(ocr_text):
                    continue
                sample_time = frame_index * sample_interval_seconds
                hit_windows.append(
                    (
                        max(0.0, sample_time - self.OCR_HIT_PADDING_SECONDS),
                        min(duration_seconds, sample_time + self.OCR_HIT_PADDING_SECONDS),
                    )
                )

        return self._merge_offset_ranges(hit_windows, merge_gap_seconds=self.OCR_MERGE_GAP_SECONDS)

    def _run_tesseract(self, frame_path: Path) -> str:
        cmd = [
            "tesseract",
            str(frame_path),
            "stdout",
            "--psm",
            "6",
        ]
        try:
            result = subprocess.run(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.SubprocessError):
            return ""
        if result.returncode != 0:
            return ""
        return result.stdout.lower()

    def _ocr_text_matches_twitch_overlay(self, text: str) -> bool:
        normalized = " ".join(text.lower().split())
        collapsed = normalized.replace(" ", "")
        if "commercial break in progress" in normalized:
            return True
        if "break in progress" in normalized and any(
            token in normalized for token in ("commercial", "ommercial", "twitch")
        ):
            return True
        if any(
            token in normalized
            for token in ("preparing your stream", "preparing your strea", "preparing stream")
        ):
            return True
        return any(
            token in collapsed
            for token in ("preparingyourstream", "preparingyourstrea", "preparingstream")
        )

    def _merge_offset_ranges(
        self,
        ranges: list[tuple[float, float]],
        *,
        merge_gap_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        merged: list[tuple[float, float]] = []
        for start, end in sorted(ranges):
            if end <= start:
                continue
            if not merged:
                merged.append((start, end))
                continue
            prev_start, prev_end = merged[-1]
            if start <= prev_end + merge_gap_seconds:
                merged[-1] = (prev_start, max(prev_end, end))
            else:
                merged.append((start, end))
        return merged

    def _render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        watchable_path.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory(prefix="watchable_", dir=str(self.recordings_path)) as temp_dir:
            temp_root = Path(temp_dir)
            segment_paths: list[Path] = []

            for index, (start_seconds, end_seconds) in enumerate(keep_ranges):
                segment_path = temp_root / f"segment_{index:03d}.mp4"
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-ss",
                    f"{start_seconds:.3f}",
                    "-to",
                    f"{end_seconds:.3f}",
                    "-i",
                    str(source_path),
                    "-c:v",
                    "libx264",
                    "-c:a",
                    "aac",
                    "-movflags",
                    "+faststart",
                    str(segment_path),
                ]
                result = subprocess.run(cmd, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip() or "ffmpeg segment render failed")
                segment_paths.append(segment_path)

            if not segment_paths:
                raise RuntimeError("no keep ranges were rendered")

            if len(segment_paths) == 1:
                shutil.move(str(segment_paths[0]), str(watchable_path))
                return

            concat_file = temp_root / "concat.txt"
            concat_lines: list[str] = []
            for segment_path in segment_paths:
                escaped = str(segment_path).replace("'", "'\\''")
                concat_lines.append(f"file '{escaped}'\n")
            concat_file.write_text(
                "".join(concat_lines),
                encoding="utf-8",
            )
            concat_cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_file),
                "-c",
                "copy",
                str(watchable_path),
            ]
            concat_result = subprocess.run(
                concat_cmd, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
            )
            if concat_result.returncode != 0:
                raise RuntimeError(concat_result.stderr.strip() or "ffmpeg concat failed")
