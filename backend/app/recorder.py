from __future__ import annotations

import json
import shutil
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

    def is_recording(self, channel: str) -> bool:
        recording = self._active.get(channel)
        return bool(recording and recording.process.poll() is None)

    def is_in_ad_break(self, channel: str) -> bool:
        recording = self._active.get(channel)
        if not recording:
            return False
        with recording.lock:
            return recording.ad_break_active

    def current_output_path(self, channel: str) -> str | None:
        recording = self._active.get(channel)
        if not recording:
            return None
        return str(recording.file_path)

    def active_count(self) -> int:
        self.poll()
        return len(self._active)

    def poll(self) -> list[RecordingResult]:
        finished_results: list[RecordingResult] = []
        finished_channels = [
            channel
            for channel, recording in self._active.items()
            if recording.process.poll() is not None
        ]
        for channel in finished_channels:
            recording = self._active.pop(channel, None)
            if recording is None:
                continue
            exit_code = recording.process.wait()
            self._join_stderr_thread(recording)
            finished_results.append(
                self._finalize_recording(
                    recording=recording,
                    ended_at=datetime.now(UTC),
                    exit_code=exit_code,
                    state="completed" if exit_code == 0 else "failed",
                )
            )
        return finished_results

    def start_recording(self, channel: str) -> str:
        self.poll()
        if channel in self._active:
            return str(self._active[channel].file_path)

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
            state="recording",
            clean_output_path=None,
            clean_output_state="pending",
            clean_output_error=None,
        )
        recording.stderr_thread = self._start_stderr_thread(recording)
        self._active[channel] = recording
        return str(output_path)

    def stop_recording(self, channel: str) -> RecordingResult | None:
        self.poll()
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
        return self._finalize_recording(
            recording=recording,
            ended_at=datetime.now(UTC),
            exit_code=exit_code,
            state="stopped",
        )

    def stop_all(self) -> None:
        for channel in list(self._active):
            self.stop_recording(channel)

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

    def _finalize_recording(
        self,
        recording: ActiveRecording,
        ended_at: datetime,
        exit_code: int,
        state: str,
    ) -> RecordingResult:
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
            events_snapshot = list(recording.events)

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
        state: str,
        clean_output_path: str | None,
        clean_output_state: str,
        clean_output_error: str | None,
        ad_break_count_override: int | None = None,
    ) -> None:
        with recording.lock:
            events_payload = [event.as_dict() for event in recording.events]
        if ad_break_count_override is None:
            ad_break_count = sum(1 for event in events_payload if event["type"] == "ad_break_started")
        else:
            ad_break_count = max(0, int(ad_break_count_override))

        payload = {
            "channel": recording.channel,
            "file_path": str(recording.file_path),
            "started_at": recording.started_at.isoformat(),
            "ended_at": ended_at.isoformat() if ended_at else None,
            "state": state,
            "events": events_payload,
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

        ad_windows = self._collect_ad_windows(events, ended_at=ended_at)
        if not ad_windows:
            ad_windows = self._collect_timed_id3_ad_windows(
                source_path=source_path,
                started_at=started_at,
                ended_at=ended_at,
            )
        ad_break_count = len(ad_windows)
        keep_ranges = self._build_keep_ranges(
            started_at,
            ended_at,
            ad_windows,
            trim_start_seconds=float(self.watchable_trim_start_seconds),
        )
        if not keep_ranges:
            return None, "failed", "recording duration too short to produce watchable output", ad_break_count

        watchable_path = source_path.with_name(f"{source_path.stem}.watchable{source_path.suffix}")
        try:
            self._render_watchable(source_path=source_path, watchable_path=watchable_path, keep_ranges=keep_ranges)
            return str(watchable_path), "ready", None, ad_break_count
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            return None, "failed", str(exc), ad_break_count

    def _collect_timed_id3_ad_windows(
        self,
        *,
        source_path: Path,
        started_at: datetime,
        ended_at: datetime,
    ) -> list[tuple[datetime, datetime]]:
        offsets = self._extract_timed_id3_ad_offsets(source_path)
        if not offsets:
            return []

        duration_seconds = max(0.0, (ended_at - started_at).total_seconds())
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

    def _extract_timed_id3_ad_offsets(self, source_path: Path) -> list[tuple[float, float]]:
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

        base_pts: float | None = None
        current_pts: float | None = None
        packet_has_ad = False
        marker_times: list[float] = []
        ad_tokens = (
            "content.ad",
            "stitched-ad",
            "twitch-ads",
            "ad_signifier",
            "amazon-adsystem",
        )

        for raw_line in result.stdout.splitlines():
            line = raw_line.strip()
            if line.startswith("pts_time="):
                if current_pts is not None and packet_has_ad:
                    marker_times.append(current_pts)
                try:
                    current_pts = float(line.split("=", 1)[1])
                except ValueError:
                    current_pts = None
                    packet_has_ad = False
                    continue
                if base_pts is None:
                    base_pts = current_pts
                packet_has_ad = False
                continue

            if line == "[/PACKET]":
                if current_pts is not None and packet_has_ad:
                    marker_times.append(current_pts)
                current_pts = None
                packet_has_ad = False
                continue

            lower = line.lower()
            if any(token in lower for token in ad_tokens):
                packet_has_ad = True

        if current_pts is not None and packet_has_ad:
            marker_times.append(current_pts)

        if not marker_times or base_pts is None:
            return []

        relative_times = sorted({max(0.0, pts - base_pts) for pts in marker_times})
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
        trim_start = min(duration, max(0.0, trim_start_seconds))

        clipped: list[tuple[float, float]] = []
        for ad_start, ad_end in sorted(ad_windows):
            start_offset = max(trim_start, (ad_start - started_at).total_seconds())
            end_offset = min(duration, (ad_end - started_at).total_seconds())
            if end_offset <= start_offset:
                continue
            clipped.append((start_offset, end_offset))

        if not clipped:
            return [(trim_start, duration)] if duration - trim_start >= 0.25 else []

        merged: list[tuple[float, float]] = []
        for start, end in clipped:
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
        if cursor < duration:
            keep_ranges.append((cursor, duration))

        return [(start, end) for start, end in keep_ranges if end - start >= 0.25]

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
