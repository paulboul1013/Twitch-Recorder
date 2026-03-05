from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .models import RecordingInfo


@dataclass(slots=True)
class ActiveRecording:
    channel: str
    process: subprocess.Popen[str]
    file_path: Path
    started_at: datetime


@dataclass(slots=True)
class RecordingResult:
    channel: str
    file_path: Path
    started_at: datetime
    ended_at: datetime
    exit_code: int
    state: str


class RecorderManager:
    def __init__(self, recordings_path: Path, preferred_qualities: tuple[str, ...]) -> None:
        self.recordings_path = recordings_path
        self.preferred_qualities = preferred_qualities
        self._active: dict[str, ActiveRecording] = {}

    def is_recording(self, channel: str) -> bool:
        recording = self._active.get(channel)
        return bool(recording and recording.process.poll() is None)

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
            finished_results.append(
                RecordingResult(
                    channel=channel,
                    file_path=recording.file_path,
                    started_at=recording.started_at,
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
        quality = ",".join(self.preferred_qualities)
        cmd = [
            "streamlink",
            f"https://twitch.tv/{channel}",
            quality,
            "-o",
            str(output_path),
        ]
        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        self._active[channel] = ActiveRecording(
            channel=channel,
            process=process,
            file_path=output_path,
            started_at=datetime.now(UTC),
        )
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

        return RecordingResult(
            channel=channel,
            file_path=recording.file_path,
            started_at=recording.started_at,
            ended_at=datetime.now(UTC),
            exit_code=exit_code,
            state="stopped",
        )

    def stop_all(self) -> None:
        for channel in list(self._active):
            self.stop_recording(channel)

    def list_recordings(self) -> list[RecordingInfo]:
        files = sorted(self.recordings_path.glob("*.mp4"), key=lambda item: item.stat().st_mtime, reverse=True)
        results: list[RecordingInfo] = []
        for file_path in files:
            stat = file_path.stat()
            results.append(
                RecordingInfo(
                    channel=file_path.stem.split("_", 1)[0],
                    file_path=str(file_path),
                    file_name=file_path.name,
                    size_bytes=stat.st_size,
                    modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                )
            )
        return results
