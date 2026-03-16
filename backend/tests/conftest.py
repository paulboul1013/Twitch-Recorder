from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.recorder import RecorderManager
from app.service import MonitorService
from app.store import RecordingHistoryStore, StreamerStore
from app.twitch import TwitchClient


class FakeProcess:
    class _FakeStderr:
        def __init__(self, lines: list[str] | None = None) -> None:
            self._lines = [f"{line}\n" for line in (lines or [])]

        def __iter__(self):
            return iter(self._lines)

        def close(self) -> None:
            return None

    def __init__(self, stderr_lines: list[str] | None = None) -> None:
        self.returncode = None
        self.stderr = self._FakeStderr(stderr_lines)

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


def _status_for(payload: list[dict], name: str) -> dict:
    return next(item for item in payload if item["name"] == name)


def _load_metadata(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@contextmanager
def build_test_client(tmp_path: Path):
    settings = Settings(
        poll_interval_seconds=999,
        recordings_path=tmp_path / "recordings",
        config_path=tmp_path / "config",
    )
    settings.ensure_directories()
    service = MonitorService(
        settings=settings,
        store=StreamerStore(settings.streamers_file),
        recording_store=RecordingHistoryStore(settings.recordings_file),
        twitch_client=TwitchClient("", ""),
        recorder=RecorderManager(settings.recordings_path, settings.preferred_qualities),
    )
    app = create_app(service=service, enable_background=False)
    with TestClient(app, backend_options={"use_uvloop": True}) as client:
        yield client


def build_test_service(
    tmp_path: Path,
    grace_seconds: int = 20,
    start_delay_seconds: int = 15,
) -> MonitorService:
    settings = Settings(
        poll_interval_seconds=999,
        offline_grace_period_seconds=grace_seconds,
        recording_start_delay_seconds=start_delay_seconds,
        recordings_path=tmp_path / "recordings",
        config_path=tmp_path / "config",
    )
    settings.ensure_directories()
    return MonitorService(
        settings=settings,
        store=StreamerStore(settings.streamers_file),
        recording_store=RecordingHistoryStore(settings.recordings_file),
        twitch_client=TwitchClient("", ""),
        recorder=RecorderManager(settings.recordings_path, settings.preferred_qualities),
    )
