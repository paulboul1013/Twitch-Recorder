from __future__ import annotations

import asyncio
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models import StreamStatus
from app.recorder import RecorderManager
from app.service import MonitorService
from app.store import RecordingHistoryStore, StreamerStore, TrackedRecording
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


def test_add_list_and_delete_streamers(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        created = client.post("/streamers", json={"name": "TestChannel"})
        assert created.status_code == 201
        assert created.json() == {"name": "testchannel"}

        listed = client.get("/streamers")
        assert listed.status_code == 200
        assert listed.json() == [{"name": "testchannel"}]

        deleted = client.delete("/streamers/testchannel")
        assert deleted.status_code == 204

        listed_again = client.get("/streamers")
        assert listed_again.json() == []


def test_recordings_endpoint_lists_saved_files(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        sample = recordings_dir / "alpha_20250301_120000.mp4"
        sample.write_bytes(b"video-data")
        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(sample),
                watchable_file_path=str(sample),
                watchable_state="ready",
                ad_break_count=0,
            )
        )

        response = client.get("/recordings")
        assert response.status_code == 200

        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["channel"] == "alpha"
        assert payload[0]["file_name"] == sample.name
        assert payload[0]["source_file_name"] == sample.name
        assert payload[0]["watchable_available"] is True
        assert payload[0]["watchable_state"] == "ready"
        assert payload[0]["ad_break_count"] == 0


def test_recordings_endpoint_ignores_untracked_mp4_files(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        (recordings_dir / "manual_clip.mp4").write_bytes(b"video-data")

        response = client.get("/recordings")
        assert response.status_code == 200
        assert response.json() == []


def test_refresh_without_credentials_reports_status(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        client.post("/streamers", json={"name": "alpha"})

        response = client.post("/refresh")
        assert response.status_code == 200

        payload = response.json()
        assert payload[0]["name"] == "alpha"
        assert payload[0]["is_live"] is False
        assert "TWITCH_CLIENT_ID" in payload[0]["last_error"]


def test_stop_recording_returns_not_stopped_when_idle(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        client.post("/streamers", json={"name": "alpha"})

        response = client.post("/streamers/alpha/stop")

        assert response.status_code == 200
        assert response.json() == {"name": "alpha", "stopped": False}


def test_stop_recording_updates_status_fields(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        client.post("/streamers", json={"name": "alpha"})

        service: MonitorService = client.app.state.monitor_service
        with (
            patch("app.recorder.subprocess.Popen", return_value=FakeProcess()),
            patch.object(
                RecorderManager,
                "_build_watchable_output",
                side_effect=lambda self, **kwargs: (str(kwargs["source_path"]), "ready", None, 0),
                autospec=True,
            ),
        ):
            output_path = service.recorder.start_recording("alpha")
            assert output_path.endswith(".mp4")
            Path(output_path).write_bytes(b"video-data")

            response = client.post("/streamers/alpha/stop")
        assert response.status_code == 200
        assert response.json() == {"name": "alpha", "stopped": True}

        statuses = client.get("/status")
        assert statuses.status_code == 200
        alpha = _status_for(statuses.json(), "alpha")
        assert alpha["is_recording"] is False
        assert alpha["recording_state"] == "stopped"
        assert alpha["recording_exit_code"] is not None
        assert alpha["recording_started_at"] is not None
        assert alpha["recording_ended_at"] is not None
        assert alpha["output_path"] is not None

        recordings = client.get("/recordings")
        payload = recordings.json()
        assert len(payload) == 1
        assert payload[0]["watchable_available"] is True
        assert payload[0]["watchable_state"] == "ready"


def test_start_recording_returns_not_started_when_offline(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        client.post("/streamers", json={"name": "alpha"})

        response = client.post("/streamers/alpha/start")

        assert response.status_code == 200
        assert response.json() == {"name": "alpha", "started": False}


def test_offline_recording_enters_grace_period_before_stop(tmp_path: Path) -> None:
    service = build_test_service(tmp_path, grace_seconds=30)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = service._statuses.get("alpha") or StreamStatus(name="alpha")

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        service.recorder.start_recording("alpha")

    async def fake_get_live_streams(usernames):
        return {}

    service.twitch_client.get_live_streams = fake_get_live_streams
    asyncio.run(service.refresh_once())

    alpha = service.list_statuses()[0]
    assert alpha.is_recording is True
    assert alpha.recording_state == "grace_period"
    assert alpha.offline_since is not None
    assert alpha.stop_after_at is not None


def test_offline_recording_stops_after_grace_period(tmp_path: Path) -> None:
    service = build_test_service(tmp_path, grace_seconds=0)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = service._statuses.get("alpha") or StreamStatus(name="alpha")

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        service.recorder.start_recording("alpha")

    async def fake_get_live_streams(usernames):
        return {}

    service.twitch_client.get_live_streams = fake_get_live_streams
    asyncio.run(service.refresh_once())

    alpha = service.list_statuses()[0]
    assert alpha.is_recording is False
    assert alpha.recording_state == "stopped"
    assert alpha.recording_ended_at is not None


def test_refresh_includes_profile_image_url(tmp_path: Path) -> None:
    service = build_test_service(tmp_path)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = service._statuses.get("alpha") or StreamStatus(name="alpha")

    async def fake_get_live_streams(usernames):
        return {}

    async def fake_get_users(usernames):
        return {
            "alpha": type(
                "FakeUser",
                (),
                {"login": "alpha", "profile_image_url": "https://example.com/alpha.jpg"},
            )()
        }

    service.twitch_client.get_live_streams = fake_get_live_streams
    service.twitch_client.get_users = fake_get_users
    asyncio.run(service.refresh_once())

    alpha = service.list_statuses()[0]
    assert alpha.profile_image_url == "https://example.com/alpha.jpg"


def test_live_lookup_failure_does_not_stop_active_recording(tmp_path: Path) -> None:
    service = build_test_service(tmp_path, grace_seconds=0)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = StreamStatus(
        name="alpha",
        is_live=True,
        recording_state="recording",
    )

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        output_path = service.recorder.start_recording("alpha")

    async def fake_get_live_streams(usernames):
        raise httpx.ConnectError("dns failed")

    async def fake_get_users(usernames):
        return {}

    service.twitch_client.get_live_streams = fake_get_live_streams
    service.twitch_client.get_users = fake_get_users
    asyncio.run(service.refresh_once())

    alpha = service.list_statuses()[0]
    assert alpha.is_recording is True
    assert alpha.recording_state == "recording"
    assert alpha.output_path == output_path
    assert alpha.offline_since is None
    assert alpha.stop_after_at is None
    assert alpha.last_error == "dns failed"


def test_manual_stop_prevents_immediate_restart_while_stream_is_live(tmp_path: Path) -> None:
    service = build_test_service(tmp_path)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = StreamStatus(name="alpha")

    class FakeLiveStream:
        title = "Live now"
        game_name = "Just Chatting"
        viewer_count = 10
        started_at = None

    async def fake_get_live_streams(usernames):
        return {"alpha": FakeLiveStream()}

    async def fake_get_users(usernames):
        return {}

    service.twitch_client.get_live_streams = fake_get_live_streams
    service.twitch_client.get_users = fake_get_users

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        asyncio.run(service.refresh_once())
        assert service.recorder.is_recording("alpha") is True

        response = asyncio.run(service.stop_streamer_recording("alpha"))
        assert response.stopped is True

        asyncio.run(service.refresh_once())

    alpha = service.list_statuses()[0]
    assert alpha.is_live is True
    assert alpha.is_recording is False
    assert alpha.recording_state == "stopped"

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        response = asyncio.run(service.start_streamer_recording("alpha"))

    assert response.started is True
    alpha = service.list_statuses()[0]
    assert alpha.is_recording is True
    assert alpha.recording_state == "recording"

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        async def fake_get_live_streams_offline(usernames):
            return {}

        service.twitch_client.get_live_streams = fake_get_live_streams_offline
        asyncio.run(service.refresh_once())

        service.twitch_client.get_live_streams = fake_get_live_streams
        asyncio.run(service.refresh_once())
    alpha = service.list_statuses()[0]
    assert alpha.is_recording is True
    assert alpha.recording_state == "recording"


def test_active_recording_shows_ad_break_state(tmp_path: Path) -> None:
    service = build_test_service(tmp_path)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = StreamStatus(
        name="alpha",
        is_live=True,
        recording_state="recording",
    )
    with patch(
        "app.recorder.subprocess.Popen",
        return_value=FakeProcess(stderr_lines=["Commercial break started"]),
    ):
        service.recorder.start_recording("alpha")

    alpha = service.list_statuses()[0]
    assert alpha.is_recording is True
    assert alpha.recording_state == "ad_break"


def test_auto_start_waits_for_recording_start_delay(tmp_path: Path) -> None:
    service = build_test_service(tmp_path, start_delay_seconds=15)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = StreamStatus(name="alpha")

    class FakeLiveStream:
        title = "Live now"
        game_name = "Just Chatting"
        viewer_count = 10
        started_at = datetime.now(UTC)

    async def fake_get_live_streams(usernames):
        return {"alpha": FakeLiveStream()}

    async def fake_get_users(usernames):
        return {}

    service.twitch_client.get_live_streams = fake_get_live_streams
    service.twitch_client.get_users = fake_get_users

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()) as popen:
        asyncio.run(service.refresh_once())

    alpha = service.list_statuses()[0]
    assert popen.call_count == 0
    assert alpha.is_live is True
    assert alpha.is_recording is False
    assert alpha.recording_state == "start_delay"


def test_watchable_output_is_rendered_even_without_ad_windows(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)

    def fake_render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        watchable_path.write_bytes(b"watchable")

    with (
        patch.object(RecorderManager, "_render_watchable", fake_render_watchable),
        patch.object(RecorderManager, "_repair_watchable_output", return_value=(None, 0)),
    ):
        watchable_path, watchable_state, watchable_error, ad_break_count = recorder._build_watchable_output(
            source_path=source_path,
            started_at=started_at,
            ended_at=ended_at,
            events=[],
        )

    assert watchable_state == "ready"
    assert watchable_error is None
    assert ad_break_count == 0
    assert watchable_path is not None
    assert watchable_path.endswith(".watchable.mp4")


def test_watchable_output_applies_trim_start_seconds(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",), watchable_trim_start_seconds=12)
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)
    captured: dict[str, list[tuple[float, float]]] = {}

    def fake_render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        captured["keep_ranges"] = keep_ranges
        watchable_path.write_bytes(b"watchable")

    with (
        patch.object(RecorderManager, "_render_watchable", fake_render_watchable),
        patch.object(RecorderManager, "_repair_watchable_output", return_value=(None, 0)),
    ):
        _, watchable_state, watchable_error, ad_break_count = recorder._build_watchable_output(
            source_path=source_path,
            started_at=started_at,
            ended_at=ended_at,
            events=[],
        )

    assert watchable_state == "ready"
    assert watchable_error is None
    assert ad_break_count == 0
    assert captured["keep_ranges"] == [(12.0, 30.0)]


def test_watchable_output_uses_timed_id3_fallback_for_ad_breaks(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)
    captured: dict[str, list[tuple[float, float]]] = {}

    def fake_render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        captured["keep_ranges"] = keep_ranges
        watchable_path.write_bytes(b"watchable")

    with (
        patch.object(RecorderManager, "_extract_timed_id3_ad_offsets", return_value=[(0.0, 10.0)]),
        patch.object(RecorderManager, "_render_watchable", fake_render_watchable),
        patch.object(RecorderManager, "_repair_watchable_output", return_value=(None, 0)),
    ):
        _, watchable_state, watchable_error, ad_break_count = recorder._build_watchable_output(
            source_path=source_path,
            started_at=started_at,
            ended_at=ended_at,
            events=[],
        )

    assert watchable_state == "ready"
    assert watchable_error is None
    assert ad_break_count == 1
    assert captured["keep_ranges"] == [(10.0, 30.0)]


def test_watchable_output_fails_when_verification_still_detects_ad_overlay(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)

    def fake_render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        watchable_path.write_bytes(b"watchable")

    with (
        patch.object(RecorderManager, "_render_watchable", fake_render_watchable),
        patch.object(
            RecorderManager,
            "_repair_watchable_output",
            return_value=("watchable verification still detected commercial break overlay", 1),
        ),
    ):
        watchable_path, watchable_state, watchable_error, ad_break_count = recorder._build_watchable_output(
            source_path=source_path,
            started_at=started_at,
            ended_at=ended_at,
            events=[],
        )

    assert watchable_path is None
    assert watchable_state == "failed"
    assert watchable_error == "watchable verification still detected commercial break overlay"
    assert ad_break_count == 1
    assert not source_path.with_name("sample.watchable.mp4").exists()


def test_extract_timed_id3_ad_offsets_groups_consecutive_markers() -> None:
    recorder = RecorderManager(Path("."), ("best",))
    ffprobe_stdout = """
[PACKET]
pts_time=64.000000
[/PACKET]
[PACKET]
pts_time=64.001000
data=
00000010: 000b 0000 0363 6f6e 7465 6e74 0061 64    .....content.ad
[/PACKET]
[PACKET]
pts_time=66.001000
data=
00000010: 000b 0000 0363 6f6e 7465 6e74 0061 64    .....content.ad
[/PACKET]
[PACKET]
pts_time=120.001000
data=
00000010: 000b 0000 0363 6f6e 7465 6e74 0061 64    .....content.ad
[/PACKET]
"""
    ffprobe_result = subprocess.CompletedProcess(
        args=["ffprobe"],
        returncode=0,
        stdout=ffprobe_stdout,
        stderr="",
    )

    with patch("app.recorder.subprocess.run", return_value=ffprobe_result):
        windows = recorder._extract_timed_id3_ad_offsets(Path("sample.mp4"))

    assert len(windows) == 2
    first_start, first_end = windows[0]
    second_start, second_end = windows[1]
    assert round(first_start, 3) == 0.001
    assert round(first_end, 3) == 4.501
    assert round(second_start, 3) == 56.001
    assert round(second_end, 3) == 58.501
