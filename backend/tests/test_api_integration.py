from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import httpx

from app.api_integration import build_streamlink_command
from app.clean_export import CleanExportJob
from app.models import StreamStatus
from app.recorder import RecorderManager
from app.service import MonitorService
from app.store import TrackedRecording
from conftest import FakeProcess, _status_for, build_test_client, build_test_service

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


def test_streamer_recording_directories_endpoint_lists_only_deletable_segment_native_dirs(
    tmp_path: Path,
) -> None:
    with build_test_client(tmp_path) as client:
        alpha_root = tmp_path / "recordings" / "alpha"
        deletable_root = alpha_root / "alpha_20260320_120000_000001"
        processing_root = alpha_root / "alpha_20260320_121000_000002"
        legacy_source = tmp_path / "recordings" / "alpha_legacy_20260320_122000.ts"
        other_root = tmp_path / "recordings" / "bravo" / "bravo_20260320_120000_000003"

        for recording_root in (deletable_root, processing_root, other_root):
            (recording_root / "segments").mkdir(parents=True, exist_ok=True)
            (recording_root / "segments" / "segment_000000.ts").write_bytes(b"video-data")

        legacy_source.parent.mkdir(parents=True, exist_ok=True)
        legacy_source.write_bytes(b"legacy-data")

        client.post("/streamers", json={"name": "alpha"})

        service: MonitorService = client.app.state.monitor_service
        service.recording_store.save(
            [
                TrackedRecording(
                    channel="alpha",
                    source_file_path=str(deletable_root / "segments" / "segment_000000.ts"),
                    recording_id="rec-delete-ok",
                    artifact_mode="segment_native",
                    started_at="2026-03-20T12:00:00+00:00",
                    ended_at="2026-03-20T12:05:00+00:00",
                ),
                TrackedRecording(
                    channel="alpha",
                    source_file_path=str(processing_root / "segments" / "segment_000000.ts"),
                    recording_id="rec-processing",
                    artifact_mode="segment_native",
                    clean_compact_state="processing",
                    started_at="2026-03-20T12:10:00+00:00",
                    ended_at="2026-03-20T12:15:00+00:00",
                ),
                TrackedRecording(
                    channel="alpha",
                    source_file_path=str(legacy_source),
                    recording_id="rec-legacy",
                    artifact_mode="legacy",
                    started_at="2026-03-20T12:20:00+00:00",
                    ended_at="2026-03-20T12:25:00+00:00",
                ),
                TrackedRecording(
                    channel="bravo",
                    source_file_path=str(other_root / "segments" / "segment_000000.ts"),
                    recording_id="rec-other",
                    artifact_mode="segment_native",
                    started_at="2026-03-20T12:30:00+00:00",
                    ended_at="2026-03-20T12:35:00+00:00",
                ),
            ]
        )

        response = client.get("/streamers/alpha/recording-directories")
        assert response.status_code == 200
        payload = response.json()
        assert payload == [
            {
                "recording_id": "rec-delete-ok",
                "channel": "alpha",
                "directory_name": "alpha_20260320_120000_000001",
                "started_at": "2026-03-20T12:00:00+00:00",
                "ended_at": "2026-03-20T12:05:00+00:00",
                "modified_at": payload[0]["modified_at"],
            }
        ]


def test_recordings_endpoint_lists_saved_files(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        source = recordings_dir / "alpha_20250301_120000.ts"
        watchable = recordings_dir / "alpha_20250301_120000.watchable.mp4"
        source.write_bytes(b"source-video-data")
        watchable.write_bytes(b"watchable-video-data")
        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(source),
                watchable_file_path=str(watchable),
                watchable_state="ready",
                ad_break_count=0,
            )
        )

        response = client.get("/recordings")
        assert response.status_code == 200

        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["channel"] == "alpha"
        assert payload[0]["file_name"] == source.name
        assert payload[0]["source_file_name"] == source.name
        assert payload[0]["watchable_available"] is True
        assert payload[0]["watchable_file_name"] == watchable.name
        assert payload[0]["watchable_state"] == "ready"
        assert payload[0]["ad_break_count"] == 0


def test_recordings_endpoint_ignores_untracked_mp4_files(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        (recordings_dir / "manual_clip.mp4").write_bytes(b"video-data")

        response = client.get("/recordings")
        assert response.status_code == 200
        assert response.json() == []


def test_recordings_endpoint_lists_watchable_when_source_is_missing(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        source = recordings_dir / "alpha_20250301_120000.ts"
        watchable = recordings_dir / "alpha_20250301_120000.watchable.mp4"
        watchable.write_bytes(b"watchable-video-data")
        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(source),
                watchable_file_path=str(watchable),
                watchable_state="ready",
                ad_break_count=0,
            )
        )

        response = client.get("/recordings")
        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["channel"] == "alpha"
        assert payload[0]["source_file_name"] == source.name
        assert payload[0]["watchable_file_name"] == watchable.name
        assert payload[0]["watchable_available"] is True
        assert payload[0]["source_available"] is False


def test_recordings_endpoint_ignores_entries_when_source_and_watchable_are_missing(
    tmp_path: Path,
) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        source = recordings_dir / "alpha_20250301_120000.ts"
        watchable = recordings_dir / "alpha_20250301_120000.watchable.mp4"
        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(source),
                watchable_file_path=str(watchable),
                watchable_state="ready",
                ad_break_count=0,
            )
        )

        response = client.get("/recordings")
        assert response.status_code == 200
        assert response.json() == []


def test_recordings_endpoint_prefers_watchable_size_and_timestamp(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        source = recordings_dir / "alpha_20250301_120000.ts"
        watchable = recordings_dir / "alpha_20250301_120000.watchable.mp4"
        source.write_bytes(b"raw")
        watchable.write_bytes(b"watchable-video-data")
        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(source),
                watchable_file_path=str(watchable),
                watchable_state="ready",
                ad_break_count=0,
            )
        )

        response = client.get("/recordings")
        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["size_bytes"] == watchable.stat().st_size
        modified_at = datetime.fromisoformat(payload[0]["modified_at"])
    assert abs(modified_at.timestamp() - watchable.stat().st_mtime) < 1.0


def test_recordings_endpoint_prefers_clean_ts_when_compact_is_ready(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recordings_dir = tmp_path / "recordings"
        source = recordings_dir / "alpha_20260318_120000.ts"
        clean_manifest = recordings_dir / "alpha_20260318_120000" / "manifests" / "clean.m3u8"
        clean_ts = recordings_dir / "alpha_20260318_120000" / "exports" / "clean.ts"
        recordings_dir.mkdir(parents=True, exist_ok=True)
        source.write_bytes(b"source-video-data")
        clean_manifest.parent.mkdir(parents=True, exist_ok=True)
        clean_ts.parent.mkdir(parents=True, exist_ok=True)
        clean_manifest.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")
        clean_ts.write_bytes(b"compact-video-data")
        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(source),
                watchable_file_path=str(clean_manifest),
                watchable_state="ready",
                artifact_mode="segment_native",
                clean_artifact_path=str(clean_manifest),
                clean_compact_state="ready",
                clean_compact_path=str(clean_ts),
                ad_break_count=0,
            )
        )

        response = client.get("/recordings")
        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["clean_compact_state"] == "ready"
        assert payload[0]["watchable_available"] is True
        assert payload[0]["watchable_file_name"] == clean_ts.name
        assert payload[0]["size_bytes"] == clean_ts.stat().st_size


def test_recordings_endpoint_lists_active_recording_with_current_file_size(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        service: MonitorService = client.app.state.monitor_service
        with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
            output_path = Path(service.recorder.start_recording("alpha"))
        output_path.write_bytes(b"active-recording-data")

        response = client.get("/recordings")
        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["channel"] == "alpha"
        assert payload[0]["is_recording"] is True
        assert payload[0]["source_file_name"] == output_path.name
        assert payload[0]["size_bytes"] == output_path.stat().st_size

        with patch.object(
            RecorderManager,
            "_build_watchable_output",
            side_effect=lambda self, **kwargs: (str(kwargs["source_path"]), "ready", None, 0),
            autospec=True,
        ):
            service.recorder.stop_all(wait_for_finalize=True)


def test_streamer_recording_directories_delete_endpoint_removes_dirs_and_store_entries(
    tmp_path: Path,
) -> None:
    with build_test_client(tmp_path) as client:
        alpha_root = tmp_path / "recordings" / "alpha"
        delete_root = alpha_root / "alpha_20260320_120000_000001"
        keep_root = alpha_root / "alpha_20260320_121000_000002"

        for recording_root in (delete_root, keep_root):
            (recording_root / "segments").mkdir(parents=True, exist_ok=True)
            (recording_root / "segments" / "segment_000000.ts").write_bytes(b"video-data")

        client.post("/streamers", json={"name": "alpha"})

        service: MonitorService = client.app.state.monitor_service
        service.recording_store.save(
            [
                TrackedRecording(
                    channel="alpha",
                    source_file_path=str(delete_root / "segments" / "segment_000000.ts"),
                    recording_id="rec-delete-me",
                    artifact_mode="segment_native",
                ),
                TrackedRecording(
                    channel="alpha",
                    source_file_path=str(keep_root / "segments" / "segment_000000.ts"),
                    recording_id="rec-keep-me",
                    artifact_mode="segment_native",
                ),
            ]
        )

        response = client.post(
            "/streamers/alpha/recording-directories/delete",
            json={"recording_ids": ["rec-delete-me"]},
        )
        assert response.status_code == 200
        assert response.json() == {
            "channel": "alpha",
            "deleted_recording_ids": ["rec-delete-me"],
            "deleted_directory_names": ["alpha_20260320_120000_000001"],
        }
        assert delete_root.exists() is False
        assert keep_root.exists() is True

        remaining_ids = [tracked.recording_id for tracked in service.recording_store.load()]
        assert remaining_ids == ["rec-keep-me"]

        second_response = client.post(
            "/streamers/alpha/recording-directories/delete",
            json={"recording_ids": ["rec-keep-me"]},
        )
        assert second_response.status_code == 200
        assert alpha_root.exists() is False


def test_streamer_recording_directories_delete_endpoint_rejects_processing_directories(
    tmp_path: Path,
) -> None:
    with build_test_client(tmp_path) as client:
        alpha_root = tmp_path / "recordings" / "alpha"
        processing_root = alpha_root / "alpha_20260320_120000_000001"
        (processing_root / "segments").mkdir(parents=True, exist_ok=True)
        (processing_root / "segments" / "segment_000000.ts").write_bytes(b"video-data")

        client.post("/streamers", json={"name": "alpha"})

        service: MonitorService = client.app.state.monitor_service
        service.recording_store.save(
            [
                TrackedRecording(
                    channel="alpha",
                    source_file_path=str(processing_root / "segments" / "segment_000000.ts"),
                    recording_id="rec-processing",
                    artifact_mode="segment_native",
                    clean_compact_state="processing",
                )
            ]
        )

        response = client.post(
            "/streamers/alpha/recording-directories/delete",
            json={"recording_ids": ["rec-processing"]},
        )
        assert response.status_code == 400
        assert response.json()["detail"] == "one or more recording directories are still processing"
        assert processing_root.exists() is True
        assert [tracked.recording_id for tracked in service.recording_store.load()] == ["rec-processing"]


def test_segment_native_download_and_export_status_endpoints(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recording_root = tmp_path / "recordings" / "alpha_20250301_120000_000001"
        segments_dir = recording_root / "segments"
        manifests_dir = recording_root / "manifests"
        exports_dir = recording_root / "exports"
        segments_dir.mkdir(parents=True, exist_ok=True)
        manifests_dir.mkdir(parents=True, exist_ok=True)
        exports_dir.mkdir(parents=True, exist_ok=True)

        segment_path = segments_dir / "segment_000000.ts"
        full_manifest = manifests_dir / "full.m3u8"
        clean_manifest = manifests_dir / "clean.m3u8"
        clean_export = exports_dir / "clean.mp4"
        segment_path.write_bytes(b"video-data")
        full_manifest.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")
        clean_manifest.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")
        clean_export.write_bytes(b"clean-video")

        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(segment_path),
                recording_id="rec-segment-1",
                artifact_mode="segment_native",
                full_artifact_path=str(full_manifest),
                clean_artifact_path=str(clean_manifest),
                full_segment_count=1,
                clean_segment_count=1,
                clean_export_state="ready",
                clean_export_path=str(clean_export),
                watchable_state="ready",
            )
        )

        full_response = client.get("/recordings/rec-segment-1/download/full")
        assert full_response.status_code == 200
        manifest_response = client.get("/recordings/rec-segment-1/download/clean-manifest")
        assert manifest_response.status_code == 200
        export_download_response = client.get("/recordings/rec-segment-1/download/clean-mp4")
        assert export_download_response.status_code == 200

        status_response = client.get("/recordings/rec-segment-1/exports/clean-mp4")
        assert status_response.status_code == 200
        assert status_response.json()["state"] == "ready"


def test_segment_native_export_creation_endpoint_returns_queued_job(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recording_root = tmp_path / "recordings" / "alpha_20250301_120000_000002"
        segments_dir = recording_root / "segments"
        manifests_dir = recording_root / "manifests"
        segments_dir.mkdir(parents=True, exist_ok=True)
        manifests_dir.mkdir(parents=True, exist_ok=True)

        segment_path = segments_dir / "segment_000000.ts"
        clean_manifest = manifests_dir / "clean.m3u8"
        segment_path.write_bytes(b"video-data")
        clean_manifest.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")

        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(segment_path),
                recording_id="rec-segment-2",
                artifact_mode="segment_native",
                full_artifact_path=str(manifests_dir / "full.m3u8"),
                clean_artifact_path=str(clean_manifest),
                full_segment_count=1,
                clean_segment_count=1,
                clean_export_state="none",
                watchable_state="ready",
            )
        )

        fake_job = CleanExportJob(
            job_id="job-1",
            recording_id="rec-segment-2",
            state="queued",
            manifest_path=str(clean_manifest),
            output_path=str(recording_root / "exports" / "clean.mp4"),
            created_at=0.0,
            updated_at=0.0,
        )
        with patch.object(service._clean_export_manager, "enqueue", return_value=fake_job):
            response = client.post("/recordings/rec-segment-2/exports/clean-mp4")

        assert response.status_code == 202
        assert response.json() == {
            "recording_id": "rec-segment-2",
            "job_id": "job-1",
            "state": "queued",
            "output_path": None,
            "error": None,
        }


def test_segment_native_export_creation_prefers_clean_ts_when_compact_ready(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recording_root = tmp_path / "recordings" / "alpha_20260318_120000_000003"
        segments_dir = recording_root / "segments"
        manifests_dir = recording_root / "manifests"
        exports_dir = recording_root / "exports"
        segments_dir.mkdir(parents=True, exist_ok=True)
        manifests_dir.mkdir(parents=True, exist_ok=True)
        exports_dir.mkdir(parents=True, exist_ok=True)

        segment_path = segments_dir / "segment_000000.ts"
        clean_manifest = manifests_dir / "clean.m3u8"
        clean_ts = exports_dir / "clean.ts"
        segment_path.write_bytes(b"video-data")
        clean_manifest.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")
        clean_ts.write_bytes(b"compact-video-data")

        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(segment_path),
                recording_id="rec-segment-3",
                artifact_mode="segment_native",
                full_artifact_path=str(manifests_dir / "full.m3u8"),
                clean_artifact_path=str(clean_manifest),
                clean_compact_state="ready",
                clean_compact_path=str(clean_ts),
                clean_export_state="none",
                watchable_state="ready",
            )
        )

        fake_job = CleanExportJob(
            job_id="job-2",
            recording_id="rec-segment-3",
            state="queued",
            manifest_path=str(clean_ts),
            output_path=str(exports_dir / "clean.mp4"),
            created_at=0.0,
            updated_at=0.0,
        )
        with patch.object(service._clean_export_manager, "enqueue", return_value=fake_job) as enqueue:
            response = client.post("/recordings/rec-segment-3/exports/clean-mp4")

        assert response.status_code == 202
        assert response.json()["state"] == "queued"
        assert enqueue.call_count == 1
        assert Path(enqueue.call_args.kwargs["manifest_path"]) == clean_ts


def test_segment_native_export_creation_rejects_empty_clean_segments(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        recording_root = tmp_path / "recordings" / "alpha_20260318_120000_000004"
        segments_dir = recording_root / "segments"
        manifests_dir = recording_root / "manifests"
        segments_dir.mkdir(parents=True, exist_ok=True)
        manifests_dir.mkdir(parents=True, exist_ok=True)

        segment_path = segments_dir / "segment_000000.ts"
        clean_manifest = manifests_dir / "clean.m3u8"
        segment_path.write_bytes(b"video-data")
        clean_manifest.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")

        service: MonitorService = client.app.state.monitor_service
        service.recording_store.upsert(
            TrackedRecording(
                channel="alpha",
                source_file_path=str(segment_path),
                recording_id="rec-segment-4",
                artifact_mode="segment_native",
                full_artifact_path=str(manifests_dir / "full.m3u8"),
                clean_artifact_path=str(clean_manifest),
                full_segment_count=1,
                clean_segment_count=0,
                clean_export_state="none",
                watchable_state="ready",
            )
        )

        response = client.post("/recordings/rec-segment-4/exports/clean-mp4")
        assert response.status_code == 400
        assert "no playable segments" in response.json()["detail"]


def test_build_streamlink_command_uses_mpegts_for_ts_output_and_oauth_header() -> None:
    output_path = Path("/tmp/alpha_20260317_120000.ts")
    cmd, source_mode = build_streamlink_command(
        channel="alpha",
        output_path=output_path,
        preferred_qualities=("1080p60", "best"),
        twitch_user_oauth_token="token123",
    )

    assert cmd[:4] == ["streamlink", "https://twitch.tv/alpha", "1080p60,best", "-o"]
    assert cmd[4] == str(output_path)
    assert "--ffmpeg-fout" in cmd
    ffmpeg_fout_index = cmd.index("--ffmpeg-fout")
    assert cmd[ffmpeg_fout_index + 1] == "mpegts"
    assert "--twitch-api-header" in cmd
    assert "Authorization=OAuth token123" in cmd
    assert source_mode == "authenticated"


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
            assert output_path.endswith(".ts")
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

    alpha = asyncio.run(service.list_statuses())[0]
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

    alpha = asyncio.run(service.list_statuses())[0]
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

    alpha = asyncio.run(service.list_statuses())[0]
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

    alpha = asyncio.run(service.list_statuses())[0]
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

    alpha = asyncio.run(service.list_statuses())[0]
    assert alpha.is_live is True
    assert alpha.is_recording is False
    assert alpha.recording_state == "stopped"

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        response = asyncio.run(service.start_streamer_recording("alpha"))

    assert response.started is True
    alpha = asyncio.run(service.list_statuses())[0]
    assert alpha.is_recording is True
    assert alpha.recording_state == "recording"

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        async def fake_get_live_streams_offline(usernames):
            return {}

        service.twitch_client.get_live_streams = fake_get_live_streams_offline
        asyncio.run(service.refresh_once())

        service.twitch_client.get_live_streams = fake_get_live_streams
        asyncio.run(service.refresh_once())
    alpha = asyncio.run(service.list_statuses())[0]
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

    alpha = asyncio.run(service.list_statuses())[0]
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

    alpha = asyncio.run(service.list_statuses())[0]
    assert popen.call_count == 0
    assert alpha.is_live is True
    assert alpha.is_recording is False
    assert alpha.recording_state == "start_delay"


def test_old_finalize_result_does_not_override_new_active_recording_status(tmp_path: Path) -> None:
    service = build_test_service(tmp_path, start_delay_seconds=0)
    service._streamers = ["alpha"]
    service._statuses["alpha"] = StreamStatus(name="alpha", is_live=True)
    finalize_started = threading.Event()
    allow_finalize = threading.Event()

    def fake_build_watchable_output(self, **kwargs):
        finalize_started.set()
        allow_finalize.wait(timeout=2)
        return (str(kwargs["source_path"]), "ready", None, 0)

    with (
        patch("app.recorder.subprocess.Popen", side_effect=[FakeProcess(), FakeProcess()]),
        patch.object(RecorderManager, "_build_watchable_output", autospec=True, side_effect=fake_build_watchable_output),
    ):
        first_output = Path(service.recorder.start_recording("alpha"))
        first_output.write_bytes(b"video-data")

        stop_response = asyncio.run(service.stop_streamer_recording("alpha"))
        assert stop_response.stopped is True
        assert finalize_started.wait(timeout=1)

        second_response = asyncio.run(service.start_streamer_recording("alpha"))
        assert second_response.started is True
        second_output = service.recorder.current_output_path("alpha")
        assert second_output is not None

        allow_finalize.set()
        service.recorder.wait_for_pending_finalizations()
        alpha = asyncio.run(service.list_statuses())[0]

    assert alpha.is_recording is True
    assert alpha.recording_state == "recording"
    assert alpha.output_path == second_output

    service.recorder.stop_all(wait_for_finalize=True)
