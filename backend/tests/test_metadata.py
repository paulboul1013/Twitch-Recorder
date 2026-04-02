from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from app.recorder import RecorderManager
from app.recording_types import ActiveRecording
from conftest import FakeProcess, _load_metadata

def test_stop_recording_metadata_includes_streamlink_diagnostics(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    stderr_lines = [f"streamlink line {index}" for index in range(50)]

    with (
        patch("app.recorder.subprocess.Popen", return_value=FakeProcess(stderr_lines=stderr_lines)),
        patch("app.recorder.time.perf_counter", side_effect=[10.0, 11.25]),
        patch.object(
            RecorderManager,
            "_build_watchable_output",
            side_effect=lambda self, **kwargs: (str(kwargs["source_path"]), "ready", None, 0),
            autospec=True,
        ),
    ):
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"video-data")
        result = recorder.stop_recording("alpha", wait_for_finalize=True)

    assert result is not None
    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["state"] == "stopped"
    assert metadata["exit_code"] == -15
    assert metadata["streamlink_stderr_tail"] == stderr_lines[-recorder.STDERR_TAIL_MAX_LINES :]
    assert metadata["watchable_processing_seconds"] == 1.25
    assert metadata["watchable_strategy"] is None
    assert metadata["ad_detection_sources"] == []
    assert metadata["prepare_mitigation"] == []


def test_completed_recording_metadata_includes_streamlink_diagnostics(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    stderr_lines = [
        "[cli][info] Opening stream: best",
        "[download][warning] Playlist ended unexpectedly",
        "[stream][info] Stream disconnected",
    ]
    process = FakeProcess(stderr_lines=stderr_lines)

    with (
        patch("app.recorder.subprocess.Popen", return_value=process),
        patch("app.recorder.time.perf_counter", side_effect=[5.0, 20.0, 22.5]),
        patch.object(
            RecorderManager,
            "_build_watchable_output",
            side_effect=lambda self, **kwargs: (str(kwargs["source_path"]), "ready", None, 0),
            autospec=True,
        ),
    ):
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"video-data")
        process.returncode = 0
        recorder.poll()
        recorder.wait_for_pending_finalizations()
        recorder.poll()

    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["state"] == "completed"
    assert metadata["exit_code"] == 0
    assert metadata["streamlink_stderr_tail"] == stderr_lines
    assert metadata["watchable_processing_seconds"] == 2.5
    assert metadata["watchable_strategy"] is None
    assert metadata["ad_detection_sources"] == []
    assert metadata["prepare_mitigation"] == []


def test_metadata_pending_state_has_new_watchable_fields(tmp_path: Path) -> None:
    recorder = RecorderManager(
        tmp_path,
        ("best",),
        recording_start_delay_seconds=25,
    )

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        output_path = Path(recorder.start_recording("alpha"))

    assert output_path.suffix == ".ts"
    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["state"] == "recording"
    assert metadata["clean_output_state"] == "pending"
    assert metadata["watchable_processing_seconds"] is None
    assert metadata["watchable_strategy"] is None
    assert metadata["ad_detection_sources"] == []
    assert metadata["prepare_mitigation"] == ["start_delay"]
    assert metadata["source_available"] is True
    assert metadata["source_deleted_on_success"] is False
    assert metadata["source_delete_error"] is None
    assert metadata["clean_compact_state"] == "none"
    assert metadata["clean_compact_path"] is None
    assert metadata["clean_compact_error"] is None


def test_start_recording_groups_legacy_outputs_under_channel_directory(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))

    with patch("app.recorder.subprocess.Popen", return_value=FakeProcess()):
        output_path = Path(recorder.start_recording("alpha"))

    assert output_path.parent == tmp_path / "alpha"
    assert output_path.name.startswith("alpha_")
    assert output_path.with_suffix(".meta.json").parent == tmp_path / "alpha"


def test_start_recording_groups_segment_native_outputs_under_channel_directory(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",), recording_mode="segment_native")

    with patch("app.recorder.subprocess.Popen", side_effect=[FakeProcess(), FakeProcess()]):
        output_path = Path(recorder.start_recording("alpha"))

    recording_root = output_path.parent.parent
    assert recording_root.parent == tmp_path / "alpha"
    assert output_path.parent == recording_root / "segments"
    assert (recording_root / "manifests").is_dir()
    assert (recording_root / "exports").is_dir()
    recorder.stop_all()


def test_resume_segment_native_recording_reuses_directory_and_continues_segment_numbering(
    tmp_path: Path,
) -> None:
    recorder = RecorderManager(tmp_path, ("best",), recording_mode="segment_native")

    class FakeStdoutProcess(FakeProcess):
        def __init__(self) -> None:
            super().__init__()
            self.stdout = io.BytesIO()

    stream_process = FakeStdoutProcess()
    segmenter_process = FakeProcess()
    resumed_stream_process = FakeStdoutProcess()
    resumed_segmenter_process = FakeProcess()

    with (
        patch.object(RecorderManager, "_start_playlist_ad_window_tracking", autospec=True),
        patch.object(RecorderManager, "_stop_playlist_ad_window_tracking", autospec=True),
        patch(
            "app.recorder.subprocess.Popen",
            side_effect=[
                stream_process,
                segmenter_process,
                resumed_stream_process,
                resumed_segmenter_process,
            ],
        ) as popen,
    ):
        output_path = Path(recorder.start_recording("alpha"))
        recording_root = output_path.parent.parent
        (recording_root / "segments" / "segment_000000.ts").write_bytes(b"a")
        (recording_root / "segments" / "segment_000001.ts").write_bytes(b"b")

        stream_process.returncode = 0
        segmenter_process.returncode = 0
        assert recorder.poll() == []
        assert recorder.has_session("alpha") is True
        assert recorder.is_recording("alpha") is False

        resumed_path = Path(recorder.resume_recording("alpha") or "")

    assert resumed_path == output_path
    assert recording_root.parent == tmp_path / "alpha"
    segmenter_calls = [
        call.args[0]
        for call in popen.call_args_list
        if call.args and isinstance(call.args[0], list) and call.args[0] and call.args[0][0] == "ffmpeg"
    ]
    resumed_segmenter_cmd = segmenter_calls[-1]
    assert "-segment_start_number" in resumed_segmenter_cmd
    start_index = resumed_segmenter_cmd.index("-segment_start_number")
    assert resumed_segmenter_cmd[start_index + 1] == "2"


def test_metadata_ready_state_includes_strategy_and_detection_sources(tmp_path: Path) -> None:
    recorder = RecorderManager(
        tmp_path,
        ("best",),
        recording_start_delay_seconds=25,
    )

    def fake_remux_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
    ) -> None:
        watchable_path.write_bytes(b"watchable")

    with (
        patch("app.recorder.subprocess.Popen", return_value=FakeProcess()),
        patch.object(RecorderManager, "_confirm_timed_id3_ad_offsets_with_ocr", return_value=[]),
        patch.object(RecorderManager, "_remux_watchable", autospec=True, side_effect=fake_remux_watchable),
    ):
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"video-data")
        recorder.stop_recording("alpha", wait_for_finalize=True)

    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["clean_output_state"] == "ready"
    assert metadata["watchable_strategy"] == "remux"
    assert metadata["ad_detection_sources"] == []
    assert metadata["prepare_mitigation"] == ["start_delay"]
    assert metadata["clean_compact_state"] == "none"
    assert metadata["clean_compact_path"] is None
    assert metadata["clean_compact_error"] is None


def test_metadata_failed_state_includes_strategy_and_sources(tmp_path: Path) -> None:
    recorder = RecorderManager(
        tmp_path,
        ("best",),
        recording_start_delay_seconds=25,
    )

    def fake_build_watchable_output(self, **kwargs):
        self._set_last_watchable_context(
            watchable_strategy="fallback_reencode",
            ad_detection_sources=["stderr"],
            prepare_mitigation=["start_delay", "reencode_fallback"],
        )
        return (None, "failed", "forced failure", 1)

    with (
        patch("app.recorder.subprocess.Popen", return_value=FakeProcess(stderr_lines=["ad break started"])),
        patch.object(
            RecorderManager,
            "_build_watchable_output",
            autospec=True,
            side_effect=fake_build_watchable_output,
        ),
    ):
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"video-data")
        recorder.stop_recording("alpha", wait_for_finalize=True)

    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["clean_output_state"] == "failed"
    assert metadata["clean_output_error"] == "forced failure"
    assert metadata["watchable_strategy"] == "fallback_reencode"
    assert metadata["ad_detection_sources"] == ["stderr"]
    assert metadata["prepare_mitigation"] == ["start_delay", "reencode_fallback"]
    assert metadata["clean_compact_state"] == "none"
    assert metadata["clean_compact_path"] is None
    assert metadata["clean_compact_error"] is None


def test_raw_source_is_deleted_after_successful_watchable_finalize(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))

    def fake_build_watchable_output(self, **kwargs):
        source_path: Path = kwargs["source_path"]
        watchable_path = source_path.with_name(f"{source_path.stem}.watchable.mp4")
        watchable_path.write_bytes(b"watchable")
        return (str(watchable_path), "ready", None, 0)

    with (
        patch("app.recorder.subprocess.Popen", return_value=FakeProcess()),
        patch.object(
            RecorderManager,
            "_build_watchable_output",
            autospec=True,
            side_effect=fake_build_watchable_output,
        ),
    ):
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"raw-video-data")
        result = recorder.stop_recording("alpha", wait_for_finalize=True)

    assert result is not None
    assert result.clean_output_state == "ready"
    assert output_path.exists() is False
    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["source_available"] is False
    assert metadata["source_deleted_on_success"] is True
    assert metadata["source_delete_error"] is None


def test_raw_source_is_kept_when_watchable_finalize_fails(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))

    with (
        patch("app.recorder.subprocess.Popen", return_value=FakeProcess()),
        patch.object(
            RecorderManager,
            "_build_watchable_output",
            autospec=True,
            side_effect=lambda self, **kwargs: (None, "failed", "forced failure", 0),
        ),
    ):
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"raw-video-data")
        result = recorder.stop_recording("alpha", wait_for_finalize=True)

    assert result is not None
    assert result.clean_output_state == "failed"
    assert output_path.exists() is True
    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["source_available"] is True
    assert metadata["source_deleted_on_success"] is False
    assert metadata["source_delete_error"] is None


def test_raw_delete_failure_records_error_without_overriding_watchable_ready(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))

    def fake_build_watchable_output(self, **kwargs):
        source_path: Path = kwargs["source_path"]
        watchable_path = source_path.with_name(f"{source_path.stem}.watchable.mp4")
        watchable_path.write_bytes(b"watchable")
        return (str(watchable_path), "ready", None, 0)

    original_unlink = Path.unlink

    def fake_unlink(path_obj: Path, missing_ok: bool = False) -> None:
        if path_obj.suffix == ".ts":
            raise OSError("permission denied")
        original_unlink(path_obj, missing_ok=missing_ok)

    with (
        patch("app.recorder.subprocess.Popen", return_value=FakeProcess()),
        patch.object(
            RecorderManager,
            "_build_watchable_output",
            autospec=True,
            side_effect=fake_build_watchable_output,
        ),
        patch("pathlib.Path.unlink", new=fake_unlink),
    ):
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"raw-video-data")
        result = recorder.stop_recording("alpha", wait_for_finalize=True)

    assert result is not None
    assert result.clean_output_state == "ready"
    assert output_path.exists() is True
    metadata = _load_metadata(output_path.with_suffix(".meta.json"))
    assert metadata["clean_output_state"] == "ready"
    assert metadata["source_available"] is True
    assert metadata["source_deleted_on_success"] is False
    assert "permission denied" in metadata["source_delete_error"]


def test_segment_native_artifacts_use_daterange_windows_for_clean_manifest(tmp_path: Path) -> None:
    recorder = RecorderManager(
        tmp_path,
        ("best",),
        recording_mode="segment_native",
        segment_ad_padding_seconds=2.0,
    )
    recording_root = tmp_path / "alpha_20260318_120000_000001"
    segments_dir = recording_root / "segments"
    manifests_dir = recording_root / "manifests"
    segments_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    segment_0 = segments_dir / "segment_000000.ts"
    segment_1 = segments_dir / "segment_000001.ts"
    segment_0.write_bytes(b"a")
    segment_1.write_bytes(b"b")

    started_at = datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)
    recording = ActiveRecording(
        recording_id="rec-1",
        artifact_mode="segment_native",
        channel="alpha",
        process=FakeProcess(),
        segmenter_process=FakeProcess(),
        file_path=segment_0,
        metadata_path=recording_root / "recording.meta.json",
        started_at=started_at,
        source_mode="unauthenticated",
        recording_root=recording_root,
        full_artifact_path=manifests_dir / "full.m3u8",
        clean_artifact_path=manifests_dir / "clean.m3u8",
    )
    recording.playlist_markers_seen = True
    recording.playlist_ad_windows = [
        (
            started_at + timedelta(seconds=8),
            started_at + timedelta(seconds=12),
        )
    ]

    with patch.object(
        RecorderManager,
        "_probe_media_duration",
        side_effect=[10.0, 10.0],
    ):
        (
            _full_path,
            _clean_path,
            full_segment_count,
            clean_segment_count,
            unknown_ad_confidence,
            clean_output_state,
            _clean_output_error,
            ad_break_count,
        ) = recorder._build_segment_native_artifacts(
            recording=recording,
            started_at=started_at,
            ended_at=started_at + timedelta(seconds=20),
            events=[],
        )

    assert full_segment_count == 2
    assert clean_segment_count == 0
    assert unknown_ad_confidence is False
    assert clean_output_state == "ready"
    assert ad_break_count == 1

    segment_index = _load_metadata(recording_root / "segment_index.json")
    assert segment_index["unknown_ad_confidence"] is False
    assert [segment["is_ad"] for segment in segment_index["segments"]] == [True, True]


def test_segment_native_without_daterange_keeps_all_segments_and_marks_unknown_confidence(
    tmp_path: Path,
) -> None:
    recorder = RecorderManager(
        tmp_path,
        ("best",),
        recording_mode="segment_native",
    )
    recording_root = tmp_path / "alpha_20260318_120000_000002"
    segments_dir = recording_root / "segments"
    manifests_dir = recording_root / "manifests"
    segments_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    segment_0 = segments_dir / "segment_000000.ts"
    segment_1 = segments_dir / "segment_000001.ts"
    segment_0.write_bytes(b"a")
    segment_1.write_bytes(b"b")

    started_at = datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)
    recording = ActiveRecording(
        recording_id="rec-2",
        artifact_mode="segment_native",
        channel="alpha",
        process=FakeProcess(),
        segmenter_process=FakeProcess(),
        file_path=segment_0,
        metadata_path=recording_root / "recording.meta.json",
        started_at=started_at,
        source_mode="unauthenticated",
        recording_root=recording_root,
        full_artifact_path=manifests_dir / "full.m3u8",
        clean_artifact_path=manifests_dir / "clean.m3u8",
    )
    recording.playlist_markers_seen = False
    recording.playlist_ad_windows = []

    with patch.object(
        RecorderManager,
        "_probe_media_duration",
        side_effect=[10.0, 10.0],
    ):
        (
            _full_path,
            _clean_path,
            full_segment_count,
            clean_segment_count,
            unknown_ad_confidence,
            clean_output_state,
            _clean_output_error,
            ad_break_count,
        ) = recorder._build_segment_native_artifacts(
            recording=recording,
            started_at=started_at,
            ended_at=started_at + timedelta(seconds=20),
            events=[],
        )

    assert full_segment_count == 2
    assert clean_segment_count == 2
    assert unknown_ad_confidence is True
    assert clean_output_state == "ready"
    assert ad_break_count == 0


def _build_segment_native_finalize_fixture(
    tmp_path: Path,
    *,
    recording_id: str,
    markers_seen: bool = True,
) -> tuple[RecorderManager, ActiveRecording, Path, Path, Path, datetime]:
    recorder = RecorderManager(
        tmp_path,
        ("best",),
        recording_mode="segment_native",
    )
    recording_root = tmp_path / recording_id
    segments_dir = recording_root / "segments"
    manifests_dir = recording_root / "manifests"
    segments_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    segment_0 = segments_dir / "segment_000000.ts"
    segment_1 = segments_dir / "segment_000001.ts"
    segment_0.write_bytes(b"segment-0")
    segment_1.write_bytes(b"segment-1")

    started_at = datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)
    recording = ActiveRecording(
        recording_id=recording_id,
        artifact_mode="segment_native",
        channel="alpha",
        process=FakeProcess(),
        segmenter_process=FakeProcess(),
        file_path=segment_0,
        metadata_path=recording_root / "recording.meta.json",
        started_at=started_at,
        source_mode="unauthenticated",
        recording_root=recording_root,
        full_artifact_path=manifests_dir / "full.m3u8",
        clean_artifact_path=manifests_dir / "clean.m3u8",
    )
    recording.playlist_markers_seen = markers_seen
    recording.playlist_ad_windows = []
    return recorder, recording, segment_0, segment_1, recording_root, started_at


def test_segment_native_finalize_compacts_clean_manifest_and_deletes_segments(tmp_path: Path) -> None:
    recorder, recording, _segment_0, _segment_1, recording_root, started_at = _build_segment_native_finalize_fixture(
        tmp_path,
        recording_id="rec-compact-success",
    )

    class FakeCompletedProcess:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(cmd, stdout=None, stderr=None, text=None, encoding=None, errors=None, check=None):
        assert "-f" in cmd
        format_index = cmd.index("-f")
        assert cmd[format_index + 1] == "mpegts"
        Path(cmd[-1]).write_bytes(b"clean-ts")
        return FakeCompletedProcess()

    with (
        patch.object(RecorderManager, "_probe_media_duration", side_effect=[10.0, 10.0]),
        patch("app.recorder.subprocess.run", side_effect=fake_run),
    ):
        result = recorder._finalize_recording(
            recording=recording,
            ended_at=started_at + timedelta(seconds=20),
            exit_code=0,
            state="stopped",
        )

    compact_path = recording_root / "exports" / "clean.ts"
    metadata = _load_metadata(recording.metadata_path)
    assert result.clean_compact_state == "ready"
    assert result.clean_compact_path == str(compact_path)
    assert result.clean_compact_error is None
    assert compact_path.exists() is True
    assert compact_path.read_bytes() == b"clean-ts"
    assert not list((recording_root / "segments").glob("segment_*.ts"))
    assert metadata["clean_compact_state"] == "ready"
    assert metadata["clean_compact_path"] == str(compact_path)
    assert metadata["clean_compact_error"] is None


def test_segment_native_finalize_keeps_segments_when_compact_fails(tmp_path: Path) -> None:
    recorder, recording, _segment_0, _segment_1, recording_root, started_at = _build_segment_native_finalize_fixture(
        tmp_path,
        recording_id="rec-compact-fail",
    )

    class FakeCompletedProcess:
        returncode = 1
        stdout = ""
        stderr = "ffmpeg exploded"

    def fake_run(cmd, stdout=None, stderr=None, text=None, encoding=None, errors=None, check=None):
        return FakeCompletedProcess()

    with (
        patch.object(RecorderManager, "_probe_media_duration", side_effect=[10.0, 10.0]),
        patch("app.recorder.subprocess.run", side_effect=fake_run),
    ):
        result = recorder._finalize_recording(
            recording=recording,
            ended_at=started_at + timedelta(seconds=20),
            exit_code=0,
            state="stopped",
        )

    metadata = _load_metadata(recording.metadata_path)
    assert result.clean_compact_state == "failed"
    assert result.clean_compact_path is None
    assert "ffmpeg exploded" in (result.clean_compact_error or "")
    assert list((recording_root / "segments").glob("segment_*.ts"))
    assert not (recording_root / "exports" / "clean.ts").exists()
    assert metadata["clean_compact_state"] == "failed"
    assert metadata["clean_compact_path"] is None
    assert "ffmpeg exploded" in (metadata["clean_compact_error"] or "")


def test_segment_native_finalize_records_segment_delete_error_without_failing_compact(
    tmp_path: Path,
) -> None:
    recorder, recording, _segment_0, _segment_1, recording_root, started_at = _build_segment_native_finalize_fixture(
        tmp_path,
        recording_id="rec-compact-delete-error",
    )

    class FakeCompletedProcess:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(cmd, stdout=None, stderr=None, text=None, encoding=None, errors=None, check=None):
        Path(cmd[-1]).write_bytes(b"clean-ts")
        return FakeCompletedProcess()

    original_unlink = Path.unlink

    def fake_unlink(path_obj: Path, missing_ok: bool = False) -> None:
        if path_obj.suffix == ".ts":
            raise OSError("permission denied")
        original_unlink(path_obj, missing_ok=missing_ok)

    with (
        patch.object(RecorderManager, "_probe_media_duration", side_effect=[10.0, 10.0]),
        patch("app.recorder.subprocess.run", side_effect=fake_run),
        patch("pathlib.Path.unlink", new=fake_unlink),
    ):
        result = recorder._finalize_recording(
            recording=recording,
            ended_at=started_at + timedelta(seconds=20),
            exit_code=0,
            state="stopped",
        )

    compact_path = recording_root / "exports" / "clean.ts"
    metadata = _load_metadata(recording.metadata_path)
    assert result.clean_compact_state == "ready"
    assert result.clean_compact_path == str(compact_path)
    assert "permission denied" in (result.clean_compact_error or "")
    assert compact_path.exists() is True
    assert list((recording_root / "segments").glob("segment_*.ts"))
    assert metadata["clean_compact_state"] == "ready"
    assert metadata["clean_compact_path"] == str(compact_path)
    assert "permission denied" in (metadata["clean_compact_error"] or "")


def test_segment_native_finalize_skips_compact_when_clean_manifest_has_no_segments(
    tmp_path: Path,
) -> None:
    recorder, recording, _segment_0, _segment_1, recording_root, started_at = _build_segment_native_finalize_fixture(
        tmp_path,
        recording_id="rec-compact-empty-clean",
        markers_seen=True,
    )
    # Mark all timeline as ad so clean manifest becomes empty.
    recording.playlist_ad_windows = [
        (
            started_at + timedelta(seconds=0),
            started_at + timedelta(seconds=25),
        )
    ]

    with (
        patch.object(RecorderManager, "_probe_media_duration", side_effect=[10.0, 10.0]),
        patch("app.recorder.subprocess.run") as run_mock,
    ):
        result = recorder._finalize_recording(
            recording=recording,
            ended_at=started_at + timedelta(seconds=20),
            exit_code=0,
            state="stopped",
        )

    metadata = _load_metadata(recording.metadata_path)
    assert result.clean_segment_count == 0
    assert result.clean_compact_state == "none"
    assert result.clean_compact_path is None
    assert result.clean_compact_error is None
    assert list((recording_root / "segments").glob("segment_*.ts"))
    assert run_mock.call_count == 0
    assert metadata["clean_compact_state"] == "none"
    assert metadata["clean_compact_path"] is None
    assert metadata["clean_compact_error"] is None
