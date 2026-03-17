from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from app.recorder import RecorderManager
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
