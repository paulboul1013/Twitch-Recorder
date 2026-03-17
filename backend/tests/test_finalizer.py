from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from app.recorder import RecorderManager, RecordingEvent
from conftest import FakeProcess, _load_metadata

def test_watchable_output_is_remuxed_even_without_ad_windows(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)

    def fake_remux_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
    ) -> None:
        watchable_path.write_bytes(b"watchable")

    with (
        patch.object(
            RecorderManager,
            "_remux_watchable",
            autospec=True,
            side_effect=fake_remux_watchable,
        ) as remux_mock,
        patch.object(RecorderManager, "_render_watchable") as render_mock,
        patch.object(RecorderManager, "_collect_ocr_ad_windows") as ocr_mock,
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
    remux_mock.assert_called_once()
    render_mock.assert_not_called()
    ocr_mock.assert_not_called()


def test_watchable_output_from_ts_source_keeps_mp4_watchable_suffix(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    source_path = tmp_path / "sample.ts"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)

    def fake_remux_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
    ) -> None:
        watchable_path.write_bytes(b"watchable")

    with patch.object(
        RecorderManager,
        "_remux_watchable",
        autospec=True,
        side_effect=fake_remux_watchable,
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
    assert not watchable_path.endswith(".watchable.ts")


def test_watchable_output_uses_trim_copy_fast_path_when_trim_enabled(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",), watchable_trim_start_seconds=12)
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)

    def fake_trim_copy_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        trim_start_seconds: float,
    ) -> None:
        assert trim_start_seconds == 12
        watchable_path.write_bytes(b"watchable")

    with (
        patch.object(
            RecorderManager,
            "_trim_copy_watchable",
            autospec=True,
            side_effect=fake_trim_copy_watchable,
        ) as trim_copy_mock,
        patch.object(RecorderManager, "_has_prepare_overlay_in_prefix", return_value=False) as verify_mock,
        patch.object(RecorderManager, "_render_watchable") as render_mock,
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
    trim_copy_mock.assert_called_once()
    verify_mock.assert_called_once()
    render_mock.assert_not_called()
    watchable_context = recorder._consume_last_watchable_context()
    assert watchable_context.watchable_strategy == "trim_copy"
    assert "trim_copy_fallback" in watchable_context.prepare_mitigation


def test_trim_copy_falls_back_to_reencode_when_prepare_overlay_remains(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",), watchable_trim_start_seconds=12)
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)
    captured: dict[str, list[tuple[float, float]]] = {}

    def fake_trim_copy_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        trim_start_seconds: float,
    ) -> None:
        watchable_path.write_bytes(b"watchable")

    def fake_render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        captured["keep_ranges"] = keep_ranges
        watchable_path.write_bytes(b"reencoded")

    with (
        patch.object(
            RecorderManager,
            "_trim_copy_watchable",
            autospec=True,
            side_effect=fake_trim_copy_watchable,
        ) as trim_copy_mock,
        patch.object(RecorderManager, "_has_prepare_overlay_in_prefix", return_value=True) as verify_mock,
        patch.object(RecorderManager, "_render_watchable", autospec=True, side_effect=fake_render_watchable),
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
    trim_copy_mock.assert_called_once()
    verify_mock.assert_called_once()
    watchable_context = recorder._consume_last_watchable_context()
    assert watchable_context.watchable_strategy == "fallback_reencode"
    assert "trim_copy_fallback" in watchable_context.prepare_mitigation
    assert "reencode_fallback" in watchable_context.prepare_mitigation


def test_timed_id3_candidate_without_ocr_confirmation_does_not_cut_output(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    source_path = tmp_path / "sample.mp4"
    source_path.write_bytes(b"video-data")
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=30)

    def fake_remux_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
    ) -> None:
        watchable_path.write_bytes(b"watchable")

    with (
        patch.object(RecorderManager, "_extract_timed_id3_ad_offsets", return_value=[(0.0, 10.0)]),
        patch.object(RecorderManager, "_collect_ocr_ad_windows", return_value=[]),
        patch.object(RecorderManager, "_remux_watchable", autospec=True, side_effect=fake_remux_watchable) as remux_mock,
        patch.object(RecorderManager, "_render_watchable") as render_mock,
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
    remux_mock.assert_called_once()
    render_mock.assert_not_called()
    watchable_context = recorder._consume_last_watchable_context()
    assert watchable_context.watchable_strategy == "remux"
    assert "timed_id3_confirmed_by_ocr" not in watchable_context.ad_detection_sources


def test_timed_id3_candidate_confirmed_by_ocr_creates_ad_window(tmp_path: Path) -> None:
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
        patch.object(RecorderManager, "_collect_ocr_ad_windows", return_value=[(1.0, 8.0)]),
        patch.object(
            RecorderManager,
            "_render_watchable",
            autospec=True,
            side_effect=fake_render_watchable,
        ) as render_mock,
        patch.object(RecorderManager, "_contains_overlay_in_ranges", return_value=False),
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
    render_mock.assert_called_once()
    watchable_context = recorder._consume_last_watchable_context()
    assert watchable_context.watchable_strategy == "segment_transcode"
    assert "timed_id3_confirmed_by_ocr" in watchable_context.ad_detection_sources


def test_watchable_output_falls_back_to_render_when_remux_fails(tmp_path: Path) -> None:
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
        patch.object(RecorderManager, "_remux_watchable", side_effect=RuntimeError("remux failed")) as remux_mock,
        patch.object(
            RecorderManager,
            "_render_watchable",
            autospec=True,
            side_effect=fake_render_watchable,
        ) as render_mock,
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
    remux_mock.assert_called_once()
    render_mock.assert_called_once()
    watchable_context = recorder._consume_last_watchable_context()
    assert watchable_context.watchable_strategy == "fallback_reencode"
    assert "reencode_fallback" in watchable_context.prepare_mitigation


def test_watchable_output_fails_when_segment_local_verification_still_detects_overlay(
    tmp_path: Path,
) -> None:
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

    ad_start = started_at + timedelta(seconds=5)
    ad_end = started_at + timedelta(seconds=10)
    events = [
        RecordingEvent(type="recording_started", at=started_at),
        RecordingEvent(type="ad_break_started", at=ad_start),
        RecordingEvent(type="ad_break_ended", at=ad_end),
    ]

    with (
        patch.object(RecorderManager, "_render_watchable", fake_render_watchable),
        patch.object(RecorderManager, "_contains_overlay_in_ranges", side_effect=[True, True]),
        patch.object(RecorderManager, "_reencode_existing_watchable_output"),
    ):
        watchable_path, watchable_state, watchable_error, ad_break_count = recorder._build_watchable_output(
            source_path=source_path,
            started_at=started_at,
            ended_at=ended_at,
            events=events,
        )

    assert watchable_path is None
    assert watchable_state == "failed"
    assert watchable_error == "watchable verification still detected Twitch playback overlay"
    assert ad_break_count == 1
    assert not source_path.with_name("sample.watchable.mp4").exists()


def test_repair_watchable_output_rerenders_when_ocr_finds_ad_overlay(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    watchable_path = tmp_path / "sample.watchable.mp4"
    watchable_path.write_bytes(b"watchable")
    captured: dict[str, list[tuple[float, float]]] = {}

    def fake_render_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        keep_ranges: list[tuple[float, float]],
    ) -> None:
        captured["keep_ranges"] = keep_ranges
        watchable_path.write_bytes(b"clean")

    with (
        patch.object(RecorderManager, "_probe_media_duration", side_effect=[30.0, 26.0]),
        patch.object(RecorderManager, "_collect_ocr_ad_windows", side_effect=[[(8.0, 12.0)], []]),
        patch.object(RecorderManager, "_render_watchable", fake_render_watchable),
    ):
        repair_error, ad_break_count = recorder._repair_watchable_output(watchable_path)

    assert repair_error is None
    assert ad_break_count == 1
    assert captured["keep_ranges"] == [(0.0, 8.0), (12.0, 30.0)]
    assert watchable_path.read_bytes() == b"clean"


def test_stop_recording_returns_processing_before_background_finalize_completes(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
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
        output_path = Path(recorder.start_recording("alpha"))
        output_path.write_bytes(b"video-data")

        stop_result = recorder.stop_recording("alpha")
        assert stop_result is not None
        assert stop_result.clean_output_state == "processing"
        assert stop_result.clean_output_path is None

        assert finalize_started.wait(timeout=1)
        processing_metadata = _load_metadata(output_path.with_suffix(".meta.json"))
        assert processing_metadata["clean_output_state"] == "processing"
        assert processing_metadata["watchable_processing_seconds"] is None
        assert "watchable_strategy" in processing_metadata
        assert "ad_detection_sources" in processing_metadata
        assert "prepare_mitigation" in processing_metadata
        assert recorder.poll() == []

        allow_finalize.set()
        recorder.wait_for_pending_finalizations()
        completed_results = recorder.poll()

    assert len(completed_results) == 1
    assert completed_results[0].clean_output_state == "ready"
    assert completed_results[0].clean_output_path == str(output_path)


def test_background_finalizers_run_one_at_a_time(tmp_path: Path) -> None:
    recorder = RecorderManager(tmp_path, ("best",))
    first_started = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()
    release_second = threading.Event()
    state_lock = threading.Lock()
    call_count = 0
    active_finalizers = 0
    max_active_finalizers = 0

    def fake_build_watchable_output(self, **kwargs):
        nonlocal call_count, active_finalizers, max_active_finalizers
        with state_lock:
            call_count += 1
            current_call = call_count
            active_finalizers += 1
            max_active_finalizers = max(max_active_finalizers, active_finalizers)
        try:
            if current_call == 1:
                first_started.set()
                release_first.wait(timeout=2)
            else:
                second_started.set()
                release_second.wait(timeout=2)
            return (str(kwargs["source_path"]), "ready", None, 0)
        finally:
            with state_lock:
                active_finalizers -= 1

    with (
        patch("app.recorder.subprocess.Popen", side_effect=[FakeProcess(), FakeProcess()]),
        patch.object(RecorderManager, "_build_watchable_output", autospec=True, side_effect=fake_build_watchable_output),
    ):
        first_output = Path(recorder.start_recording("alpha"))
        second_output = Path(recorder.start_recording("beta"))
        first_output.write_bytes(b"alpha")
        second_output.write_bytes(b"beta")

        first_stop_result = recorder.stop_recording("alpha")
        second_stop_result = recorder.stop_recording("beta")

        assert first_stop_result is not None
        assert second_stop_result is not None
        assert first_started.wait(timeout=1)
        assert second_started.wait(timeout=0.2) is False

        release_first.set()
        assert second_started.wait(timeout=1)
        release_second.set()

        recorder.wait_for_pending_finalizations()
        completed_results = recorder.poll()

    assert len(completed_results) == 2
    assert max_active_finalizers == 1
