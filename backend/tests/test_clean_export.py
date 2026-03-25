from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from app.clean_export import CleanExportJob, CleanExportManager


@dataclass
class _FakeCompletedProcess:
    returncode: int
    stdout: str = ""
    stderr: str = ""


def _build_job(tmp_path: Path, *, manifest_suffix: str = ".m3u8") -> tuple[CleanExportJob, Path]:
    recording_root = tmp_path / "recordings" / "alpha_20260325_120000_000001"
    manifests_dir = recording_root / "manifests"
    exports_dir = recording_root / "exports"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifests_dir / f"clean{manifest_suffix}"
    output_path = exports_dir / "clean.mp4"
    manifest_path.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")
    return (
        CleanExportJob(
            job_id="job-1",
            recording_id="rec-1",
            state="queued",
            manifest_path=str(manifest_path),
            output_path=str(output_path),
            created_at=0.0,
            updated_at=0.0,
        ),
        output_path,
    )


def test_run_ffmpeg_export_uses_temp_output_and_replaces_target_atomically(tmp_path: Path) -> None:
    manager = CleanExportManager()
    job, output_path = _build_job(tmp_path)
    output_path.write_bytes(b"old-output")

    def fake_run(cmd, **kwargs):
        temp_output = Path(cmd[-1])
        assert temp_output != output_path
        assert temp_output.parent == output_path.parent
        assert temp_output.name.startswith(f".{output_path.name}.")
        assert temp_output.suffix == ".tmp"
        temp_output.write_bytes(b"new-output")
        return _FakeCompletedProcess(returncode=0)

    with patch("app.clean_export.subprocess.run", side_effect=fake_run):
        manager._run_ffmpeg_export(job)

    assert output_path.read_bytes() == b"new-output"
    assert not list(output_path.parent.glob(f".{output_path.name}.*.tmp"))


def test_run_ffmpeg_export_failure_keeps_previous_output_and_cleans_temp_file(tmp_path: Path) -> None:
    manager = CleanExportManager()
    job, output_path = _build_job(tmp_path)
    output_path.write_bytes(b"old-output")

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"partial-output")
        return _FakeCompletedProcess(returncode=1, stderr="ffmpeg exploded")

    with patch("app.clean_export.subprocess.run", side_effect=fake_run):
        try:
            manager._run_ffmpeg_export(job)
            assert False, "expected runtime error"
        except RuntimeError as exc:
            assert "ffmpeg exploded" in str(exc)

    assert output_path.read_bytes() == b"old-output"
    assert not list(output_path.parent.glob(f".{output_path.name}.*.tmp"))


def test_run_ffmpeg_export_rejects_empty_output_even_when_ffmpeg_returns_zero(tmp_path: Path) -> None:
    manager = CleanExportManager()
    job, output_path = _build_job(tmp_path)

    with patch(
        "app.clean_export.subprocess.run",
        return_value=_FakeCompletedProcess(returncode=0),
    ):
        try:
            manager._run_ffmpeg_export(job)
            assert False, "expected runtime error"
        except RuntimeError as exc:
            assert "produced no output" in str(exc)

    assert output_path.exists() is False
    assert not list(output_path.parent.glob(f".{output_path.name}.*.tmp"))


def test_enqueue_requeues_ready_job_when_output_file_is_missing(tmp_path: Path) -> None:
    manager = CleanExportManager()
    manifest_path = tmp_path / "clean.m3u8"
    output_path = tmp_path / "clean.mp4"
    manifest_path.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")

    with manager._lock:
        manager._jobs["rec-1"] = CleanExportJob(
            job_id="existing-job",
            recording_id="rec-1",
            state="ready",
            manifest_path=str(manifest_path),
            output_path=str(output_path),
            created_at=1.0,
            updated_at=1.0,
        )

    with patch.object(manager, "_start_workers_locked", return_value=None):
        job = manager.enqueue(
            recording_id="rec-1",
            manifest_path=manifest_path,
            output_path=output_path,
        )

    assert job.state == "queued"
    assert job.job_id != "existing-job"


def test_enqueue_returns_existing_ready_job_when_output_file_exists(tmp_path: Path) -> None:
    manager = CleanExportManager()
    manifest_path = tmp_path / "clean.m3u8"
    output_path = tmp_path / "clean.mp4"
    manifest_path.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")
    output_path.write_bytes(b"ready-output")

    existing = CleanExportJob(
        job_id="existing-job",
        recording_id="rec-1",
        state="ready",
        manifest_path=str(manifest_path),
        output_path=str(output_path),
        created_at=1.0,
        updated_at=1.0,
    )
    with manager._lock:
        manager._jobs["rec-1"] = existing

    with patch.object(manager, "_start_workers_locked", return_value=None) as start_workers:
        job = manager.enqueue(
            recording_id="rec-1",
            manifest_path=manifest_path,
            output_path=output_path,
        )

    assert job.job_id == "existing-job"
    assert job.state == "ready"
    start_workers.assert_not_called()
