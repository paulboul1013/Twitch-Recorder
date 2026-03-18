from __future__ import annotations

import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable


@dataclass(slots=True)
class CleanExportJob:
    job_id: str
    recording_id: str
    state: str
    manifest_path: str
    output_path: str
    error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


class CleanExportManager:
    def __init__(
        self,
        *,
        max_concurrency: int = 1,
        on_state_change: Callable[[CleanExportJob], None] | None = None,
    ) -> None:
        self.max_concurrency = max(1, int(max_concurrency))
        self.on_state_change = on_state_change
        self._lock = threading.Lock()
        self._queue: deque[str] = deque()
        self._jobs: dict[str, CleanExportJob] = {}
        self._workers: set[threading.Thread] = set()
        self._active_worker_count = 0
        self._stopped = False

    def enqueue(
        self,
        *,
        recording_id: str,
        manifest_path: Path,
        output_path: Path,
    ) -> CleanExportJob:
        now = time.time()
        with self._lock:
            existing = self._jobs.get(recording_id)
            if existing is not None:
                if existing.state in {"queued", "processing"}:
                    return replace(existing)
                if existing.state == "ready" and Path(existing.output_path).exists():
                    return replace(existing)

            job = CleanExportJob(
                job_id=uuid.uuid4().hex,
                recording_id=recording_id,
                state="queued",
                manifest_path=str(manifest_path),
                output_path=str(output_path),
                created_at=now,
                updated_at=now,
            )
            self._jobs[recording_id] = job
            self._queue.append(recording_id)
            self._start_workers_locked()
        self._emit_state_change(job)
        return replace(job)

    def get(self, recording_id: str) -> CleanExportJob | None:
        with self._lock:
            job = self._jobs.get(recording_id)
            return replace(job) if job is not None else None

    def shutdown(self) -> None:
        with self._lock:
            self._stopped = True
            workers = list(self._workers)
        for worker in workers:
            worker.join()

    def _start_workers_locked(self) -> None:
        while (
            not self._stopped
            and self._active_worker_count < self.max_concurrency
            and self._queue
        ):
            worker = threading.Thread(
                target=self._worker_loop,
                daemon=True,
                name="clean-export-worker",
            )
            self._workers.add(worker)
            self._active_worker_count += 1
            worker.start()

    def _worker_loop(self) -> None:
        current_thread = threading.current_thread()
        try:
            while True:
                with self._lock:
                    if self._stopped:
                        return
                    if not self._queue:
                        return
                    recording_id = self._queue.popleft()
                    job = self._jobs.get(recording_id)
                    if job is None:
                        continue
                    job.state = "processing"
                    job.updated_at = time.time()
                    processing_snapshot = replace(job)
                self._emit_state_change(processing_snapshot)

                error: str | None = None
                try:
                    self._run_ffmpeg_export(processing_snapshot)
                except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
                    error = str(exc) or "clean export failed"

                with self._lock:
                    latest = self._jobs.get(recording_id)
                    if latest is None:
                        continue
                    latest.state = "failed" if error else "ready"
                    latest.error = error
                    latest.updated_at = time.time()
                    completed_snapshot = replace(latest)
                self._emit_state_change(completed_snapshot)
        finally:
            with self._lock:
                self._active_worker_count = max(0, self._active_worker_count - 1)
                self._workers.discard(current_thread)
                if self._queue and not self._stopped:
                    self._start_workers_locked()

    def _run_ffmpeg_export(self, job: CleanExportJob) -> None:
        manifest_path = Path(job.manifest_path)
        output_path = Path(job.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if manifest_path.suffix.lower() == ".ts":
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(manifest_path),
                "-c",
                "copy",
                str(output_path),
            ]
        else:
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-allowed_extensions",
                "ALL",
                "-protocol_whitelist",
                "file,crypto,data",
                "-i",
                str(manifest_path),
                "-c",
                "copy",
                str(output_path),
            ]
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if result.returncode == 0:
            return
        error_text = (result.stderr or result.stdout or "").strip()
        if len(error_text) > 500:
            error_text = error_text[-500:]
        raise RuntimeError(error_text or "ffmpeg export failed")

    def _emit_state_change(self, job: CleanExportJob) -> None:
        if self.on_state_change is None:
            return
        self.on_state_change(replace(job))
