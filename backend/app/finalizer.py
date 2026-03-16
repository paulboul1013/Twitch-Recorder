from __future__ import annotations

import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from .ad_detection import (
    collect_timed_id3_ad_windows,
    extract_timed_id3_ad_offsets,
    merge_offset_ranges,
    normalize_timed_id3_marker_times,
    ocr_text_matches_ad_overlay,
    ocr_text_matches_prepare_overlay,
    ocr_text_matches_twitch_overlay,
    ranges_intersect,
)


class RecordingFinalizer:
    def __init__(
        self,
        *,
        recordings_path: Path,
        watchable_trim_start_seconds: int,
        ocr_verify_interval_seconds: float,
        ocr_hit_padding_seconds: float,
        ocr_merge_gap_seconds: float,
        ocr_local_validation_padding_seconds: float,
        timed_id3_discontinuity_min_seconds: float,
        timed_id3_discontinuity_factor: float,
        max_watchable_repair_passes: int,
        trim_prepare_verify_seconds: float,
        segment_verify_neighbor_seconds: float,
    ) -> None:
        self.recordings_path = recordings_path
        self.watchable_trim_start_seconds = max(0, int(watchable_trim_start_seconds))
        self.ocr_verify_interval_seconds = ocr_verify_interval_seconds
        self.ocr_hit_padding_seconds = ocr_hit_padding_seconds
        self.ocr_merge_gap_seconds = ocr_merge_gap_seconds
        self.ocr_local_validation_padding_seconds = ocr_local_validation_padding_seconds
        self.timed_id3_discontinuity_min_seconds = timed_id3_discontinuity_min_seconds
        self.timed_id3_discontinuity_factor = timed_id3_discontinuity_factor
        self.max_watchable_repair_passes = max_watchable_repair_passes
        self.trim_prepare_verify_seconds = trim_prepare_verify_seconds
        self.segment_verify_neighbor_seconds = segment_verify_neighbor_seconds

    def can_remux_watchable(self, *, ad_offsets: list[tuple[float, float]]) -> bool:
        return not ad_offsets and self.watchable_trim_start_seconds <= 0

    def trim_copy_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
        trim_start_seconds: float,
    ) -> None:
        watchable_path.unlink(missing_ok=True)
        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{max(0.0, trim_start_seconds):.3f}",
            "-i",
            str(source_path),
            "-map",
            "0:v?",
            "-map",
            "0:a?",
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(watchable_path),
        ]
        result = subprocess.run(
            cmd,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "ffmpeg trim-copy failed")

    def confirm_timed_id3_ad_offsets_with_ocr(
        self,
        *,
        source_path: Path,
        duration_seconds: float,
        extract_offsets: Callable[[Path, float | None], list[tuple[float, float]]] | None = None,
        collect_ocr_windows: (
            Callable[[Path, float, float, list[tuple[float, float]] | None, Callable[[str], bool] | None], list[tuple[float, float]]]
            | None
        ) = None,
    ) -> list[tuple[float, float]]:
        extract = extract_offsets or (
            lambda path, expected_duration: self.extract_timed_id3_ad_offsets(
                path,
                expected_duration_seconds=expected_duration,
            )
        )
        collect = collect_ocr_windows or (
            lambda path, duration, interval, sample_ranges, matcher: self.collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges,
                matcher=matcher,
            )
        )

        candidate_offsets = extract(source_path, duration_seconds)
        if not candidate_offsets or duration_seconds <= 0:
            return []

        expanded_ranges = merge_offset_ranges(
            [
                (
                    max(0.0, start - self.ocr_local_validation_padding_seconds),
                    min(duration_seconds, end + self.ocr_local_validation_padding_seconds),
                )
                for start, end in candidate_offsets
                if end > start
            ]
        )
        if not expanded_ranges:
            return []

        ocr_hits = collect(
            source_path,
            duration_seconds,
            self.ocr_verify_interval_seconds,
            expanded_ranges,
            ocr_text_matches_ad_overlay,
        )
        if not ocr_hits:
            return []

        confirmed: list[tuple[float, float]] = []
        for start, end in candidate_offsets:
            expanded_start = max(0.0, start - self.ocr_local_validation_padding_seconds)
            expanded_end = min(duration_seconds, end + self.ocr_local_validation_padding_seconds)
            if any(
                ranges_intersect((expanded_start, expanded_end), ocr_range)
                for ocr_range in ocr_hits
            ):
                confirmed.append((start, end))

        return merge_offset_ranges(confirmed)

    def has_prepare_overlay_in_prefix(
        self,
        source_path: Path,
        *,
        verify_seconds: float,
        probe_duration: Callable[[Path], float | None] | None = None,
        collect_ocr_windows: (
            Callable[[Path, float, float, list[tuple[float, float]] | None, Callable[[str], bool] | None], list[tuple[float, float]]]
            | None
        ) = None,
    ) -> bool:
        probe = probe_duration or self.probe_media_duration
        collect = collect_ocr_windows or (
            lambda path, duration, interval, sample_ranges, matcher: self.collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges,
                matcher=matcher,
            )
        )

        duration_seconds = probe(source_path)
        if duration_seconds is None or duration_seconds <= 0:
            return True
        inspect_window = min(duration_seconds, max(0.0, verify_seconds))
        if inspect_window <= 0:
            return False
        overlay_windows = collect(
            source_path,
            duration_seconds,
            self.ocr_verify_interval_seconds,
            [(0.0, inspect_window)],
            ocr_text_matches_prepare_overlay,
        )
        return bool(overlay_windows)

    def contains_overlay_in_ranges(
        self,
        source_path: Path,
        *,
        sample_ranges: list[tuple[float, float]],
        probe_duration: Callable[[Path], float | None] | None = None,
        collect_ocr_windows: (
            Callable[[Path, float, float, list[tuple[float, float]] | None, Callable[[str], bool] | None], list[tuple[float, float]]]
            | None
        ) = None,
    ) -> bool:
        probe = probe_duration or self.probe_media_duration
        collect = collect_ocr_windows or (
            lambda path, duration, interval, sample_ranges_arg, matcher: self.collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges_arg,
                matcher=matcher,
            )
        )

        duration_seconds = probe(source_path)
        if duration_seconds is None or duration_seconds <= 0:
            return True
        overlay_windows = collect(
            source_path,
            duration_seconds,
            self.ocr_verify_interval_seconds,
            sample_ranges,
            ocr_text_matches_twitch_overlay,
        )
        return bool(overlay_windows)

    def build_segment_verification_ranges(
        self,
        keep_ranges: list[tuple[float, float]],
    ) -> list[tuple[float, float]]:
        if not keep_ranges:
            return []

        total_duration = sum(max(0.0, end - start) for start, end in keep_ranges)
        if total_duration <= 0:
            return []

        boundary_ranges: list[tuple[float, float]] = []
        cursor = 0.0
        for start, end in keep_ranges[:-1]:
            cursor += max(0.0, end - start)
            boundary_ranges.append(
                (
                    max(0.0, cursor - self.segment_verify_neighbor_seconds),
                    min(total_duration, cursor + self.segment_verify_neighbor_seconds),
                )
            )

        # Always include output head/tail neighborhoods to catch boundary leakage.
        boundary_ranges.append((0.0, min(total_duration, self.segment_verify_neighbor_seconds)))
        boundary_ranges.append(
            (
                max(0.0, total_duration - self.segment_verify_neighbor_seconds),
                total_duration,
            )
        )
        return merge_offset_ranges(boundary_ranges)

    def reencode_existing_watchable_output(
        self,
        watchable_path: Path,
        *,
        probe_duration: Callable[[Path], float | None] | None = None,
        build_keep_ranges_from_offsets: Callable[[float, list[tuple[float, float]], float], list[tuple[float, float]]] | None = None,
        render_watchable: Callable[[Path, Path, list[tuple[float, float]]], None] | None = None,
    ) -> None:
        probe = probe_duration or self.probe_media_duration
        build_ranges = build_keep_ranges_from_offsets or (
            lambda duration, remove_ranges, trim_start: self.build_keep_ranges_from_offsets(
                duration,
                remove_ranges,
                trim_start_seconds=trim_start,
            )
        )
        render = render_watchable or (
            lambda source, output, keep_ranges: self.render_watchable(
                source_path=source,
                watchable_path=output,
                keep_ranges=keep_ranges,
            )
        )

        duration_seconds = probe(watchable_path)
        if duration_seconds is None:
            raise RuntimeError("failed to probe watchable output duration")

        keep_ranges = build_ranges(duration_seconds, [], 0.0)
        if not keep_ranges:
            raise RuntimeError("watchable output duration too short to re-encode")

        fallback_path = watchable_path.with_name(f"{watchable_path.stem}.fallback{watchable_path.suffix}")
        fallback_path.unlink(missing_ok=True)
        render(watchable_path, fallback_path, keep_ranges)
        fallback_path.replace(watchable_path)

    def remux_watchable(
        self,
        *,
        source_path: Path,
        watchable_path: Path,
    ) -> None:
        watchable_path.unlink(missing_ok=True)
        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(source_path),
            "-map",
            "0:v?",
            "-map",
            "0:a?",
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(watchable_path),
        ]
        result = subprocess.run(
            cmd,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "ffmpeg remux failed")

    def collect_timed_id3_ad_windows(
        self,
        *,
        source_path: Path,
        started_at: datetime,
        duration_seconds: float,
        run_cmd: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> list[tuple[datetime, datetime]]:
        return collect_timed_id3_ad_windows(
            source_path=source_path,
            started_at=started_at,
            duration_seconds=duration_seconds,
            expected_duration_seconds=duration_seconds,
            discontinuity_min_seconds=self.timed_id3_discontinuity_min_seconds,
            discontinuity_factor=self.timed_id3_discontinuity_factor,
            run_cmd=run_cmd,
        )

    def extract_timed_id3_ad_offsets(
        self,
        source_path: Path,
        *,
        expected_duration_seconds: float | None = None,
        run_cmd: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> list[tuple[float, float]]:
        return extract_timed_id3_ad_offsets(
            source_path,
            expected_duration_seconds=expected_duration_seconds,
            discontinuity_min_seconds=self.timed_id3_discontinuity_min_seconds,
            discontinuity_factor=self.timed_id3_discontinuity_factor,
            run_cmd=run_cmd,
        )

    def normalize_timed_id3_marker_times(
        self,
        *,
        packet_pts: list[float],
        marker_times: list[float],
        expected_duration_seconds: float | None = None,
    ) -> list[float]:
        return normalize_timed_id3_marker_times(
            packet_pts=packet_pts,
            marker_times=marker_times,
            expected_duration_seconds=expected_duration_seconds,
            discontinuity_min_seconds=self.timed_id3_discontinuity_min_seconds,
            discontinuity_factor=self.timed_id3_discontinuity_factor,
        )

    def build_keep_ranges(
        self,
        started_at: datetime,
        ended_at: datetime,
        ad_windows: list[tuple[datetime, datetime]],
        trim_start_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        duration = max(0.0, (ended_at - started_at).total_seconds())
        if duration <= 0:
            return []

        clipped = [
            (
                max(0.0, (ad_start - started_at).total_seconds()),
                min(duration, (ad_end - started_at).total_seconds()),
            )
            for ad_start, ad_end in sorted(ad_windows)
            if ad_end > ad_start
        ]
        return self.build_keep_ranges_from_offsets(
            duration,
            clipped,
            trim_start_seconds=trim_start_seconds,
        )

    def build_keep_ranges_from_offsets(
        self,
        duration_seconds: float,
        remove_ranges: list[tuple[float, float]],
        *,
        trim_start_seconds: float = 0.0,
    ) -> list[tuple[float, float]]:
        if duration_seconds <= 0:
            return []
        trim_start = min(duration_seconds, max(0.0, trim_start_seconds))

        merged: list[tuple[float, float]] = []
        for start, end in sorted(remove_ranges):
            start = max(trim_start, start)
            end = min(duration_seconds, end)
            if end <= start:
                continue
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
        if cursor < duration_seconds:
            keep_ranges.append((cursor, duration_seconds))

        return [(start, end) for start, end in keep_ranges if end - start >= 0.25]

    def repair_watchable_output(
        self,
        watchable_path: Path,
        *,
        probe_duration: Callable[[Path], float | None] | None = None,
        collect_ocr_windows: (
            Callable[[Path, float, float, list[tuple[float, float]] | None, Callable[[str], bool] | None], list[tuple[float, float]]]
            | None
        ) = None,
        build_keep_ranges_from_offsets: Callable[[float, list[tuple[float, float]], float], list[tuple[float, float]]] | None = None,
        render_watchable: Callable[[Path, Path, list[tuple[float, float]]], None] | None = None,
    ) -> tuple[str | None, int]:
        probe = probe_duration or self.probe_media_duration
        collect = collect_ocr_windows or (
            lambda path, duration, interval, sample_ranges, matcher: self.collect_ocr_ad_windows(
                path,
                duration_seconds=duration,
                sample_interval_seconds=interval,
                sample_ranges=sample_ranges,
                matcher=matcher,
            )
        )
        build_ranges = build_keep_ranges_from_offsets or (
            lambda duration, remove_ranges, trim_start: self.build_keep_ranges_from_offsets(
                duration,
                remove_ranges,
                trim_start_seconds=trim_start,
            )
        )
        render = render_watchable or (
            lambda source, output, keep_ranges: self.render_watchable(
                source_path=source,
                watchable_path=output,
                keep_ranges=keep_ranges,
            )
        )

        detected_ad_break_count = 0
        for _ in range(self.max_watchable_repair_passes):
            duration_seconds = probe(watchable_path)
            if duration_seconds is None:
                return "failed to probe watchable output duration", detected_ad_break_count

            overlay_windows = collect(
                watchable_path,
                duration_seconds,
                self.ocr_verify_interval_seconds,
                None,
                None,
            )
            if not overlay_windows:
                return None, detected_ad_break_count

            detected_ad_break_count = max(detected_ad_break_count, len(overlay_windows))
            keep_ranges = build_ranges(duration_seconds, overlay_windows, 0.0)
            if not keep_ranges:
                return "Twitch playback overlay covered the entire watchable output", detected_ad_break_count

            repaired_path = watchable_path.with_name(f"{watchable_path.stem}.repair{watchable_path.suffix}")
            render(watchable_path, repaired_path, keep_ranges)
            repaired_path.replace(watchable_path)

        residual_duration = probe(watchable_path)
        residual_windows = (
            collect(
                watchable_path,
                residual_duration,
                self.ocr_verify_interval_seconds,
                None,
                None,
            )
            if residual_duration is not None
            else []
        )
        if residual_windows:
            detected_ad_break_count = max(detected_ad_break_count, len(residual_windows))
            return "watchable verification still detected Twitch playback overlay", detected_ad_break_count
        return None, detected_ad_break_count

    def probe_media_duration(self, source_path: Path) -> float | None:
        cmd = [
            "ffprobe",
            "-hide_banner",
            "-loglevel",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(source_path),
        ]
        try:
            result = subprocess.run(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        try:
            duration = float(result.stdout.strip())
        except ValueError:
            return None
        if duration <= 0:
            return None
        return duration

    def collect_ocr_ad_windows(
        self,
        source_path: Path,
        *,
        duration_seconds: float,
        sample_interval_seconds: float,
        sample_ranges: list[tuple[float, float]] | None = None,
        matcher: Callable[[str], bool] | None = None,
    ) -> list[tuple[float, float]]:
        if duration_seconds <= 0 or sample_interval_seconds <= 0:
            return []
        if matcher is None:
            matcher = ocr_text_matches_twitch_overlay

        scan_ranges = sample_ranges or [(0.0, duration_seconds)]
        merged_scan_ranges = merge_offset_ranges(
            [
                (max(0.0, start), min(duration_seconds, end))
                for start, end in scan_ranges
                if end > start
            ]
        )
        if not merged_scan_ranges:
            return []

        filter_chain = (
            "fps=1/"
            f"{sample_interval_seconds},"
            "crop=iw*0.78:ih*0.30:iw*0.11:ih*0.24,"
            "scale=1400:-1,"
            "format=gray"
        )
        with tempfile.TemporaryDirectory(prefix="ocrscan_") as temp_dir:
            temp_root = Path(temp_dir)
            hit_windows: list[tuple[float, float]] = []
            for range_index, (range_start, range_end) in enumerate(merged_scan_ranges):
                if range_end <= range_start:
                    continue
                frame_pattern = f"range_{range_index:03d}_frame_%06d.png"
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-ss",
                    f"{range_start:.3f}",
                    "-to",
                    f"{range_end:.3f}",
                    "-i",
                    str(source_path),
                    "-vf",
                    filter_chain,
                    "-start_number",
                    "0",
                    str(temp_root / frame_pattern),
                ]
                try:
                    result = subprocess.run(
                        cmd,
                        text=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                    )
                except (OSError, subprocess.SubprocessError):
                    continue
                if result.returncode != 0:
                    continue

                for frame_path in sorted(temp_root.glob(f"range_{range_index:03d}_frame_*.png")):
                    try:
                        frame_index = int(frame_path.stem.rsplit("_", 1)[1])
                    except (IndexError, ValueError):
                        continue
                    ocr_text = self.run_tesseract(frame_path)
                    if not matcher(ocr_text):
                        continue
                    sample_time = range_start + (frame_index * sample_interval_seconds)
                    hit_windows.append(
                        (
                            max(0.0, sample_time - self.ocr_hit_padding_seconds),
                            min(duration_seconds, sample_time + self.ocr_hit_padding_seconds),
                        )
                    )

        return merge_offset_ranges(hit_windows, merge_gap_seconds=self.ocr_merge_gap_seconds)

    def run_tesseract(self, frame_path: Path) -> str:
        cmd = [
            "tesseract",
            str(frame_path),
            "stdout",
            "--psm",
            "6",
        ]
        try:
            result = subprocess.run(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.SubprocessError):
            return ""
        if result.returncode != 0:
            return ""
        return result.stdout.lower()

    def render_watchable(
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
