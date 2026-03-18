from __future__ import annotations

import statistics
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from .recording_types import RecordingEvent


def detect_ad_transition(line: str, ad_break_active: bool) -> str | None:
    lower = line.lower()
    if any(
        token in lower
        for token in (
            "ad_break_ended",
            "ad break ended",
            "commercial break ended",
            "ads ended",
            "ads complete",
            "ad finished",
        )
    ):
        return "ad_break_ended"

    if any(
        token in lower
        for token in (
            "ad_break_started",
            "ad break started",
            "commercial break started",
            "commercial break",
            "midroll",
            "mid-roll",
            "ad break",
        )
    ):
        if any(token in lower for token in ("ended", "finished", "resume", "resuming", "back")):
            return "ad_break_ended"
        return "ad_break_started"

    if "discontinuity" in lower and not ad_break_active:
        return "ad_break_started"

    if ad_break_active and any(
        token in lower for token in ("resuming stream", "stream resumed", "back to stream", "playback resumed")
    ):
        return "ad_break_ended"

    return None


def count_ad_breaks(events: list[RecordingEvent]) -> int:
    return sum(1 for event in events if event.type == "ad_break_started")


def infer_ad_detection_sources_from_events(events: list[RecordingEvent]) -> list[str]:
    if any(event.type == "ad_break_started" for event in events):
        return ["stderr"]
    return []


def collect_ad_windows(
    events: list[RecordingEvent],
    *,
    ended_at: datetime,
) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    ad_started_at: datetime | None = None

    for event in events:
        if event.type == "ad_break_started":
            if ad_started_at is None:
                ad_started_at = event.at
        elif event.type == "ad_break_ended":
            if ad_started_at is None:
                continue
            if event.at > ad_started_at:
                windows.append((ad_started_at, event.at))
            ad_started_at = None

    if ad_started_at is not None and ended_at > ad_started_at:
        windows.append((ad_started_at, ended_at))

    return windows


def _split_m3u8_attributes(payload: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    for char in payload:
        if char == '"':
            in_quotes = not in_quotes
            current.append(char)
            continue
        if char == "," and not in_quotes:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_m3u8_attribute_list(payload: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for part in _split_m3u8_attributes(payload):
        if "=" not in part:
            continue
        key, raw_value = part.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]
        attributes[key] = value
    return attributes


def _parse_iso8601(value: str) -> datetime | None:
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def parse_twitch_daterange_ad_windows(playlist_text: str) -> tuple[list[tuple[datetime, datetime]], bool]:
    windows: list[tuple[datetime, datetime]] = []
    markers_seen = False
    for raw_line in playlist_text.splitlines():
        line = raw_line.strip()
        if not line.startswith("#EXT-X-DATERANGE:"):
            continue
        attributes = parse_m3u8_attribute_list(line.split(":", 1)[1])
        ad_class = str(attributes.get("CLASS", "")).lower()
        ad_id = str(attributes.get("ID", "")).lower()
        has_twitch_ad_field = any(key.startswith("X-TV-TWITCH-AD") for key in attributes)
        if not (
            has_twitch_ad_field
            or "stitched-ad" in ad_class
            or "ad" in ad_class
            or "stitched-ad" in ad_id
        ):
            continue

        start_raw = attributes.get("START-DATE")
        if not start_raw:
            continue
        start_dt = _parse_iso8601(start_raw)
        if start_dt is None:
            continue

        end_dt: datetime | None = None
        end_raw = attributes.get("END-DATE")
        if end_raw:
            end_dt = _parse_iso8601(end_raw)

        if end_dt is None:
            duration_raw = attributes.get("DURATION") or attributes.get("PLANNED-DURATION")
            if duration_raw:
                try:
                    duration_seconds = float(duration_raw)
                except ValueError:
                    duration_seconds = 0.0
                if duration_seconds > 0:
                    end_dt = start_dt + timedelta(seconds=duration_seconds)

        if end_dt is None:
            continue
        if end_dt <= start_dt:
            continue
        markers_seen = True
        windows.append((start_dt, end_dt))

    windows.sort(key=lambda item: item[0])
    return windows, markers_seen


def merge_offset_ranges(
    ranges: list[tuple[float, float]],
    *,
    merge_gap_seconds: float = 0.0,
) -> list[tuple[float, float]]:
    merged: list[tuple[float, float]] = []
    for start, end in sorted(ranges):
        if end <= start:
            continue
        if not merged:
            merged.append((start, end))
            continue
        prev_start, prev_end = merged[-1]
        if start <= prev_end + merge_gap_seconds:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def ranges_intersect(
    left: tuple[float, float],
    right: tuple[float, float],
) -> bool:
    left_start, left_end = left
    right_start, right_end = right
    return min(left_end, right_end) > max(left_start, right_start)


def ocr_text_matches_ad_overlay(text: str) -> bool:
    normalized = " ".join(text.lower().split())
    if "commercial break in progress" in normalized:
        return True
    if "break in progress" in normalized and any(
        token in normalized for token in ("commercial", "ommercial", "twitch")
    ):
        return True
    return any(
        token in normalized
        for token in (
            "ad break started",
            "commercial break",
            "ads in progress",
        )
    )


def ocr_text_matches_prepare_overlay(text: str) -> bool:
    normalized = " ".join(text.lower().split())
    collapsed = normalized.replace(" ", "")
    if any(
        token in normalized
        for token in ("preparing your stream", "preparing your strea", "preparing stream")
    ):
        return True
    return any(
        token in collapsed
        for token in ("preparingyourstream", "preparingyourstrea", "preparingstream")
    )


def ocr_text_matches_twitch_overlay(text: str) -> bool:
    return ocr_text_matches_ad_overlay(text) or ocr_text_matches_prepare_overlay(text)


def normalize_timed_id3_marker_times(
    *,
    packet_pts: list[float],
    marker_times: list[float],
    expected_duration_seconds: float | None = None,
    discontinuity_min_seconds: float,
    discontinuity_factor: float,
) -> list[float]:
    if not packet_pts or not marker_times:
        return []

    deltas = [
        current - previous
        for previous, current in zip(packet_pts, packet_pts[1:])
        if 0.0 < current - previous <= discontinuity_min_seconds
    ]
    typical_gap = statistics.median(deltas) if deltas else 2.0
    discontinuity_threshold = max(discontinuity_min_seconds, typical_gap * discontinuity_factor)

    normalized_pts: list[float] = [packet_pts[0]]
    for previous_raw, current_raw in zip(packet_pts, packet_pts[1:]):
        delta = current_raw - previous_raw
        if delta <= 0.0:
            delta = typical_gap
        elif expected_duration_seconds is not None and delta > discontinuity_threshold:
            delta = typical_gap
        normalized_pts.append(normalized_pts[-1] + delta)

    marker_counts: dict[float, int] = {}
    for marker in marker_times:
        marker_counts[marker] = marker_counts.get(marker, 0) + 1

    normalized_markers: list[float] = []
    for raw_pts, normalized_pts_value in zip(packet_pts, normalized_pts):
        count = marker_counts.get(raw_pts, 0)
        if count <= 0:
            continue
        normalized_markers.extend([normalized_pts_value] * count)

    if not normalized_markers:
        return []

    base_pts = normalized_pts[0]
    relative_times = sorted({max(0.0, pts - base_pts) for pts in normalized_markers})
    if expected_duration_seconds is None:
        return relative_times
    return [
        offset
        for offset in relative_times
        if offset <= expected_duration_seconds + discontinuity_min_seconds
    ]


def extract_timed_id3_ad_offsets(
    source_path: Path,
    *,
    expected_duration_seconds: float | None = None,
    discontinuity_min_seconds: float,
    discontinuity_factor: float,
    run_cmd: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> list[tuple[float, float]]:
    cmd = [
        "ffprobe",
        "-hide_banner",
        "-loglevel",
        "error",
        "-select_streams",
        "d:0",
        "-show_packets",
        "-show_entries",
        "packet=pts_time,data",
        "-show_data",
        str(source_path),
    ]
    result = run_cmd(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        return []

    current_pts: float | None = None
    packet_has_ad = False
    packet_pts: list[float] = []
    marker_times: list[float] = []
    ad_tokens = (
        "content.ad",
        "stitched-ad",
        "twitch-ads",
        "ad_signifier",
        "amazon-adsystem",
    )

    def flush_packet() -> None:
        if current_pts is None:
            return
        packet_pts.append(current_pts)
        if packet_has_ad:
            marker_times.append(current_pts)

    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if line.startswith("pts_time="):
            flush_packet()
            try:
                current_pts = float(line.split("=", 1)[1])
            except ValueError:
                current_pts = None
                packet_has_ad = False
                continue
            packet_has_ad = False
            continue

        if line == "[/PACKET]":
            flush_packet()
            current_pts = None
            packet_has_ad = False
            continue

        lower = line.lower()
        if any(token in lower for token in ad_tokens):
            packet_has_ad = True

    flush_packet()
    if not marker_times or not packet_pts:
        return []

    relative_times = normalize_timed_id3_marker_times(
        packet_pts=packet_pts,
        marker_times=marker_times,
        expected_duration_seconds=expected_duration_seconds,
        discontinuity_min_seconds=discontinuity_min_seconds,
        discontinuity_factor=discontinuity_factor,
    )
    if not relative_times:
        return []

    max_gap_seconds = 6.0
    tail_padding_seconds = 2.5
    windows: list[tuple[float, float]] = []
    group_start = relative_times[0]
    previous = relative_times[0]

    for current in relative_times[1:]:
        if current - previous <= max_gap_seconds:
            previous = current
            continue
        windows.append((group_start, previous + tail_padding_seconds))
        group_start = current
        previous = current
    windows.append((group_start, previous + tail_padding_seconds))
    return windows


def collect_timed_id3_ad_windows(
    *,
    source_path: Path,
    started_at: datetime,
    duration_seconds: float,
    expected_duration_seconds: float | None,
    discontinuity_min_seconds: float,
    discontinuity_factor: float,
    run_cmd: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> list[tuple[datetime, datetime]]:
    offsets = extract_timed_id3_ad_offsets(
        source_path,
        expected_duration_seconds=expected_duration_seconds,
        discontinuity_min_seconds=discontinuity_min_seconds,
        discontinuity_factor=discontinuity_factor,
        run_cmd=run_cmd,
    )
    if not offsets:
        return []

    windows: list[tuple[datetime, datetime]] = []
    for start_offset, end_offset in offsets:
        start = max(0.0, min(duration_seconds, start_offset))
        end = max(0.0, min(duration_seconds, end_offset))
        if end <= start:
            continue
        windows.append(
            (
                started_at + timedelta(seconds=start),
                started_at + timedelta(seconds=end),
            )
        )
    return windows
