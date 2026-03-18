from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

from app.ad_detection import parse_twitch_daterange_ad_windows
from app.recorder import RecorderManager

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


def test_extract_timed_id3_ad_offsets_normalizes_large_pts_jump() -> None:
    recorder = RecorderManager(Path("."), ("best",))
    ffprobe_stdout = """
[PACKET]
pts_time=4762.066000
[/PACKET]
[PACKET]
pts_time=4764.066000
[/PACKET]
[PACKET]
pts_time=4766.066000
[/PACKET]
[PACKET]
pts_time=95503.718689
data=
00000010: 000b 0000 0363 6f6e 7465 6e74 0061 64    .....content.ad
[/PACKET]
[PACKET]
pts_time=95505.718689
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
        windows = recorder._extract_timed_id3_ad_offsets(
            Path("sample.mp4"),
            expected_duration_seconds=300.0,
        )

    assert len(windows) == 1
    start, end = windows[0]
    assert round(start, 3) == 6.0
    assert round(end, 3) == 10.5


def test_ocr_text_matches_twitch_overlay_detects_preparing_stream() -> None:
    recorder = RecorderManager(Path("."), ("best",))

    assert recorder._ocr_text_matches_twitch_overlay("Preparing your stream")
    assert recorder._ocr_text_matches_twitch_overlay("Preparing your strea")
    assert recorder._ocr_text_matches_twitch_overlay("Commercial break in progress")
    assert recorder._ocr_text_matches_twitch_overlay("preparingyourstream")
    assert recorder._ocr_text_matches_twitch_overlay("preparing stream")
    assert not recorder._ocr_text_matches_twitch_overlay("live gameplay with no overlay")


def test_parse_twitch_daterange_ad_windows_extracts_ad_ranges() -> None:
    playlist_text = """
#EXTM3U
#EXT-X-DATERANGE:ID="stitched-ad-1",CLASS="twitch-stitched-ad",START-DATE="2026-03-18T12:00:00Z",END-DATE="2026-03-18T12:00:30Z",X-TV-TWITCH-AD-RADS-TOKEN="abc"
#EXT-X-DATERANGE:ID="stitched-ad-2",CLASS="twitch-stitched-ad",START-DATE="2026-03-18T12:01:00Z",DURATION=15.0,X-TV-TWITCH-AD-RADS-TOKEN="def"
"""
    windows, markers_seen = parse_twitch_daterange_ad_windows(playlist_text)

    assert markers_seen is True
    assert len(windows) == 2
    assert windows[0][0].isoformat().startswith("2026-03-18T12:00:00")
    assert windows[0][1].isoformat().startswith("2026-03-18T12:00:30")
    assert windows[1][0].isoformat().startswith("2026-03-18T12:01:00")
    assert windows[1][1].isoformat().startswith("2026-03-18T12:01:15")


def test_parse_twitch_daterange_ad_windows_reports_unknown_when_markers_missing() -> None:
    playlist_text = """
#EXTM3U
#EXT-X-VERSION:3
#EXTINF:6.000,
segment.ts
"""
    windows, markers_seen = parse_twitch_daterange_ad_windows(playlist_text)

    assert windows == []
    assert markers_seen is False

