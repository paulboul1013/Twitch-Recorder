from __future__ import annotations

import json
from pathlib import Path

from app.store import RecordingHistoryStore


def test_recording_store_migrates_legacy_entries_with_default_fields(tmp_path: Path) -> None:
    store_path = tmp_path / "recordings.json"
    legacy_payload = [
        {
            "channel": "alpha",
            "file_path": str(tmp_path / "recordings" / "alpha_20250301_120000.ts"),
            "watchable_file_path": str(tmp_path / "recordings" / "alpha_20250301_120000.watchable.mp4"),
            "watchable_state": "ready",
        }
    ]
    store_path.write_text(json.dumps(legacy_payload), encoding="utf-8")

    store = RecordingHistoryStore(store_path)
    entries = store.load()

    assert len(entries) == 1
    entry = entries[0]
    assert entry.recording_id
    assert entry.artifact_mode == "legacy"
    assert entry.full_artifact_path == entry.source_file_path
    assert entry.clean_artifact_path == legacy_payload[0]["watchable_file_path"]
    assert entry.clean_export_state == "none"
    assert entry.clean_export_path is None
    assert entry.clean_export_error is None
