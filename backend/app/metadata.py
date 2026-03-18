from __future__ import annotations

import json
from datetime import datetime

from .ad_detection import count_ad_breaks, infer_ad_detection_sources_from_events
from .recording_types import ActiveRecording


class RecordingMetadataWriter:
    def __init__(self, *, recording_start_delay_seconds: int) -> None:
        self.recording_start_delay_seconds = max(0, int(recording_start_delay_seconds))

    def base_prepare_mitigation(self) -> list[str]:
        if self.recording_start_delay_seconds > 0:
            return ["start_delay"]
        return []

    def write(
        self,
        *,
        recording: ActiveRecording,
        ended_at: datetime | None,
        exit_code: int | None,
        state: str,
        full_artifact_path: str | None,
        clean_artifact_path: str | None,
        full_segment_count: int,
        clean_segment_count: int,
        clean_export_state: str,
        clean_export_path: str | None,
        clean_export_error: str | None,
        unknown_ad_confidence: bool,
        clean_output_path: str | None,
        clean_output_state: str,
        clean_output_error: str | None,
        watchable_processing_seconds: float | None,
        ad_break_count_override: int | None = None,
        watchable_strategy: str | None = None,
        ad_detection_sources: list[str] | None = None,
        prepare_mitigation: list[str] | None = None,
        source_available: bool | None = None,
        source_deleted_on_success: bool = False,
        source_delete_error: str | None = None,
    ) -> None:
        with recording.lock:
            events_payload = [event.as_dict() for event in recording.events]
            stderr_tail = list(recording.stderr_tail)
            events_snapshot = list(recording.events)

        if ad_break_count_override is None:
            ad_break_count = count_ad_breaks(events_snapshot)
        else:
            ad_break_count = max(0, int(ad_break_count_override))

        if ad_detection_sources is None:
            ad_detection_sources = infer_ad_detection_sources_from_events(events_snapshot)

        if prepare_mitigation is None:
            prepare_mitigation = self.base_prepare_mitigation()

        payload = {
            "recording_id": recording.recording_id,
            "artifact_mode": recording.artifact_mode,
            "channel": recording.channel,
            "file_path": str(recording.file_path),
            "started_at": recording.started_at.isoformat(),
            "ended_at": ended_at.isoformat() if ended_at else None,
            "exit_code": exit_code,
            "state": state,
            "events": events_payload,
            "streamlink_stderr_tail": stderr_tail,
            "full_artifact_path": full_artifact_path,
            "clean_artifact_path": clean_artifact_path,
            "full_segment_count": max(0, int(full_segment_count)),
            "clean_segment_count": max(0, int(clean_segment_count)),
            "clean_export_state": clean_export_state,
            "clean_export_path": clean_export_path,
            "clean_export_error": clean_export_error,
            "unknown_ad_confidence": bool(unknown_ad_confidence),
            "clean_output_path": clean_output_path,
            "clean_output_state": clean_output_state,
            "clean_output_error": clean_output_error,
            "watchable_processing_seconds": watchable_processing_seconds,
            "source_mode": recording.source_mode,
            "ad_break_count": ad_break_count,
            "watchable_strategy": watchable_strategy,
            "ad_detection_sources": list(dict.fromkeys(ad_detection_sources)),
            "prepare_mitigation": list(dict.fromkeys(prepare_mitigation)),
            "source_available": (
                bool(source_available)
                if source_available is not None
                else (state == "recording" or recording.file_path.exists())
            ),
            "source_deleted_on_success": bool(source_deleted_on_success),
            "source_delete_error": source_delete_error,
        }
        recording.metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
