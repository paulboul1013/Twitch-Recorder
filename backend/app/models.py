from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class StreamerCreate(BaseModel):
    name: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_]+$")


class StreamerInfo(BaseModel):
    name: str


class StreamStatus(BaseModel):
    name: str
    profile_image_url: str | None = None
    is_live: bool = False
    is_recording: bool = False
    recording_state: str | None = None
    title: str | None = None
    game_name: str | None = None
    viewer_count: int | None = None
    started_at: datetime | None = None
    last_checked_at: datetime | None = None
    offline_since: datetime | None = None
    stop_after_at: datetime | None = None
    output_path: str | None = None
    recording_started_at: datetime | None = None
    recording_ended_at: datetime | None = None
    recording_exit_code: int | None = None
    last_error: str | None = None


class RecordingInfo(BaseModel):
    recording_id: str
    artifact_mode: str = "legacy"
    is_recording: bool = False
    channel: str
    file_path: str
    file_name: str
    source_file_path: str
    source_file_name: str
    source_available: bool = True
    watchable_file_path: str | None = None
    watchable_file_name: str | None = None
    watchable_available: bool = False
    watchable_state: str = "pending"
    ad_break_count: int = 0
    source_mode: str = "unauthenticated"
    full_artifact_path: str | None = None
    clean_artifact_path: str | None = None
    full_segment_count: int = 0
    clean_segment_count: int = 0
    clean_export_state: str = "none"
    clean_export_path: str | None = None
    clean_export_error: str | None = None
    unknown_ad_confidence: bool = False
    size_bytes: int
    modified_at: datetime


class StopRecordingResponse(BaseModel):
    name: str
    stopped: bool


class StartRecordingResponse(BaseModel):
    name: str
    started: bool


class HealthResponse(BaseModel):
    ok: bool


class CleanExportStatusResponse(BaseModel):
    recording_id: str
    job_id: str | None = None
    state: str
    output_path: str | None = None
    error: str | None = None
