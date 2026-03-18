from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .config import Settings
from .models import (
    CleanExportStatusResponse,
    HealthResponse,
    RecordingInfo,
    StartRecordingResponse,
    StopRecordingResponse,
    StreamStatus,
    StreamerCreate,
    StreamerInfo,
)
from .recorder import RecorderManager
from .service import MonitorService
from .store import RecordingHistoryStore, StreamerStore
from .twitch import TwitchClient


def build_service(settings: Settings | None = None) -> MonitorService:
    settings = settings or Settings.from_env()
    settings.ensure_directories()
    store = StreamerStore(settings.streamers_file)
    recording_store = RecordingHistoryStore(settings.recordings_file)
    twitch_client = TwitchClient(
        settings.twitch_client_id,
        settings.twitch_client_secret,
        max_batch_size=settings.twitch_api_batch_size,
        min_request_interval_seconds=settings.twitch_api_min_request_interval_seconds,
        max_retries=settings.twitch_api_max_retries,
        base_backoff_seconds=settings.twitch_api_base_backoff_seconds,
        max_backoff_seconds=settings.twitch_api_max_backoff_seconds,
        retry_jitter_ratio=settings.twitch_api_retry_jitter_ratio,
    )
    recorder_kwargs: dict[str, object] = {
        "twitch_user_oauth_token": settings.twitch_user_oauth_token,
        "twitch_user_login": settings.twitch_user_login,
        "watchable_trim_start_seconds": settings.watchable_trim_start_seconds,
        "recording_start_delay_seconds": settings.recording_start_delay_seconds,
        "recording_mode": settings.recording_mode,
        "segment_ad_padding_seconds": settings.segment_ad_padding_seconds,
    }
    if hasattr(settings, "recording_raw_container"):
        recorder_kwargs["recording_raw_container"] = settings.recording_raw_container
    if hasattr(settings, "delete_raw_on_success"):
        recorder_kwargs["delete_raw_on_success"] = settings.delete_raw_on_success

    recorder = RecorderManager(
        settings.recordings_path,
        settings.preferred_qualities,
        **recorder_kwargs,
    )
    return MonitorService(
        settings=settings,
        store=store,
        recording_store=recording_store,
        twitch_client=twitch_client,
        recorder=recorder,
    )


def create_app(service: MonitorService | None = None, enable_background: bool = True) -> FastAPI:
    service = service or build_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.monitor_service = service
        if enable_background:
            await service.start()
        yield
        await service.stop()

    app = FastAPI(title="Twitch Recorder API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(service.settings.allowed_origins),
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def get_service() -> MonitorService:
        return app.state.monitor_service

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(ok=True)

    @app.get("/streamers", response_model=list[StreamerInfo])
    async def list_streamers(monitor_service: MonitorService = Depends(get_service)) -> list[StreamerInfo]:
        return monitor_service.list_streamers()

    @app.post("/streamers", response_model=StreamerInfo, status_code=status.HTTP_201_CREATED)
    async def add_streamer(
        payload: StreamerCreate,
        monitor_service: MonitorService = Depends(get_service),
    ) -> StreamerInfo:
        return await monitor_service.add_streamer(payload.name)

    @app.delete("/streamers/{name}", status_code=status.HTTP_204_NO_CONTENT)
    async def remove_streamer(
        name: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> Response:
        if not any(streamer.name == name.lower() for streamer in monitor_service.list_streamers()):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="streamer not found")
        await monitor_service.remove_streamer(name)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/streamers/{name}/stop", response_model=StopRecordingResponse)
    async def stop_streamer_recording(
        name: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> StopRecordingResponse:
        if not any(streamer.name == name.lower() for streamer in monitor_service.list_streamers()):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="streamer not found")
        return await monitor_service.stop_streamer_recording(name)

    @app.post("/streamers/{name}/start", response_model=StartRecordingResponse)
    async def start_streamer_recording(
        name: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> StartRecordingResponse:
        if not any(streamer.name == name.lower() for streamer in monitor_service.list_streamers()):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="streamer not found")
        return await monitor_service.start_streamer_recording(name)

    @app.get("/status", response_model=list[StreamStatus])
    async def list_status(monitor_service: MonitorService = Depends(get_service)) -> list[StreamStatus]:
        return await monitor_service.list_statuses()

    @app.get("/recordings", response_model=list[RecordingInfo])
    async def list_recordings(
        monitor_service: MonitorService = Depends(get_service),
    ) -> list[RecordingInfo]:
        return await monitor_service.list_recordings()

    @app.get("/recordings/{recording_id}/download/full")
    async def download_full_artifact(
        recording_id: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> FileResponse:
        try:
            artifact_path = monitor_service.get_download_full_path(recording_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return FileResponse(path=artifact_path, filename=artifact_path.name)

    @app.get("/recordings/{recording_id}/download/clean-manifest")
    async def download_clean_manifest(
        recording_id: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> FileResponse:
        try:
            manifest_path = monitor_service.get_download_clean_manifest_path(recording_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return FileResponse(path=manifest_path, filename=manifest_path.name)

    @app.get("/recordings/{recording_id}/download/clean-mp4")
    async def download_clean_mp4(
        recording_id: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> FileResponse:
        try:
            export_path = monitor_service.get_download_clean_export_path(recording_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return FileResponse(path=export_path, filename=export_path.name)

    @app.post(
        "/recordings/{recording_id}/exports/clean-mp4",
        response_model=CleanExportStatusResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_clean_mp4_export(
        recording_id: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> CleanExportStatusResponse:
        try:
            return monitor_service.create_clean_export(recording_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @app.get(
        "/recordings/{recording_id}/exports/clean-mp4",
        response_model=CleanExportStatusResponse,
    )
    async def get_clean_mp4_export_status(
        recording_id: str,
        monitor_service: MonitorService = Depends(get_service),
    ) -> CleanExportStatusResponse:
        try:
            return monitor_service.get_clean_export_status(recording_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @app.post("/refresh", response_model=list[StreamStatus])
    async def refresh(monitor_service: MonitorService = Depends(get_service)) -> list[StreamStatus]:
        await monitor_service.refresh_once()
        return await monitor_service.list_statuses()

    return app


app = create_app()
