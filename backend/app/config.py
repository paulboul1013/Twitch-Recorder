from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    twitch_client_id: str = ""
    twitch_client_secret: str = ""
    twitch_user_oauth_token: str = ""
    twitch_user_login: str = ""
    max_concurrent_streamers: int = 3
    poll_interval_seconds: int = 30
    offline_grace_period_seconds: int = 20
    recording_start_delay_seconds: int = 15
    watchable_trim_start_seconds: int = 0
    recordings_path: Path = Path("recordings")
    config_path: Path = Path("config")
    preferred_qualities: tuple[str, ...] = ("1080p60", "1080p", "720p60", "best")
    allowed_origins: tuple[str, ...] = ("http://localhost:3000", "http://127.0.0.1:3000")

    @property
    def streamers_file(self) -> Path:
        return self.config_path / "streamers.json"

    @property
    def recordings_file(self) -> Path:
        return self.config_path / "recordings.json"

    @classmethod
    def from_env(cls) -> "Settings":
        recordings_path = Path(os.getenv("RECORDINGS_PATH", "recordings"))
        config_path = Path(os.getenv("CONFIG_PATH", "config"))
        qualities = tuple(
            value.strip()
            for value in os.getenv("PREFERRED_QUALITIES", "1080p60,1080p,720p60,best").split(",")
            if value.strip()
        )
        allowed_origins = tuple(
            value.strip()
            for value in os.getenv(
                "ALLOWED_ORIGINS",
                "http://localhost:3000,http://127.0.0.1:3000",
            ).split(",")
            if value.strip()
        )
        return cls(
            twitch_client_id=os.getenv("TWITCH_CLIENT_ID", ""),
            twitch_client_secret=os.getenv("TWITCH_CLIENT_SECRET", ""),
            twitch_user_oauth_token=os.getenv("TWITCH_USER_OAUTH_TOKEN", ""),
            twitch_user_login=os.getenv("TWITCH_USER_LOGIN", ""),
            max_concurrent_streamers=int(os.getenv("MAX_CONCURRENT_STREAMERS", "3")),
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "30")),
            offline_grace_period_seconds=int(os.getenv("OFFLINE_GRACE_PERIOD_SECONDS", "20")),
            recording_start_delay_seconds=int(os.getenv("RECORDING_START_DELAY_SECONDS", "15")),
            watchable_trim_start_seconds=int(os.getenv("WATCHABLE_TRIM_START_SECONDS", "0")),
            recordings_path=recordings_path,
            config_path=config_path,
            preferred_qualities=qualities or cls.preferred_qualities,
            allowed_origins=allowed_origins or cls.allowed_origins,
        )

    def ensure_directories(self) -> None:
        self.recordings_path.mkdir(parents=True, exist_ok=True)
        self.config_path.mkdir(parents=True, exist_ok=True)
