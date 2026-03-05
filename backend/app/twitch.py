from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx


class TwitchAuthError(RuntimeError):
    pass


@dataclass(slots=True)
class LiveStream:
    user_login: str
    title: str | None
    game_name: str | None
    viewer_count: int | None
    started_at: datetime | None


@dataclass(slots=True)
class TwitchUser:
    login: str
    profile_image_url: str | None


class TwitchClient:
    token_url = "https://id.twitch.tv/oauth2/token"
    streams_url = "https://api.twitch.tv/helix/streams"
    users_url = "https://api.twitch.tv/helix/users"

    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None
        self._token_expires_at: datetime | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.client_id and self.client_secret)

    async def _get_app_token(self) -> str:
        if not self.enabled:
            raise TwitchAuthError("TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET are required")

        now = datetime.now(UTC)
        if self._token and self._token_expires_at and now < self._token_expires_at:
            return self._token

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                self.token_url,
                params={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                },
            )
            response.raise_for_status()
            payload = response.json()

        access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 0))
        if not access_token:
            raise TwitchAuthError("Twitch auth response did not include access_token")

        self._token = access_token
        self._token_expires_at = now + timedelta(seconds=max(expires_in - 60, 60))
        return self._token

    async def get_live_streams(self, usernames: list[str]) -> dict[str, LiveStream]:
        normalized = [name.strip().lower() for name in usernames if name.strip()]
        if not normalized:
            return {}

        token = await self._get_app_token()
        params = [("user_login", username) for username in normalized]

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                self.streams_url,
                params=params,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Client-Id": self.client_id,
                },
            )
            response.raise_for_status()
            payload = response.json()

        streams: dict[str, LiveStream] = {}
        for item in payload.get("data", []):
            started_at = None
            if item.get("started_at"):
                started_at = datetime.fromisoformat(item["started_at"].replace("Z", "+00:00"))
            username = item["user_login"].lower()
            streams[username] = LiveStream(
                user_login=username,
                title=item.get("title"),
                game_name=item.get("game_name"),
                viewer_count=item.get("viewer_count"),
                started_at=started_at,
            )

        return streams

    async def get_users(self, usernames: list[str]) -> dict[str, TwitchUser]:
        normalized = [name.strip().lower() for name in usernames if name.strip()]
        if not normalized:
            return {}

        token = await self._get_app_token()
        params = [("login", username) for username in normalized]

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                self.users_url,
                params=params,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Client-Id": self.client_id,
                },
            )
            response.raise_for_status()
            payload = response.json()

        users: dict[str, TwitchUser] = {}
        for item in payload.get("data", []):
            login = item["login"].lower()
            users[login] = TwitchUser(
                login=login,
                profile_image_url=item.get("profile_image_url"),
            )

        return users
