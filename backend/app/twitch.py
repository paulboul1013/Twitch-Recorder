from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

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
    MAX_HELIX_BATCH_SIZE = 100

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        max_batch_size: int = 100,
        min_request_interval_seconds: float = 0.2,
        max_retries: int = 3,
        base_backoff_seconds: float = 0.5,
        max_backoff_seconds: float = 8.0,
        retry_jitter_ratio: float = 0.2,
        request_timeout_seconds: float = 15.0,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_func: Callable[[], float] = random.random,
        now_func: Callable[[], datetime] | None = None,
        monotonic_func: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_batch_size = max(1, min(self.MAX_HELIX_BATCH_SIZE, int(max_batch_size)))
        self.min_request_interval_seconds = max(0.0, float(min_request_interval_seconds))
        self.max_retries = max(0, int(max_retries))
        self.base_backoff_seconds = max(0.0, float(base_backoff_seconds))
        self.max_backoff_seconds = max(self.base_backoff_seconds, float(max_backoff_seconds))
        self.retry_jitter_ratio = max(0.0, float(retry_jitter_ratio))
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self._transport = transport
        self._sleep_func = sleep_func
        self._random_func = random_func
        self._now_func = now_func or (lambda: datetime.now(UTC))
        self._monotonic_func = monotonic_func

        self._token: str | None = None
        self._token_expires_at: datetime | None = None
        self._token_lock = asyncio.Lock()

        self._request_lock = asyncio.Lock()
        self._next_request_not_before: float = 0.0
        self._rate_limited_until_epoch: float = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def _build_client(self) -> httpx.AsyncClient:
        kwargs: dict[str, Any] = {"timeout": self.request_timeout_seconds}
        if self._transport is not None:
            kwargs["transport"] = self._transport
        return httpx.AsyncClient(**kwargs)

    def _normalize_usernames(self, usernames: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in usernames:
            name = raw.strip().lower()
            if not name or name in seen:
                continue
            seen.add(name)
            normalized.append(name)
        return normalized

    def _chunk_values(self, values: list[str]) -> list[list[str]]:
        return [values[index : index + self.max_batch_size] for index in range(0, len(values), self.max_batch_size)]

    def _invalidate_token(self) -> None:
        self._token = None
        self._token_expires_at = None

    async def _get_app_token(self) -> str:
        if not self.enabled:
            raise TwitchAuthError("TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET are required")

        async with self._token_lock:
            now = self._now_func()
            if self._token and self._token_expires_at and now < self._token_expires_at:
                return self._token

            async with self._build_client() as client:
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

    def _parse_retry_after_seconds(self, response: httpx.Response) -> float | None:
        retry_after = response.headers.get("Retry-After")
        if retry_after is None:
            return None
        try:
            seconds = float(retry_after)
        except ValueError:
            return None
        return max(0.0, seconds)

    def _parse_rate_limit_reset_seconds(self, response: httpx.Response) -> float | None:
        reset_at_raw = response.headers.get("Ratelimit-Reset")
        if reset_at_raw is None:
            return None
        try:
            reset_at = float(reset_at_raw)
        except ValueError:
            return None
        return max(0.0, reset_at - self._now_func().timestamp())

    def _apply_rate_limit_state(self, response: httpx.Response) -> None:
        remaining_raw = response.headers.get("Ratelimit-Remaining")
        reset_delay_seconds = self._parse_rate_limit_reset_seconds(response)
        if reset_delay_seconds is None:
            return

        remaining = None
        if remaining_raw is not None:
            try:
                remaining = int(remaining_raw)
            except ValueError:
                remaining = None

        if response.status_code == 429 or remaining == 0:
            self._rate_limited_until_epoch = max(
                self._rate_limited_until_epoch,
                self._now_func().timestamp() + reset_delay_seconds,
            )

    def _compute_backoff_seconds(self, attempt: int) -> float:
        backoff = min(self.max_backoff_seconds, self.base_backoff_seconds * (2**attempt))
        if backoff <= 0:
            return 0.0
        jitter = backoff * self.retry_jitter_ratio * self._random_func()
        return min(self.max_backoff_seconds, backoff + jitter)

    def _compute_retry_delay_seconds(self, response: httpx.Response, attempt: int) -> float:
        retry_after = self._parse_retry_after_seconds(response)
        if retry_after is not None:
            return retry_after

        reset_delay = self._parse_rate_limit_reset_seconds(response)
        if reset_delay is not None:
            return reset_delay

        return self._compute_backoff_seconds(attempt)

    async def _wait_for_request_window(self) -> None:
        async with self._request_lock:
            now_monotonic = self._monotonic_func()
            wait_seconds = 0.0

            if self._rate_limited_until_epoch > 0:
                now_epoch = self._now_func().timestamp()
                if now_epoch < self._rate_limited_until_epoch:
                    wait_seconds = max(wait_seconds, self._rate_limited_until_epoch - now_epoch)
                else:
                    self._rate_limited_until_epoch = 0.0

            if now_monotonic < self._next_request_not_before:
                wait_seconds = max(wait_seconds, self._next_request_not_before - now_monotonic)

            if wait_seconds > 0:
                await self._sleep_func(wait_seconds)
                now_monotonic = self._monotonic_func()

            self._next_request_not_before = now_monotonic + self.min_request_interval_seconds

    async def _request_helix_chunk(
        self,
        *,
        client: httpx.AsyncClient,
        url: str,
        param_name: str,
        values: list[str],
    ) -> dict[str, Any]:
        params = [(param_name, value) for value in values]

        for attempt in range(self.max_retries + 1):
            await self._wait_for_request_window()
            token = await self._get_app_token()

            try:
                response = await client.get(
                    url,
                    params=params,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Client-Id": self.client_id,
                    },
                )
            except httpx.RequestError:
                if attempt >= self.max_retries:
                    raise
                backoff_seconds = self._compute_backoff_seconds(attempt)
                if backoff_seconds > 0:
                    await self._sleep_func(backoff_seconds)
                continue

            self._apply_rate_limit_state(response)

            if response.status_code == 401 and attempt < self.max_retries:
                self._invalidate_token()
                backoff_seconds = self._compute_backoff_seconds(attempt)
                if backoff_seconds > 0:
                    await self._sleep_func(backoff_seconds)
                continue

            if response.status_code == 429 and attempt < self.max_retries:
                await self._sleep_func(self._compute_retry_delay_seconds(response, attempt))
                continue

            if 500 <= response.status_code < 600 and attempt < self.max_retries:
                backoff_seconds = self._compute_backoff_seconds(attempt)
                if backoff_seconds > 0:
                    await self._sleep_func(backoff_seconds)
                continue

            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return {}

        raise RuntimeError("unexpected Twitch retry exhaustion")

    async def _fetch_helix_data(
        self,
        *,
        url: str,
        param_name: str,
        values: list[str],
    ) -> list[dict[str, Any]]:
        if not values:
            return []

        data_rows: list[dict[str, Any]] = []
        chunks = self._chunk_values(values)
        async with self._build_client() as client:
            for chunk in chunks:
                payload = await self._request_helix_chunk(
                    client=client,
                    url=url,
                    param_name=param_name,
                    values=chunk,
                )
                chunk_rows = payload.get("data", [])
                if not isinstance(chunk_rows, list):
                    continue
                for row in chunk_rows:
                    if isinstance(row, dict):
                        data_rows.append(row)
        return data_rows

    async def get_live_streams(self, usernames: list[str]) -> dict[str, LiveStream]:
        normalized = self._normalize_usernames(usernames)
        if not normalized:
            return {}

        rows = await self._fetch_helix_data(
            url=self.streams_url,
            param_name="user_login",
            values=normalized,
        )

        streams: dict[str, LiveStream] = {}
        for item in rows:
            started_at = None
            if item.get("started_at"):
                try:
                    started_at = datetime.fromisoformat(str(item["started_at"]).replace("Z", "+00:00"))
                except ValueError:
                    started_at = None
            username = str(item.get("user_login", "")).lower()
            if not username:
                continue
            streams[username] = LiveStream(
                user_login=username,
                title=item.get("title"),
                game_name=item.get("game_name"),
                viewer_count=item.get("viewer_count"),
                started_at=started_at,
            )

        return streams

    async def get_users(self, usernames: list[str]) -> dict[str, TwitchUser]:
        normalized = self._normalize_usernames(usernames)
        if not normalized:
            return {}

        rows = await self._fetch_helix_data(
            url=self.users_url,
            param_name="login",
            values=normalized,
        )

        users: dict[str, TwitchUser] = {}
        for item in rows:
            login = str(item.get("login", "")).lower()
            if not login:
                continue
            users[login] = TwitchUser(
                login=login,
                profile_image_url=item.get("profile_image_url"),
            )

        return users
