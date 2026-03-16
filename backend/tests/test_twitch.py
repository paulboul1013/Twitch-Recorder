from __future__ import annotations

import asyncio
import time

import httpx

from app.twitch import TwitchClient


def _make_token_response(token: str = "token") -> httpx.Response:
    return httpx.Response(200, json={"access_token": token, "expires_in": 3600})


def test_get_live_streams_batches_and_deduplicates_requests() -> None:
    stream_calls: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth2/token"):
            return _make_token_response("token-1")

        if request.url.path.endswith("/helix/streams"):
            batch = request.url.params.get_list("user_login")
            stream_calls.append(batch)
            now_iso = "2026-03-16T12:00:00Z"
            return httpx.Response(
                200,
                headers={
                    "Ratelimit-Remaining": "99",
                    "Ratelimit-Reset": str(int(time.time()) + 60),
                },
                json={
                    "data": [
                        {
                            "user_login": login,
                            "title": f"title-{login}",
                            "game_name": "game",
                            "viewer_count": 1,
                            "started_at": now_iso,
                        }
                        for login in batch
                    ]
                },
            )

        raise AssertionError(f"Unexpected request URL: {request.url}")

    client = TwitchClient(
        "client",
        "secret",
        max_batch_size=2,
        min_request_interval_seconds=0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
    )

    streams = asyncio.run(client.get_live_streams(["Alpha", "beta", "alpha", "Gamma"]))

    assert stream_calls == [["alpha", "beta"], ["gamma"]]
    assert sorted(streams) == ["alpha", "beta", "gamma"]


def test_get_users_clamps_batch_size_to_twitch_limit() -> None:
    user_calls: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth2/token"):
            return _make_token_response("token-1")

        if request.url.path.endswith("/helix/users"):
            batch = request.url.params.get_list("login")
            user_calls.append(batch)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "login": login,
                            "profile_image_url": f"https://example.com/{login}.jpg",
                        }
                        for login in batch
                    ]
                },
            )

        raise AssertionError(f"Unexpected request URL: {request.url}")

    # Intentionally higher than Twitch's hard per-request limit (100).
    client = TwitchClient(
        "client",
        "secret",
        max_batch_size=500,
        min_request_interval_seconds=0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
    )

    usernames = [f"user_{index}" for index in range(105)]
    users = asyncio.run(client.get_users(usernames))

    assert len(user_calls) == 2
    assert len(user_calls[0]) == 100
    assert len(user_calls[1]) == 5
    assert len(users) == 105


def test_retries_on_429_using_retry_after() -> None:
    sleep_calls: list[float] = []
    stream_attempts = 0

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stream_attempts

        if request.url.path.endswith("/oauth2/token"):
            return _make_token_response("token-1")

        if request.url.path.endswith("/helix/streams"):
            stream_attempts += 1
            if stream_attempts == 1:
                return httpx.Response(
                    429,
                    headers={"Retry-After": "0.05", "Ratelimit-Remaining": "0"},
                    json={"error": "Too Many Requests"},
                )
            return httpx.Response(200, json={"data": []})

        raise AssertionError(f"Unexpected request URL: {request.url}")

    client = TwitchClient(
        "client",
        "secret",
        min_request_interval_seconds=0,
        max_retries=2,
        base_backoff_seconds=0,
        max_backoff_seconds=0,
        retry_jitter_ratio=0,
        sleep_func=fake_sleep,
        transport=httpx.MockTransport(handler),
    )

    asyncio.run(client.get_live_streams(["alpha"]))

    assert stream_attempts == 2
    assert sleep_calls
    assert any(delay >= 0.05 for delay in sleep_calls)


def test_retries_on_401_by_refreshing_token() -> None:
    token_calls = 0
    stream_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls, stream_calls

        if request.url.path.endswith("/oauth2/token"):
            token_calls += 1
            return _make_token_response(f"token-{token_calls}")

        if request.url.path.endswith("/helix/streams"):
            stream_calls += 1
            auth = request.headers.get("Authorization", "")
            if auth == "Bearer token-1":
                return httpx.Response(401, json={"error": "Unauthorized"})
            if auth == "Bearer token-2":
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            {
                                "user_login": "alpha",
                                "title": "live",
                                "game_name": "game",
                                "viewer_count": 1,
                                "started_at": "2026-03-16T12:00:00Z",
                            }
                        ]
                    },
                )
            raise AssertionError(f"Unexpected auth header: {auth}")

        raise AssertionError(f"Unexpected request URL: {request.url}")

    client = TwitchClient(
        "client",
        "secret",
        min_request_interval_seconds=0,
        max_retries=2,
        base_backoff_seconds=0,
        max_backoff_seconds=0,
        retry_jitter_ratio=0,
        transport=httpx.MockTransport(handler),
    )

    streams = asyncio.run(client.get_live_streams(["alpha"]))

    assert token_calls == 2
    assert stream_calls == 2
    assert "alpha" in streams


def test_retries_network_error_with_backoff() -> None:
    sleep_calls: list[float] = []
    stream_attempts = 0

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stream_attempts

        if request.url.path.endswith("/oauth2/token"):
            return _make_token_response("token-1")

        if request.url.path.endswith("/helix/streams"):
            stream_attempts += 1
            if stream_attempts == 1:
                raise httpx.ConnectError("connection failed", request=request)
            return httpx.Response(200, json={"data": []})

        raise AssertionError(f"Unexpected request URL: {request.url}")

    client = TwitchClient(
        "client",
        "secret",
        min_request_interval_seconds=0,
        max_retries=1,
        base_backoff_seconds=0.05,
        max_backoff_seconds=0.05,
        retry_jitter_ratio=0,
        sleep_func=fake_sleep,
        transport=httpx.MockTransport(handler),
    )

    asyncio.run(client.get_live_streams(["alpha"]))

    assert stream_attempts == 2
    assert sleep_calls == [0.05]
