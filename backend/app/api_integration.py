from __future__ import annotations

from pathlib import Path


def build_streamlink_command(
    *,
    channel: str,
    output_path: Path | None,
    preferred_qualities: tuple[str, ...],
    twitch_user_oauth_token: str,
    raw_container: str = "ts",
    output_to_stdout: bool = False,
) -> tuple[list[str], str]:
    quality = ",".join(preferred_qualities)
    cmd = ["streamlink", f"https://twitch.tv/{channel}", quality]
    if output_to_stdout:
        cmd.append("-O")
    else:
        if output_path is None:
            raise ValueError("output_path is required when output_to_stdout is False")
        cmd.extend(["-o", str(output_path)])

    source_mode = "unauthenticated"
    if twitch_user_oauth_token:
        cmd.extend(
            [
                "--twitch-api-header",
                f"Authorization=OAuth {twitch_user_oauth_token}",
            ]
        )
        source_mode = "authenticated"

    normalized_container = raw_container.strip().lower().lstrip(".")
    if normalized_container == "ts":
        cmd.extend(["--ffmpeg-fout", "mpegts"])

    return cmd, source_mode


def build_streamlink_stream_url_command(
    *,
    channel: str,
    preferred_qualities: tuple[str, ...],
    twitch_user_oauth_token: str,
) -> tuple[list[str], str]:
    quality = ",".join(preferred_qualities)
    cmd = [
        "streamlink",
        "--stream-url",
        f"https://twitch.tv/{channel}",
        quality,
    ]
    source_mode = "unauthenticated"
    if twitch_user_oauth_token:
        cmd.extend(
            [
                "--twitch-api-header",
                f"Authorization=OAuth {twitch_user_oauth_token}",
            ]
        )
        source_mode = "authenticated"
    return cmd, source_mode
