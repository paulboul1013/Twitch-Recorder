from __future__ import annotations

from pathlib import Path


def build_streamlink_command(
    *,
    channel: str,
    output_path: Path,
    preferred_qualities: tuple[str, ...],
    twitch_user_oauth_token: str,
    raw_container: str = "ts",
) -> tuple[list[str], str]:
    quality = ",".join(preferred_qualities)
    cmd = [
        "streamlink",
        f"https://twitch.tv/{channel}",
        quality,
        "-o",
        str(output_path),
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

    normalized_container = raw_container.strip().lower().lstrip(".")
    if normalized_container == "ts":
        cmd.extend(["--ffmpeg-fout", "mpegts"])

    return cmd, source_mode
