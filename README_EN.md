# Twitch Recorder

![Twitch Recorder Demo](./show_demo.png)

Twitch Recorder helps you monitor Twitch channels and automatically start recording as soon as a streamer goes live, so you do not have to stay in front of your screen waiting for the stream to begin.

This project is useful if you want to:

- Automatically save Twitch streams from specific channels
- Follow multiple streamers at the same time
- Manage your watch list from a browser
- Avoid checking stream status manually
- Keep streams that may be deleted later or locked behind subscriber-only VOD access

## What It Does

- Add Twitch streamers you want to monitor
- Check whether they are live automatically
- Start recording as soon as a stream begins
- Stop recording automatically when the stream ends
- Show who is live and who is currently being recorded
- List recorded video files
- Ad-break mitigation pipeline: optional authenticated capture (best-effort) + ad detection + watchable post-processing

## What You Need Before Starting

- A computer with Docker installed
- A Twitch application key pair
  - `TWITCH_CLIENT_ID`
  - `TWITCH_CLIENT_SECRET`

You can think of these as the credentials that allow this app to request public stream information from Twitch. Without them, the app cannot check live status or fetch channel details.

If you do not have them yet, you can get them like this:

1. Sign in to your Twitch account.
2. Open the Twitch Developer Console.
3. Create a new application.
4. After the application is created, copy the `Client ID`.
5. Generate a `Client Secret`.
6. Paste both values into your `.env` file.

![Twitch API Setup](./twitch_api.png)

If Twitch asks for an `OAuth Redirect URL`, you can use a local address such as `http://localhost`. This project only needs app credentials to query stream information, so you do not need a complex login flow.

## Quick Start

1. Create a `.env` file in the project root.

Put the following values in it:

```env
TWITCH_CLIENT_ID=your_client_id
TWITCH_CLIENT_SECRET=your_client_secret
TWITCH_USER_OAUTH_TOKEN=
TWITCH_USER_LOGIN=
MAX_CONCURRENT_STREAMERS=3
POLL_INTERVAL_SECONDS=30
OFFLINE_GRACE_PERIOD_SECONDS=20
RECORDING_START_DELAY_SECONDS=25
WATCHABLE_TRIM_START_SECONDS=0
RECORDINGS_PATH=/recordings
CONFIG_PATH=/config
ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

Optional values (safe to leave empty):

- `TWITCH_USER_OAUTH_TOKEN`: user OAuth token for authenticated recording mode to best-effort reduce ads and "prepare your stream" delays
- `TWITCH_USER_LOGIN`: optional Twitch login name tied to the token; if omitted, the app still runs in best-effort mode
- `RECORDING_START_DELAY_SECONDS`: delay recording start after stream goes live (default: 25s) to avoid initial `Preparing your stream` segments; this is the primary mitigation
- `WATCHABLE_TRIM_START_SECONDS`: fixed number of seconds to trim from the beginning of watchable output (default: 0); use this as a fallback only if the primary start-delay mitigation is still insufficient (common range: `10~20`)

2. Start the project:

```bash
docker compose up -d --build
```

3. Open your browser:

- Dashboard: `http://localhost:3000`

## How To Use It

1. Open the dashboard.
2. Enter the Twitch channel name you want to monitor.
3. Click the add button.
4. The app will keep checking whether the streamer is live.
5. When the stream starts, recording starts automatically.
6. Recorded files are saved in the `recordings/` folder.

## What You Will See On The Dashboard

- Whether the streamer is currently live
- Stream title
- Game or category
- Viewer count
- Whether recording is in progress
- Recording start time
- Output file path

## Where Recordings Are Saved

All recorded files are stored in the `recordings/` folder inside this project.

## Ad-Break Mitigation (Hybrid Mode)

- Without user auth token: recording runs in normal public mode
- With `TWITCH_USER_OAUTH_TOKEN`: recorder first attempts authenticated capture (best-effort), then automatically falls back if needed
- Recorder events are treated as high-confidence ad signals; `timed_id3` is only a candidate and must be confirmed by localized OCR before being used as an ad cut
- Watchable output now prefers fast paths (remux / trim-copy) when no high-confidence ad windows are present; segment transcode is used only when ad windows are confirmed
- The recordings list (API and dashboard) includes watchable status and ad break count

## Common Commands

Start:

```bash
docker compose up -d --build
```

Check logs:

```bash
docker compose logs -f
```

Stop:

```bash
docker compose down
```
