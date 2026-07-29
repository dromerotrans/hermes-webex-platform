# hermes-webex-platform

A [Webex](https://www.webex.com/) messaging adapter for [Hermes Agent](https://github.com/NousResearch/hermes-agent) (Nous Research). Lets a Hermes profile chat over Cisco Webex — direct 1:1 spaces and group spaces (with @mention etiquette) — with no public webhook or inbound exposure required.

## Why polling instead of a webhook

Webex supports pushing messages to your bot via a registered webhook, but that requires a public HTTPS endpoint, a shared webhook secret, and signature verification. This adapter instead polls the Webex REST API (`GET /v1/rooms` + `GET /v1/messages`) on an interval and replies via `POST /v1/messages`. The gateway process only ever makes outbound calls to `webexapis.com` — nothing new to expose. This mirrors how Hermes's bundled Telegram (long-poll) and Mattermost (websocket) adapters already work.

No external SDK is required — only [`httpx`](https://www.python-httpx.org/), which is already a Hermes dependency.

## Features

- Direct (1:1) and group Webex spaces
- Group-space etiquette: only responds when @mentioned in a group, matching native Webex bot behavior; every message counts in a 1:1
- Per-room "last seen" high-water marks persisted to disk, so a gateway restart doesn't replay history or silently skip messages sent while it was down
- Allowlist enforcement via real, Webex-verified sender identity (`personEmail`), not a spoofable client-side field
- Markdown replies (Webex renders it natively)
- Automatic message chunking for Webex's per-message character limit
- Cron / notification delivery support (`deliver=webex`) with a configurable home room, including out-of-process delivery when `hermes cron` runs standalone from the gateway
- Native file/media attachment upload (images, documents, voice, video) — both live and via standalone cron delivery
- Native file/media attachment **download**: files a user sends to the bot are fetched and cached locally so the agent can actually read them, not just see that "a file was sent"
- Every reply threads to the message that triggered it via Webex's native `parentId` reply feature — including every part of a response that gets split across multiple messages — so multi-turn conversations (and busy group spaces) read like a normal threaded chat instead of a flat stream of disconnected messages
- Threading is configurable: a global on/off default, plus per-room overrides and a room participation allowlist via an optional `rooms.yaml` — no code changes or gateway restart needed to adjust
- Rate-limit (429) and auth-failure (401) handling with backoff

## Installation

1. Create a Webex bot at [developer.webex.com](https://developer.webex.com/my-apps) → **Create a New App** → **Bot**. Copy the bot's access token (shown only once).
2. Copy this plugin into your Hermes profile's plugin directory:
   ```
   $HERMES_HOME/plugins/webex/
   ├── __init__.py
   ├── adapter.py
   └── plugin.yaml
   ```
   (`$HERMES_HOME` is your Hermes profile's home directory — for the default profile this is usually `~/.hermes`; for a named profile it's that profile's own home.)
3. Enable the plugin for the profile (`config.yaml`):
   ```yaml
   plugins:
     enabled:
       - webex-platform
   ```
4. Set the required environment variable in that profile's `.env`:
   ```
   WEBEX_BOT_TOKEN=your-bot-token-here
   ```
5. (Recommended) Restrict who can talk to the bot:
   ```
   WEBEX_ALLOWED_EMAILS=you@example.com,teammate@example.com
   ```
   Without this, `WEBEX_ALLOW_ALL_USERS=true` would let anyone who can message the bot use it — fine for testing, not recommended for anything else.
6. (Optional) Copy [`rooms.yaml.example`](rooms.yaml.example) to `$HERMES_HOME/platforms/webex/rooms.yaml` if you want per-room threading overrides or a room participation allowlist — see "Per-room configuration" below. Skip this step entirely if the global defaults are fine.
7. Restart the gateway for the profile. The adapter self-registers via `register(ctx)` — no core Hermes files need editing.
8. Verify it connected: `hermes -p <profile> gateway status` should show `webex` among the running platforms, and the gateway log (`$HERMES_HOME/logs/gateway.log`) should have a line like `[Webex] Connected as you@yourbot.webex.bot — polling every 5s`.

### Files in this repo

| File | Goes where | Required |
|---|---|---|
| `adapter.py`, `__init__.py`, `plugin.yaml` | `$HERMES_HOME/plugins/webex/` | yes |
| `rooms.yaml.example` | copy to `$HERMES_HOME/platforms/webex/rooms.yaml` (rename, drop `.example`) | no — only if you want per-room config |
| `LICENSE`, `README.md` | reference only, not needed at runtime | no |

## Configuration reference

| Env var | Required | Description |
|---|---|---|
| `WEBEX_BOT_TOKEN` | yes | Bot access token from developer.webex.com |
| `WEBEX_ALLOWED_EMAILS` | no | Comma-separated allowlist of Webex person emails |
| `WEBEX_ALLOW_ALL_USERS` | no | `true`/`false` — allow any user who can message the bot (dev only) |
| `WEBEX_POLL_INTERVAL_SECONDS` | no | Seconds between polls (default: `5`) |
| `WEBEX_MARKDOWN` | no | `true`/`false` — send replies as Webex markdown (default: `true`) |
| `WEBEX_THREAD_REPLIES` | no | `true`/`false` — global default for threading replies via `parentId` (default: `true`). Per-room overrides live in `rooms.yaml`, not here — see "Per-room configuration" below |
| `WEBEX_HOME_ROOM_ID` | no | Default Webex `roomId` for cron/notification delivery (`deliver=webex`) |
| `WEBEX_HOME_ROOM_NAME` | no | Human label for the home room |

## How it works

- `check_requirements()` / `validate_config()` / `is_connected()` gate whether the adapter is installable, configured, and connected, matching the contract every Hermes platform plugin implements.
- `_env_enablement()` seeds `PlatformConfig.extra` from env vars before the adapter is constructed, so `hermes gateway status` and `get_connected_platforms()` reflect env-only configuration without needing to stand up an HTTP client.
- `_standalone_send()` provides out-of-process delivery so `deliver=webex` cron jobs work even when `hermes cron` runs in a separate process from the live gateway (no in-process adapter to reuse).
- State (per-room last-seen timestamps) is persisted to `$HERMES_HOME/platforms/webex/state.json`. On the very first run (no state file yet), the adapter snapshots current room activity as a baseline instead of replaying pre-existing history — a freshly wired-up bot won't dump replies to months-old messages.
- Per-room config (`$HERMES_HOME/platforms/webex/rooms.yaml`, optional) is re-read every poll cycle via `_load_rooms_config()` — see "Per-room configuration" below.

## File / media attachments

Unlike some chat APIs, Webex has no separate "upload then reference" step — a local file is attached directly in the same `multipart/form-data POST /v1/messages` request that sends the message, via a single `files` part. This adapter implements that for both delivery paths:

- **Live gateway**: `send_image_file`, `send_document`, `send_voice`, and `send_video` all route through a shared `_send_local_file()` helper that validates the path with Hermes's `validate_media_delivery_path()` security gate (see `gateway/platforms/base.py`), checks the file exists and is under Webex's 100MB attachment cap, then uploads it with the message as its caption.
- **Standalone / cron delivery**: `_standalone_send(media_files=[...])` uploads the first valid file as the initial message's attachment (with `message` as its caption); any additional files are sent as their own follow-up messages.

Webex enforces exactly **one attachment per message** — a second `files` part in the same request gets rejected with HTTP 400 — so multiple attachments always become multiple messages, never a single multi-file message. If a path fails validation, doesn't exist, or exceeds 100MB, the adapter logs a warning and falls back to a plain text message (`⚠️ Couldn't deliver the attachment...`) rather than silently dropping it or leaking the local file path.

**Receiving** a file works the other way around: a Webex message's `files` field is a list of content URLs, not inline bytes — the adapter re-fetches each one with the bot's own Bearer token (Webex requires the same auth to read a file as to read the message referencing it), extracts the real filename from the `Content-Disposition` header, and hands the bytes to Hermes's shared `cache_media_bytes()` helper (the same entry point the bundled Telegram adapter uses), which classifies it as an image/video/audio/document, validates it, and caches it locally where the agent's tools can read it. A message that's *only* a file with no caption text is still dispatched to the agent — earlier versions silently dropped these. Downloads are capped at 20MB (more conservative than the 100MB outbound cap, matching other adapters' default for unsolicited inbound content) and are skipped (not queued/retried) if that's exceeded.

## Conversation threading

Every outbound reply passes `parentId` set to the Webex message that triggered it (the base Hermes gateway already computes this reply anchor generically; this adapter just honors it). Unlike the initial version, **every** chunk of a response that gets split across Webex's per-message character limit threads to the same parent — so a long answer still reads as one connected reply thread under the user's message rather than only its first line being visually linked.

## Per-room configuration

Threading and room participation can both be tuned without touching code or `WEBEX_*` env vars, via an optional `$HERMES_HOME/platforms/webex/rooms.yaml` — copy [`rooms.yaml.example`](rooms.yaml.example) as a starting point. It's re-read every poll cycle (no gateway restart needed):

```yaml
# Optional. If missing, every room the bot is added to is allowed, and
# threading follows WEBEX_THREAD_REPLIES (default: true).
restrict_to_configured_rooms: false   # true = allowlist mode (see below)

rooms:
  <room_id>:
    allowed: true          # set false to make the bot ignore this room entirely
    thread_replies: false  # overrides WEBEX_THREAD_REPLIES just for this room
```

Two independent controls:

- **Threading override** — set `thread_replies` under a specific room to flip threading on/off just there, regardless of the global `WEBEX_THREAD_REPLIES` default. Useful when one space is a fast-moving group chat where threaded replies feel disconnected, but a 1:1 works better threaded (or vice versa).
- **Participation allowlist** — set `restrict_to_configured_rooms: true` and the bot only engages with rooms explicitly listed under `rooms:` (with `allowed: true`, the default once a room is listed at all); every other room — including ones it gets added to later — is silently skipped, with its message queue advanced past (not accumulated) so nothing floods in if it's later allowed. Independent of that switch, any individual room can be blocked with `allowed: false`.

Room ids are the `roomId` field from `GET /v1/rooms`, or whatever value shows up as `chat_id` in Hermes's own logs for that room.

## Known limitations

- No typing indicator — Webex's bot API doesn't expose one.
- Polling has inherent latency (bounded by `WEBEX_POLL_INTERVAL_SECONDS`) versus a push webhook; this is a deliberate tradeoff for zero inbound exposure.

## License

MIT — see [LICENSE](LICENSE).
