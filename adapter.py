"""
Webex Messaging platform adapter (Hermes plugin).

Polls the Webex REST API (``GET /v1/rooms`` + ``GET /v1/messages``) for new
messages in every 1:1 or group space the bot belongs to, and publishes
replies via ``POST /v1/messages``. No external SDK — only httpx, which is
already a Hermes dependency.

Deliberately outbound-only (no inbound webhook, no public Route): Webex
*can* push messages via a registered webhook, but that requires a public
HTTPS endpoint, a shared webhook secret, and signature verification — extra
attack surface this deployment's other messaging adapters avoid (Telegram
long-polls, Mattermost holds a websocket). Polling keeps Webex in the same
shape: the pod only ever makes outbound calls to webexapis.com.

This adapter ships as a Hermes *user* plugin under
``$HERMES_HOME/plugins/webex/`` (not bundled with the image — Webex isn't
one of the platforms Hermes ships out of the box). The Hermes plugin loader
scans ``$HERMES_HOME/plugins/`` at startup, calls :func:`register`, and the
platform becomes available to ``gateway/run.py`` and ``tools/send_message_tool``
through the registry — no edits to core files required.

Configuration (env vars, read at adapter construct time; env wins over
config.yaml ``extra``):

    WEBEX_BOT_TOKEN             Bot access token from developer.webex.com (required)
    WEBEX_ALLOWED_EMAILS        Allowlist of Webex person emails (comma-separated)
    WEBEX_ALLOW_ALL_USERS       Allow any user who can message the bot — dev only
    WEBEX_POLL_INTERVAL_SECONDS Seconds between polls (default: 5)
    WEBEX_MARKDOWN              "true"/"1"/"yes" sends replies as markdown (default: true)
    WEBEX_HOME_ROOM_ID          Default roomId for cron / notification delivery
    WEBEX_HOME_ROOM_NAME        Human label for the home room

Identity model: Webex has a real authenticated user identity per message
(``personEmail`` / ``personId``, verified server-side by Webex — unlike
ntfy's publisher-controlled ``title``). ``WEBEX_ALLOWED_EMAILS`` is a real
trust boundary, enforced generically by the gateway via the
``allowed_users_env`` hook (see :func:`register`).

Group-space etiquette: in a Webex space with other humans, the bot only
acts on messages where it is @mentioned (``mentionedPeople`` includes the
bot's own personId) — matching native Webex bot UX. In a direct (1:1)
room every message from an allowed sender is processed.

State: per-room "last seen" high-water marks are persisted to
``$HERMES_HOME/platforms/webex/state.json`` so a gateway restart doesn't
either replay old history or (worse) silently skip messages sent while the
pod was down. On first-ever run (no state file) the adapter snapshots
current room activity as a baseline instead of processing history — a
fresh bot shouldn't reply to months of old messages the moment it's wired up.
"""

import asyncio
import json
import logging
import mimetypes
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    validate_media_delivery_path,
)

# Webex message attachments (developer.webex.com/messaging/docs/basics):
#   - uploaded via a multipart/form-data POST to /v1/messages instead of
#     the JSON body used for plain text (JSON only accepts public URLs)
#   - exactly one ``files`` part per message — the API returns 400 for more
#   - capped at 100MB per attachment
MAX_ATTACHMENT_BYTES = 100 * 1024 * 1024

# Inbound attachments a *user* sends to the bot: each entry in a message's
# ``files`` array is a URL that must be re-fetched with the bot's own Bearer
# token (Webex requires the same auth to read a file as to read the message
# that references it). Capped more conservatively than the 100MB outbound
# limit above — matches the other bundled adapters' conservative default for
# unsolicited inbound content (e.g. Telegram's 20MB default) rather than
# Webex's own outbound ceiling.
MAX_INBOUND_ATTACHMENT_BYTES = 20 * 1024 * 1024

logger = logging.getLogger(__name__)


class _FatalPollError(Exception):
    """Raised when a poll error is unrecoverable (e.g. 401)."""


API_BASE = "https://webexapis.com/v1"
MAX_MESSAGE_LENGTH = 7439  # Webex's documented per-message character limit
DEFAULT_POLL_INTERVAL = 5.0
ROOMS_PAGE_SIZE = 100
MESSAGES_PAGE_SIZE = 25
DEDUP_MAX_SIZE = 2000
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]


def _bool_env(raw: str) -> bool:
    return raw.strip().lower() in ("1", "true", "yes")


def _state_path() -> Path:
    home = os.environ.get("HERMES_HOME", "").strip() or os.path.expanduser("~/.hermes")
    return Path(home) / "platforms" / "webex" / "state.json"


def _load_state() -> Dict[str, str]:
    path = _state_path()
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save_state(state: Dict[str, str]) -> None:
    path = _state_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state))
    except Exception as e:
        logger.debug("webex: failed to persist state: %s", e)


def check_requirements() -> bool:
    """Check whether the Webex adapter is installable and minimally configured."""
    if not HTTPX_AVAILABLE:
        return False
    return bool(os.getenv("WEBEX_BOT_TOKEN", "").strip())


def validate_config(config) -> bool:
    """Validate that the configured Webex platform has a bot token set."""
    extra = getattr(config, "extra", {}) or {}
    token = extra.get("bot_token") or os.getenv("WEBEX_BOT_TOKEN", "")
    return bool(token)


def is_connected(config) -> bool:
    """Check whether Webex is configured (env or config.yaml)."""
    extra = getattr(config, "extra", {}) or {}
    token = os.getenv("WEBEX_BOT_TOKEN") or extra.get("bot_token", "")
    return bool(token)


class WebexAdapter(BasePlatformAdapter):
    """Webex Messaging adapter.

    Polls ``GET /v1/rooms`` (sorted by last activity) to cheaply detect
    which rooms changed, then fetches new messages only for those rooms via
    ``GET /v1/messages``. Replies go out via ``POST /v1/messages``.
    """

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    supports_code_blocks = True
    splits_long_messages = True

    def __init__(self, config: PlatformConfig):
        platform = Platform("webex")
        super().__init__(config=config, platform=platform)
        extra = config.extra or {}

        self._token: str = extra.get("bot_token") or os.getenv("WEBEX_BOT_TOKEN", "")
        self._poll_interval: float = DEFAULT_POLL_INTERVAL
        try:
            raw_interval = extra.get("poll_interval") or os.getenv(
                "WEBEX_POLL_INTERVAL_SECONDS", ""
            )
            if raw_interval:
                self._poll_interval = max(1.0, float(raw_interval))
        except (TypeError, ValueError):
            pass

        markdown_raw = str(
            extra.get("markdown")
            if extra.get("markdown") is not None
            else os.getenv("WEBEX_MARKDOWN", "true")
        )
        self._markdown_enabled = _bool_env(markdown_raw)

        self._http_client: Optional["httpx.AsyncClient"] = None
        self._poll_task: Optional[asyncio.Task] = None

        self._bot_person_id: Optional[str] = None
        self._bot_email: Optional[str] = None

        # room_id -> last-seen ISO "created" timestamp we've processed.
        self._room_last_seen: Dict[str, str] = {}
        # message_id -> processed-at monotonic time, for dedup within a run.
        self._seen_messages: Dict[str, float] = {}

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Webex: verify the bot token and start the poll loop."""
        if not HTTPX_AVAILABLE:
            logger.warning("[%s] httpx not installed. Run: pip install httpx", self.name)
            return False
        if not self._token:
            logger.warning("[%s] WEBEX_BOT_TOKEN not configured", self.name)
            return False

        self._http_client = httpx.AsyncClient(
            base_url=API_BASE, headers=self._auth_headers(), timeout=20.0
        )

        try:
            resp = await self._http_client.get("/people/me")
            if resp.status_code == 401:
                self._set_fatal_error(
                    "webex_unauthorized",
                    "Webex rejected the bot token (401). Check WEBEX_BOT_TOKEN.",
                    retryable=False,
                )
                await self._http_client.aclose()
                self._http_client = None
                return False
            resp.raise_for_status()
            me = resp.json()
            self._bot_person_id = me.get("id")
            emails = me.get("emails") or []
            self._bot_email = emails[0] if emails else None
        except Exception as e:
            logger.error("[%s] Failed to verify bot identity: %s", self.name, e)
            if self._http_client:
                await self._http_client.aclose()
                self._http_client = None
            return False

        # Restore persisted per-room high-water marks (survives restarts).
        self._room_last_seen = _load_state()
        if not self._room_last_seen and not is_reconnect:
            # First-ever run: snapshot current activity as a baseline
            # instead of replaying pre-existing room history.
            await self._snapshot_baseline()

        self._poll_task = asyncio.create_task(self._run_poll_loop())
        self._mark_connected()
        logger.info(
            "[%s] Connected as %s — polling every %.0fs",
            self.name, self._bot_email or self._bot_person_id, self._poll_interval,
        )
        return True

    async def _snapshot_baseline(self) -> None:
        """Record current room activity without processing it as new messages."""
        try:
            rooms = await self._list_rooms()
        except Exception as e:
            logger.warning("[%s] Baseline snapshot failed (will retry on next poll): %s", self.name, e)
            return
        for room in rooms:
            room_id = room.get("id")
            last_activity = room.get("lastActivity")
            if room_id and last_activity:
                self._room_last_seen[room_id] = last_activity
        _save_state(self._room_last_seen)

    async def disconnect(self) -> None:
        """Disconnect from Webex."""
        self._running = False
        self._mark_disconnected()
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        _save_state(self._room_last_seen)
        logger.info("[%s] Disconnected", self.name)

    # -- Poll loop ------------------------------------------------------------

    async def _run_poll_loop(self) -> None:
        backoff_idx = 0
        cycle_start = 0.0
        while self._running:
            try:
                cycle_start = time.monotonic()
                await self._poll_once()
                backoff_idx = 0
            except asyncio.CancelledError:
                return
            except _FatalPollError:
                self._running = False
                return
            except Exception as e:
                if not self._running:
                    return
                logger.warning("[%s] Poll error: %s", self.name, e)
                delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
                backoff_idx += 1
                await asyncio.sleep(delay)
                continue

            elapsed = time.monotonic() - cycle_start
            await asyncio.sleep(max(0.0, self._poll_interval - elapsed))

    async def _list_rooms(self) -> List[Dict[str, Any]]:
        resp = await self._http_client.get(
            "/rooms", params={"max": ROOMS_PAGE_SIZE, "sortBy": "lastactivity"}
        )
        if resp.status_code == 401:
            self._set_fatal_error(
                "webex_unauthorized", "Webex rejected the bot token (401).", retryable=False,
            )
            raise _FatalPollError("401 Unauthorized")
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "5"))
            await asyncio.sleep(retry_after)
            return []
        resp.raise_for_status()
        return resp.json().get("items", [])

    async def _poll_once(self) -> None:
        rooms = await self._list_rooms()
        for room in rooms:
            room_id = room.get("id")
            last_activity = room.get("lastActivity")
            if not room_id or not last_activity:
                continue
            known = self._room_last_seen.get(room_id)
            if known is not None and last_activity <= known:
                continue  # no new activity in this room since last poll
            await self._poll_room_messages(room, since=known)

        self._prune_seen_messages()
        _save_state(self._room_last_seen)

    async def _poll_room_messages(self, room: Dict[str, Any], since: Optional[str]) -> None:
        room_id = room["id"]
        room_type = room.get("type", "direct")  # "direct" (1:1) or "group"

        resp = await self._http_client.get(
            "/messages", params={"roomId": room_id, "max": MESSAGES_PAGE_SIZE}
        )
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "5"))
            await asyncio.sleep(retry_after)
            return
        if resp.status_code == 403:
            # Bot lost access to the room (removed from space) — nothing to do.
            logger.debug("[%s] No access to room %s, skipping", self.name, room_id)
            return
        resp.raise_for_status()
        items = resp.json().get("items", [])

        # Webex returns newest-first; process oldest-first so replies arrive
        # in the same order the messages were sent.
        newest_created = since
        for message in reversed(items):
            created = message.get("created")
            msg_id = message.get("id")
            if not created or not msg_id:
                continue
            if since is not None and created <= since:
                continue
            if msg_id in self._seen_messages:
                continue

            person_id = message.get("personId")
            if person_id and person_id == self._bot_person_id:
                # Never react to our own messages (echo-loop prevention).
                if not newest_created or created > newest_created:
                    newest_created = created
                continue

            if room_type == "group":
                mentioned = message.get("mentionedPeople") or []
                if self._bot_person_id not in mentioned:
                    # Group-space etiquette: only act when @mentioned,
                    # matching native Webex bot behavior.
                    if not newest_created or created > newest_created:
                        newest_created = created
                    continue

            await self._dispatch_message(message, room)
            self._seen_messages[msg_id] = time.time()
            if not newest_created or created > newest_created:
                newest_created = created

        if newest_created:
            self._room_last_seen[room_id] = newest_created

    async def _dispatch_message(self, message: Dict[str, Any], room: Dict[str, Any]) -> None:
        text = (message.get("text") or "").strip()
        file_urls = message.get("files") or []
        if not text and not file_urls:
            return

        room_id = room["id"]
        room_type = room.get("type", "direct")
        person_email = message.get("personEmail") or message.get("personId") or "unknown"

        source = self.build_source(
            chat_id=room_id,
            chat_name=room.get("title") or person_email,
            chat_type="dm" if room_type == "direct" else "group",
            user_id=person_email,
            user_name=person_email,
        )

        message_event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message.get("id"),
            raw_message=message,
        )

        if file_urls:
            await self._cache_incoming_files(file_urls, message_event)

        logger.debug(
            "[%s] Message in room %s from %s: %s", self.name, room_id, person_email,
            (message_event.text or "")[:80],
        )
        await self.handle_message(message_event)

    # -- Inbound attachments ------------------------------------------------
    #
    # A Webex message's ``files`` field is a list of content URLs — the
    # bytes aren't inline, they have to be re-fetched with the bot's own
    # Bearer token (same auth used for every other Webex API call; Webex
    # requires it to read a file, not just to read the message that
    # references it). Downloaded bytes are handed to Hermes's shared
    # ``cache_media_bytes()`` (the same entry point Telegram/Discord use for
    # inbound media) so the result lands in the standard local cache dir with
    # the standard image/video/audio/document classification, sandbox-visible
    # path translation, and image validation — nothing Webex-specific about
    # storage or path safety needs reinventing here.

    async def _cache_incoming_files(self, file_urls: List[str], event: MessageEvent) -> None:
        from gateway.platforms.base import cache_media_bytes

        cached_notes: List[str] = []
        for file_url in file_urls:
            try:
                cached = await self._download_webex_file(file_url)
            except Exception as exc:
                logger.warning(
                    "[%s] Failed to download incoming attachment: %s", self.name, exc,
                    exc_info=True,
                )
                continue
            if cached is None:
                continue

            data, filename, mime_type = cached
            try:
                result = cache_media_bytes(data, filename=filename, mime_type=mime_type)
            except Exception as exc:
                logger.warning("[%s] Failed to cache incoming attachment: %s", self.name, exc)
                continue
            if result is None:
                continue

            event.media_urls.append(result.path)
            event.media_types.append(result.media_type)
            if len(event.media_urls) == 1:
                if result.kind == "image":
                    event.message_type = MessageType.PHOTO
                elif result.kind == "video":
                    event.message_type = MessageType.VIDEO
                elif result.kind == "audio":
                    event.message_type = MessageType.VOICE
                else:
                    event.message_type = MessageType.DOCUMENT
            cached_notes.append(result.context_note())
            logger.info("[%s] Cached incoming %s at %s", self.name, result.kind, result.path)

        for note in cached_notes:
            event.text = f"{event.text}\n\n{note}" if event.text else note

    async def _download_webex_file(self, file_url: str) -> Optional[tuple]:
        """Download one Webex attachment URL. Returns (bytes, filename, mime) or None."""
        if not self._http_client:
            return None

        async with self._http_client.stream("GET", file_url, follow_redirects=True) as resp:
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "5"))
                await asyncio.sleep(retry_after)
                return None
            if resp.status_code >= 300:
                logger.warning(
                    "[%s] Attachment download failed HTTP %d for %s",
                    self.name, resp.status_code, file_url,
                )
                return None

            content_length = resp.headers.get("content-length")
            if content_length:
                try:
                    if int(content_length) > MAX_INBOUND_ATTACHMENT_BYTES:
                        logger.warning(
                            "[%s] Incoming attachment exceeds %d bytes, skipping: %s",
                            self.name, MAX_INBOUND_ATTACHMENT_BYTES, file_url,
                        )
                        return None
                except ValueError:
                    pass

            data = bytearray()
            async for chunk in resp.aiter_bytes():
                data.extend(chunk)
                if len(data) > MAX_INBOUND_ATTACHMENT_BYTES:
                    logger.warning(
                        "[%s] Incoming attachment exceeded %d bytes mid-download, discarding: %s",
                        self.name, MAX_INBOUND_ATTACHMENT_BYTES, file_url,
                    )
                    return None

            content_type = resp.headers.get("content-type", "").split(";")[0].strip()
            filename = self._filename_from_content_disposition(
                resp.headers.get("content-disposition", "")
            )
            if not filename:
                guessed_ext = mimetypes.guess_extension(content_type) or ""
                filename = f"webex-attachment{guessed_ext}"

        return bytes(data), filename, content_type

    @staticmethod
    def _filename_from_content_disposition(header: str) -> str:
        if not header:
            return ""
        match = re.search(r'filename\*=(?:UTF-8\'\')?"?([^";]+)"?', header, re.IGNORECASE)
        if not match:
            match = re.search(r'filename="?([^";]+)"?', header, re.IGNORECASE)
        if not match:
            return ""
        name = match.group(1).strip()
        # Defense in depth: cache_media_bytes/cache_document_from_bytes already
        # reject path traversal, but strip any directory component up front so
        # a hostile Content-Disposition can't even reach that check with a
        # crafted display name.
        return os.path.basename(name)

    def _prune_seen_messages(self) -> None:
        if len(self._seen_messages) <= DEDUP_MAX_SIZE:
            return
        cutoff = time.time() - 3600
        self._seen_messages = {k: v for k, v in self._seen_messages.items() if v > cutoff}

    # -- Outbound messaging ---------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send one or more messages to a Webex room, chunked to the API limit."""
        if not self._http_client:
            return SendResult(success=False, error="HTTP client not initialized")

        chunks = self.truncate_message(content, max_length=self.MAX_MESSAGE_LENGTH)
        last_message_id: Optional[str] = None
        continuation_ids: List[str] = []

        for i, chunk in enumerate(chunks):
            body: Dict[str, Any] = {"roomId": chat_id}
            if self._markdown_enabled:
                body["markdown"] = chunk
            else:
                body["text"] = chunk
            if reply_to:
                # Thread every chunk to the same triggering message (not to
                # the previous chunk) so a long, split response still reads
                # as one Webex reply thread under the user's message, rather
                # than only the first part being visually linked and the
                # rest trailing off as unthreaded room messages.
                body["parentId"] = reply_to

            try:
                resp = await self._http_client.post("/messages", json=body, timeout=15.0)
            except httpx.TimeoutException:
                return SendResult(success=False, error="Timeout sending to Webex", retryable=True)
            except Exception as e:
                logger.error("[%s] Send error: %s", self.name, e)
                return SendResult(success=False, error=str(e))

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "5"))
                return SendResult(
                    success=False, error="Rate limited by Webex", retryable=True,
                    retry_after=retry_after,
                )
            if resp.status_code >= 300:
                body_text = resp.text
                logger.warning("[%s] Send failed HTTP %d: %s", self.name, resp.status_code, body_text[:200])
                return SendResult(success=False, error=f"HTTP {resp.status_code}: {body_text[:200]}")

            data = resp.json()
            msg_id = data.get("id") or uuid.uuid4().hex[:12]
            if last_message_id is not None:
                continuation_ids.append(last_message_id)
            last_message_id = msg_id

        return SendResult(
            success=True,
            message_id=last_message_id,
            continuation_message_ids=tuple(continuation_ids),
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Webex has no typing-indicator API for bots."""
        pass

    # -- Local file / media delivery ---------------------------------------
    #
    # Webex has no separate "upload then reference" step like some chat
    # APIs (e.g. Mattermost) — the file is attached directly in the same
    # multipart/form-data POST that creates the message. One attachment per
    # message; larger deliveries (e.g. several generated images) go out as
    # one message per file, same as the base class's default
    # ``send_multiple_images`` loop already does.

    async def _send_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> SendResult:
        """Upload a local file to a Webex room as a message attachment."""
        if not self._http_client:
            return SendResult(success=False, error="HTTP client not initialized")

        safe_path = self.validate_media_delivery_path(file_path)
        if not safe_path:
            logger.warning(
                "[%s] Refusing to upload unsafe local path: %s", self.name, file_path
            )
            return await self.send(
                chat_id=chat_id,
                content=self._attachment_failure_text(caption, "the attachment"),
                reply_to=reply_to,
            )

        path = Path(safe_path)
        if not path.is_file():
            logger.warning(
                "[%s] Local file not found, skipping upload: %s", self.name, safe_path
            )
            return await self.send(
                chat_id=chat_id,
                content=self._attachment_failure_text(
                    caption, "the attachment (file not found)"
                ),
                reply_to=reply_to,
            )

        if path.stat().st_size > MAX_ATTACHMENT_BYTES:
            logger.warning(
                "[%s] File exceeds Webex's 100MB attachment limit, skipping: %s",
                self.name, safe_path,
            )
            return await self.send(
                chat_id=chat_id,
                content=self._attachment_failure_text(
                    caption, "the attachment (over Webex's 100MB limit)"
                ),
                reply_to=reply_to,
            )

        display_name = file_name or path.name
        content_type = mimetypes.guess_type(display_name)[0] or "application/octet-stream"

        form_data: Dict[str, str] = {"roomId": chat_id}
        if caption:
            form_data["markdown" if self._markdown_enabled else "text"] = caption
        if reply_to:
            form_data["parentId"] = reply_to

        try:
            with path.open("rb") as fh:
                resp = await self._http_client.post(
                    "/messages",
                    data=form_data,
                    files={"files": (display_name, fh, content_type)},
                    timeout=60.0,
                )
        except httpx.TimeoutException:
            return SendResult(
                success=False, error="Timeout uploading file to Webex", retryable=True
            )
        except OSError as e:
            return SendResult(success=False, error=f"Could not read local file: {e}")

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "5"))
            return SendResult(
                success=False, error="Rate limited by Webex", retryable=True,
                retry_after=retry_after,
            )
        if resp.status_code >= 300:
            body_text = resp.text
            logger.warning(
                "[%s] File upload failed HTTP %d: %s",
                self.name, resp.status_code, body_text[:200],
            )
            return SendResult(success=False, error=f"HTTP {resp.status_code}: {body_text[:200]}")

        data = resp.json()
        return SendResult(success=True, message_id=data.get("id") or uuid.uuid4().hex[:12])

    @staticmethod
    def _attachment_failure_text(caption: Optional[str], what: str) -> str:
        text = f"⚠️ Couldn't deliver {what}."
        return f"{caption}\n{text}" if caption else text

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local image file as a native Webex attachment."""
        return await self._send_local_file(chat_id, image_path, caption, reply_to)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local file as a native Webex attachment."""
        return await self._send_local_file(chat_id, file_path, caption, reply_to, file_name)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Upload an audio file as a native Webex attachment.

        Webex bots have no distinct "voice bubble" API — the file is
        delivered as a regular playable/downloadable attachment.
        """
        return await self._send_local_file(chat_id, audio_path, caption, reply_to)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Upload a video file as a native Webex attachment."""
        return await self._send_local_file(chat_id, video_path, caption, reply_to)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic info about a Webex room."""
        if not self._http_client:
            return {"name": chat_id, "type": "dm"}
        try:
            resp = await self._http_client.get(f"/rooms/{chat_id}")
            resp.raise_for_status()
            data = resp.json()
            return {
                "name": data.get("title", chat_id),
                "type": "dm" if data.get("type") == "direct" else "group",
            }
        except Exception:
            return {"name": chat_id, "type": "dm"}

    # -- Helpers ----------------------------------------------------------

    def _auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token.strip()}"}


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def _env_enablement() -> dict | None:
    """Seed ``PlatformConfig.extra`` from env vars during gateway config load.

    Called by the platform registry's env-enablement hook BEFORE adapter
    construction, so ``gateway status`` and ``get_connected_platforms()``
    reflect env-only configuration without instantiating an HTTP client.
    Returns ``None`` when Webex isn't minimally configured; the caller
    skips auto-enabling.
    """
    token = os.getenv("WEBEX_BOT_TOKEN", "").strip()
    if not token:
        return None

    seed: dict = {"bot_token": token}

    poll_interval = os.getenv("WEBEX_POLL_INTERVAL_SECONDS", "").strip()
    if poll_interval:
        seed["poll_interval"] = poll_interval

    markdown = os.getenv("WEBEX_MARKDOWN", "").strip().lower()
    if markdown:
        seed["markdown"] = markdown in ("1", "true", "yes")

    home_room = os.getenv("WEBEX_HOME_ROOM_ID", "").strip()
    if home_room:
        seed["home_channel"] = {
            "chat_id": home_room,
            "name": os.getenv("WEBEX_HOME_ROOM_NAME", home_room),
        }

    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process send for cron / send_message_tool fallbacks.

    Used when the gateway runner is not in this process (e.g. ``hermes
    cron`` running standalone). Without this hook, ``deliver=webex`` cron
    jobs fail with "No live adapter for platform".

    Webex allows exactly one file attachment per message (see
    developer.webex.com/messaging/docs/basics — a second ``files`` part
    gets a 400). When ``media_files`` has more than one entry, the first
    is attached to the initial message (with ``message`` as its caption)
    and every remaining file goes out as its own follow-up message.

    ``force_document`` is accepted for signature parity with other
    standalone senders — Webex has no separate "send as document" mode,
    every attachment is delivered the same way regardless of type.
    """
    if not HTTPX_AVAILABLE:
        return {"error": "webex standalone send: httpx not installed"}

    extra = getattr(pconfig, "extra", {}) or {}
    token = extra.get("bot_token") or os.getenv("WEBEX_BOT_TOKEN", "").strip()
    if not token:
        return {"error": "webex standalone send: WEBEX_BOT_TOKEN not configured"}

    room_id = chat_id or extra.get("home_channel", {}).get("chat_id")
    if not room_id:
        return {"error": "webex standalone send: no roomId (chat_id) provided"}

    markdown_env = os.getenv("WEBEX_MARKDOWN", "").strip().lower()
    markdown_enabled = (
        bool(extra.get("markdown")) if extra.get("markdown") is not None
        else (markdown_env in ("1", "true", "yes") if markdown_env else True)
    )
    text = message[:MAX_MESSAGE_LENGTH]

    def _safe_local_file(raw_path: str) -> Optional[Path]:
        safe = validate_media_delivery_path(raw_path)
        if not safe:
            logger.warning("webex standalone send: refusing unsafe path %s", raw_path)
            return None
        path = Path(safe)
        if not path.is_file():
            logger.warning("webex standalone send: file not found %s", safe)
            return None
        if path.stat().st_size > MAX_ATTACHMENT_BYTES:
            logger.warning("webex standalone send: file over 100MB, skipping %s", safe)
            return None
        return path

    async def _post_with_file(client: "httpx.AsyncClient", path: Path, caption: str) -> Dict[str, Any]:
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        form_data: Dict[str, str] = {"roomId": room_id}
        if caption:
            form_data["markdown" if markdown_enabled else "text"] = caption
        if thread_id:
            form_data["parentId"] = thread_id
        with path.open("rb") as fh:
            return await client.post(
                "/messages", data=form_data, files={"files": (path.name, fh, content_type)},
            )

    async def _post_text_only(client: "httpx.AsyncClient", caption: str) -> Dict[str, Any]:
        body: Dict[str, Any] = {"roomId": room_id}
        if caption:
            body["markdown" if markdown_enabled else "text"] = caption
        if thread_id:
            body["parentId"] = thread_id
        return await client.post("/messages", json=body)

    try:
        async with httpx.AsyncClient(
            base_url=API_BASE, headers={"Authorization": f"Bearer {token}"}, timeout=60.0
        ) as client:
            paths = [p for p in (_safe_local_file(f) for f in (media_files or [])) if p]
            sent_ids: List[str] = []

            if paths:
                resp = await _post_with_file(client, paths[0], text)
            else:
                resp = await _post_text_only(client, text)
            if resp.status_code >= 300:
                return {"error": f"webex HTTP {resp.status_code}: {resp.text[:200]}"}
            data = resp.json()
            sent_ids.append(data.get("id") or uuid.uuid4().hex[:12])

            for path in paths[1:]:
                try:
                    resp = await _post_with_file(client, path, "")
                except Exception as e:
                    logger.warning("webex standalone send: failed uploading %s: %s", path.name, e)
                    continue
                if resp.status_code >= 300:
                    logger.warning(
                        "webex standalone send: HTTP %d uploading %s: %s",
                        resp.status_code, path.name, resp.text[:200],
                    )
                    continue
                sent_ids.append(resp.json().get("id") or uuid.uuid4().hex[:12])

        return {
            "success": True, "platform": "webex", "chat_id": room_id,
            "message_id": sent_ids[0] if sent_ids else None,
        }
    except Exception as e:
        return {"error": f"webex standalone send failed: {e}"}


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup."""
    ctx.register_platform(
        name="webex",
        label="Webex",
        adapter_factory=lambda cfg: WebexAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["WEBEX_BOT_TOKEN"],
        install_hint="pip install httpx   # already a Hermes dependency",
        env_enablement_fn=_env_enablement,
        # Cron home-channel delivery support — `deliver=webex` cron jobs
        # route to WEBEX_HOME_ROOM_ID when set.
        cron_deliver_env_var="WEBEX_HOME_ROOM_ID",
        # Out-of-process cron delivery. Without this hook, deliver=webex
        # cron jobs fail with "No live adapter" when cron runs separately
        # from the gateway.
        standalone_sender_fn=_standalone_send,
        # Auth env vars for the gateway's generic allowlist enforcement.
        allowed_users_env="WEBEX_ALLOWED_EMAILS",
        allow_all_env="WEBEX_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="💬",
        pii_safe=False,  # personEmail is a real identity — redact in logs like Telegram/Discord
        allow_update_command=True,
        platform_hint=(
            "You are communicating via Cisco Webex. Markdown is supported "
            "natively (bold, italics, fenced code, links). In shared Webex "
            "spaces you only see messages where you were @mentioned; in a "
            "1:1 conversation every message is yours to answer. Keep "
            f"replies under {MAX_MESSAGE_LENGTH} characters per message — "
            "longer replies are split automatically."
        ),
    )
