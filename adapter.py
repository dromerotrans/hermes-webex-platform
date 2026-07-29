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
import os
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
)

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
        if not text:
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
        logger.debug("[%s] Message in room %s from %s: %s", self.name, room_id, person_email, text[:80])
        await self.handle_message(message_event)

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
            if reply_to and i == 0:
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
    jobs fail with "No live adapter for platform". ``media_files`` is
    accepted for signature parity only — file upload isn't implemented in
    this first cut (see module docstring for scope).
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

    body: Dict[str, Any] = {"roomId": room_id}
    text = message[:MAX_MESSAGE_LENGTH]
    if markdown_enabled:
        body["markdown"] = text
    else:
        body["text"] = text
    if thread_id:
        body["parentId"] = thread_id

    try:
        async with httpx.AsyncClient(
            base_url=API_BASE, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
        ) as client:
            resp = await client.post("/messages", json=body)
        if resp.status_code >= 300:
            return {"error": f"webex HTTP {resp.status_code}: {resp.text[:200]}"}
        data = resp.json()
        return {
            "success": True, "platform": "webex", "chat_id": room_id,
            "message_id": data.get("id") or uuid.uuid4().hex[:12],
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
