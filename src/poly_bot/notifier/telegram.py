"""Thin async Telegram Bot API client.

Just enough to send formatted alert messages and discover the chat id
during onboarding (``getUpdates``). No long-poll / webhook server — the
alerter pushes one-way notifications.
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)

_API = "https://api.telegram.org"


class TelegramClient:
    def __init__(
        self,
        token: str,
        *,
        chat_id: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._token = token
        self._chat_id = chat_id
        self._base = f"{_API}/bot{token}"
        self._timeout = timeout

    async def send_message(
        self,
        text: str,
        *,
        chat_id: str | None = None,
        parse_mode: str = "Markdown",
        disable_notification: bool = False,
    ) -> bool:
        target = chat_id or self._chat_id
        if not target:
            raise ValueError("Telegram chat_id is not configured")
        payload = {
            "chat_id": target,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
            "disable_notification": disable_notification,
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(f"{self._base}/sendMessage", json=payload)
                if resp.status_code != 200:
                    logger.warning(
                        "telegram_send_failed",
                        status=resp.status_code,
                        body=resp.text[:200],
                    )
                    return False
            return True
        except Exception:
            logger.exception("telegram_send_error")
            return False

    async def get_updates(self) -> list[dict[str, Any]]:
        """Return recent ``getUpdates`` payload (used by onboarding helper)."""
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.get(f"{self._base}/getUpdates")
            resp.raise_for_status()
            data = resp.json()
        updates = data.get("result", [])
        return updates if isinstance(updates, list) else []
