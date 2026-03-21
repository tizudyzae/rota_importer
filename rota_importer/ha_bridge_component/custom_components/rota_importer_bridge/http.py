"""HTTP view for a Home Assistant-native rota ask endpoint."""

from __future__ import annotations

from typing import Any
import asyncio
import json
import logging
import os

from aiohttp import ClientError
from homeassistant.components.http import HomeAssistantView
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from yarl import URL

from .bridge_logic import validate_bridge_payload

DEFAULT_ADDON_ASK_URL = "http://addon_rota_importer:8099/api/ask"
DISALLOWED_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_LOGGER = logging.getLogger(__name__)


class RotaImporterAskView(HomeAssistantView):
    """Bridge HA-authenticated requests to the add-on ask endpoint."""

    url = "/api/rota_importer/ask"
    name = "api:rota_importer:ask"
    requires_auth = True

    @staticmethod
    def _resolve_addon_ask_url() -> URL:
        configured_url = URL(os.environ.get("ROTA_IMPORTER_ADDON_ASK_URL", DEFAULT_ADDON_ASK_URL))
        host = configured_url.host or ""
        if host in DISALLOWED_LOCAL_HOSTS:
            fallback_url = URL(DEFAULT_ADDON_ASK_URL)
            _LOGGER.warning(
                "[bridge] refusing local host target '%s'; using add-on host '%s' instead",
                host,
                fallback_url.host,
            )
            return fallback_url
        return configured_url

    @classmethod
    async def post(cls, request):
        """Handle POST /api/rota_importer/ask."""
        try:
            body = await request.json()
        except ValueError:
            return cls.json({"error": "Invalid JSON payload"}, status_code=400)

        parsed_payload, validation_error = validate_bridge_payload(body)
        if validation_error:
            return cls.json(validation_error[1], status_code=validation_error[0])

        question = parsed_payload["question"]
        person = parsed_payload["person"]

        auth_header = request.headers.get("Authorization", "")
        if not auth_header:
            return cls.json({"error": "unauthorized"}, status_code=401)

        addon_ask_url = cls._resolve_addon_ask_url()
        _LOGGER.info(
            "[bridge] resolved target host=%s port=%s path=%s",
            addon_ask_url.host,
            addon_ask_url.port,
            addon_ask_url.path,
        )
        payload: dict[str, Any] = {"question": question}
        if person is not None:
            payload["person"] = person

        session = async_get_clientsession(request.app["hass"])
        request_attempted = False
        _LOGGER.info("[bridge] forwarding POST to %s", addon_ask_url)
        try:
            request_attempted = True
            async with session.post(
                addon_ask_url,
                json=payload,
                headers={
                    "Authorization": auth_header,
                    "Content-Type": "application/json",
                },
                timeout=10,
            ) as response:
                response_text = await response.text()
                try:
                    forwarded_payload = json.loads(response_text)
                except ValueError:
                    forwarded_payload = {"error": "Bridge upstream returned non-JSON response"}
                if response.status != 200:
                    _LOGGER.warning(
                        "[bridge] upstream non-200: status=%s body=%s",
                        response.status,
                        response_text,
                    )
                return cls.json(forwarded_payload, status_code=response.status)
        except asyncio.TimeoutError as err:
            _LOGGER.error(
                "[bridge] request failed: %s: %s (attempted=%s)",
                type(err).__name__,
                "timeout while calling add-on ask endpoint",
                request_attempted,
            )
            return cls.json({"error": "Bridge timed out reaching add-on ask endpoint"}, status_code=504)
        except (ClientError, OSError) as err:
            _LOGGER.error(
                "[bridge] request failed: %s: %s (attempted=%s)",
                type(err).__name__,
                err,
                request_attempted,
            )
            return cls.json({"error": "Bridge could not reach add-on ask endpoint"}, status_code=502)
