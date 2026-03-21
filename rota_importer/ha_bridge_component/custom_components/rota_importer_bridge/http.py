"""HTTP view for a Home Assistant-native rota ask endpoint."""

from __future__ import annotations

from typing import Any
import os

from aiohttp import ClientError
from homeassistant.components.http import HomeAssistantView
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from yarl import URL

from .bridge_logic import validate_bridge_payload

DEFAULT_ADDON_ASK_URL = "http://127.0.0.1:8099/api/ask"


class RotaImporterAskView(HomeAssistantView):
    """Bridge HA-authenticated requests to the add-on ask endpoint."""

    url = "/api/rota_importer/ask"
    name = "api:rota_importer:ask"
    requires_auth = True

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

        addon_ask_url = URL(os.environ.get("ROTA_IMPORTER_ADDON_ASK_URL", DEFAULT_ADDON_ASK_URL))
        payload: dict[str, Any] = {"question": question}
        if person is not None:
            payload["person"] = person

        session = async_get_clientsession(request.app["hass"])
        try:
            async with session.post(
                addon_ask_url,
                json=payload,
                headers={
                    "Authorization": auth_header,
                    "Content-Type": "application/json",
                },
                timeout=10,
            ) as response:
                try:
                    forwarded_payload = await response.json()
                except ValueError:
                    forwarded_payload = {"error": "Bridge upstream returned non-JSON response"}
                return cls.json(forwarded_payload, status_code=response.status)
        except ClientError:
            return cls.json({"error": "Bridge could not reach add-on ask endpoint"}, status_code=502)
