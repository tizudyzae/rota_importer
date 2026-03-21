"""HTTP view for a Home Assistant-native rota ask endpoint."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from homeassistant.components.http import HomeAssistantView

from .ask_shared import build_ask_response
from .bridge_logic import validate_bridge_payload

DEFAULT_DB_PATH = "/config/rota.db"
_LOGGER = logging.getLogger(__name__)


class RotaImporterAskView(HomeAssistantView):
    """Serve HA-authenticated ask requests directly from shared rota logic."""

    url = "/api/rota_importer/ask"
    name = "api:rota_importer:ask"
    requires_auth = True

    @staticmethod
    def _resolve_db_path() -> Path:
        return Path(os.environ.get("ROTA_IMPORTER_DB_PATH", DEFAULT_DB_PATH))

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

        db_path = cls._resolve_db_path()
        _LOGGER.info("[bridge] handling ask request locally via shared logic db_path=%s", db_path)
        try:
            payload = build_ask_response(db_path=db_path, question=question, person=person)
        except ValueError as err:
            return cls.json({"error": str(err)}, status_code=400)
        except Exception as err:  # pragma: no cover - defensive logging for HA runtime
            _LOGGER.exception("[bridge] local ask processing failed: %s: %s", type(err).__name__, err)
            return cls.json({"error": "Bridge failed to process ask request"}, status_code=500)
        return cls.json(payload, status_code=200)
