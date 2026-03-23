"""HTTP view for a Home Assistant-native rota ask endpoint."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import json
import sqlite3

from homeassistant.components.http import HomeAssistantView

from .ask_shared import build_ask_response
from .bridge_logic import validate_bridge_payload

DEFAULT_DB_PATH = "/config/rota.db"
_LOGGER = logging.getLogger(__name__)


def _clean_cell(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\n", " ").split()).strip()


def _sanitize_person_key(value: str) -> str:
    return _clean_cell(value).strip()


def _parse_json_object(raw: str) -> dict:
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(raw: str) -> list:
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _resolve_notify_target(db_path: Path, requested_person: str) -> tuple[str, str] | None:
    person_key = _sanitize_person_key(requested_person).lower()
    if not person_key:
        return None

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM notification_settings WHERE id = 1").fetchone()
        if row is None or int(row["enabled"] or 0) != 1:
            return None

        subject_names = [
            _sanitize_person_key(name)
            for name in _parse_json_list(row["subject_names_json"])
            if _sanitize_person_key(name)
        ]
        if not subject_names:
            return None

        subject_service_map = {
            _sanitize_person_key(key): _clean_cell(value)
            for key, value in _parse_json_object(row["subject_service_map_json"]).items()
            if _sanitize_person_key(key) and _clean_cell(value)
        }

    subject_by_key = {name.lower(): name for name in subject_names}
    subject_name = subject_by_key.get(person_key)
    if subject_name is None:
        return None

    notify_service = _clean_cell(subject_service_map.get(subject_name))
    if not notify_service or "." not in notify_service:
        return None

    return subject_name, notify_service


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

        if person:
            try:
                notify_target = _resolve_notify_target(db_path=db_path, requested_person=person)
                if notify_target is not None:
                    _subject_name, notify_service = notify_target
                    domain, service = notify_service.split(".", 1)
                    await request.app["hass"].services.async_call(
                        domain,
                        service,
                        {
                            "title": "Rota ask response",
                            "message": _clean_cell(payload.get("answer", "")),
                        },
                        blocking=True,
                    )
            except Exception as err:  # pragma: no cover - avoid failing bridge on notify issues
                _LOGGER.warning("[bridge] ask notification failed for person=%s: %s", person, err)
        return cls.json(payload, status_code=200)
