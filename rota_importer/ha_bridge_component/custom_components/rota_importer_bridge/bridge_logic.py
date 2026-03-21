"""Shared validation helpers for the HA-native ask bridge endpoint."""

from __future__ import annotations

from typing import Any


def validate_bridge_payload(body: Any) -> tuple[dict[str, str | None] | None, tuple[int, dict[str, str]] | None]:
    """Validate bridge request payload shape and value types."""
    if not isinstance(body, dict):
        return None, (400, {"error": "JSON body must be an object"})

    question = body.get("question")
    person = body.get("person")

    if not isinstance(question, str) or not question.strip():
        return None, (400, {"error": "question is required and must be a non-empty string"})

    if person is not None and not isinstance(person, str):
        return None, (400, {"error": "person must be a string"})

    cleaned_question = question.strip()
    cleaned_person = person.strip() if isinstance(person, str) else None
    return {"question": cleaned_question, "person": cleaned_person}, None
