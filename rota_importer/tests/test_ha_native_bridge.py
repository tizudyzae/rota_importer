from pathlib import Path
import sys

sys.path.append(
    str(
        Path(__file__).resolve().parents[1]
        / "ha_bridge_component"
        / "custom_components"
        / "rota_importer_bridge"
    )
)

import bridge_logic


def test_validate_bridge_payload_success_question_only():
    payload, error = bridge_logic.validate_bridge_payload({"question": "who is opening tomorrow?"})

    assert error is None
    assert payload == {"question": "who is opening tomorrow?", "person": None}


def test_validate_bridge_payload_success_question_and_person_trimmed():
    payload, error = bridge_logic.validate_bridge_payload(
        {"question": "  who am i working with today?  ", "person": " Nathan "}
    )

    assert error is None
    assert payload == {"question": "who am i working with today?", "person": "Nathan"}


def test_validate_bridge_payload_invalid_object_shape():
    payload, error = bridge_logic.validate_bridge_payload(["invalid"])

    assert payload is None
    assert error == (400, {"error": "JSON body must be an object"})


def test_validate_bridge_payload_missing_question():
    payload, error = bridge_logic.validate_bridge_payload({"person": "Nathan"})

    assert payload is None
    assert error == (400, {"error": "question is required and must be a non-empty string"})


def test_validate_bridge_payload_person_must_be_string():
    payload, error = bridge_logic.validate_bridge_payload({"question": "who is opening tomorrow?", "person": 123})

    assert payload is None
    assert error == (400, {"error": "person must be a string"})
