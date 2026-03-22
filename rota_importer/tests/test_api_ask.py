from datetime import datetime
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))
import app as app_module


def _seed_sample_data(today: str, tomorrow: str) -> None:
    with app_module.get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO uploads (original_filename, stored_filename, uploaded_at) VALUES (?, ?, ?)",
            ("sample.pdf", "sample.pdf", datetime.utcnow().isoformat(timespec="seconds")),
        )
        upload_id = cur.lastrowid

        rows = [
            (upload_id, "Tom", "sat", "Sat", today, "06:00-14:00", "06:00", "14:00", "8", 1),
            (upload_id, "Nathan", "sat", "Sat", today, "06:00-14:00", "06:00", "14:00", "8", 2),
            (upload_id, "Alex", "sat", "Sat", today, "12:00-20:00", "12:00", "20:00", "8", 3),
            (upload_id, "Sam", "sat", "Sat", today, "14:00-22:00", "14:00", "22:00", "8", 4),
            (upload_id, "Debbie", "sat", "Sat", today, "OFF", "", "", "", 5),
            (upload_id, "Jill", "sat", "Sat", today, "13:00-22:00", "13:00", "22:00", "9", 6),
            (upload_id, "Tom", "sun", "Sun", tomorrow, "06:00-14:00", "06:00", "14:00", "8", 7),
            (upload_id, "Jill", "sun", "Sun", tomorrow, "14:00-23:00", "14:00", "23:00", "9", 8),
        ]

        conn.executemany(
            """
            INSERT INTO shifts (
                upload_id, employee, day_name, day_header, shift_date, raw_cell,
                start_time, end_time, total_hours, row_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        conn.execute(
            """
            INSERT INTO app_preferences (singleton_key, color_preferences, alias_preferences, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(singleton_key) DO UPDATE SET
              color_preferences=excluded.color_preferences,
              alias_preferences=excluded.alias_preferences,
              updated_at=excluded.updated_at
            """,
            (
                "global",
                "{}",
                '{"Nathan": "Boss", "Alex": "Lex"}',
                datetime.utcnow().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()


def _build_client(tmp_path):
    app_module.DB_PATH = tmp_path / "rota.db"
    app_module.UPLOAD_DIR = tmp_path / "uploads"
    app_module.EXPORT_DIR = tmp_path / "exports"
    app_module.start_auto_notification_worker = lambda: None
    app_module.stop_auto_notification_worker = lambda: None
    app_module._ask_auth_cache.clear()
    app_module._ask_rate_limit_state.clear()
    os.environ["ASK_API_TOKEN"] = "test-token"
    app_module.init_db()

    today = datetime.now().date().isoformat()
    tomorrow = app_module.resolve_question_date("who is opening tomorrow?")[0]
    _seed_sample_data(today=today, tomorrow=tomorrow)
    return TestClient(app_module.app), today, tomorrow


def test_intent_matching():
    assert app_module.parse_ask_intent("who is working today?") == "who_is_working_today"
    assert app_module.parse_ask_intent("who am I working with?") == "who_am_i_working_with_today"
    assert app_module.parse_ask_intent("who is opening tomorrow?") == "opening_shift"
    assert app_module.parse_ask_intent("who closes today?") == "closing_shift"
    assert app_module.parse_ask_intent("what is the weather?") == "unknown"


def test_date_parsing():
    now_value = datetime(2026, 3, 21, 9, 0, 0)
    today, today_label = app_module.resolve_question_date("who is working?", now_value=now_value)
    tomorrow, tomorrow_label = app_module.resolve_question_date("who is opening tomorrow?", now_value=now_value)
    next_week, next_week_label = app_module.resolve_question_date("weekly summary next week", now_value=now_value)

    assert today == "2026-03-21"
    assert today_label == "today"
    assert tomorrow == "2026-03-22"
    assert tomorrow_label == "tomorrow"
    assert next_week == "2026-03-23"
    assert next_week_label == "next week"


def test_date_parsing_uses_configured_timezone(monkeypatch):
    monkeypatch.setenv("TZ", "Pacific/Honolulu")
    expected_today = datetime.now(ZoneInfo("Pacific/Honolulu")).date().isoformat()
    today, today_label = app_module.resolve_question_date("who is working today?")

    assert today == expected_today
    assert today_label == "today"


def test_api_ask_successful_responses(tmp_path):
    client, today, tomorrow = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    opening = client.post("/api/ask", json={"question": "who is opening tomorrow?"}, headers=headers)
    assert opening.status_code == 200
    assert opening.json() == {
        "answer": "Tom is opening tomorrow.",
        "date": tomorrow,
        "matched_intent": "opening_shift",
    }

    working_with = client.post(
        "/api/ask",
        json={"question": "who am I working with today?", "person": "Nathan"},
        headers=headers,
    )
    assert working_with.status_code == 200
    assert working_with.json() == {
        "answer": "Boss is working with Lex, Jill, and Tom today.",
        "date": today,
        "matched_intent": "who_am_i_working_with_today",
    }

    who_is_working = client.post(
        "/api/ask",
        json={"question": "who is on shift at 3pm today?"},
        headers=headers,
    )
    assert who_is_working.status_code == 200
    assert who_is_working.json() == {
        "answer": "Lex, Sam, and Jill are on at 3:00pm today.",
        "date": today,
        "matched_intent": "who_is_working_today",
    }


def test_api_ask_alias_recognition_and_overlap_alias(tmp_path):
    client, today, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post(
        "/api/ask",
        json={"question": "who am I working with today?", "person": "Boss"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Boss is working with Lex, Jill, and Tom today.",
        "date": today,
        "matched_intent": "who_am_i_working_with_today",
    }


def test_api_ask_opening_with_management_priority(tmp_path):
    client, today, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post(
        "/api/ask",
        json={"question": "who is first in today?"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Boss is opening today, with Tom starting at the same time.",
        "date": today,
        "matched_intent": "opening_shift",
    }


def test_api_ask_closing_with_management_priority(tmp_path):
    client, today, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post(
        "/api/ask",
        json={"question": "who is last out today?"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Jill and Sam are closing today.",
        "date": today,
        "matched_intent": "closing_shift",
    }


def test_api_ask_closing_management_tie(tmp_path):
    client, today, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    with app_module.get_conn() as conn:
        upload_id = conn.execute("SELECT id FROM uploads ORDER BY id DESC LIMIT 1").fetchone()[0]
        conn.execute(
            """
            INSERT INTO shifts (
                upload_id, employee, day_name, day_header, shift_date, raw_cell,
                start_time, end_time, total_hours, row_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (upload_id, "Nathan", "sat", "Sat", today, "14:00-22:00", "14:00", "22:00", "8", 99),
        )
        conn.commit()

    response = client.post(
        "/api/ask",
        json={"question": "who is closing today?"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Boss is closing today, with Jill and Sam finishing at the same time.",
        "date": today,
        "matched_intent": "closing_shift",
    }


def test_api_ask_opening_fallback_when_no_management(tmp_path, monkeypatch):
    monkeypatch.setenv("ASK_MANAGEMENT_NAMES", "noone")
    client, today, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post(
        "/api/ask",
        json={"question": "who is opening today?"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Boss and Tom are opening today.",
        "date": today,
        "matched_intent": "opening_shift",
    }


def test_api_ask_ambiguous_question_fallback(tmp_path):
    client, today, _ = _build_client(tmp_path)
    response = client.post(
        "/api/ask",
        json={"question": "can you help with rota"},
        headers={"Authorization": "Bearer test-token"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Sorry, I could not understand that rota question. Try adding a day or person.",
        "date": today,
        "matched_intent": "unknown",
    }


def test_api_ask_missing_question(tmp_path):
    client, _, _ = _build_client(tmp_path)
    response = client.post(
        "/api/ask",
        json={"person": "Nathan"},
        headers={"Authorization": "Bearer test-token"},
    )
    assert response.status_code == 400
    assert response.json() == {"error": "question is required and must be a non-empty string"}


def test_api_ask_missing_person_for_person_intent(tmp_path):
    client, _, _ = _build_client(tmp_path)
    response = client.post(
        "/api/ask",
        json={"question": "who am I working with?"},
        headers={"Authorization": "Bearer test-token"},
    )
    assert response.status_code == 400
    assert response.json() == {"error": "person is required for this question type"}


def test_api_ask_excludes_off_staff_for_coworker_question(tmp_path):
    client, today, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post(
        "/api/ask",
        json={"question": "who am I working with today?", "person": "Debbie"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Debbie is not scheduled today.",
        "date": today,
        "matched_intent": "who_am_i_working_with_today",
    }


def test_api_ask_unknown_question(tmp_path):
    client, today, _ = _build_client(tmp_path)
    response = client.post(
        "/api/ask",
        json={"question": "can you sing me a song?"},
        headers={"Authorization": "Bearer test-token"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Sorry, I could not understand that rota question. Try adding a day or person.",
        "date": today,
        "matched_intent": "unknown",
    }


def test_api_ask_unauthorized_missing_or_invalid_token(tmp_path):
    client, _, _ = _build_client(tmp_path)

    missing = client.post("/api/ask", json={"question": "who is working today?"})
    assert missing.status_code == 401
    assert missing.json() == {"error": "unauthorized"}

    invalid = client.post(
        "/api/ask",
        json={"question": "who is working today?"},
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert invalid.status_code == 401
    assert invalid.json() == {"error": "unauthorized"}


def test_api_ha_bridge_successful_request(tmp_path):
    client, _, tomorrow = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post("/api/ha/ask", json={"question": "who is opening tomorrow?"}, headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Tom is opening tomorrow.",
        "date": tomorrow,
        "matched_intent": "opening_shift",
    }


def test_api_ha_bridge_unauthorized_when_missing_token(tmp_path):
    client, _, _ = _build_client(tmp_path)

    response = client.post("/api/ha/ask", json={"question": "who is opening tomorrow?"})
    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}


def test_api_ha_bridge_invalid_payload(tmp_path):
    client, _, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post("/api/ha/ask", content="not-json", headers=headers)
    assert response.status_code == 400
    assert response.json() == {"error": "Invalid JSON payload"}


def test_api_ha_bridge_unknown_question_fallback(tmp_path):
    client, today, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post("/api/ha/ask", json={"question": "can you sing me a song?"}, headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Sorry, I could not understand that rota question. Try adding a day or person.",
        "date": today,
        "matched_intent": "unknown",
    }


def test_api_ha_bridge_matches_api_ask_response_shape(tmp_path):
    client, _, _ = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}
    payload = {"question": "who is opening tomorrow?", "person": "Nathan"}

    api_ask_response = client.post("/api/ask", json=payload, headers=headers)
    bridge_response = client.post("/api/ha/ask", json=payload, headers=headers)

    assert api_ask_response.status_code == 200
    assert bridge_response.status_code == 200
    assert set(api_ask_response.json().keys()) == {"answer", "date", "matched_intent"}
    assert set(bridge_response.json().keys()) == {"answer", "date", "matched_intent"}


def test_api_ask_falls_back_to_latest_upload_with_requested_date(tmp_path):
    client, today, tomorrow = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    with app_module.get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO uploads (original_filename, stored_filename, uploaded_at) VALUES (?, ?, ?)",
            ("newer.pdf", "newer.pdf", datetime.utcnow().isoformat(timespec="seconds")),
        )
        newer_upload_id = cur.lastrowid
        conn.execute(
            """
            INSERT INTO shifts (
                upload_id, employee, day_name, day_header, shift_date, raw_cell,
                start_time, end_time, total_hours, row_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (newer_upload_id, "Future Person", "sun", "Sun", tomorrow, "09:00-17:00", "09:00", "17:00", "8", 1),
        )
        conn.commit()

    who_is_working = client.post(
        "/api/ask",
        json={"question": "who is working today?"},
        headers=headers,
    )

    assert who_is_working.status_code == 200
    assert who_is_working.json() == {
        "answer": "Tom, Boss, Lex, Sam, and Jill are working today.",
        "date": today,
        "matched_intent": "who_is_working_today",
    }
