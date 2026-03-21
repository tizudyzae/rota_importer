from datetime import datetime
from pathlib import Path
import sys

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
            (upload_id, "Nathan", "sat", "Sat", today, "08:00-16:00", "08:00", "16:00", "8", 2),
            (upload_id, "Alex", "sat", "Sat", today, "12:00-20:00", "12:00", "20:00", "8", 3),
            (upload_id, "Sam", "sat", "Sat", today, "14:00-22:00", "14:00", "22:00", "8", 4),
            (upload_id, "Tom", "sun", "Sun", tomorrow, "06:00-14:00", "06:00", "14:00", "8", 5),
            (upload_id, "Jill", "sun", "Sun", tomorrow, "14:00-23:00", "14:00", "23:00", "9", 6),
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
        conn.commit()


def _build_client(tmp_path):
    app_module.DB_PATH = tmp_path / "rota.db"
    app_module.UPLOAD_DIR = tmp_path / "uploads"
    app_module.EXPORT_DIR = tmp_path / "exports"
    app_module.start_auto_notification_worker = lambda: None
    app_module.stop_auto_notification_worker = lambda: None
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

    assert today == "2026-03-21"
    assert today_label == "today"
    assert tomorrow == "2026-03-22"
    assert tomorrow_label == "tomorrow"


def test_api_ask_successful_responses(tmp_path):
    client, today, tomorrow = _build_client(tmp_path)

    opening = client.post("/api/ask", json={"question": "who is opening tomorrow?"})
    assert opening.status_code == 200
    assert opening.json() == {
        "answer": "Tom is opening tomorrow.",
        "date": tomorrow,
        "matched_intent": "opening_shift",
    }

    working_with = client.post(
        "/api/ask",
        json={"question": "who am I working with today?", "person": "Nathan"},
    )
    assert working_with.status_code == 200
    assert working_with.json() == {
        "answer": "You are working with Alex, Sam, and Tom today.",
        "date": today,
        "matched_intent": "who_am_i_working_with_today",
    }


def test_api_ask_missing_question(tmp_path):
    client, _, _ = _build_client(tmp_path)
    response = client.post("/api/ask", json={"person": "Nathan"})
    assert response.status_code == 400
    assert response.json() == {"error": "question is required and must be a non-empty string"}


def test_api_ask_missing_person_for_person_intent(tmp_path):
    client, _, _ = _build_client(tmp_path)
    response = client.post("/api/ask", json={"question": "who am I working with?"})
    assert response.status_code == 400
    assert response.json() == {"error": "person is required for this question type"}


def test_api_ask_unknown_question(tmp_path):
    client, today, _ = _build_client(tmp_path)
    response = client.post("/api/ask", json={"question": "can you sing me a song?"})
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Sorry, I could not understand that rota question.",
        "date": today,
        "matched_intent": "unknown",
    }
