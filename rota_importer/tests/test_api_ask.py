from datetime import datetime, timedelta
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))
import app as app_module


def _seed_sample_data(today: str) -> dict[str, str]:
    base = datetime.fromisoformat(today).date()
    days = {
        "today": base.isoformat(),
        "tomorrow": (base + timedelta(days=1)).isoformat(),
    }

    for weekday_name, weekday_index in [
        ("monday", 0),
        ("tuesday", 1),
        ("wednesday", 2),
        ("thursday", 3),
        ("friday", 4),
        ("saturday", 5),
        ("sunday", 6),
    ]:
        delta = (weekday_index - base.weekday()) % 7
        days[weekday_name] = (base + timedelta(days=delta)).isoformat()
    if days["saturday"] == days["today"]:
        days["saturday"] = (base + timedelta(days=7)).isoformat()
    if days["sunday"] == days["today"]:
        days["sunday"] = (base + timedelta(days=7)).isoformat()

    next_monday_delta = (7 - base.weekday()) % 7
    if next_monday_delta == 0:
        next_monday_delta = 7
    days["next_monday"] = (base + timedelta(days=next_monday_delta)).isoformat()

    with app_module.get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO uploads (original_filename, stored_filename, uploaded_at) VALUES (?, ?, ?)",
            ("sample.pdf", "sample.pdf", datetime.utcnow().isoformat(timespec="seconds")),
        )
        upload_id = cur.lastrowid

        rows = [
            # today
            (upload_id, "Nathan", "mon", "Mon", days["today"], "06:00-14:00", "06:00", "14:00", "8", 1),
            (upload_id, "Tom", "mon", "Mon", days["today"], "06:00-14:00", "06:00", "14:00", "8", 2),
            (upload_id, "Alex", "mon", "Mon", days["today"], "12:00-20:00", "12:00", "20:00", "8", 3),
            (upload_id, "Sam", "mon", "Mon", days["today"], "14:00-22:00", "14:00", "22:00", "8", 4),
            (upload_id, "Jacqueline", "mon", "Mon", days["today"], "13:00-22:00", "13:00", "22:00", "9", 5),
            (upload_id, "Debbie", "mon", "Mon", days["today"], "OFF", "", "", "", 6),
            # tomorrow
            (upload_id, "Nathan", "tue", "Tue", days["tomorrow"], "10:00-18:00", "10:00", "18:00", "8", 11),
            (upload_id, "Tom", "tue", "Tue", days["tomorrow"], "06:00-14:00", "06:00", "14:00", "8", 12),
            (upload_id, "Alex", "tue", "Tue", days["tomorrow"], "12:00-20:00", "12:00", "20:00", "8", 13),
            (upload_id, "Sam", "tue", "Tue", days["tomorrow"], "12:00-16:00", "12:00", "16:00", "4", 14),
            (upload_id, "Laura", "tue", "Tue", days["tomorrow"], "14:00-23:00", "14:00", "23:00", "9", 15),
            # friday and weekend
            (upload_id, "Nathan", "fri", "Fri", days["friday"], "09:00-17:00", "09:00", "17:00", "8", 21),
            (upload_id, "Alex", "fri", "Fri", days["friday"], "11:00-19:00", "11:00", "19:00", "8", 22),
            (upload_id, "Sam", "sat", "Sat", days["saturday"], "09:00-13:00", "09:00", "13:00", "4", 23),
            (upload_id, "Laura", "sun", "Sun", days["sunday"], "12:00-18:00", "12:00", "18:00", "6", 24),
            # next week
            (upload_id, "Nathan", "mon", "Mon", days["next_monday"], "08:00-16:00", "08:00", "16:00", "8", 31),
            (upload_id, "Alex", "mon", "Mon", days["next_monday"], "12:00-18:00", "12:00", "18:00", "6", 32),
            (upload_id, "Sam", "mon", "Mon", days["next_monday"], "10:00-14:00", "10:00", "14:00", "4", 33),
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
                '{"raw:nathan": "Boss", "raw:alex": "Lex"}',
                datetime.utcnow().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()

    return days


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
    days = _seed_sample_data(today=today)
    return TestClient(app_module.app), days


def test_date_resolution_today_tomorrow_weekday_and_next_week():
    now_value = datetime(2026, 3, 22, 9, 0, 0)
    assert app_module.resolve_question_date("who is working today", now_value=now_value) == ("2026-03-22", "today")
    assert app_module.resolve_question_date("who is working tomorrow", now_value=now_value) == ("2026-03-23", "tomorrow")
    assert app_module.resolve_question_date("am i working on friday", now_value=now_value) == ("2026-03-27", "friday")
    assert app_module.resolve_question_date("who is working next week", now_value=now_value) == ("2026-03-23", "next week")


def test_date_parsing_uses_configured_timezone(monkeypatch):
    monkeypatch.setenv("TZ", "Pacific/Honolulu")
    expected_today = datetime.now(ZoneInfo("Pacific/Honolulu")).date().isoformat()
    today, label = app_module.resolve_question_date("who is working today?")
    assert today == expected_today
    assert label == "today"


def test_intent_matching_labels_are_specific():
    assert app_module.parse_ask_intent("am i working on friday") == "am_i_working"
    assert app_module.parse_ask_intent("am i off tomorrow") == "am_i_off"
    assert app_module.parse_ask_intent("who is working this morning") == "who_is_working_morning"
    assert app_module.parse_ask_intent("when do i next work") == "next_shift_for_person"
    assert app_module.parse_ask_intent("when are alex and sam next working together") == "next_overlap_between_people"
    assert app_module.parse_ask_intent("what time is alex working on friday") == "person_shift_time"


def test_api_ask_friday_specific_question_not_today(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post("/api/ask", json={"question": "am i working on friday?", "person": "Nathan"}, headers=headers)

    assert response.status_code == 200
    assert response.json() == {
        "answer": "You are working on friday from 09:00 to 17:00.",
        "date": days["friday"],
        "matched_intent": "am_i_working",
    }


def test_api_ask_morning_evening_windows(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    morning = client.post("/api/ask", json={"question": "who is working this morning"}, headers=headers)
    evening = client.post("/api/ask", json={"question": "who is working tomorrow evening"}, headers=headers)

    assert morning.json() == {
        "answer": "Boss and Tom are working today.",
        "date": days["today"],
        "matched_intent": "who_is_working_morning",
    }
    assert evening.json() == {
        "answer": "Boss, Lex, and Laura are working tomorrow.",
        "date": days["tomorrow"],
        "matched_intent": "who_is_working_evening",
    }


def test_api_ask_opening_and_closing_management_priority(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    opening = client.post("/api/ask", json={"question": "who is first in today"}, headers=headers)
    closing = client.post("/api/ask", json={"question": "who is last out tomorrow"}, headers=headers)

    assert opening.json() == {
        "answer": "Boss is opening today, with Tom starting at the same time.",
        "date": days["today"],
        "matched_intent": "opening_shift",
    }
    assert closing.json() == {
        "answer": "Laura is closing tomorrow.",
        "date": days["tomorrow"],
        "matched_intent": "closing_shift",
    }


def test_api_ask_am_i_off_and_start_finish_time(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    off = client.post("/api/ask", json={"question": "am i off today", "person": "Debbie"}, headers=headers)
    start = client.post("/api/ask", json={"question": "when do i start today", "person": "Nathan"}, headers=headers)
    finish = client.post("/api/ask", json={"question": "when do i finish friday", "person": "Nathan"}, headers=headers)

    assert off.json() == {
        "answer": "Yes, you are off on today.",
        "date": days["today"],
        "matched_intent": "am_i_off",
    }
    assert start.json() == {
        "answer": "You start at 06:00 on today.",
        "date": days["today"],
        "matched_intent": "my_start_time",
    }
    assert finish.json() == {
        "answer": "You finish at 17:00 on friday.",
        "date": days["friday"],
        "matched_intent": "my_finish_time",
    }


def test_api_ask_coverage_relative_and_week_queries(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    weekday = client.post("/api/ask", json={"question": "who is working on friday"}, headers=headers)
    week = client.post("/api/ask", json={"question": "who is working next week"}, headers=headers)

    assert weekday.json() == {
        "answer": "Boss and Lex are working friday.",
        "date": days["friday"],
        "matched_intent": "who_is_working",
    }
    assert week.status_code == 200
    assert week.json()["matched_intent"] == "who_is_working_week"


def test_api_ask_overlap_canonical_and_alias(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    canonical = client.post("/api/ask", json={"question": "who is Nathan working with today"}, headers=headers)
    alias = client.post("/api/ask", json={"question": "who am i working with tomorrow", "person": "Boss"}, headers=headers)

    assert canonical.json() == {
        "answer": "Boss is working with Lex, Jacqueline, and Tom today.",
        "date": days["today"],
        "matched_intent": "overlap",
    }
    assert alias.json() == {
        "answer": "Boss is working with Lex, Laura, Sam, and Tom tomorrow.",
        "date": days["tomorrow"],
        "matched_intent": "overlap",
    }


def test_api_ask_specific_person_time_on_weekday(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    response = client.post(
        "/api/ask",
        json={"question": "what time is Alex working on friday?"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Lex is working friday from 11:00 to 19:00.",
        "date": days["friday"],
        "matched_intent": "person_shift_time",
    }


def test_api_ask_specific_person_time_alias_and_not_working(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    alias = client.post(
        "/api/ask",
        json={"question": "when is Boss working on friday?"},
        headers=headers,
    )
    off = client.post(
        "/api/ask",
        json={"question": "what time is Debbie working on friday?"},
        headers=headers,
    )

    assert alias.status_code == 200
    assert alias.json() == {
        "answer": "Boss is working friday from 09:00 to 17:00.",
        "date": days["friday"],
        "matched_intent": "person_shift_time",
    }
    assert off.status_code == 200
    assert off.json() == {
        "answer": "Debbie is not working friday.",
        "date": days["friday"],
        "matched_intent": "person_shift_time",
    }


def test_next_shift_and_next_overlap(tmp_path):
    _client, days = _build_client(tmp_path)
    now_value = datetime.fromisoformat(days["today"]).replace(hour=23, minute=30)
    tomorrow_name = datetime.fromisoformat(days["tomorrow"]).strftime("%A")

    next_shift = app_module.build_ask_response("when is my next shift", person="Nathan", now_value=now_value)
    next_overlap = app_module.build_ask_response("when are Alex and Sam next working together", now_value=now_value)
    alias_overlap = app_module.build_ask_response("when am i next working with Lex", person="Boss", now_value=now_value)

    assert next_shift == {
        "answer": f"Your next shift is {days['tomorrow']} from 10:00 to 18:00.",
        "date": days["tomorrow"],
        "matched_intent": "next_shift_for_person",
    }
    assert next_overlap == {
        "answer": f"Lex and Sam next work together on {tomorrow_name} from 12:00 to 16:00.",
        "date": days["tomorrow"],
        "matched_intent": "next_overlap_between_people",
    }
    assert alias_overlap == {
        "answer": f"You next work with Lex on {tomorrow_name} from 12:00 to 18:00.",
        "date": days["tomorrow"],
        "matched_intent": "next_overlap_with_person",
    }


def test_rota_summary_questions(tmp_path):
    client, days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    today = client.post("/api/ask", json={"question": "what is the rota today"}, headers=headers)
    mine = client.post("/api/ask", json={"question": "what is my rota this week", "person": "Nathan"}, headers=headers)

    assert today.status_code == 200
    assert today.json()["matched_intent"] == "rota_summary"
    assert today.json()["date"] == days["today"]

    assert mine.status_code == 200
    assert mine.json()["matched_intent"] == "my_rota_summary"


def test_ambiguous_and_unresolved_fallbacks(tmp_path):
    client, _days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}

    ambiguous = client.post("/api/ask", json={"question": "am i working", "person": "Nathan"}, headers=headers)
    unresolved = client.post("/api/ask", json={"question": "when am i next working with NotARealName", "person": "Nathan"}, headers=headers)

    assert ambiguous.json()["answer"] == "I need a date for that request."
    assert unresolved.json()["answer"] == "I could not resolve that person."


def test_api_shape_and_auth_and_bridge_compatibility(tmp_path):
    client, _days = _build_client(tmp_path)
    headers = {"Authorization": "Bearer test-token"}
    payload = {"question": "who is opening tomorrow?", "person": "Nathan"}

    api = client.post("/api/ask", json=payload, headers=headers)
    bridge = client.post("/api/ha/ask", json=payload, headers=headers)

    assert api.status_code == 200
    assert bridge.status_code == 200
    assert set(api.json().keys()) == {"answer", "date", "matched_intent"}
    assert set(bridge.json().keys()) == {"answer", "date", "matched_intent"}

    unauthorized = client.post("/api/ask", json={"question": "who is working today?"})
    assert unauthorized.status_code == 401
    assert unauthorized.json() == {"error": "unauthorized"}
