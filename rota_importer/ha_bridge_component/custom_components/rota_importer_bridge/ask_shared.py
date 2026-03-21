"""Shared pure-Python rota question-answer logic for add-on and HA bridge."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import re
import sqlite3


def clean_cell(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\n", " ").split()).strip()


def sanitize_person_key(value: str) -> str:
    return re.sub(r"\s+", " ", clean_cell(value)).strip()


def join_human_names(names: list[str]) -> str:
    cleaned = [clean_cell(name) for name in names if clean_cell(name)]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def hhmm_to_minutes(value: str) -> Optional[int]:
    clean_value = clean_cell(value)
    if not re.fullmatch(r"\d{2}:\d{2}", clean_value):
        return None
    hours, minutes = clean_value.split(":")
    total = int(hours) * 60 + int(minutes)
    if total < 0 or total > 1440:
        return None
    return total


def resolve_question_date(question: str, now_value: Optional[datetime] = None) -> tuple[str, str]:
    now_local = now_value or datetime.now()
    question_clean = clean_cell(question).lower()

    if "tomorrow" in question_clean:
        date_value = now_local.date() + timedelta(days=1)
        return date_value.isoformat(), "tomorrow"

    date_value = now_local.date()
    return date_value.isoformat(), "today"


def parse_ask_intent(question: str) -> str:
    question_clean = clean_cell(question).lower()
    question_normalized = re.sub(r"\s+", " ", question_clean)

    if re.search(r"\bwho\b.*\bam i\b.*\bworking with\b", question_normalized):
        return "who_am_i_working_with_today"

    if "opening" in question_normalized or re.search(r"\bwho\b.*\bopens?\b", question_normalized):
        return "opening_shift"

    if "closing" in question_normalized or re.search(r"\bwho\b.*\bcloses?\b", question_normalized):
        return "closing_shift"

    if "who is working" in question_normalized or "who's working" in question_normalized:
        return "who_is_working_today"

    return "unknown"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _get_latest_upload_id(db_path: Path) -> Optional[int]:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id
            FROM uploads
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    return row["id"] if row else None


def _get_day_shifts(db_path: Path, upload_id: int, shift_date: str) -> list[sqlite3.Row]:
    with _connect(db_path) as conn:
        return conn.execute(
            """
            SELECT employee, start_time, end_time
            FROM shifts
            WHERE upload_id = ? AND shift_date = ?
            ORDER BY row_index ASC, id ASC
            """,
            (upload_id, shift_date),
        ).fetchall()


def _get_coworkers_for_person(db_path: Path, upload_id: int, shift_date: str, person: str) -> tuple[bool, list[str]]:
    day_rows = _get_day_shifts(db_path, upload_id, shift_date)
    person_clean = person.lower()

    person_working = any(clean_cell(row["employee"]).lower() == person_clean for row in day_rows)
    coworkers: list[str] = []
    for row in day_rows:
        name = clean_cell(row["employee"])
        if not name or name.lower() == person_clean or name in coworkers:
            continue
        coworkers.append(name)
    return person_working, coworkers


def _get_opening_people(db_path: Path, upload_id: int, shift_date: str) -> list[str]:
    day_rows = _get_day_shifts(db_path, upload_id, shift_date)
    opening_minutes: Optional[int] = None
    opening_people: list[str] = []

    for row in day_rows:
        start_minutes = hhmm_to_minutes(clean_cell(row["start_time"]))
        employee = clean_cell(row["employee"])
        if start_minutes is None or not employee:
            continue
        if opening_minutes is None or start_minutes < opening_minutes:
            opening_minutes = start_minutes
            opening_people = [employee]
        elif start_minutes == opening_minutes and employee not in opening_people:
            opening_people.append(employee)

    return opening_people


def _get_closing_people(db_path: Path, upload_id: int, shift_date: str) -> list[str]:
    day_rows = _get_day_shifts(db_path, upload_id, shift_date)
    closing_minutes: Optional[int] = None
    closing_people: list[str] = []

    for row in day_rows:
        end_minutes = hhmm_to_minutes(clean_cell(row["end_time"]))
        employee = clean_cell(row["employee"])
        if end_minutes is None or not employee:
            continue
        if closing_minutes is None or end_minutes > closing_minutes:
            closing_minutes = end_minutes
            closing_people = [employee]
        elif end_minutes == closing_minutes and employee not in closing_people:
            closing_people.append(employee)

    return closing_people


def build_ask_response(db_path: str | Path, question: str, person: Optional[str] = None, now_value: Optional[datetime] = None) -> dict:
    db_file = Path(db_path)
    matched_intent = parse_ask_intent(question)
    resolved_date, day_word = resolve_question_date(question, now_value=now_value)
    default_unknown = {
        "answer": "Sorry, I could not understand that rota question.",
        "date": resolved_date,
        "matched_intent": "unknown",
    }

    if not db_file.exists():
        return {
            "answer": f"I could not find rota data for {day_word}.",
            "date": resolved_date,
            "matched_intent": matched_intent,
        }

    upload_id = _get_latest_upload_id(db_file)
    if not upload_id:
        return {
            "answer": f"I could not find rota data for {day_word}.",
            "date": resolved_date,
            "matched_intent": matched_intent,
        }

    if matched_intent == "who_is_working_today":
        day_rows = _get_day_shifts(db_file, upload_id, resolved_date)
        people = []
        for row in day_rows:
            name = clean_cell(row["employee"])
            if name and name not in people:
                people.append(name)
        if not people:
            answer = f"No one is scheduled to work {day_word}."
        elif len(people) == 1:
            answer = f"{people[0]} is working {day_word}."
        else:
            answer = f"{join_human_names(people)} are working {day_word}."
        return {"answer": answer, "date": resolved_date, "matched_intent": matched_intent}

    if matched_intent == "who_am_i_working_with_today":
        person_key = sanitize_person_key(person or "")
        if not person_key:
            raise ValueError("person is required for this question type")

        person_working, coworkers = _get_coworkers_for_person(db_file, upload_id, resolved_date, person_key)
        if person_working:
            if coworkers:
                answer = f"You are working with {join_human_names(coworkers)} {day_word}."
            else:
                answer = f"You are not working with anyone else {day_word}."
        else:
            answer = f"{person_key} is not scheduled to work {day_word}."
        return {"answer": answer, "date": resolved_date, "matched_intent": matched_intent}

    if matched_intent == "opening_shift":
        opening_people = _get_opening_people(db_file, upload_id, resolved_date)
        if not opening_people:
            answer = f"I could not find an opening shift for {day_word}."
        elif len(opening_people) == 1:
            answer = f"{opening_people[0]} is opening {day_word}."
        else:
            answer = f"{join_human_names(opening_people)} are opening {day_word}."
        return {"answer": answer, "date": resolved_date, "matched_intent": matched_intent}

    if matched_intent == "closing_shift":
        closing_people = _get_closing_people(db_file, upload_id, resolved_date)
        if not closing_people:
            answer = f"I could not find a closing shift for {day_word}."
        elif len(closing_people) == 1:
            answer = f"{closing_people[0]} is closing {day_word}."
        else:
            answer = f"{join_human_names(closing_people)} are closing {day_word}."
        return {"answer": answer, "date": resolved_date, "matched_intent": matched_intent}

    return default_unknown
