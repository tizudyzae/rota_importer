"""Shared pure-Python rota question-answer logic for add-on and HA bridge."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo
import os
import re
import sqlite3

WEEKDAY_INDEX = {
    "monday": 0,
    "mon": 0,
    "tuesday": 1,
    "tue": 1,
    "wednesday": 2,
    "wed": 2,
    "thursday": 3,
    "thu": 3,
    "friday": 4,
    "fri": 4,
    "saturday": 5,
    "sat": 5,
    "sunday": 6,
    "sun": 6,
}
FILLER_WORDS = {
    "please",
    "could",
    "you",
    "me",
    "tell",
    "what",
    "the",
    "a",
    "an",
    "rota",
    "schedule",
    "for",
    "is",
    "are",
    "do",
    "does",
    "on",
}
LEGACY_MANAGEMENT_NAMES = {"samantha", "elizabeth", "joshua", "laura", "nathan"}


@dataclass
class StructuredQuery:
    intent: str
    person: Optional[str]
    target_people: list[str]
    role_filter: Optional[str]
    date_range: tuple[str, str]
    specific_time: Optional[str]
    shift_phase: Optional[str]
    overlap_target: Optional[str]
    summary_scope: Optional[str]
    matched_intent: str
    day_word: str


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


def _resolve_local_now(now_value: Optional[datetime] = None) -> datetime:
    if now_value is not None:
        return now_value

    tz_name = clean_cell(
        os.environ.get("TZ", "")
        or os.environ.get("HA_TIME_ZONE", "")
        or os.environ.get("HASS_TIME_ZONE", "")
    )
    if tz_name:
        try:
            return datetime.now(ZoneInfo(tz_name))
        except Exception:
            pass

    return datetime.now().astimezone()


def _normalize_question(question: str) -> str:
    lowered = clean_cell(question).lower()
    lowered = re.sub(r"[^a-z0-9:\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _remove_filler_words(question_normalized: str) -> str:
    tokens = [token for token in question_normalized.split() if token not in FILLER_WORDS]
    return " ".join(tokens)


def _parse_specific_time(question_normalized: str) -> Optional[str]:
    match = re.search(r"\b(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", question_normalized)
    if not match:
        return None
    hours = int(match.group(1))
    minutes = int(match.group(2) or "00")
    meridian = (match.group(3) or "").lower()
    if meridian:
        if hours == 12:
            hours = 0
        if meridian == "pm":
            hours += 12
    if hours > 23 or minutes > 59:
        return None
    return f"{hours:02d}:{minutes:02d}"


def _extract_date_range(question_normalized: str, now_local: datetime) -> tuple[date, date, str, Optional[str]]:
    today = now_local.date()
    tomorrow = today + timedelta(days=1)

    if "next week" in question_normalized:
        days_until_next_monday = (7 - today.weekday()) % 7
        if days_until_next_monday == 0:
            days_until_next_monday = 7
        start = today + timedelta(days=days_until_next_monday)
        end = start + timedelta(days=6)
        return start, end, "next week", "week"

    if "this weekend" in question_normalized:
        days_until_saturday = (5 - today.weekday()) % 7
        saturday = today + timedelta(days=days_until_saturday)
        return saturday, saturday + timedelta(days=1), "this weekend", "weekend"

    if "tomorrow" in question_normalized:
        return tomorrow, tomorrow, "tomorrow", "day"

    if "tonight" in question_normalized:
        return today, today, "tonight", "day"

    for token in question_normalized.split():
        if token in WEEKDAY_INDEX:
            target = WEEKDAY_INDEX[token]
            delta = (target - today.weekday()) % 7
            target_date = today + timedelta(days=delta)
            return target_date, target_date, token, "day"

    return today, today, "today", "day"


def resolve_question_date(question: str, now_value: Optional[datetime] = None) -> tuple[str, str]:
    now_local = _resolve_local_now(now_value=now_value)
    question_normalized = _normalize_question(question)
    start, _end, day_word, _scope = _extract_date_range(question_normalized, now_local)
    return start.isoformat(), day_word


def parse_ask_intent(question: str) -> str:
    query = normalize_to_structured_query(question=question, person=None)
    return query.matched_intent


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


def _get_latest_upload_id_for_date(db_path: Path, shift_date: str) -> Optional[int]:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT u.id
            FROM uploads u
            WHERE EXISTS (
                SELECT 1
                FROM shifts s
                WHERE s.upload_id = u.id
                  AND s.shift_date = ?
            )
            ORDER BY u.uploaded_at DESC, u.id DESC
            LIMIT 1
            """,
            (shift_date,),
        ).fetchone()
    return row["id"] if row else None


def _get_shifts_in_range(db_path: Path, upload_id: int, start_date: str, end_date: str) -> list[sqlite3.Row]:
    with _connect(db_path) as conn:
        return conn.execute(
            """
            SELECT employee, shift_date, start_time, end_time
            FROM shifts
            WHERE upload_id = ?
              AND shift_date >= ?
              AND shift_date <= ?
            ORDER BY shift_date ASC, row_index ASC, id ASC
            """,
            (upload_id, start_date, end_date),
        ).fetchall()


def _extract_valid_shift_people(day_rows: list[sqlite3.Row]) -> list[str]:
    people: list[str] = []
    for row in day_rows:
        name = clean_cell(row["employee"])
        start_minutes = hhmm_to_minutes(clean_cell(row["start_time"]))
        end_minutes = hhmm_to_minutes(clean_cell(row["end_time"]))
        if not name or start_minutes is None or end_minutes is None or name in people:
            continue
        people.append(name)
    return people


def _is_management_person(name: str, management_terms: set[str]) -> bool:
    normalized = clean_cell(name).lower()
    return any(term in normalized for term in management_terms)


def _get_management_terms() -> set[str]:
    raw = clean_cell(os.environ.get("ASK_MANAGEMENT_NAMES", ""))
    if not raw:
        return set(LEGACY_MANAGEMENT_NAMES)
    configured = {clean_cell(item).lower() for item in raw.split(",") if clean_cell(item)}
    return configured or set(LEGACY_MANAGEMENT_NAMES)


def _resolve_alias_map(db_path: Path) -> dict[str, str]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT alias_preferences FROM app_preferences WHERE singleton_key = 'global'"
        ).fetchone()
    if not row:
        return {}
    try:
        import json

        payload = json.loads(row["alias_preferences"] or "{}")
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    resolved: dict[str, str] = {}
    for canonical, alias in payload.items():
        canonical_clean = sanitize_person_key(str(canonical))
        alias_clean = sanitize_person_key(str(alias))
        if canonical_clean and alias_clean:
            resolved[canonical_clean] = alias_clean
    return resolved


def _resolve_person_lookup(day_rows: list[sqlite3.Row], aliases: dict[str, str]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        if not name:
            continue
        lookup[name.lower()] = name
        alias = aliases.get(name)
        if alias:
            lookup[alias.lower()] = name
    return lookup


def _best_fuzzy_match(candidate: str, choices: list[str]) -> Optional[str]:
    if not candidate:
        return None
    best_name = None
    best_score = 0.0
    candidate_lower = candidate.lower()
    for name in choices:
        score = SequenceMatcher(None, candidate_lower, name.lower()).ratio()
        if score > best_score:
            best_score = score
            best_name = name
    return best_name if best_score >= 0.72 else None


def _resolve_person_name(
    raw_name: str,
    day_rows: list[sqlite3.Row],
    aliases: dict[str, str],
) -> Optional[str]:
    person_clean = sanitize_person_key(raw_name)
    if not person_clean:
        return None

    lookup = _resolve_person_lookup(day_rows, aliases)
    exact_alias = lookup.get(person_clean.lower())
    if exact_alias:
        return exact_alias

    names = sorted({sanitize_person_key(row["employee"]) for row in day_rows if sanitize_person_key(row["employee"])})
    exact_real = next((name for name in names if name.lower() == person_clean.lower()), None)
    if exact_real:
        return exact_real

    return _best_fuzzy_match(person_clean, names)


def _extract_overlap_target(question_normalized: str) -> Optional[str]:
    match = re.search(r"\bwith\s+([a-z][a-z\s']+)$", question_normalized)
    if match:
        return sanitize_person_key(match.group(1))
    match = re.search(r"\bwho\s+is\s+([a-z][a-z\s']+)\s+working\s+with\b", question_normalized)
    if match:
        return sanitize_person_key(match.group(1))
    return None


def normalize_to_structured_query(question: str, person: Optional[str], now_value: Optional[datetime] = None) -> StructuredQuery:
    now_local = _resolve_local_now(now_value=now_value)
    question_normalized = _normalize_question(question)
    question_reduced = _remove_filler_words(question_normalized)

    start_date, end_date, day_word, detected_scope = _extract_date_range(question_normalized, now_local)
    specific_time = _parse_specific_time(question_normalized)

    intent = "unknown"
    matched_intent = "unknown"
    shift_phase: Optional[str] = None
    summary_scope: Optional[str] = None
    overlap_target: Optional[str] = None

    if any(token in question_normalized for token in ["summary", "summarise", "summarize"]):
        intent = "rota_summary"
        summary_scope = "week" if "week" in question_normalized else "day"
        matched_intent = "weekly_rota_summary" if summary_scope == "week" else "daily_rota_summary"
    elif "opening" in question_normalized or "first in" in question_normalized or re.search(r"\bopens?\b", question_normalized):
        intent = "opening"
        shift_phase = "management_led_opening"
        matched_intent = "opening_shift"
    elif "closing" in question_normalized or "last out" in question_normalized or re.search(r"\bcloses?\b", question_normalized):
        intent = "closing"
        shift_phase = "management_led_closing"
        matched_intent = "closing_shift"
    elif "working with" in question_normalized or "overlap" in question_normalized:
        intent = "overlap"
        matched_intent = "who_am_i_working_with_today"
        overlap_target = _extract_overlap_target(question_normalized)
    elif re.search(r"\bam i\b.*\b(working|in|off)\b", question_normalized):
        intent = "am_i_working"
        matched_intent = "am_i_working_today"
    elif re.search(r"\bwho\b", question_reduced) and (
        "working" in question_normalized
        or "on shift" in question_normalized
        or "who is in" in question_normalized
        or specific_time is not None
    ):
        intent = "who_is_working"
        matched_intent = "who_is_working_today"
    elif "who is in" in question_normalized:
        intent = "who_is_working"
        matched_intent = "who_is_working_today"

    return StructuredQuery(
        intent=intent,
        person=sanitize_person_key(person or "") or None,
        target_people=[],
        role_filter=None,
        date_range=(start_date.isoformat(), end_date.isoformat()),
        specific_time=specific_time,
        shift_phase=shift_phase,
        overlap_target=overlap_target,
        summary_scope=summary_scope or detected_scope,
        matched_intent=matched_intent,
        day_word=day_word,
    )


def _display_name(name: str, aliases: dict[str, str]) -> str:
    return aliases.get(name) or name


def _pick_opening_or_closing(
    day_rows: list[sqlite3.Row],
    phase: str,
    management_terms: set[str],
) -> tuple[list[str], Optional[int], bool]:
    is_open = phase in {"management_led_opening", "earliest_start_overall"}
    key_fn = (lambda row: hhmm_to_minutes(clean_cell(row["start_time"]))) if is_open else (lambda row: hhmm_to_minutes(clean_cell(row["end_time"])))

    valid_rows: list[tuple[str, int]] = []
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        minute_value = key_fn(row)
        if not name or minute_value is None:
            continue
        valid_rows.append((name, minute_value))

    if not valid_rows:
        return [], None, False

    target_time = min(item[1] for item in valid_rows) if is_open else max(item[1] for item in valid_rows)
    at_target = [name for name, minute in valid_rows if minute == target_time]
    at_target_unique = sorted(set(at_target), key=lambda x: x.lower())

    managers = [name for name in at_target_unique if _is_management_person(name, management_terms)]
    if phase.startswith("management_led") and managers:
        ordered = managers + [name for name in at_target_unique if name not in managers]
        return ordered, target_time, True

    return at_target_unique, target_time, False


def _people_on_at_time(day_rows: list[sqlite3.Row], minute_mark: int) -> list[str]:
    names: list[str] = []
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        start_minutes = hhmm_to_minutes(clean_cell(row["start_time"]))
        end_minutes = hhmm_to_minutes(clean_cell(row["end_time"]))
        if not name or start_minutes is None or end_minutes is None:
            continue
        if start_minutes <= minute_mark < end_minutes and name not in names:
            names.append(name)
    return names


def _get_overlap_people(day_rows: list[sqlite3.Row], person_name: str) -> tuple[bool, list[str]]:
    roster = {}
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        start_minutes = hhmm_to_minutes(clean_cell(row["start_time"]))
        end_minutes = hhmm_to_minutes(clean_cell(row["end_time"]))
        if not name or start_minutes is None or end_minutes is None:
            continue
        roster[name] = (start_minutes, end_minutes)

    if person_name not in roster:
        return False, []

    start_a, end_a = roster[person_name]
    overlaps = sorted(
        [
            name
            for name, (start_b, end_b) in roster.items()
            if name != person_name and start_a < end_b and end_a > start_b
        ],
        key=str.lower,
    )
    return True, overlaps


def _summarize_period(rows: list[sqlite3.Row], aliases: dict[str, str]) -> str:
    by_day: dict[str, list[str]] = {}
    for row in rows:
        shift_date = clean_cell(row["shift_date"])
        if not shift_date:
            continue
        name = sanitize_person_key(row["employee"])
        if not name:
            continue
        by_day.setdefault(shift_date, [])
        if name not in by_day[shift_date]:
            by_day[shift_date].append(name)

    if not by_day:
        return "No shifts found."

    if len(by_day) == 1:
        day, names = next(iter(by_day.items()))
        return f"{day}: {join_human_names([_display_name(name, aliases) for name in names])}."

    parts = []
    for day in sorted(by_day.keys()):
        names = by_day[day]
        parts.append(f"{day}: {len(names)} on")
    return "; ".join(parts) + "."


def build_ask_response(db_path: str | Path, question: str, person: Optional[str] = None, now_value: Optional[datetime] = None) -> dict:
    db_file = Path(db_path)
    query = normalize_to_structured_query(question=question, person=person, now_value=now_value)

    default_unknown = {
        "answer": "Sorry, I could not understand that rota question. Try adding a day or person.",
        "date": query.date_range[0],
        "matched_intent": "unknown",
    }

    if not db_file.exists():
        return {
            "answer": f"I could not find rota data for {query.day_word}.",
            "date": query.date_range[0],
            "matched_intent": query.matched_intent,
        }

    upload_id = _get_latest_upload_id_for_date(db_file, query.date_range[0]) or _get_latest_upload_id(db_file)
    if not upload_id:
        return {
            "answer": f"I could not find rota data for {query.day_word}.",
            "date": query.date_range[0],
            "matched_intent": query.matched_intent,
        }

    aliases = _resolve_alias_map(db_file)
    management_terms = _get_management_terms()
    rows = _get_shifts_in_range(db_file, upload_id, query.date_range[0], query.date_range[1])
    day_rows = [row for row in rows if clean_cell(row["shift_date"]) == query.date_range[0]]

    if query.intent == "unknown":
        return default_unknown

    if query.intent == "who_is_working":
        if query.specific_time:
            minute_mark = hhmm_to_minutes(query.specific_time)
            people = _people_on_at_time(day_rows, minute_mark) if minute_mark is not None else []
            time_label = datetime.strptime(query.specific_time, "%H:%M").strftime("%-I:%M%p").lower()
            if not people:
                answer = f"No one is on at {time_label} {query.day_word}."
            else:
                answer = f"{join_human_names([_display_name(name, aliases) for name in people])} {'is' if len(people)==1 else 'are'} on at {time_label} {query.day_word}."
            return {"answer": answer, "date": query.date_range[0], "matched_intent": query.matched_intent}

        people = _extract_valid_shift_people(day_rows)
        if not people:
            answer = f"No one is scheduled {query.day_word}."
        else:
            display_people = [_display_name(name, aliases) for name in people]
            answer = f"{join_human_names(display_people)} {'is' if len(display_people)==1 else 'are'} working {query.day_word}."
        return {"answer": answer, "date": query.date_range[0], "matched_intent": query.matched_intent}

    if query.intent == "am_i_working":
        subject = _resolve_person_name(query.person or "", day_rows, aliases)
        if not subject:
            raise ValueError("person is required for this question type")
        people = set(_extract_valid_shift_people(day_rows))
        if subject in people:
            answer = f"Yes, {_display_name(subject, aliases)} is working {query.day_word}."
        else:
            answer = f"No, {_display_name(subject, aliases)} is off {query.day_word}."
        return {"answer": answer, "date": query.date_range[0], "matched_intent": query.matched_intent}

    if query.intent == "overlap":
        subject_raw = query.person or query.overlap_target or ""
        subject = _resolve_person_name(subject_raw, day_rows, aliases)
        if not subject:
            raise ValueError("person is required for this question type")
        person_working, coworkers = _get_overlap_people(day_rows, subject)
        if not person_working:
            answer = f"{_display_name(subject, aliases)} is not scheduled {query.day_word}."
        elif coworkers:
            answer = f"{_display_name(subject, aliases)} is working with {join_human_names([_display_name(name, aliases) for name in coworkers])} {query.day_word}."
        else:
            answer = f"{_display_name(subject, aliases)} is working solo {query.day_word}."
        return {"answer": answer, "date": query.date_range[0], "matched_intent": query.matched_intent}

    if query.intent in {"opening", "closing"}:
        phase = query.shift_phase or ("earliest_start_overall" if query.intent == "opening" else "latest_finish_overall")
        selected, _minute, used_management = _pick_opening_or_closing(day_rows, phase, management_terms)
        if not selected:
            answer = f"I could not find a {query.intent} shift for {query.day_word}."
            return {"answer": answer, "date": query.date_range[0], "matched_intent": query.matched_intent}

        display_selected = [_display_name(name, aliases) for name in selected]
        lead = display_selected[0]
        peers = display_selected[1:]
        verb = "is" if len(display_selected) == 1 else "are"

        if used_management and peers:
            if query.intent == "opening":
                answer = f"{lead} is opening {query.day_word}, with {join_human_names(peers)} starting at the same time."
            else:
                answer = f"{lead} is closing {query.day_word}, with {join_human_names(peers)} finishing at the same time."
        else:
            if query.intent == "opening":
                answer = f"{join_human_names(display_selected)} {verb} opening {query.day_word}."
            else:
                answer = f"{join_human_names(display_selected)} {verb} closing {query.day_word}."
        return {"answer": answer, "date": query.date_range[0], "matched_intent": query.matched_intent}

    if query.intent == "rota_summary":
        scope_label = "week" if query.summary_scope == "week" else "day"
        summary = _summarize_period(rows, aliases)
        return {
            "answer": f"{scope_label.title()} summary: {summary}",
            "date": query.date_range[0],
            "matched_intent": query.matched_intent,
        }

    return default_unknown
