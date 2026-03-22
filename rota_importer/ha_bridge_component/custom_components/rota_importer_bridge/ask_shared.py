"""Shared pure-Python rota question-answer logic for add-on and HA bridge."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo
import json
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
WEEKDAY_LABELS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
LEGACY_MANAGEMENT_NAMES = {"samantha", "elizabeth", "joshua", "laura", "nathan"}
FILLER_PHRASES = [
    "can you tell me",
    "do you know",
    "let me know",
    "for me",
    "please",
    "actually",
    "then",
]
QUESTION_PREFIXES = ["what is", "what's", "whats", "give me", "tell me"]
MORNING_WINDOW = ("05:00", "11:59")
EVENING_WINDOW = ("17:00", "23:59")
SELF_WORDS = {"i", "me", "my", "i am", "i'm", "am i"}


@dataclass
class DateSelection:
    start_date: date
    end_date: date
    day_word: str
    relative_date: Optional[str]
    summary_scope: str
    explicit: bool


@dataclass
class StructuredQuery:
    intent: str
    person: Optional[str]
    target_people: list[str]
    date: str
    date_range: tuple[str, str]
    weekday: Optional[str]
    relative_date: Optional[str]
    time_window: Optional[str]
    specific_time: Optional[str]
    shift_phase: Optional[str]
    overlap_target: Optional[str]
    summary_scope: Optional[str]
    future_only: bool
    matched_intent: str
    ambiguous_reason: Optional[str] = None


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
    text = clean_cell(value)
    if not re.fullmatch(r"\d{2}:\d{2}", text):
        return None
    hours, minutes = text.split(":")
    total = int(hours) * 60 + int(minutes)
    if total < 0 or total > 1440:
        return None
    return total


def _resolve_local_now(now_value: Optional[datetime] = None) -> datetime:
    if now_value is not None:
        return now_value

    tz_name = clean_cell(os.environ.get("TZ", "") or os.environ.get("HA_TIME_ZONE", "") or os.environ.get("HASS_TIME_ZONE", ""))
    if tz_name:
        try:
            return datetime.now(ZoneInfo(tz_name))
        except Exception:
            pass

    return datetime.now().astimezone()


def _normalize_input(question: str) -> str:
    text = clean_cell(question).lower()
    text = text.replace("who's", "who is")
    text = text.replace("i'm", "i am")
    text = text.replace("today’s", "today")
    text = text.replace("tonight", "this evening")
    text = text.replace("first in", "opening")
    text = text.replace("last out", "closing")
    text = text.replace("opens", "opening")
    text = text.replace("closes", "closing")
    text = text.replace("on shift", "working")
    text = text.replace("share a shift", "working together")
    text = re.sub(r"[^a-z0-9:\s']", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    for filler in FILLER_PHRASES:
        text = re.sub(rf"\b{re.escape(filler)}\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_specific_time(text: str) -> Optional[str]:
    m = re.search(r"\b(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", text)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2) or "00")
    ampm = (m.group(3) or "").lower()
    if ampm:
        if hh == 12:
            hh = 0
        if ampm == "pm":
            hh += 12
    if hh > 23 or mm > 59:
        return None
    return f"{hh:02d}:{mm:02d}"


def _extract_date_selection(text: str, now_local: datetime) -> DateSelection:
    today = now_local.date()
    tomorrow = today + timedelta(days=1)

    if "next week" in text:
        days_until_next_monday = (7 - today.weekday()) % 7
        if days_until_next_monday == 0:
            days_until_next_monday = 7
        start = today + timedelta(days=days_until_next_monday)
        return DateSelection(start, start + timedelta(days=6), "next week", "next week", "week", True)

    if "this week" in text:
        start = today - timedelta(days=today.weekday())
        return DateSelection(start, start + timedelta(days=6), "this week", "this week", "week", True)

    if "this weekend" in text:
        days_until_sat = (5 - today.weekday()) % 7
        sat = today + timedelta(days=days_until_sat)
        return DateSelection(sat, sat + timedelta(days=1), "this weekend", "this weekend", "weekend", True)

    if "tomorrow" in text:
        return DateSelection(tomorrow, tomorrow, "tomorrow", "tomorrow", "day", True)

    if "today" in text or "this morning" in text or "this evening" in text:
        return DateSelection(today, today, "today", "today", "day", True)

    wd_match = re.search(r"\b(next\s+)?(monday|mon|tuesday|tue|wednesday|wed|thursday|thu|friday|fri|saturday|sat|sunday|sun)\b", text)
    if wd_match:
        is_next = wd_match.group(1) is not None
        token = wd_match.group(2)
        target = WEEKDAY_INDEX[token]
        delta = (target - today.weekday()) % 7
        if is_next:
            delta = delta + 7 if delta != 0 else 7
        target_date = today + timedelta(days=delta)
        label = f"next {token}" if is_next else token
        return DateSelection(target_date, target_date, label, token, "day", True)

    return DateSelection(today, today, "today", "today", "day", False)


def resolve_question_date(question: str, now_value: Optional[datetime] = None) -> tuple[str, str]:
    now_local = _resolve_local_now(now_value=now_value)
    text = _normalize_input(question)
    date_selection = _extract_date_selection(text, now_local)
    return date_selection.start_date.isoformat(), date_selection.day_word


def _extract_quoted_people(question: str) -> list[str]:
    return [sanitize_person_key(item) for item in re.findall(r'"([^"]+)"', question) if sanitize_person_key(item)]


def _extract_overlap_people(raw_question: str, normalized: str) -> tuple[Optional[str], list[str]]:
    quoted = _extract_quoted_people(raw_question)
    if len(quoted) >= 2:
        return None, quoted[:2]
    if len(quoted) == 1:
        return quoted[0], []

    two = re.search(
        r"\b(?:when\s+(?:are|do)\s+)?([a-z][a-z\s']+?)\s+and\s+([a-z][a-z\s']+?)\s+next\s+(?:working|work)\s+together\b",
        normalized,
    )
    if two:
        return None, [sanitize_person_key(two.group(1)), sanitize_person_key(two.group(2))]

    nww = re.search(r"\bwho\s+is\s+([a-z][a-z\s']+?)\s+working\s+with(?:\s+(?:today|tomorrow|on\s+\w+|this\s+\w+))?\b", normalized)
    if nww:
        return sanitize_person_key(nww.group(1)), []

    with_match = re.search(r"\bwith\s+([a-z][a-z\s']+?)(?:\s+(?:today|tomorrow|this morning|this evening|morning|evening|on\s+\w+))?$", normalized)
    if with_match:
        return sanitize_person_key(with_match.group(1)), []

    return None, []


def _extract_named_person_for_shift_time(normalized: str) -> Optional[str]:
    patterns = [
        r"\b(?:what time|when)\s+is\s+([a-z][a-z\s']+?)\s+(?:working|in|on)\b",
        r"\b(?:what time|when)\s+does\s+([a-z][a-z\s']+?)\s+(?:work|start|finish)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            return sanitize_person_key(match.group(1))
    return None


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _get_latest_upload_id(db_path: Path) -> Optional[int]:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT id FROM uploads ORDER BY uploaded_at DESC, id DESC LIMIT 1").fetchone()
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
            SELECT employee, shift_date, start_time, end_time, row_index, id
            FROM shifts
            WHERE upload_id = ?
              AND shift_date >= ?
              AND shift_date <= ?
            ORDER BY shift_date ASC, row_index ASC, id ASC
            """,
            (upload_id, start_date, end_date),
        ).fetchall()


def _get_shifts_from_date(db_path: Path, upload_id: int, start_date: str) -> list[sqlite3.Row]:
    with _connect(db_path) as conn:
        return conn.execute(
            """
            SELECT employee, shift_date, start_time, end_time, row_index, id
            FROM shifts
            WHERE upload_id = ?
              AND shift_date >= ?
            ORDER BY shift_date ASC, row_index ASC, id ASC
            """,
            (upload_id, start_date),
        ).fetchall()


def _resolve_alias_map(db_path: Path) -> dict[str, str]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        row = conn.execute("SELECT alias_preferences FROM app_preferences WHERE singleton_key = 'global'").fetchone()
    if not row:
        return {}
    try:
        payload = json.loads(row["alias_preferences"] or "{}")
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    resolved: dict[str, str] = {}
    for canonical, alias in payload.items():
        canonical_text = str(canonical).strip()
        canonical_key = re.sub(r"^(raw:|clean:)", "", canonical_text, flags=re.IGNORECASE)
        canonical_clean = sanitize_person_key(canonical_key)
        alias_clean = sanitize_person_key(str(alias))
        if canonical_clean and alias_clean:
            resolved[canonical_clean] = alias_clean
    return resolved


def _resolve_person_lookup(rows: list[sqlite3.Row], aliases: dict[str, str]) -> tuple[dict[str, str], dict[str, str], list[str]]:
    alias_to_canonical: dict[str, str] = {}
    canonical_lookup: dict[str, str] = {}
    names: list[str] = []
    for row in rows:
        name = sanitize_person_key(row["employee"])
        if not name:
            continue
        key = name.lower()
        canonical_lookup[key] = name
        if name not in names:
            names.append(name)
        alias = aliases.get(name)
        if alias:
            alias_to_canonical[alias.lower()] = name
        elif lowered := name.lower():
            alias_from_normalized_key = aliases.get(lowered)
            if alias_from_normalized_key:
                alias_to_canonical[alias_from_normalized_key.lower()] = name
    return alias_to_canonical, canonical_lookup, names


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


def _resolve_person_name(raw_name: str, rows: list[sqlite3.Row], aliases: dict[str, str]) -> Optional[str]:
    person_clean = sanitize_person_key(raw_name)
    if not person_clean:
        return None
    alias_lookup, canonical_lookup, names = _resolve_person_lookup(rows, aliases)

    exact_alias = alias_lookup.get(person_clean.lower())
    if exact_alias:
        return exact_alias

    exact_canonical = canonical_lookup.get(person_clean.lower())
    if exact_canonical:
        return exact_canonical

    return _best_fuzzy_match(person_clean, names)


def _display_name(name: str, aliases: dict[str, str]) -> str:
    return aliases.get(name) or aliases.get(name.lower()) or name


def _extract_valid_shift_people(day_rows: list[sqlite3.Row]) -> list[str]:
    seen: set[str] = set()
    people: list[str] = []
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        start_m = hhmm_to_minutes(clean_cell(row["start_time"]))
        end_m = hhmm_to_minutes(clean_cell(row["end_time"]))
        if not name or start_m is None or end_m is None or name in seen:
            continue
        seen.add(name)
        people.append(name)
    return people


def _in_time_window(start_time: str, end_time: str, window_start: str, window_end: str) -> bool:
    shift_start = hhmm_to_minutes(start_time)
    shift_end = hhmm_to_minutes(end_time)
    win_start = hhmm_to_minutes(window_start)
    win_end = hhmm_to_minutes(window_end)
    if shift_start is None or shift_end is None or win_start is None or win_end is None:
        return False
    return shift_start <= win_end and shift_end > win_start


def _people_in_named_window(day_rows: list[sqlite3.Row], target_window: str) -> list[str]:
    window = MORNING_WINDOW if target_window == "morning" else EVENING_WINDOW
    people: list[str] = []
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        if not name:
            continue
        if _in_time_window(clean_cell(row["start_time"]), clean_cell(row["end_time"]), window[0], window[1]) and name not in people:
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


def _pick_open_or_close(day_rows: list[sqlite3.Row], intent: str, management_terms: set[str]) -> tuple[list[str], bool]:
    is_open = intent == "opening"
    chosen_time: Optional[int] = None
    candidates: list[tuple[str, int]] = []
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        mm = hhmm_to_minutes(clean_cell(row["start_time"] if is_open else row["end_time"]))
        if not name or mm is None:
            continue
        candidates.append((name, mm))
        if chosen_time is None:
            chosen_time = mm
        else:
            chosen_time = min(chosen_time, mm) if is_open else max(chosen_time, mm)
    if chosen_time is None:
        return [], False

    tied = sorted({name for name, mm in candidates if mm == chosen_time}, key=str.lower)
    managers = [name for name in tied if _is_management_person(name, management_terms)]
    if managers:
        ordered = managers + [name for name in tied if name not in managers]
        return ordered, len(managers) > 0 and len(tied) > 1
    return tied, False


def _person_shift_for_day(day_rows: list[sqlite3.Row], person_name: str) -> Optional[tuple[str, str]]:
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        if name != person_name:
            continue
        start = clean_cell(row["start_time"])
        end = clean_cell(row["end_time"])
        if hhmm_to_minutes(start) is None or hhmm_to_minutes(end) is None:
            continue
        return start, end
    return None


def _get_overlap_people(day_rows: list[sqlite3.Row], person_name: str) -> tuple[bool, list[str]]:
    roster: dict[str, tuple[int, int]] = {}
    for row in day_rows:
        name = sanitize_person_key(row["employee"])
        start = hhmm_to_minutes(clean_cell(row["start_time"]))
        end = hhmm_to_minutes(clean_cell(row["end_time"]))
        if not name or start is None or end is None:
            continue
        roster[name] = (start, end)
    if person_name not in roster:
        return False, []
    a_start, a_end = roster[person_name]
    overlaps = sorted(
        [name for name, (b_start, b_end) in roster.items() if name != person_name and a_start < b_end and a_end > b_start],
        key=str.lower,
    )
    return True, overlaps


def _find_next_shift(rows: list[sqlite3.Row], person_name: str, now_local: datetime) -> Optional[sqlite3.Row]:
    now_mark = now_local.strftime("%Y-%m-%d %H:%M")
    for row in rows:
        name = sanitize_person_key(row["employee"])
        shift_date = clean_cell(row["shift_date"])
        start_time = clean_cell(row["start_time"])
        if name != person_name or not shift_date or hhmm_to_minutes(start_time) is None:
            continue
        if f"{shift_date} {start_time}" > now_mark:
            return row
    return None


def _find_next_overlap(rows: list[sqlite3.Row], person_a: str, person_b: str, now_local: datetime) -> Optional[tuple[str, str, str]]:
    by_day: dict[str, dict[str, list[tuple[int, int]]]] = {}
    for row in rows:
        shift_date = clean_cell(row["shift_date"])
        name = sanitize_person_key(row["employee"])
        start = hhmm_to_minutes(clean_cell(row["start_time"]))
        end = hhmm_to_minutes(clean_cell(row["end_time"]))
        if not shift_date or not name or start is None or end is None:
            continue
        by_day.setdefault(shift_date, {}).setdefault(name, []).append((start, end))

    now_mark = now_local.strftime("%Y-%m-%d %H:%M")
    for day in sorted(by_day.keys()):
        shifts = by_day[day]
        if person_a not in shifts or person_b not in shifts:
            continue
        best_start = None
        best_end = None
        for a_start, a_end in shifts[person_a]:
            for b_start, b_end in shifts[person_b]:
                overlap_start = max(a_start, b_start)
                overlap_end = min(a_end, b_end)
                if overlap_start >= overlap_end:
                    continue
                overlap_start_text = f"{overlap_start // 60:02d}:{overlap_start % 60:02d}"
                if f"{day} {overlap_start_text}" <= now_mark:
                    continue
                if best_start is None or overlap_start < best_start:
                    best_start = overlap_start
                    best_end = overlap_end
        if best_start is not None and best_end is not None:
            return day, f"{best_start // 60:02d}:{best_start % 60:02d}", f"{best_end // 60:02d}:{best_end % 60:02d}"
    return None


def _summarize_period(rows: list[sqlite3.Row], aliases: dict[str, str], include_times: bool = False) -> str:
    by_day: dict[str, list[tuple[str, str, str]]] = {}
    for row in rows:
        shift_date = clean_cell(row["shift_date"])
        name = sanitize_person_key(row["employee"])
        start = clean_cell(row["start_time"])
        end = clean_cell(row["end_time"])
        if not shift_date or not name:
            continue
        by_day.setdefault(shift_date, [])
        by_day[shift_date].append((name, start, end))

    if not by_day:
        return "No shifts found."

    if include_times:
        parts = []
        for day in sorted(by_day.keys()):
            spans = [f"{_display_name(name, aliases)} {start}-{end}" for name, start, end in by_day[day] if hhmm_to_minutes(start) is not None]
            if spans:
                parts.append(f"{day}: {', '.join(spans)}")
        return "; ".join(parts) + "."

    parts = []
    for day in sorted(by_day.keys()):
        people = []
        seen = set()
        for name, _start, _end in by_day[day]:
            if name not in seen:
                seen.add(name)
                people.append(_display_name(name, aliases))
        parts.append(f"{day}: {join_human_names(people)}")
    return "; ".join(parts) + "."


def _weekday_label_from_date(day: str) -> str:
    dt = datetime.strptime(day, "%Y-%m-%d").date()
    return WEEKDAY_LABELS[dt.weekday()]


def normalize_to_structured_query(question: str, person: Optional[str], now_value: Optional[datetime] = None) -> StructuredQuery:
    now_local = _resolve_local_now(now_value=now_value)
    normalized = _normalize_input(question)
    date_selection = _extract_date_selection(normalized, now_local)
    specific_time = _parse_specific_time(normalized)

    window: Optional[str] = None
    if "morning" in normalized:
        window = "morning"
    elif "evening" in normalized:
        window = "evening"

    future_only = "next" in normalized and any(token in normalized for token in ["work", "shift", "overlap", "together", "with"])

    intent = "unknown"
    matched = "unknown"
    overlap_target: Optional[str] = None
    target_people: list[str] = []
    shift_phase: Optional[str] = None
    ambiguous_reason: Optional[str] = None

    has_self = any(phrase in normalized for phrase in ["am i", "i am", "i working", "my rota", "do i"])
    asks_who = normalized.startswith("who") or " who " in f" {normalized} "

    if "next" in normalized and any(token in normalized for token in ["working together", "next overlap", "next working with", "next work with"]):
        overlap_target, target_people = _extract_overlap_people(question, normalized)
        if len(target_people) == 2:
            intent = "next_overlap_between_people"
            matched = "next_overlap_between_people"
        else:
            intent = "next_overlap_with_person"
            matched = "next_overlap_with_person"
    elif re.search(r"\bwhen\s+(?:am|do)\s+i\s+next\s+(?:working|work)\b", normalized) or "my next shift" in normalized or "next shift" in normalized:
        intent = "next_shift_for_person"
        matched = "next_shift_for_person"
    elif "my rota" in normalized or ("my shifts" in normalized):
        intent = "my_rota_summary"
        matched = "my_rota_summary"
    elif any(prefix in normalized for prefix in QUESTION_PREFIXES) and "rota" in normalized:
        intent = "rota_summary"
        matched = "rota_summary"
    elif "opening" in normalized:
        intent = "opening"
        matched = "opening_shift"
        shift_phase = "management_led_opening"
    elif "closing" in normalized:
        intent = "closing"
        matched = "closing_shift"
        shift_phase = "management_led_closing"
    elif _extract_named_person_for_shift_time(normalized) and any(token in normalized for token in ["what time", "when"]):
        intent = "person_shift_time"
        matched = "person_shift_time"
        overlap_target = _extract_named_person_for_shift_time(normalized)
    elif ("working with" in normalized or re.search(r"\bwho\s+am\s+i\s+with\b", normalized) or re.search(r"\bwho\s+is\s+[a-z].*\s+working\s+with\b", normalized)):
        intent = "overlap"
        matched = "overlap"
        overlap_target, target_people = _extract_overlap_people(question, normalized)
    elif has_self and any(token in normalized for token in ["start", "in on", "what time am i in"]):
        intent = "my_start_time"
        matched = "my_start_time"
    elif has_self and any(token in normalized for token in ["finish", "end", "out"]):
        intent = "my_finish_time"
        matched = "my_finish_time"
    elif has_self and "what shift" in normalized:
        intent = "my_shift_detail"
        matched = "my_shift_detail"
    elif has_self and any(token in normalized for token in ["am i off", "i off"]):
        intent = "am_i_off"
        matched = "am_i_off"
    elif has_self and any(token in normalized for token in ["am i working", "am i in", "am i on", "i working"]):
        intent = "am_i_working"
        matched = "am_i_working"
    elif asks_who and any(token in normalized for token in ["working", "in ", "on ", "on at"]):
        intent = "who_is_working"
        if window:
            matched = f"who_is_working_{window}"
        elif date_selection.summary_scope == "week":
            matched = "who_is_working_week"
        else:
            matched = "who_is_working"
    elif "rota" in normalized:
        intent = "rota_summary"
        matched = "rota_summary"

    if intent in {"my_rota_summary", "next_shift_for_person", "am_i_working", "am_i_off", "my_shift_detail", "my_start_time", "my_finish_time"} and not date_selection.explicit and intent not in {"next_shift_for_person", "my_rota_summary"}:
        ambiguous_reason = "I need a date for that request."

    return StructuredQuery(
        intent=intent,
        person=sanitize_person_key(person or "") or None,
        target_people=target_people,
        date=date_selection.start_date.isoformat(),
        date_range=(date_selection.start_date.isoformat(), date_selection.end_date.isoformat()),
        weekday=date_selection.relative_date,
        relative_date=date_selection.relative_date,
        time_window=window,
        specific_time=specific_time,
        shift_phase=shift_phase,
        overlap_target=overlap_target,
        summary_scope=date_selection.summary_scope,
        future_only=future_only,
        matched_intent=matched,
        ambiguous_reason=ambiguous_reason,
    )


def parse_ask_intent(question: str) -> str:
    return normalize_to_structured_query(question=question, person=None).matched_intent


def build_ask_response(db_path: str | Path, question: str, person: Optional[str] = None, now_value: Optional[datetime] = None) -> dict:
    now_local = _resolve_local_now(now_value=now_value)
    query = normalize_to_structured_query(question=question, person=person, now_value=now_value)
    db_file = Path(db_path)

    default_unknown = {
        "answer": "Sorry, I could not understand that rota question. Try adding a day or person.",
        "date": query.date,
        "matched_intent": "unknown",
    }

    if query.ambiguous_reason:
        return {"answer": query.ambiguous_reason, "date": query.date, "matched_intent": query.matched_intent}

    if query.intent == "unknown":
        return default_unknown

    if not db_file.exists():
        return {"answer": f"I could not find rota data for {query.relative_date or 'that day' }.", "date": query.date, "matched_intent": query.matched_intent}

    upload_id = _get_latest_upload_id_for_date(db_file, query.date) or _get_latest_upload_id(db_file)
    if not upload_id:
        return {"answer": f"I could not find rota data for {query.relative_date or 'that day'}.", "date": query.date, "matched_intent": query.matched_intent}

    aliases = _resolve_alias_map(db_file)
    management_terms = _get_management_terms()

    rows = _get_shifts_in_range(db_file, upload_id, query.date_range[0], query.date_range[1])
    day_rows = [r for r in rows if clean_cell(r["shift_date"]) == query.date]
    all_rows = _get_shifts_from_date(db_file, upload_id, "0001-01-01")
    future_rows = _get_shifts_from_date(db_file, upload_id, now_local.date().isoformat())
    label = query.relative_date or _weekday_label_from_date(query.date)

    if query.intent == "who_is_working":
        if query.summary_scope in {"week", "weekend"}:
            answer = _summarize_period(rows, aliases)
            return {"answer": answer, "date": query.date, "matched_intent": query.matched_intent}

        if query.time_window:
            people = _people_in_named_window(day_rows, query.time_window)
            if not people:
                return {"answer": f"No one is scheduled {label}.", "date": query.date, "matched_intent": query.matched_intent}
            display = [_display_name(n, aliases) for n in people]
            return {
                "answer": f"{join_human_names(display)} {'is' if len(display)==1 else 'are'} working {label}.",
                "date": query.date,
                "matched_intent": query.matched_intent,
            }

        if query.specific_time:
            mark = hhmm_to_minutes(query.specific_time)
            names: list[str] = []
            for row in day_rows:
                name = sanitize_person_key(row["employee"])
                start = hhmm_to_minutes(clean_cell(row["start_time"]))
                end = hhmm_to_minutes(clean_cell(row["end_time"]))
                if name and start is not None and end is not None and mark is not None and start <= mark < end and name not in names:
                    names.append(name)
            if not names:
                return {"answer": f"No one is on at {query.specific_time} {label}.", "date": query.date, "matched_intent": query.matched_intent}
            display = [_display_name(n, aliases) for n in names]
            return {"answer": f"{join_human_names(display)} {'is' if len(display)==1 else 'are'} on at {query.specific_time} {label}.", "date": query.date, "matched_intent": query.matched_intent}

        people = _extract_valid_shift_people(day_rows)
        if not people:
            return {"answer": f"No one is scheduled {label}.", "date": query.date, "matched_intent": query.matched_intent}
        display = [_display_name(n, aliases) for n in people]
        return {"answer": f"{join_human_names(display)} {'is' if len(display)==1 else 'are'} working {label}.", "date": query.date, "matched_intent": query.matched_intent}

    if query.intent in {"opening", "closing"}:
        selected, used_management = _pick_open_or_close(day_rows, query.intent, management_terms)
        if not selected:
            return {"answer": f"I could not find a {query.intent} shift for {label}.", "date": query.date, "matched_intent": query.matched_intent}
        display = [_display_name(n, aliases) for n in selected]
        if used_management and len(display) > 1:
            lead = display[0]
            peers = join_human_names(display[1:])
            if query.intent == "opening":
                answer = f"{lead} is opening {label}, with {peers} starting at the same time."
            else:
                answer = f"{lead} is closing {label}, with {peers} finishing at the same time."
        else:
            verb = "is" if len(display) == 1 else "are"
            answer = f"{join_human_names(display)} {verb} {query.intent} {label}."
        return {"answer": answer, "date": query.date, "matched_intent": query.matched_intent}

    if query.intent in {"am_i_working", "am_i_off", "my_shift_detail", "my_start_time", "my_finish_time"}:
        subject = _resolve_person_name(query.person or "", day_rows, aliases)
        if not subject:
            raise ValueError("person is required for this question type")
        shift = _person_shift_for_day(day_rows, subject)
        is_working = shift is not None

        if query.intent == "am_i_working":
            if is_working:
                return {"answer": f"You are working on {label} from {shift[0]} to {shift[1]}.", "date": query.date, "matched_intent": query.matched_intent}
            return {"answer": f"You are not working on {label}.", "date": query.date, "matched_intent": query.matched_intent}

        if query.intent == "am_i_off":
            if is_working:
                return {"answer": f"No, you are working on {label} from {shift[0]} to {shift[1]}.", "date": query.date, "matched_intent": query.matched_intent}
            return {"answer": f"Yes, you are off on {label}.", "date": query.date, "matched_intent": query.matched_intent}

        if not is_working:
            return {"answer": f"You are not working on {label}.", "date": query.date, "matched_intent": query.matched_intent}

        if query.intent == "my_shift_detail":
            return {"answer": f"You are on shift on {label} from {shift[0]} to {shift[1]}.", "date": query.date, "matched_intent": query.matched_intent}
        if query.intent == "my_start_time":
            return {"answer": f"You start at {shift[0]} on {label}.", "date": query.date, "matched_intent": query.matched_intent}
        return {"answer": f"You finish at {shift[1]} on {label}.", "date": query.date, "matched_intent": query.matched_intent}

    if query.intent == "person_shift_time":
        requested_name = query.overlap_target or ""
        subject = _resolve_person_name(requested_name, all_rows, aliases)
        if not subject:
            return {"answer": "I could not resolve that person.", "date": query.date, "matched_intent": query.matched_intent}
        shift = _person_shift_for_day(day_rows, subject)
        if not shift:
            return {
                "answer": f"{_display_name(subject, aliases)} is not working {label}.",
                "date": query.date,
                "matched_intent": query.matched_intent,
            }
        return {
            "answer": f"{_display_name(subject, aliases)} is working {label} from {shift[0]} to {shift[1]}.",
            "date": query.date,
            "matched_intent": query.matched_intent,
        }

    if query.intent == "next_shift_for_person":
        subject = _resolve_person_name(query.person or "", all_rows, aliases)
        if not subject:
            return {"answer": "I could not tell who you are. Add a person or current-user context.", "date": query.date, "matched_intent": query.matched_intent}
        next_shift = _find_next_shift(future_rows, subject, now_local)
        if not next_shift:
            return {"answer": "I could not find a future shift for you.", "date": query.date, "matched_intent": query.matched_intent}
        shift_date = clean_cell(next_shift["shift_date"])
        start = clean_cell(next_shift["start_time"])
        end = clean_cell(next_shift["end_time"])
        return {"answer": f"Your next shift is {shift_date} from {start} to {end}.", "date": shift_date, "matched_intent": query.matched_intent}

    if query.intent == "overlap":
        subject_raw = query.overlap_target or query.person or ""
        subject = _resolve_person_name(subject_raw, day_rows, aliases)
        if not subject and query.person:
            subject = _resolve_person_name(query.person, day_rows, aliases)
        if not subject:
            raise ValueError("person is required for this question type")
        person_working, coworkers = _get_overlap_people(day_rows, subject)
        if not person_working:
            return {"answer": f"{_display_name(subject, aliases)} is not scheduled {label}.", "date": query.date, "matched_intent": query.matched_intent}
        if not coworkers:
            return {"answer": f"{_display_name(subject, aliases)} is working solo {label}.", "date": query.date, "matched_intent": query.matched_intent}
        display = [_display_name(n, aliases) for n in coworkers]
        return {"answer": f"{_display_name(subject, aliases)} is working with {join_human_names(display)} {label}.", "date": query.date, "matched_intent": query.matched_intent}

    if query.intent in {"next_overlap_with_person", "next_overlap_between_people"}:
        if query.intent == "next_overlap_between_people":
            if len(query.target_people) != 2:
                return {"answer": "Please name the two people for the overlap check.", "date": query.date, "matched_intent": query.matched_intent}
            first = _resolve_person_name(query.target_people[0], all_rows, aliases)
            second = _resolve_person_name(query.target_people[1], all_rows, aliases)
        else:
            first = _resolve_person_name(query.person or "", all_rows, aliases)
            second = _resolve_person_name(query.overlap_target or "", all_rows, aliases)
            if not first:
                return {"answer": "I could not tell who you are. Add a person or current-user context.", "date": query.date, "matched_intent": query.matched_intent}
            if not second:
                return {"answer": "I could not resolve that person.", "date": query.date, "matched_intent": query.matched_intent}

        if not first or not second:
            return {"answer": "I could not confidently resolve both people.", "date": query.date, "matched_intent": query.matched_intent}

        overlap = _find_next_overlap(future_rows, first, second, now_local)
        if not overlap:
            if query.intent == "next_overlap_with_person":
                return {"answer": f"I could not find a future overlap for you and {_display_name(second, aliases)}.", "date": query.date, "matched_intent": query.matched_intent}
            return {"answer": f"I could not find a future overlap for {_display_name(first, aliases)} and {_display_name(second, aliases)}.", "date": query.date, "matched_intent": query.matched_intent}

        overlap_date, start, end = overlap
        weekday = _weekday_label_from_date(overlap_date)
        if query.intent == "next_overlap_with_person":
            return {"answer": f"You next work with {_display_name(second, aliases)} on {weekday} from {start} to {end}.", "date": overlap_date, "matched_intent": query.matched_intent}
        return {"answer": f"{_display_name(first, aliases)} and {_display_name(second, aliases)} next work together on {weekday} from {start} to {end}.", "date": overlap_date, "matched_intent": query.matched_intent}

    if query.intent in {"rota_summary", "my_rota_summary"}:
        if query.intent == "my_rota_summary":
            subject = _resolve_person_name(query.person or "", rows, aliases)
            if not subject:
                raise ValueError("person is required for this question type")
            mine = [r for r in rows if sanitize_person_key(r["employee"]) == subject]
            summary = _summarize_period(mine, aliases, include_times=True)
        else:
            summary = _summarize_period(rows, aliases)
        return {"answer": summary, "date": query.date, "matched_intent": query.matched_intent}

    return default_unknown
