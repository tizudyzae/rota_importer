import csv
import json
import os
import re
import shutil
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urlerror
from urllib import request as urlrequest

import pdfplumber
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
LOGGANNT_DIR = STATIC_DIR / "loggannt"

ADDON_DATA_DIR = Path("/config")
UPLOAD_DIR = ADDON_DATA_DIR / "uploads"
EXPORT_DIR = ADDON_DATA_DIR / "exports"
DB_PATH = Path("/homeassistant/rota.db")

HA_CORE_API_BASE = "http://supervisor/core/api"

app = FastAPI(title="Rota PDF Importer")

EMPLOYEE_ID_RE = re.compile(r"\((\d+)\)")
DATE_HEADER_RE = re.compile(r"^([A-Za-z]{3})\((\d{2})/(\d{2})\)$")
TIME_RANGE_RE = re.compile(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})")
DAY_ORDER = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
MANAGEMENT_NAMES = {"samantha", "elizabeth", "joshua", "laura", "nathan"}
AUTO_NOTIFY_POLL_SECONDS = 60
AUTO_NOTIFY_GRACE_MINUTES = 5

_auto_notify_stop = threading.Event()
_auto_notify_thread: Optional[threading.Thread] = None


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def clean_cell(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\n", " ").split()).strip()


def normalize_table_row(row: List[str], column_count: int) -> List[str]:
    cleaned = [clean_cell(x) for x in row]
    if len(cleaned) < column_count:
        cleaned += [""] * (column_count - len(cleaned))
    elif len(cleaned) > column_count:
        cleaned = cleaned[:column_count]
    return cleaned


def sanitize_bool(value: Any, default: bool = False) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "on"} else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    return 1 if default else 0


def sanitize_text(value: Any, max_len: int = 500, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()[:max_len]


def sanitize_time_hhmm(value: Any, default: str = "07:00") -> str:
    text = sanitize_text(value, max_len=5, default=default)
    if re.fullmatch(r"\d{2}:\d{2}", text):
        return text
    return default


def sanitize_subject_list(value: Any) -> str:
    if not isinstance(value, list):
        return json.dumps([])

    cleaned: list[str] = []
    for item in value:
        name = sanitize_text(item, max_len=120, default="")
        if name and name not in cleaned:
            cleaned.append(name)

    return json.dumps(cleaned)


def sanitize_subject_service_map(value: Any) -> str:
    if not isinstance(value, dict):
        return "{}"

    cleaned: dict[str, str] = {}
    for key, raw_service in value.items():
        subject = sanitize_text(key, max_len=120, default="")
        service = sanitize_text(raw_service, max_len=200, default="")
        if subject and service:
            cleaned[subject] = service

    return json.dumps(cleaned)


def sanitize_subject_critical_map(value: Any) -> str:
    if not isinstance(value, dict):
        return "{}"

    cleaned: dict[str, int] = {}
    for key, raw_enabled in value.items():
        subject = sanitize_text(key, max_len=120, default="")
        if not subject:
            continue
        cleaned[subject] = sanitize_bool(raw_enabled, default=False)

    return json.dumps(cleaned)


def sanitize_weekdays(value: Any) -> str:
    if not isinstance(value, list):
        return json.dumps(DAY_ORDER)

    cleaned = []
    for item in value:
        day = sanitize_text(item, max_len=3).lower()
        if day in DAY_ORDER and day not in cleaned:
            cleaned.append(day)

    if not cleaned:
        cleaned = DAY_ORDER.copy()

    return json.dumps(cleaned)


def parse_json_object(raw: str, fallback: dict) -> dict:
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else fallback
    except Exception:
        return fallback


def parse_json_list(raw: str, fallback: list) -> list:
    try:
        parsed = json.loads(raw or "[]")
        return parsed if isinstance(parsed, list) else fallback
    except Exception:
        return fallback


def init_db() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_filename TEXT NOT NULL,
                stored_filename TEXT NOT NULL,
                uploaded_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                upload_id INTEGER NOT NULL,
                employee TEXT NOT NULL,
                day_name TEXT NOT NULL,
                day_header TEXT NOT NULL,
                shift_date TEXT,
                raw_cell TEXT,
                start_time TEXT,
                end_time TEXT,
                total_hours TEXT,
                row_index INTEGER NOT NULL,
                FOREIGN KEY(upload_id) REFERENCES uploads(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_preferences (
                singleton_key TEXT PRIMARY KEY,
                color_preferences TEXT NOT NULL,
                alias_preferences TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_preferences (
                device_id TEXT PRIMARY KEY,
                color_preferences TEXT NOT NULL,
                alias_preferences TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                enabled INTEGER NOT NULL DEFAULT 1,
                notify_before_end_enabled INTEGER NOT NULL DEFAULT 0,
                subject_names_json TEXT NOT NULL DEFAULT '[]',
                subject_service_map_json TEXT NOT NULL DEFAULT '{}',
                weekdays_json TEXT NOT NULL DEFAULT '["sun","mon","tue","wed","thu","fri","sat"]',
                title_template TEXT NOT NULL DEFAULT 'Your rota for today',
                message_template TEXT NOT NULL DEFAULT 'You are working {{ shift }} with: {{ coworkers }}.',
                sound TEXT NOT NULL DEFAULT '',
                image_url TEXT NOT NULL DEFAULT '',
                extra_data_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_dispatch_log (
                dispatch_key TEXT PRIMARY KEY,
                dispatched_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_debug_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                subject_name TEXT NOT NULL,
                notify_service TEXT NOT NULL,
                trigger_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(notification_settings)").fetchall()
        }
        if "subject_names_json" not in columns:
            conn.execute(
                "ALTER TABLE notification_settings ADD COLUMN subject_names_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "notify_before_end_enabled" not in columns:
            conn.execute(
                "ALTER TABLE notification_settings ADD COLUMN notify_before_end_enabled INTEGER NOT NULL DEFAULT 0"
            )
        if "subject_service_map_json" not in columns:
            conn.execute(
                "ALTER TABLE notification_settings ADD COLUMN subject_service_map_json TEXT NOT NULL DEFAULT '{}'"
            )
        if "subject_critical_map_json" not in columns:
            conn.execute(
                "ALTER TABLE notification_settings ADD COLUMN subject_critical_map_json TEXT NOT NULL DEFAULT '{}'"
            )

        if "subject_name" in columns and "notify_service" in columns:
            legacy_row = conn.execute(
                """
                SELECT id, subject_name, notify_service, subject_names_json, subject_service_map_json
                FROM notification_settings
                WHERE id = 1
                """
            ).fetchone()
            if legacy_row:
                subject_names_existing = parse_json_list(legacy_row["subject_names_json"], [])
                service_map_existing = parse_json_object(legacy_row["subject_service_map_json"], {})
                has_new_values = bool(subject_names_existing) and bool(service_map_existing)
                if not has_new_values:
                    legacy_subject = clean_cell(legacy_row["subject_name"])
                    legacy_service = clean_cell(legacy_row["notify_service"])
                    subject_names_json = json.dumps([legacy_subject] if legacy_subject else [])
                    subject_service_map_json = json.dumps({legacy_subject: legacy_service}) if (legacy_subject and legacy_service) else "{}"
                    conn.execute(
                        """
                        UPDATE notification_settings
                        SET subject_names_json = ?,
                            subject_service_map_json = ?
                        WHERE id = 1
                        """,
                        (subject_names_json, subject_service_map_json),
                    )

        existing_global = conn.execute(
            "SELECT singleton_key FROM app_preferences WHERE singleton_key = 'global'"
        ).fetchone()
        if not existing_global:
            latest_device = conn.execute(
                """
                SELECT color_preferences, alias_preferences, updated_at
                FROM device_preferences
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
            if latest_device:
                conn.execute(
                    """
                    INSERT INTO app_preferences (singleton_key, color_preferences, alias_preferences, updated_at)
                    VALUES ('global', ?, ?, ?)
                    """,
                    (
                        latest_device["color_preferences"] or "{}",
                        latest_device["alias_preferences"] or "{}",
                        latest_device["updated_at"] or now_iso(),
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO app_preferences (singleton_key, color_preferences, alias_preferences, updated_at)
                    VALUES ('global', '{}', '{}', ?)
                    """,
                    (now_iso(),),
                )

        existing_notification_settings = conn.execute(
            "SELECT id FROM notification_settings WHERE id = 1"
        ).fetchone()
        if not existing_notification_settings:
            conn.execute(
                """
                INSERT INTO notification_settings (
                    id, enabled, notify_before_end_enabled, subject_names_json, subject_service_map_json,
                    subject_critical_map_json, weekdays_json, title_template, message_template, sound,
                    image_url, extra_data_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    1,
                    1,
                    0,
                    "[]",
                    "{}",
                    "{}",
                    json.dumps(DAY_ORDER),
                    "Your rota for today",
                    "You are working {{ shift }} with: {{ coworkers }}.",
                    "",
                    "",
                    "{}",
                    now_iso(),
                ),
            )

        conn.commit()


@app.on_event("startup")
def startup() -> None:
    print(f"Using DB path: {DB_PATH}")
    print(f"DB parent exists: {DB_PATH.parent.exists()}")
    init_db()
    start_auto_notification_worker()


@app.on_event("shutdown")
def shutdown() -> None:
    stop_auto_notification_worker()


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def parse_headers(header_row: List[str]) -> dict:
    headers = {}
    for idx, cell in enumerate(header_row):
        cell = clean_cell(cell)
        match = DATE_HEADER_RE.match(cell)
        if match:
            month = match.group(2)
            day = match.group(3)
            headers[idx] = {
                "day_key": match.group(1).lower(),
                "full_header": cell,
                "day": day,
                "month": month,
            }
        else:
            headers[idx] = {
                "day_key": cell.lower(),
                "full_header": cell,
                "month": "",
                "day": "",
            }
    return headers


def extract_employee_table(pdf_path: Path) -> tuple[List[str], List[List[str]]]:
    header_row: List[str] = []
    all_rows: List[List[str]] = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            table = tables[0]
            if not table or len(table) < 2:
                continue

            page_header = normalize_table_row(table[0], len(table[0]))
            if not header_row:
                header_row = page_header

            expected_cols = len(header_row)

            for row in table[1:]:
                if not row or not any(clean_cell(cell) for cell in row):
                    continue
                all_rows.append(normalize_table_row(row, expected_cols))

    return header_row, all_rows


def fix_orphan_id_row(rows: List[List[str]]) -> List[List[str]]:
    fixed: List[List[str]] = []

    for row in rows:
        first_cell = clean_cell(row[0]) if row else ""
        orphan_match = EMPLOYEE_ID_RE.fullmatch(first_cell)

        if orphan_match and all(not clean_cell(cell) for cell in row[1:]):
            orphan_id = orphan_match.group(1)
            if fixed:
                prev = fixed[-1]
                prev_name = clean_cell(prev[0])
                if orphan_id not in prev_name:
                    prev[0] = f"{prev_name} ({orphan_id})"
            continue

        fixed.append(row)

    return fixed


def parse_shift_cell(cell: str) -> tuple[str, str]:
    cell = clean_cell(cell)
    if not cell:
        return "", ""

    upper = cell.upper()
    if upper.startswith("OFF"):
        return "", ""

    match = TIME_RANGE_RE.search(cell)
    if not match:
        return "", ""

    start_time, end_time = match.group(1), match.group(2)
    if start_time == "00:00" and end_time == "24:00":
        return "", ""

    return start_time, end_time


def format_shift_text(raw_cell: str, start_time: str, end_time: str) -> str:
    raw_clean = clean_cell(raw_cell)
    parsed_range = TIME_RANGE_RE.search(raw_clean)
    if parsed_range:
        return f"{parsed_range.group(1)} - {parsed_range.group(2)}"

    start_clean = clean_cell(start_time)
    end_clean = clean_cell(end_time)
    if start_clean and end_clean:
        return f"{start_clean} - {end_clean}"

    return raw_clean


def normalize_employee_name(name: str) -> str:
    cleaned = clean_cell(name)
    if not cleaned:
        return ""

    cleaned = EMPLOYEE_ID_RE.sub("", cleaned).strip()

    if "," in cleaned:
        _surname, forename = cleaned.split(",", 1)
        cleaned = forename.strip()

    return cleaned


def infer_year_from_filename(filename: str) -> str:
    match = re.search(r"(20\d{2})", filename)
    if match:
        return match.group(1)
    return str(datetime.utcnow().year)


def build_iso_date(year: str, month: str, day: str) -> str:
    if not year or not month or not day:
        return ""
    return f"{year}-{month}-{day}"


def parse_pdf_to_shift_rows(pdf_path: Path, original_filename: str) -> List[dict]:
    header_row, table_rows = extract_employee_table(pdf_path)
    if not header_row or not table_rows:
        return []

    table_rows = fix_orphan_id_row(table_rows)
    parsed_headers = parse_headers(header_row)
    year = infer_year_from_filename(original_filename)

    shifts: List[dict] = []

    for idx, row in enumerate(table_rows, start=1):
        employee = normalize_employee_name(row[0])
        if not employee or employee.lower() == "employee":
            continue

        total_hours = ""

        for col_idx in range(1, len(row)):
            header_info = parsed_headers.get(col_idx, {})
            day_key = header_info.get("day_key", "")
            full_header = header_info.get("full_header", "")
            month = header_info.get("month", "")
            day = header_info.get("day", "")
            value = clean_cell(row[col_idx])

            if "total" in day_key:
                total_hours = value
                continue

            if day_key not in DAY_ORDER:
                continue

            start_time, end_time = parse_shift_cell(value)
            shift_date = build_iso_date(year, month, day)

            shifts.append(
                {
                    "row_index": idx,
                    "employee": employee,
                    "day_name": day_key,
                    "day_header": full_header,
                    "shift_date": shift_date,
                    "raw_cell": value,
                    "start_time": start_time,
                    "end_time": end_time,
                    "total_hours": total_hours,
                }
            )

        if total_hours:
            for shift in shifts:
                if shift["row_index"] == idx and shift["employee"] == employee:
                    shift["total_hours"] = total_hours

    return shifts


def ingress_base(request: Request) -> str:
    return request.headers.get("X-Ingress-Path", "").rstrip("/")


def format_uk_date(iso_date: str) -> str:
    if not iso_date:
        return ""
    try:
        dt = datetime.strptime(iso_date, "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        return iso_date


def build_model_from_upload(upload_id: int) -> dict:
    with get_conn() as conn:
        upload = conn.execute(
            "SELECT * FROM uploads WHERE id = ?",
            (upload_id,),
        ).fetchone()

        shifts = conn.execute(
            """
            SELECT *
            FROM shifts
            WHERE upload_id = ?
            ORDER BY row_index, id
            """,
            (upload_id,),
        ).fetchall()

    if upload is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    header_map = {day: day.title() for day in DAY_ORDER}
    row_order = []
    rows_by_employee = {}

    first_date = None
    last_date = None

    for shift in shifts:
        employee = shift["employee"]
        if employee not in rows_by_employee:
            rows_by_employee[employee] = {
                "name": employee,
                "rawName": employee,
                "days": [""] * 7,
            }
            row_order.append(employee)

        day_name = shift["day_name"]
        if day_name in DAY_ORDER:
            day_idx = DAY_ORDER.index(day_name)
            rows_by_employee[employee]["days"][day_idx] = shift["raw_cell"] or ""
            if shift["day_header"]:
                header_map[day_name] = shift["day_header"]

        shift_date = shift["shift_date"] or ""
        if shift_date:
            if not first_date or shift_date < first_date:
                first_date = shift_date
            if not last_date or shift_date > last_date:
                last_date = shift_date

    day_headers = [header_map[day] for day in DAY_ORDER]
    rows = [rows_by_employee[name] for name in row_order]

    title = Path(upload["original_filename"]).stem
    if first_date and last_date:
        sub = f"Date range: {format_uk_date(first_date)} - {format_uk_date(last_date)}"
    elif first_date:
        sub = f"Date range: {format_uk_date(first_date)}"
    else:
        sub = ""

    model = {
        "title": title,
        "sub": sub,
        "dayHeaders": day_headers,
        "rows": rows,
    }

    uploaded_at = upload["uploaded_at"] or ""
    try:
        stored_at = int(datetime.fromisoformat(uploaded_at).timestamp() * 1000)
    except Exception:
        stored_at = int(datetime.utcnow().timestamp() * 1000)

    return {
        "id": f"upload-{upload_id}",
        "storedAt": stored_at,
        "metadata": {},
        "model": model,
    }


def build_sync_payload() -> list[dict]:
    with get_conn() as conn:
        uploads = conn.execute(
            "SELECT id FROM uploads ORDER BY uploaded_at DESC, id DESC"
        ).fetchall()

    return [build_model_from_upload(row["id"]) for row in uploads]


def sanitize_preferences_payload(payload: dict | None) -> tuple[dict, dict]:
    payload = payload if isinstance(payload, dict) else {}
    colors_raw = payload.get("colors")
    aliases_raw = payload.get("aliases")

    colors: dict[str, str] = {}
    if isinstance(colors_raw, dict):
        for key, value in colors_raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            key_clean = key.strip()
            value_clean = value.strip()
            if key_clean and re.fullmatch(r"#[0-9a-fA-F]{6}", value_clean):
                colors[key_clean] = value_clean.lower()

    aliases: dict[str, str] = {}
    if isinstance(aliases_raw, dict):
        for key, value in aliases_raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            key_clean = key.strip()
            value_clean = value.strip()
            if key_clean and value_clean:
                aliases[key_clean] = value_clean[:120]

    return colors, aliases


def get_notification_settings_row() -> sqlite3.Row:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM notification_settings WHERE id = 1"
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Notification settings not found")

    return row


def serialize_notification_settings(row: sqlite3.Row) -> dict:
    subject_service_map = parse_json_object(row["subject_service_map_json"], {})
    subject_critical_map = parse_json_object(row["subject_critical_map_json"], {})
    subject_names = parse_json_list(row["subject_names_json"], [])

    allowed_subjects = []
    for subject in subject_names:
        name = clean_cell(subject)
        if name and name in subject_service_map and name not in allowed_subjects:
            allowed_subjects.append(name)

    return {
        "enabled": bool(row["enabled"]),
        "notify_before_end_enabled": bool(row["notify_before_end_enabled"]),
        "subject_names": allowed_subjects,
        "subject_service_map": subject_service_map,
        "subject_critical_map": {
            subject: bool(sanitize_bool(subject_critical_map.get(subject), default=False))
            for subject in subject_service_map
        },
        "weekdays": parse_json_list(row["weekdays_json"], DAY_ORDER.copy()),
        "title_template": row["title_template"] or "",
        "message_template": row["message_template"] or "",
        "sound": row["sound"] or "",
        "image_url": row["image_url"] or "",
        "extra_data": parse_json_object(row["extra_data_json"], {}),
        "updated_at": row["updated_at"] or "",
    }


def render_simple_template(template: str, context: Dict[str, Any]) -> str:
    output = str(template or "")
    for key, value in context.items():
        token = "{{ " + key + " }}"
        output = output.replace(token, str(value))
        token_no_space = "{{" + key + "}}"
        output = output.replace(token_no_space, str(value))
    return output


def get_latest_relevant_upload_id(subject_name: str, today: str) -> Optional[int]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT upload_id
            FROM shifts
            WHERE shift_date = ?
              AND employee = ?
            ORDER BY upload_id DESC
            LIMIT 1
            """,
            (today, subject_name),
        ).fetchone()

        if row:
            return row["upload_id"]

        row = conn.execute(
            """
            SELECT upload_id
            FROM shifts
            WHERE shift_date = ?
            ORDER BY upload_id DESC
            LIMIT 1
            """,
            (today,),
        ).fetchone()

    return row["upload_id"] if row else None


def get_subject_shift_and_coworkers(subject_name: str, today: str) -> dict:
    upload_id = get_latest_relevant_upload_id(subject_name, today)

    if not upload_id:
        return {
            "status": "NOT_FOUND",
            "subject_name": subject_name,
            "today": today,
            "shift": "",
            "coworkers_list": [],
            "coworkers": "Nobody found",
            "upload_id": None,
        }

    with get_conn() as conn:
        me = conn.execute(
            """
            SELECT start_time, end_time, raw_cell
            FROM shifts
            WHERE upload_id = ?
              AND shift_date = ?
              AND employee = ?
            LIMIT 1
            """,
            (upload_id, today, subject_name),
        ).fetchone()

        if not me:
            return {
                "status": "NOT_WORKING",
                "subject_name": subject_name,
                "today": today,
                "shift": "",
                "start_time": "",
                "end_time": "",
                "coworkers_list": [],
                "coworkers": "Nobody found",
                "upload_id": upload_id,
            }

        start_time = clean_cell(me["start_time"])
        end_time = clean_cell(me["end_time"])
        raw_cell = clean_cell(me["raw_cell"])
        shift_text = format_shift_text(raw_cell, start_time, end_time)

        if not start_time or not end_time:
            return {
                "status": "NOT_WORKING",
                "subject_name": subject_name,
                "today": today,
                "shift": shift_text,
                "start_time": start_time,
                "end_time": end_time,
                "coworkers_list": [],
                "coworkers": "Nobody found",
                "upload_id": upload_id,
            }

        rows = conn.execute(
            """
            SELECT employee, start_time, end_time
            FROM shifts
            WHERE upload_id = ?
              AND shift_date = ?
              AND employee != ?
              AND TRIM(COALESCE(start_time, '')) != ''
              AND TRIM(COALESCE(end_time, '')) != ''
              AND start_time < ?
              AND end_time > ?
            ORDER BY start_time, employee
            """,
            (upload_id, today, subject_name, end_time, start_time),
        ).fetchall()

    subject_end_minutes = hhmm_to_minutes(end_time)
    closing_coworkers = []
    mid_coworkers = []

    for row in rows:
        employee = clean_cell(row["employee"])
        coworker_start = clean_cell(row["start_time"])
        coworker_end = clean_cell(row["end_time"])
        coworker_start_minutes = hhmm_to_minutes(coworker_start)
        coworker_end_minutes = hhmm_to_minutes(coworker_end)
        if not employee or coworker_end_minutes is None:
            continue

        is_mid_shift = subject_end_minutes is not None and coworker_end_minutes < subject_end_minutes
        if is_mid_shift:
            coworker_label = f"{employee} until {coworker_end}"
            sort_key = (coworker_end_minutes, coworker_start_minutes or -1, employee.lower())
            mid_coworkers.append((sort_key, coworker_label))
            continue

        coworker_label = f"{employee} ({coworker_start})" if coworker_start else employee
        sort_key = (coworker_end_minutes, coworker_start_minutes or -1, employee.lower())
        closing_coworkers.append((sort_key, coworker_label))

    closing_coworkers.sort(key=lambda item: item[0])
    mid_coworkers.sort(key=lambda item: item[0])

    coworkers_list = [label for _, label in closing_coworkers + mid_coworkers]
    coworkers = ", ".join(coworkers_list) if coworkers_list else "Nobody found"

    return {
        "status": "WORKING",
        "subject_name": subject_name,
        "today": today,
        "shift": shift_text,
        "start_time": start_time,
        "end_time": end_time,
        "coworkers_list": coworkers_list,
        "coworkers": coworkers,
        "upload_id": upload_id,
    }


def hhmm_to_minutes(value: str) -> Optional[int]:
    clean_value = clean_cell(value)
    if not re.fullmatch(r"\d{2}:\d{2}", clean_value):
        return None
    hours, minutes = clean_value.split(":")
    total = int(hours) * 60 + int(minutes)
    if total < 0 or total > 1440:
        return None
    return total


def is_management_person(name: str) -> bool:
    normalized = clean_cell(name).lower()
    return any(target in normalized for target in MANAGEMENT_NAMES)


def join_human_names(names: List[str]) -> str:
    cleaned = [clean_cell(name) for name in names if clean_cell(name)]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def build_shift_end_message(end_time: str, team_snapshot: dict) -> str:
    next_shift_people = team_snapshot.get("next_shift_people") if isinstance(team_snapshot.get("next_shift_people"), list) else []
    next_shift_team = team_snapshot.get("next_shift_team") if isinstance(team_snapshot.get("next_shift_team"), list) else []
    bridge_people = team_snapshot.get("bridge_people") if isinstance(team_snapshot.get("bridge_people"), list) else []

    next_people_text = join_human_names(next_shift_people)
    next_team_text = join_human_names(next_shift_team)
    bridge_text = join_human_names(bridge_people)

    next_day_openers = team_snapshot.get("next_day_openers") if isinstance(team_snapshot.get("next_day_openers"), list) else []
    next_day_opening_team = (
        team_snapshot.get("next_day_opening_team") if isinstance(team_snapshot.get("next_day_opening_team"), list) else []
    )

    if not next_people_text:
        if next_day_openers:
            manager_openers = [person for person in next_day_openers if is_management_person(person)]
            non_manager_openers = [person for person in next_day_openers if person not in manager_openers]

            if manager_openers:
                openers_text = join_human_names(manager_openers)
                extra_openers = non_manager_openers
            else:
                openers_text = join_human_names(next_day_openers)
                extra_openers = []

            combined_team = join_human_names(extra_openers + next_day_opening_team)

            opener_verb = "are" if len(manager_openers) != 1 else "is"
            if combined_team:
                return f"{openers_text} {opener_verb} opening tomorrow and they are working with {combined_team}."
            return f"{openers_text} {opener_verb} opening tomorrow."
        return f"No one is scheduled to take over after {end_time}."

    management_count = sum(1 for person in next_shift_people if is_management_person(person))
    management_label = ""
    if management_count == len(next_shift_people) and management_count > 1:
        management_label = " (both management)"
    elif management_count >= 1:
        management_label = " (management)"

    verb = "take" if len(next_shift_people) > 1 else "takes"

    if bridge_text:
        return f"From {end_time}, {next_people_text}{management_label} {verb} over, bridged with {bridge_text}."
    if next_team_text:
        return f"From {end_time}, {next_people_text}{management_label} {verb} over with {next_team_text}."
    return f"From {end_time}, {next_people_text}{management_label} {verb} over."


def get_shift_team_snapshot(upload_id: Optional[int], today: str, subject_name: str, shift_end: str) -> dict:
    if not upload_id or not shift_end:
        return {
            "opening": "Unknown",
            "closing": "Unknown",
            "takeover": "Nobody scheduled after shift",
            "team_with_subject": "Nobody found",
            "next_shift_people": [],
            "next_shift_time": "",
            "next_shift_team": [],
            "bridge_people": [],
            "next_day_openers": [],
            "next_day_opening_team": [],
        }

    subject_end_minutes = hhmm_to_minutes(shift_end)
    if subject_end_minutes is None:
        return {
            "opening": "Unknown",
            "closing": "Unknown",
            "takeover": "Unknown",
            "team_with_subject": "Nobody found",
            "next_shift_people": [],
            "next_shift_time": "",
            "next_shift_team": [],
            "bridge_people": [],
            "next_day_openers": [],
            "next_day_opening_team": [],
        }

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT employee, start_time, end_time
            FROM shifts
            WHERE upload_id = ?
              AND shift_date = ?
              AND TRIM(COALESCE(start_time, '')) != ''
              AND TRIM(COALESCE(end_time, '')) != ''
            ORDER BY start_time, employee
            """,
            (upload_id, today),
        ).fetchall()

    shift_rows = []
    for row in rows:
        employee = clean_cell(row["employee"])
        start_time = clean_cell(row["start_time"])
        end_time = clean_cell(row["end_time"])
        start_minutes = hhmm_to_minutes(start_time)
        end_minutes = hhmm_to_minutes(end_time)
        if not employee or start_minutes is None or end_minutes is None:
            continue
        shift_rows.append(
            {
                "employee": employee,
                "start_time": start_time,
                "end_time": end_time,
                "start_minutes": start_minutes,
                "end_minutes": end_minutes,
            }
        )

    if not shift_rows:
        return {
            "opening": "Unknown",
            "closing": "Unknown",
            "takeover": "Nobody scheduled after shift",
            "team_with_subject": "Nobody found",
            "next_shift_people": [],
            "next_shift_time": "",
            "next_shift_team": [],
            "bridge_people": [],
            "next_day_openers": [],
            "next_day_opening_team": [],
        }

    next_day = (datetime.strptime(today, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    with get_conn() as conn:
        next_day_upload_row = conn.execute(
            """
            SELECT upload_id
            FROM shifts
            WHERE shift_date = ?
            ORDER BY upload_id DESC
            LIMIT 1
            """,
            (next_day,),
        ).fetchone()

        next_day_rows = []
        if next_day_upload_row:
            next_day_rows = conn.execute(
                """
                SELECT employee, start_time, end_time
                FROM shifts
                WHERE upload_id = ?
                  AND shift_date = ?
                  AND TRIM(COALESCE(start_time, '')) != ''
                  AND TRIM(COALESCE(end_time, '')) != ''
                ORDER BY start_time, employee
                """,
                (next_day_upload_row["upload_id"], next_day),
            ).fetchall()

    next_day_shift_rows = []
    for row in next_day_rows:
        employee = clean_cell(row["employee"])
        start_time = clean_cell(row["start_time"])
        end_time = clean_cell(row["end_time"])
        start_minutes = hhmm_to_minutes(start_time)
        end_minutes = hhmm_to_minutes(end_time)
        if not employee or start_minutes is None or end_minutes is None:
            continue
        next_day_shift_rows.append(
            {
                "employee": employee,
                "start_minutes": start_minutes,
                "end_minutes": end_minutes,
            }
        )

    if next_day_shift_rows:
        earliest_next_day_start = min(item["start_minutes"] for item in next_day_shift_rows)
        next_day_openers = sorted(
            item["employee"] for item in next_day_shift_rows if item["start_minutes"] == earliest_next_day_start
        )
        next_day_opening_team = sorted(
            {
                item["employee"]
                for item in next_day_shift_rows
                if item["employee"] not in next_day_openers
                and item["start_minutes"] <= earliest_next_day_start
                and item["end_minutes"] > earliest_next_day_start
            }
        )
    else:
        next_day_openers = []
        next_day_opening_team = []

    earliest_start = min(item["start_minutes"] for item in shift_rows)
    latest_end = max(item["end_minutes"] for item in shift_rows)

    opening = ", ".join(
        sorted(item["employee"] for item in shift_rows if item["start_minutes"] == earliest_start)
    ) or "Unknown"
    closing = ", ".join(
        sorted(item["employee"] for item in shift_rows if item["end_minutes"] == latest_end)
    ) or "Unknown"

    takeover_rows = [
        item
        for item in shift_rows
        if item["employee"] != subject_name and item["start_minutes"] >= subject_end_minutes
    ]
    if takeover_rows:
        earliest_takeover = min(item["start_minutes"] for item in takeover_rows)
        takeover_people = sorted(item["employee"] for item in takeover_rows if item["start_minutes"] == earliest_takeover)
        takeover_time = next((item["start_time"] for item in takeover_rows if item["start_minutes"] == earliest_takeover), "")
        takeover = f"{', '.join(takeover_people)} at {takeover_time}" if takeover_time else ", ".join(takeover_people)

        takeover_team = sorted(
            {
                item["employee"]
                for item in shift_rows
                if item["employee"] != subject_name
                and item["employee"] not in takeover_people
                and item["start_minutes"] <= earliest_takeover
                and item["end_minutes"] > earliest_takeover
            }
        )

        bridge_people = sorted(
            {
                item["employee"]
                for item in shift_rows
                if item["employee"] != subject_name
                and item["employee"] not in takeover_people
                and item["start_minutes"] < earliest_takeover
                and item["end_minutes"] > earliest_takeover
            }
        )
    else:
        takeover = "Nobody scheduled after shift"
        takeover_people = []
        takeover_time = ""
        takeover_team = []
        bridge_people = []

    team_with_subject = sorted(
        {
            item["employee"]
            for item in shift_rows
            if item["employee"] != subject_name and item["start_minutes"] < subject_end_minutes and item["end_minutes"] > subject_end_minutes
        }
    )

    return {
        "opening": opening,
        "closing": closing,
        "takeover": takeover,
        "team_with_subject": ", ".join(team_with_subject) if team_with_subject else "Nobody found",
        "next_shift_people": takeover_people,
        "next_shift_time": takeover_time,
        "next_shift_team": takeover_team,
        "bridge_people": bridge_people,
        "next_day_openers": next_day_openers,
        "next_day_opening_team": next_day_opening_team,
    }


def build_notification_payload_from_settings() -> dict:
    settings_row = get_notification_settings_row()
    settings = serialize_notification_settings(settings_row)

    today = datetime.now().strftime("%Y-%m-%d")
    notifications = []

    for subject_name in settings["subject_names"]:
        notify_service = clean_cell(settings["subject_service_map"].get(subject_name))
        if not notify_service:
            continue
        is_critical = bool(sanitize_bool(settings.get("subject_critical_map", {}).get(subject_name), default=False))

        rota_context = get_subject_shift_and_coworkers(subject_name, today)
        context = {
            "subject_name": rota_context["subject_name"],
            "status": rota_context["status"],
            "today": rota_context["today"],
            "shift": rota_context["shift"],
            "start_time": rota_context.get("start_time") or "",
            "end_time": rota_context.get("end_time") or "",
            "coworkers": rota_context["coworkers"],
            "upload_id": rota_context["upload_id"] or "",
        }

        title = render_simple_template(settings["title_template"], context).strip()
        message = render_simple_template(settings["message_template"], context).strip()

        if rota_context["status"] in {"NOT_FOUND", "NOT_WORKING"}:
            message = ""

        if not title:
            title = "Your rota for today"

        if not message:
            if rota_context["status"] == "NOT_FOUND":
                message = "No rota uploaded for today."
            elif rota_context["status"] == "NOT_WORKING":
                message = "You are not rota'd in today."
            else:
                message = f"You're working today with: {rota_context['coworkers']}."

        data_payload = settings["extra_data"] if isinstance(settings["extra_data"], dict) else {}
        data_payload = json.loads(json.dumps(data_payload))

        if settings["sound"] or is_critical:
            push = data_payload.get("push", {})
            sound_payload = push.get("sound", {})
            if not isinstance(push, dict):
                push = {}
            if not isinstance(sound_payload, dict):
                sound_payload = {}

            sound_payload["name"] = settings["sound"] or "default"
            if is_critical:
                sound_payload["critical"] = 1
                sound_payload["volume"] = 1.0
            push["sound"] = sound_payload
            data_payload["push"] = push

        if settings["image_url"]:
            data_payload["image"] = settings["image_url"]

        trigger_at = ""
        start_time = clean_cell(rota_context.get("start_time"))
        if not re.fullmatch(r"\d{2}:\d{2}", start_time):
            parsed_range = TIME_RANGE_RE.search(clean_cell(rota_context.get("shift")))
            if parsed_range:
                start_time = parsed_range.group(1)

        if re.fullmatch(r"\d{2}:\d{2}", start_time):
            shift_start = datetime.strptime(f"{today} {start_time}", "%Y-%m-%d %H:%M")
            trigger_at = (shift_start - timedelta(hours=2)).isoformat(timespec="minutes")

        base_notification = {
            "subject_name": subject_name,
            "notify_service": notify_service,
            "title": title,
            "message": message,
            "data": data_payload,
            "context": context,
            "trigger_at": trigger_at,
            "critical": is_critical,
            "notification_kind": "shift_start",
        }
        notifications.append(base_notification)

        if not settings.get("notify_before_end_enabled"):
            continue

        end_time = clean_cell(rota_context.get("end_time"))
        if not re.fullmatch(r"\d{2}:\d{2}", end_time):
            parsed_range = TIME_RANGE_RE.search(clean_cell(rota_context.get("shift")))
            if parsed_range:
                end_time = parsed_range.group(2)

        if not re.fullmatch(r"\d{2}:\d{2}", end_time):
            continue

        shift_end = datetime.strptime(f"{today} {end_time}", "%Y-%m-%d %H:%M")
        team_snapshot = get_shift_team_snapshot(
            rota_context.get("upload_id"),
            today,
            subject_name,
            end_time,
        )

        end_context = {
            **context,
            "opening": team_snapshot["opening"],
            "closing": team_snapshot["closing"],
            "takeover": team_snapshot["takeover"],
            "team_with_subject": team_snapshot["team_with_subject"],
        }

        end_message = build_shift_end_message(end_time, team_snapshot)

        notifications.append(
            {
                "subject_name": subject_name,
                "notify_service": notify_service,
                "title": "Handover check for your shift",
                "message": end_message,
                "data": json.loads(json.dumps(data_payload)),
                "context": end_context,
                "trigger_at": (shift_end - timedelta(hours=2)).isoformat(timespec="minutes"),
                "critical": is_critical,
                "notification_kind": "shift_end",
            }
        )

    return {
        "enabled": settings["enabled"],
        "notify_before_end_enabled": settings.get("notify_before_end_enabled", False),
        "weekdays": settings["weekdays"],
        "notifications": notifications,
    }


def parse_iso_datetime(raw: str) -> Optional[datetime]:
    text = clean_cell(raw)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def make_dispatch_key(item: dict) -> str:
    context = item.get("context") if isinstance(item.get("context"), dict) else {}
    return "|".join(
        [
            clean_cell(item.get("subject_name")),
            clean_cell(context.get("today")),
            clean_cell(item.get("trigger_at")),
            clean_cell(item.get("notify_service")),
        ]
    )


def should_dispatch_now(item: dict, now: datetime) -> bool:
    trigger_at = parse_iso_datetime(item.get("trigger_at", ""))
    if not trigger_at:
        return False

    if now < trigger_at:
        return False

    grace_deadline = trigger_at + timedelta(minutes=AUTO_NOTIFY_GRACE_MINUTES)
    return now <= grace_deadline


def was_dispatched(dispatch_key: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT dispatch_key FROM notification_dispatch_log WHERE dispatch_key = ?",
            (dispatch_key,),
        ).fetchone()
    return row is not None


def record_dispatched(dispatch_key: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO notification_dispatch_log (dispatch_key, dispatched_at)
            VALUES (?, ?)
            """,
            (dispatch_key, now_iso()),
        )
        conn.commit()


def add_notification_debug_log(event_type: str, item: dict, details: Optional[dict] = None) -> None:
    details_json = details if isinstance(details, dict) else {}
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO notification_debug_log (
                event_type,
                subject_name,
                notify_service,
                trigger_at,
                created_at,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                sanitize_text(event_type, max_len=40, default="unknown"),
                clean_cell(item.get("subject_name")),
                clean_cell(item.get("notify_service")),
                clean_cell(item.get("trigger_at")),
                now_iso(),
                json.dumps(details_json),
            ),
        )
        conn.execute(
            "DELETE FROM notification_debug_log WHERE id NOT IN (SELECT id FROM notification_debug_log ORDER BY id DESC LIMIT 250)"
        )
        conn.commit()


def dispatch_notification(item: dict) -> None:
    notify_service = item["notify_service"]
    if "." not in notify_service:
        raise HTTPException(status_code=400, detail="notify_service must look like notify.some_service")

    domain, service = notify_service.split(".", 1)
    service_payload = {
        "title": item["title"],
        "message": item["message"],
        "data": item["data"],
    }

    status_code, response_payload = call_home_assistant_api(
        "POST",
        f"/services/{domain}/{service}",
        service_payload,
    )

    add_notification_debug_log(
        "attempt",
        item,
        {
            "status_code": status_code,
            "response": response_payload,
            "title": item.get("title", ""),
            "message": item.get("message", ""),
        },
    )

    if status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Home Assistant notify service call failed",
                "status_code": status_code,
                "response": response_payload,
                "subject_name": item.get("subject_name", ""),
            },
        )


def run_due_notifications() -> dict:
    payload = build_notification_payload_from_settings()
    if not payload.get("enabled"):
        return {"sent_via": [], "count": 0, "reason": "disabled"}

    today_weekday = datetime.now().strftime("%a").lower()[:3]
    weekdays = payload.get("weekdays") or []
    if today_weekday not in weekdays:
        return {"sent_via": [], "count": 0, "reason": "weekday_filtered"}

    now = datetime.now()
    sent_via = []
    for item in payload.get("notifications") or []:
        if not should_dispatch_now(item, now):
            add_notification_debug_log("skipped_not_due", item)
            continue

        dispatch_key = make_dispatch_key(item)
        if not dispatch_key or was_dispatched(dispatch_key):
            add_notification_debug_log("skipped_already_sent", item, {"dispatch_key": dispatch_key})
            continue

        dispatch_notification(item)
        record_dispatched(dispatch_key)
        sent_via.append(item["notify_service"])

    return {"sent_via": sent_via, "count": len(sent_via), "reason": "ok"}


def auto_notification_loop() -> None:
    while not _auto_notify_stop.is_set():
        try:
            run_due_notifications()
        except Exception as exc:
            print(f"Auto notification cycle failed: {exc}")
        _auto_notify_stop.wait(AUTO_NOTIFY_POLL_SECONDS)


def start_auto_notification_worker() -> None:
    global _auto_notify_thread
    if _auto_notify_thread and _auto_notify_thread.is_alive():
        return

    _auto_notify_stop.clear()
    _auto_notify_thread = threading.Thread(
        target=auto_notification_loop,
        name="rota-auto-notify",
        daemon=True,
    )
    _auto_notify_thread.start()


def stop_auto_notification_worker() -> None:
    global _auto_notify_thread
    _auto_notify_stop.set()

    if _auto_notify_thread and _auto_notify_thread.is_alive():
        _auto_notify_thread.join(timeout=2)
    _auto_notify_thread = None


def call_home_assistant_api(method: str, path: str, payload: Optional[dict] = None) -> tuple[int, Any]:
    token = os.environ.get("SUPERVISOR_TOKEN", "").strip()
    if not token:
        raise HTTPException(status_code=500, detail="SUPERVISOR_TOKEN is not available")

    url = f"{HA_CORE_API_BASE}{path}"
    body = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    req = urlrequest.Request(url, data=body, headers=headers, method=method.upper())

    try:
        with urlrequest.urlopen(req, timeout=15) as response:
            raw = response.read().decode("utf-8")
            if not raw:
                return response.status, {}
            try:
                return response.status, json.loads(raw)
            except Exception:
                return response.status, raw
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw)
        except Exception:
            return exc.code, raw
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to call Home Assistant API: {exc}") from exc


def list_notify_services() -> list[str]:
    status_code, payload = call_home_assistant_api("GET", "/services")
    if status_code >= 400:
        return []

    services = []
    if isinstance(payload, list):
        for domain_block in payload:
            if not isinstance(domain_block, dict):
                continue
            domain = domain_block.get("domain")
            if domain != "notify":
                continue
            services_block = domain_block.get("services", {})
            if not isinstance(services_block, dict):
                continue
            for service_name in services_block.keys():
                services.append(f"notify.{service_name}")

    services = sorted(set(services))
    return services


@app.get("/", response_class=HTMLResponse)
def viewer(request: Request):
    index_path = LOGGANNT_DIR / "index.html"
    html = index_path.read_text(encoding="utf-8")

    base = ingress_base(request)
    base_script = f"<script>window.__APP_BASE__={json.dumps(base)};</script>"
    if "<head>" in html:
        html = html.replace("<head>", f"<head>{base_script}", 1)
    else:
        html = base_script + html

    injected = f"<script src=\"{base}/static/loggannt/ha-bridge.js\"></script>"

    if "</body>" in html:
        html = html.replace("</body>", f"{injected}</body>")
    else:
        html += injected

    return HTMLResponse(html)


@app.get("/help", response_class=HTMLResponse)
def help_page(request: Request):
    help_path = LOGGANNT_DIR / "help.html"
    html = help_path.read_text(encoding="utf-8")
    base = ingress_base(request)

    html = html.replace("client-side", "server-backed")

    base_script = f"<script>window.__APP_BASE__={json.dumps(base)};</script>"
    if "<head>" in html:
        html = html.replace("<head>", f"<head>{base_script}", 1)
    else:
        html = base_script + html

    injected = f"<script src=\"{base}/static/loggannt/ha-bridge.js\"></script>"

    if "</body>" in html:
        html = html.replace("</body>", f"{injected}</body>")
    else:
        html += injected

    return HTMLResponse(html)


@app.get("/api/uploads")
def api_uploads():
    with get_conn() as conn:
        uploads = conn.execute(
            """
            SELECT u.id, u.original_filename, u.uploaded_at, COUNT(s.id) AS row_count
            FROM uploads u
            LEFT JOIN shifts s ON s.upload_id = u.id
            GROUP BY u.id
            ORDER BY u.uploaded_at DESC, u.id DESC
            """
        ).fetchall()

    return JSONResponse([dict(row) for row in uploads])


@app.get("/api/upload/{upload_id}/model")
def api_upload_model(upload_id: int):
    return JSONResponse(build_model_from_upload(upload_id))


@app.get("/api/viewer_sync")
def api_viewer_sync():
    return JSONResponse(build_sync_payload())


@app.get("/api/preferences")
def api_get_preferences():
    with get_conn() as conn:
        row = conn.execute(
            "SELECT color_preferences, alias_preferences FROM app_preferences WHERE singleton_key = 'global'"
        ).fetchone()

    if not row:
        return JSONResponse({"colors": {}, "aliases": {}})

    try:
        colors = json.loads(row["color_preferences"] or "{}")
    except Exception:
        colors = {}
    try:
        aliases = json.loads(row["alias_preferences"] or "{}")
    except Exception:
        aliases = {}

    return JSONResponse(
        {
            "colors": colors if isinstance(colors, dict) else {},
            "aliases": aliases if isinstance(aliases, dict) else {},
        }
    )


@app.put("/api/preferences")
async def api_put_preferences(request: Request):
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    colors, aliases = sanitize_preferences_payload(body)
    now = now_iso()

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO app_preferences (singleton_key, color_preferences, alias_preferences, updated_at)
            VALUES ('global', ?, ?, ?)
            ON CONFLICT(singleton_key) DO UPDATE SET
              color_preferences=excluded.color_preferences,
              alias_preferences=excluded.alias_preferences,
              updated_at=excluded.updated_at
            """,
            (json.dumps(colors), json.dumps(aliases), now),
        )
        conn.commit()

    return JSONResponse({"ok": True})


@app.get("/api/preferences/{device_id}")
def api_get_preferences_legacy(device_id: str):
    return api_get_preferences()


@app.put("/api/preferences/{device_id}")
async def api_put_preferences_legacy(device_id: str, request: Request):
    return await api_put_preferences(request)


@app.get("/api/notification_settings")
def api_get_notification_settings():
    row = get_notification_settings_row()
    return JSONResponse(serialize_notification_settings(row))


@app.put("/api/notification_settings")
async def api_put_notification_settings(request: Request):
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    enabled = sanitize_bool(body.get("enabled", True), default=True)
    notify_before_end_enabled = sanitize_bool(body.get("notify_before_end_enabled", False), default=False)
    subject_service_map_raw = body.get("subject_service_map")
    subject_service_map_json = sanitize_subject_service_map(subject_service_map_raw)
    subject_service_map = parse_json_object(subject_service_map_json, {})
    requested_subjects = body.get("subject_names")
    if isinstance(requested_subjects, list) and requested_subjects:
        subject_names_json = sanitize_subject_list(requested_subjects)
    else:
        subject_names_json = sanitize_subject_list(list(subject_service_map.keys()))
    subject_names = parse_json_list(subject_names_json, [])
    subject_names = [name for name in subject_names if name in subject_service_map]
    subject_names_json = json.dumps(subject_names)
    subject_critical_map_json = sanitize_subject_critical_map(body.get("subject_critical_map"))
    subject_critical_map = parse_json_object(subject_critical_map_json, {})
    subject_critical_map = {name: sanitize_bool(subject_critical_map.get(name), default=False) for name in subject_service_map}
    subject_critical_map_json = json.dumps(subject_critical_map)
    weekdays_json = sanitize_weekdays(body.get("weekdays"))
    title_template = sanitize_text(
        body.get("title_template"),
        max_len=500,
        default="Your rota for today",
    )
    message_template = sanitize_text(
        body.get("message_template"),
        max_len=2000,
        default="You are working {{ shift }} with: {{ coworkers }}.",
    )
    sound = sanitize_text(body.get("sound"), max_len=120, default="")
    image_url = sanitize_text(body.get("image_url"), max_len=500, default="")
    extra_data = body.get("extra_data", {})
    if not isinstance(extra_data, dict):
        extra_data = {}
    updated_at = now_iso()

    with get_conn() as conn:
        conn.execute(
            """
            UPDATE notification_settings
            SET enabled = ?,
                notify_before_end_enabled = ?,
                subject_names_json = ?,
                subject_service_map_json = ?,
                subject_critical_map_json = ?,
                weekdays_json = ?,
                title_template = ?,
                message_template = ?,
                sound = ?,
                image_url = ?,
                extra_data_json = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                enabled,
                notify_before_end_enabled,
                subject_names_json,
                subject_service_map_json,
                subject_critical_map_json,
                weekdays_json,
                title_template,
                message_template,
                sound,
                image_url,
                json.dumps(extra_data),
                updated_at,
            ),
        )
        conn.commit()

    return JSONResponse({"ok": True})


@app.get("/api/notification_preview")
def api_notification_preview():
    payload = build_notification_payload_from_settings()
    return JSONResponse(payload)


@app.get("/api/ha_notify_services")
def api_ha_notify_services():
    services = list_notify_services()
    return JSONResponse({"services": services})


@app.post("/api/test_notification")
def api_test_notification():
    payload = build_notification_payload_from_settings()
    notifications = payload.get("notifications") or []
    if not notifications:
        raise HTTPException(status_code=400, detail="No paired subjects/services configured")

    sent_via = []
    for item in notifications:
        dispatch_notification(item)
        sent_via.append(item["notify_service"])

    return JSONResponse(
        {
            "ok": True,
            "sent_via": sent_via,
            "count": len(sent_via),
        }
    )


@app.get("/api/notification_debug_log")
def api_notification_debug_log():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, event_type, subject_name, notify_service, trigger_at, created_at, details_json
            FROM notification_debug_log
            ORDER BY id DESC
            LIMIT 100
            """
        ).fetchall()

    events = []
    for row in rows:
        events.append(
            {
                "id": row["id"],
                "event_type": row["event_type"],
                "subject_name": row["subject_name"],
                "notify_service": row["notify_service"],
                "trigger_at": row["trigger_at"],
                "created_at": row["created_at"],
                "details": parse_json_object(row["details_json"], {}),
            }
        )

    return JSONResponse({"events": events})


@app.get("/api/notification_subjects")
def api_notification_subjects():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT employee
            FROM shifts
            WHERE TRIM(COALESCE(employee, '')) != ''
            ORDER BY employee
            """
        ).fetchall()
    return JSONResponse({"subjects": [clean_cell(row["employee"]) for row in rows if clean_cell(row["employee"])]})


@app.post("/api/upload_pdf")
async def api_upload_pdf(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file selected")

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Please upload a PDF file")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_filename = re.sub(r"[^a-zA-Z0-9._-]", "_", file.filename)
    stored_name = f"{timestamp}_{safe_filename}"
    target_path = UPLOAD_DIR / stored_name

    with target_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    shifts = parse_pdf_to_shift_rows(target_path, file.filename)

    with get_conn() as conn:
        existing_uploads = conn.execute(
            "SELECT id, stored_filename FROM uploads WHERE original_filename = ?",
            (file.filename,),
        ).fetchall()

        for row in existing_uploads:
            conn.execute("DELETE FROM shifts WHERE upload_id = ?", (row["id"],))
            conn.execute("DELETE FROM uploads WHERE id = ?", (row["id"],))

        cur = conn.execute(
            "INSERT INTO uploads (original_filename, stored_filename, uploaded_at) VALUES (?, ?, ?)",
            (file.filename, stored_name, now_iso()),
        )
        upload_id = cur.lastrowid

        for shift in shifts:
            conn.execute(
                """
                INSERT INTO shifts (
                    upload_id, employee, day_name, day_header, shift_date,
                    raw_cell, start_time, end_time, total_hours, row_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    upload_id,
                    shift["employee"],
                    shift["day_name"],
                    shift["day_header"],
                    shift["shift_date"],
                    shift["raw_cell"],
                    shift["start_time"],
                    shift["end_time"],
                    shift["total_hours"],
                    shift["row_index"],
                ),
            )
        conn.commit()

    return JSONResponse(
        {
            "ok": True,
            "upload_id": upload_id,
            "viewer_id": f"upload-{upload_id}",
            "original_filename": file.filename,
            "row_count": len(shifts),
        }
    )


@app.get("/export/{upload_id}")
def export_csv(upload_id: int):
    with get_conn() as conn:
        upload = conn.execute(
            "SELECT * FROM uploads WHERE id = ?",
            (upload_id,),
        ).fetchone()

        shifts = conn.execute(
            """
            SELECT *
            FROM shifts
            WHERE upload_id = ?
            ORDER BY row_index, id
            """,
            (upload_id,),
        ).fetchall()

    if upload is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    export_name = f"upload_{upload_id}.csv"
    export_path = EXPORT_DIR / export_name

    with export_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Employee",
                "Date",
                "Day Header",
                "Day Name",
                "Start",
                "End",
                "Total Hours",
                "Raw Cell",
            ]
        )

        for row in shifts:
            writer.writerow(
                [
                    row["employee"],
                    row["shift_date"],
                    row["day_header"],
                    row["day_name"],
                    row["start_time"],
                    row["end_time"],
                    row["total_hours"],
                    row["raw_cell"],
                ]
            )

    return FileResponse(
        path=export_path,
        filename=export_name,
        media_type="text/csv",
    )


@app.delete("/api/upload/{upload_id}")
def api_delete_upload(upload_id: int):
    with get_conn() as conn:
        upload = conn.execute(
            "SELECT id, stored_filename FROM uploads WHERE id = ?",
            (upload_id,),
        ).fetchone()
        if not upload:
            raise HTTPException(status_code=404, detail="Upload not found")

        conn.execute("DELETE FROM shifts WHERE upload_id = ?", (upload_id,))
        conn.execute("DELETE FROM uploads WHERE id = ?", (upload_id,))
        conn.commit()

    stored_filename = upload["stored_filename"] or ""
    if stored_filename:
        try:
            (UPLOAD_DIR / stored_filename).unlink(missing_ok=True)
        except Exception:
            pass

    return JSONResponse({"ok": True, "upload_id": upload_id})
