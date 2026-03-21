import csv
import hmac
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import quote

import pdfplumber
from PIL import Image, ImageDraw
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles

BRIDGE_COMPONENT_DIR = Path(__file__).resolve().parents[1] / "ha_bridge_component" / "custom_components" / "rota_importer_bridge"
if str(BRIDGE_COMPONENT_DIR) not in sys.path:
    sys.path.append(str(BRIDGE_COMPONENT_DIR))
import ask_shared

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
ASK_AUTH_CACHE_TTL_SECONDS = 300
ASK_RATE_LIMIT_WINDOW_SECONDS = 60
ASK_RATE_LIMIT_MAX_REQUESTS = 30

_ask_auth_cache: dict[str, tuple[float, bool]] = {}
_ask_rate_limit_state: dict[str, list[float]] = {}
_ask_rate_limit_lock = threading.Lock()


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


def sanitize_person_key(value: str) -> str:
    return clean_cell(value).strip()


def parse_iso_datetime_local(shift_date: str, hhmm: str) -> Optional[datetime]:
    date_clean = clean_cell(shift_date)
    time_clean = clean_cell(hhmm)
    if not date_clean or not re.fullmatch(r"\d{2}:\d{2}", time_clean):
        return None
    try:
        return datetime.strptime(f"{date_clean} {time_clean}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None


def icalendar_escape(value: str) -> str:
    return (
        str(value or "")
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace(",", "\\,")
        .replace(";", "\\;")
    )


def fold_ical_line(line: str, limit: int = 75) -> list[str]:
    encoded = line.encode("utf-8")
    if len(encoded) <= limit:
        return [line]

    folded: list[str] = []
    remaining = line
    while remaining:
        chunk = remaining
        while len(chunk.encode("utf-8")) > limit and len(chunk) > 1:
            chunk = chunk[:-1]
        if not chunk:
            break
        folded.append(chunk)
        remaining = remaining[len(chunk):]
        if remaining:
            remaining = f" {remaining}"
    return folded


def finalize_ical_lines(lines: list[str]) -> str:
    folded_lines: list[str] = []
    for line in lines:
        folded_lines.extend(fold_ical_line(line))
    return "\r\n".join(folded_lines) + "\r\n"


def get_latest_upload_id_for_person(person_name: str) -> Optional[int]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT upload_id
            FROM shifts
            WHERE employee = ?
            ORDER BY upload_id DESC
            LIMIT 1
            """,
            (person_name,),
        ).fetchone()
    return row["upload_id"] if row else None


def get_person_shifts(upload_id: int, person_name: str) -> list[sqlite3.Row]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT upload_id, employee, shift_date, day_name, day_header, raw_cell, start_time, end_time
            FROM shifts
            WHERE upload_id = ?
              AND employee = ?
            ORDER BY shift_date, start_time, id
            """,
            (upload_id, person_name),
        ).fetchall()
    return rows


def get_day_shifts(upload_id: int, shift_date: str) -> list[sqlite3.Row]:
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
            (upload_id, shift_date),
        ).fetchall()
    return rows


def build_staffing_counts(day_rows: list[sqlite3.Row]) -> list[int]:
    counts: list[int] = []
    for hour in range(25):
        minute_mark = hour * 60
        active = 0
        for row in day_rows:
            start_minutes = hhmm_to_minutes(clean_cell(row["start_time"]))
            end_minutes = hhmm_to_minutes(clean_cell(row["end_time"]))
            if start_minutes is None or end_minutes is None:
                continue
            if end_minutes <= start_minutes:
                end_minutes += 24 * 60
                if minute_mark < start_minutes:
                    minute_effective = minute_mark + 24 * 60
                else:
                    minute_effective = minute_mark
            else:
                minute_effective = minute_mark
            if start_minutes <= minute_effective < end_minutes:
                active += 1
        counts.append(active)
    return counts


def render_line_chart_png(counts: list[int], title: str) -> bytes:
    width, height = 920, 360
    margin_left, margin_right = 64, 24
    margin_top, margin_bottom = 42, 52
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    max_count = max(max(counts), 1)

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    draw.text((margin_left, 12), clean_cell(title)[:120], fill="#111827")
    draw.line(
        (
            margin_left,
            margin_top,
            margin_left,
            margin_top + plot_height,
            margin_left + plot_width,
            margin_top + plot_height,
        ),
        fill="#6b7280",
        width=2,
    )

    for y_tick in range(0, max_count + 1):
        y = margin_top + plot_height - int((y_tick / max_count) * plot_height)
        draw.line((margin_left, y, margin_left + plot_width, y), fill="#e5e7eb", width=1)
        draw.text((8, y - 7), str(y_tick), fill="#4b5563")

    for hour in range(0, 25, 2):
        x = margin_left + int((hour / 24) * plot_width)
        draw.line((x, margin_top + plot_height, x, margin_top + plot_height + 6), fill="#6b7280", width=1)
        draw.text((x - 10, margin_top + plot_height + 10), f"{hour:02d}", fill="#4b5563")

    points = []
    for hour, count in enumerate(counts):
        x = margin_left + int((hour / 24) * plot_width)
        y = margin_top + plot_height - int((count / max_count) * plot_height)
        points.append((x, y))

    if len(points) >= 2:
        draw.line(points, fill="#2563eb", width=3)
    for x, y in points:
        draw.ellipse((x - 3, y - 3, x + 3, y + 3), fill="#2563eb")

    buff = io.BytesIO()
    image.save(buff, format="PNG")
    return buff.getvalue()


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


def load_alias_preferences() -> dict[str, str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT alias_preferences FROM app_preferences WHERE singleton_key = 'global'"
        ).fetchone()

    if not row:
        return {}

    aliases = parse_json_object(row["alias_preferences"], {})
    cleaned: dict[str, str] = {}
    for key, value in aliases.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        key_clean = key.strip().lower()
        value_clean = value.strip()
        if key_clean and value_clean:
            cleaned[key_clean] = value_clean
    return cleaned


def alias_for_name(name: str, aliases: dict[str, str]) -> str:
    clean_name = clean_cell(name)
    if not clean_name:
        return ""

    raw_key = f"raw:{clean_name.lower()}"
    clean_key = f"clean:{clean_name.lower()}"
    return aliases.get(raw_key) or aliases.get(clean_key) or clean_name


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


def normalize_subject_working_phrase(message: str, subject_name: str) -> str:
    text = clean_cell(message)
    escaped_subject_name = re.escape(clean_cell(subject_name))
    if not text or not escaped_subject_name:
        return message
    return re.sub(
        rf"^{escaped_subject_name}\s+is\s+working\b",
        "You are working",
        message,
        flags=re.IGNORECASE,
    )


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


def get_latest_upload_id() -> Optional[int]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id
            FROM uploads
            ORDER BY uploaded_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    return row["id"] if row else None


def resolve_question_date(question: str, now_value: Optional[datetime] = None) -> tuple[str, str]:
    return ask_shared.resolve_question_date(question, now_value=now_value)


def parse_ask_intent(question: str) -> str:
    return ask_shared.parse_ask_intent(question)


def extract_bearer_token(authorization_header: str) -> Optional[str]:
    header = clean_cell(authorization_header)
    if not header:
        return None
    match = re.fullmatch(r"Bearer\s+(.+)", header, flags=re.IGNORECASE)
    if not match:
        return None
    token = match.group(1).strip()
    return token or None


def validate_ask_token(token: str) -> bool:
    token_clean = clean_cell(token)
    if not token_clean:
        return False

    now_ts = time.time()
    cached = _ask_auth_cache.get(token_clean)
    if cached and cached[0] > now_ts:
        return cached[1]

    configured_token = clean_cell(os.environ.get("ASK_API_TOKEN", ""))
    if configured_token:
        is_valid = hmac.compare_digest(token_clean, configured_token)
        _ask_auth_cache[token_clean] = (now_ts + ASK_AUTH_CACHE_TTL_SECONDS, is_valid)
        return is_valid

    validate_url = clean_cell(os.environ.get("ASK_AUTH_VALIDATE_URL", f"{HA_CORE_API_BASE}/auth/current_user"))
    if not validate_url:
        _ask_auth_cache[token_clean] = (now_ts + ASK_AUTH_CACHE_TTL_SECONDS, False)
        return False

    req = urlrequest.Request(
        validate_url,
        headers={
            "Authorization": f"Bearer {token_clean}",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlrequest.urlopen(req, timeout=5) as response:
            is_valid = 200 <= response.status < 300
    except Exception:
        is_valid = False

    _ask_auth_cache[token_clean] = (now_ts + ASK_AUTH_CACHE_TTL_SECONDS, is_valid)
    return is_valid


def check_ask_rate_limit(client_key: str) -> bool:
    now_ts = time.time()
    with _ask_rate_limit_lock:
        timestamps = _ask_rate_limit_state.get(client_key, [])
        recent = [ts for ts in timestamps if now_ts - ts < ASK_RATE_LIMIT_WINDOW_SECONDS]
        if len(recent) >= ASK_RATE_LIMIT_MAX_REQUESTS:
            _ask_rate_limit_state[client_key] = recent
            return False
        recent.append(now_ts)
        _ask_rate_limit_state[client_key] = recent
    return True


def get_opening_people(upload_id: int, shift_date: str) -> list[str]:
    day_rows = get_day_shifts(upload_id, shift_date)
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


def get_closing_people(upload_id: int, shift_date: str) -> list[str]:
    day_rows = get_day_shifts(upload_id, shift_date)
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


def build_ask_response(question: str, person: Optional[str] = None, now_value: Optional[datetime] = None) -> dict:
    return ask_shared.build_ask_response(
        db_path=DB_PATH,
        question=question,
        person=person,
        now_value=now_value,
    )


def personalize_name(name: str, subject_name: str, replacement: str = "you") -> str:
    clean_name = clean_cell(name)
    clean_subject = clean_cell(subject_name)
    if clean_name and clean_subject and clean_name.lower() == clean_subject.lower():
        return replacement
    return clean_name


def format_people_for_handover(
    people: List[dict],
    include_non_management_start: bool = False,
    aliases: Optional[dict[str, str]] = None,
    subject_name: str = "",
    subject_replacement: str = "you",
    reference_start_time: str = "",
) -> str:
    labels = []
    for person in people:
        if not isinstance(person, dict):
            continue
        name = alias_for_name(clean_cell(person.get("employee")), aliases or {})
        if not name:
            continue
        name = personalize_name(name, subject_name, replacement=subject_replacement)
        start_time = clean_cell(person.get("start_time"))
        show_start = (
            include_non_management_start
            and start_time
            and not is_management_person(name)
            and (not reference_start_time or start_time != reference_start_time)
        )
        if show_start:
            labels.append(f"{name} ({start_time})")
        else:
            labels.append(name)
    return join_human_names(labels)


def build_shift_end_message(
    end_time: str,
    team_snapshot: dict,
    aliases: Optional[dict[str, str]] = None,
    subject_name: str = "",
) -> str:
    handover_managers = (
        team_snapshot.get("handover_managers_details") if isinstance(team_snapshot.get("handover_managers_details"), list) else []
    )
    handover_team = (
        team_snapshot.get("handover_team_details") if isinstance(team_snapshot.get("handover_team_details"), list) else []
    )

    handover_managers_text = format_people_for_handover(
        handover_managers,
        include_non_management_start=False,
        aliases=aliases,
        subject_name=subject_name,
    )
    handover_team_text = format_people_for_handover(
        handover_team,
        include_non_management_start=True,
        aliases=aliases,
        subject_name=subject_name,
    )

    if handover_managers_text:
        manager_verb = "are" if len(handover_managers) > 1 else "is"
        if handover_team_text:
            return (
                f"From {end_time}, {handover_managers_text} {manager_verb} taking over the shift "
                f"and they are working with {handover_team_text}."
            )
        return f"From {end_time}, {handover_managers_text} {manager_verb} taking over the shift."

    next_shift_people = (
        team_snapshot.get("next_shift_people_details") if isinstance(team_snapshot.get("next_shift_people_details"), list) else []
    )
    next_shift_team = (
        team_snapshot.get("next_shift_team_details") if isinstance(team_snapshot.get("next_shift_team_details"), list) else []
    )
    bridge_people = (
        team_snapshot.get("bridge_people_details") if isinstance(team_snapshot.get("bridge_people_details"), list) else []
    )

    next_people_text = format_people_for_handover(
        next_shift_people,
        include_non_management_start=True,
        aliases=aliases,
        subject_name=subject_name,
    )
    next_team_text = format_people_for_handover(
        next_shift_team,
        include_non_management_start=True,
        aliases=aliases,
        subject_name=subject_name,
    )
    bridge_text = format_people_for_handover(
        bridge_people,
        include_non_management_start=True,
        aliases=aliases,
        subject_name=subject_name,
    )

    next_day_openers = (
        team_snapshot.get("next_day_openers_details") if isinstance(team_snapshot.get("next_day_openers_details"), list) else []
    )
    next_day_opening_team = (
        team_snapshot.get("next_day_opening_team_details")
        if isinstance(team_snapshot.get("next_day_opening_team_details"), list)
        else []
    )

    if not next_people_text:
        if next_day_openers:
            manager_openers = [person for person in next_day_openers if is_management_person(person.get("employee"))]
            non_manager_openers = [person for person in next_day_openers if person not in manager_openers]
            opener_reference_time = clean_cell(manager_openers[0].get("start_time")) if manager_openers else ""

            if manager_openers:
                openers_text = format_people_for_handover(
                    manager_openers,
                    include_non_management_start=False,
                    aliases=aliases,
                    subject_name=subject_name,
                )
                extra_openers = non_manager_openers
            else:
                opener_reference_time = clean_cell(next_day_openers[0].get("start_time")) if next_day_openers else ""
                openers_text = format_people_for_handover(
                    next_day_openers,
                    include_non_management_start=False,
                    aliases=aliases,
                    subject_name=subject_name,
                )
                extra_openers = []

            combined_team = format_people_for_handover(
                extra_openers + next_day_opening_team,
                include_non_management_start=True,
                aliases=aliases,
                subject_name=subject_name,
                reference_start_time=opener_reference_time,
            )

            opener_verb = "are" if len(manager_openers) != 1 else "is"
            if combined_team:
                return f"{openers_text} {opener_verb} opening tomorrow and they are working with {combined_team}."
            return f"{openers_text} {opener_verb} opening tomorrow."
        return f"No one is scheduled to take over after {end_time}."

    management_count = sum(1 for person in next_shift_people if is_management_person(person.get("employee")))
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
            "next_shift_people_details": [],
            "next_shift_time": "",
            "next_shift_team": [],
            "next_shift_team_details": [],
            "bridge_people": [],
            "bridge_people_details": [],
            "next_day_openers": [],
            "next_day_opening_team": [],
            "handover_managers": [],
            "handover_managers_details": [],
            "handover_team": [],
            "handover_team_details": [],
        }

    subject_end_minutes = hhmm_to_minutes(shift_end)
    if subject_end_minutes is None:
        return {
            "opening": "Unknown",
            "closing": "Unknown",
            "takeover": "Unknown",
            "team_with_subject": "Nobody found",
            "next_shift_people": [],
            "next_shift_people_details": [],
            "next_shift_time": "",
            "next_shift_team": [],
            "next_shift_team_details": [],
            "bridge_people": [],
            "bridge_people_details": [],
            "next_day_openers": [],
            "next_day_opening_team": [],
            "handover_managers": [],
            "handover_managers_details": [],
            "handover_team": [],
            "handover_team_details": [],
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
            "next_shift_people_details": [],
            "next_shift_time": "",
            "next_shift_team": [],
            "next_shift_team_details": [],
            "bridge_people": [],
            "bridge_people_details": [],
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
                "start_time": start_time,
                "start_minutes": start_minutes,
                "end_minutes": end_minutes,
            }
        )

    if next_day_shift_rows:
        earliest_next_day_start = min(item["start_minutes"] for item in next_day_shift_rows)
        next_day_openers = sorted(item["employee"] for item in next_day_shift_rows if item["start_minutes"] == earliest_next_day_start)
        next_day_openers_details = sorted(
            [
                {"employee": item["employee"], "start_time": item["start_time"]}
                for item in next_day_shift_rows
                if item["start_minutes"] == earliest_next_day_start
            ],
            key=lambda person: clean_cell(person.get("employee")).lower(),
        )
        opening_managers = [item for item in next_day_shift_rows if item["employee"] in next_day_openers and is_management_person(item["employee"])]
        opening_reference = opening_managers or [item for item in next_day_shift_rows if item["employee"] in next_day_openers]
        opening_window_start = min(item["start_minutes"] for item in opening_reference)
        opening_window_end = max(item["end_minutes"] for item in opening_reference)
        next_day_opening_team = sorted(
            {
                item["employee"]
                for item in next_day_shift_rows
                if item["employee"] not in next_day_openers
                and item["start_minutes"] < opening_window_end
                and item["end_minutes"] > opening_window_start
            }
        )
        next_day_opening_team_details = sorted(
            [
                {"employee": item["employee"], "start_time": item["start_time"]}
                for item in next_day_shift_rows
                if item["employee"] in next_day_opening_team
            ],
            key=lambda person: clean_cell(person.get("employee")).lower(),
        )
    else:
        next_day_openers = []
        next_day_openers_details = []
        next_day_opening_team = []
        next_day_opening_team_details = []

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
        takeover_people_details = sorted(
            [
                {
                    "employee": item["employee"],
                    "start_time": item["start_time"],
                }
                for item in takeover_rows
                if item["start_minutes"] == earliest_takeover
            ],
            key=lambda person: clean_cell(person.get("employee")).lower(),
        )
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
        takeover_team_details = sorted(
            [
                {"employee": item["employee"], "start_time": item["start_time"]}
                for item in shift_rows
                if item["employee"] in takeover_team
            ],
            key=lambda person: clean_cell(person.get("employee")).lower(),
        )
        bridge_people_details = sorted(
            [
                {"employee": item["employee"], "start_time": item["start_time"]}
                for item in shift_rows
                if item["employee"] in bridge_people
            ],
            key=lambda person: clean_cell(person.get("employee")).lower(),
        )
    else:
        takeover = "Nobody scheduled after shift"
        takeover_people = []
        takeover_people_details = []
        takeover_time = ""
        takeover_team = []
        takeover_team_details = []
        bridge_people = []
        bridge_people_details = []

    handover_manager_rows = sorted(
        [
            item
            for item in shift_rows
            if item["employee"] != subject_name
            and is_management_person(item["employee"])
            and item["start_minutes"] <= subject_end_minutes
            and item["end_minutes"] > subject_end_minutes
        ],
        key=lambda item: (item["start_minutes"], item["employee"].lower()),
    )
    handover_managers = [item["employee"] for item in handover_manager_rows]
    handover_managers_details = [
        {"employee": item["employee"], "start_time": item["start_time"]} for item in handover_manager_rows
    ]

    next_manager_start_minutes = min(
        (
            item["start_minutes"]
            for item in shift_rows
            if item["employee"] != subject_name
            and is_management_person(item["employee"])
            and item["start_minutes"] > subject_end_minutes
        ),
        default=None,
    )

    handover_cutoff = next_manager_start_minutes if next_manager_start_minutes is not None else float("inf")
    handover_team_rows = sorted(
        [
            item
            for item in shift_rows
            if item["employee"] != subject_name
            and item["employee"] not in handover_managers
            and item["end_minutes"] > subject_end_minutes
            and item["start_minutes"] < handover_cutoff
        ],
        key=lambda item: (item["start_minutes"], item["employee"].lower()),
    )
    handover_team = [item["employee"] for item in handover_team_rows]
    handover_team_details = [
        {"employee": item["employee"], "start_time": item["start_time"]} for item in handover_team_rows
    ]

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
        "next_shift_people_details": takeover_people_details,
        "next_shift_time": takeover_time,
        "next_shift_team": takeover_team,
        "next_shift_team_details": takeover_team_details,
        "bridge_people": bridge_people,
        "bridge_people_details": bridge_people_details,
        "next_day_openers": next_day_openers,
        "next_day_openers_details": next_day_openers_details,
        "next_day_opening_team": next_day_opening_team,
        "next_day_opening_team_details": next_day_opening_team_details,
        "handover_managers": handover_managers,
        "handover_managers_details": handover_managers_details,
        "handover_team": handover_team,
        "handover_team_details": handover_team_details,
    }


def build_notification_payload_from_settings() -> dict:
    settings_row = get_notification_settings_row()
    settings = serialize_notification_settings(settings_row)
    alias_preferences = load_alias_preferences()

    today = datetime.now().strftime("%Y-%m-%d")
    notifications = []

    for subject_name in settings["subject_names"]:
        notify_service = clean_cell(settings["subject_service_map"].get(subject_name))
        if not notify_service:
            continue
        is_critical = bool(sanitize_bool(settings.get("subject_critical_map", {}).get(subject_name), default=False))

        rota_context = get_subject_shift_and_coworkers(subject_name, today)
        subject_alias = alias_for_name(subject_name, alias_preferences)

        coworkers_list = rota_context.get("coworkers_list") if isinstance(rota_context.get("coworkers_list"), list) else []
        aliased_coworkers_list = []
        for coworker in coworkers_list:
            label = clean_cell(coworker)
            if not label:
                continue
            if " until " in label:
                base_name, end_part = label.split(" until ", 1)
                aliased_coworkers_list.append(f"{alias_for_name(base_name, alias_preferences)} until {end_part}")
                continue
            if re.fullmatch(r".+\s*\(\d{2}:\d{2}\)", label):
                base_name, time_part = label.rsplit("(", 1)
                aliased_coworkers_list.append(f"{alias_for_name(base_name.strip(), alias_preferences)} ({time_part}")
                continue
            aliased_coworkers_list.append(alias_for_name(label, alias_preferences))

        coworkers_text = ", ".join(aliased_coworkers_list) if aliased_coworkers_list else "Nobody found"

        context = {
            "subject_name": subject_alias,
            "status": rota_context["status"],
            "today": rota_context["today"],
            "shift": rota_context["shift"],
            "start_time": rota_context.get("start_time") or "",
            "end_time": rota_context.get("end_time") or "",
            "coworkers": coworkers_text,
            "upload_id": rota_context["upload_id"] or "",
        }

        title = render_simple_template(settings["title_template"], context).strip()
        message = render_simple_template(settings["message_template"], context).strip()
        message = normalize_subject_working_phrase(message, subject_alias)

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
                message = f"You're working today with: {coworkers_text}."

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

        end_message = build_shift_end_message(
            end_time,
            team_snapshot,
            aliases=alias_preferences,
            subject_name=subject_name,
        )

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


async def parse_ask_request_body(request: Request) -> tuple[Optional[dict], Optional[JSONResponse]]:
    try:
        body = await request.json()
    except Exception:
        return None, JSONResponse(status_code=400, content={"error": "Invalid JSON payload"})

    if not isinstance(body, dict):
        return None, JSONResponse(status_code=400, content={"error": "JSON body must be an object"})

    question = body.get("question")
    person = body.get("person")

    if not isinstance(question, str) or not clean_cell(question):
        return None, JSONResponse(
            status_code=400, content={"error": "question is required and must be a non-empty string"}
        )

    if person is not None and not isinstance(person, str):
        return None, JSONResponse(status_code=400, content={"error": "person must be a string"})

    return {"question": question, "person": person}, None


def build_ask_json_response(question: str, person: Optional[str]) -> JSONResponse:
    try:
        response_payload = build_ask_response(question=question, person=person)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    return JSONResponse(response_payload)


@app.post("/api/ask")
async def api_ask(request: Request):
    authorization_header = request.headers.get("Authorization", "")
    bearer_token = extract_bearer_token(authorization_header)
    if not bearer_token or not validate_ask_token(bearer_token):
        return JSONResponse(status_code=401, content={"error": "unauthorized"})

    client_ip = request.client.host if request.client and request.client.host else "unknown"
    if not check_ask_rate_limit(client_ip):
        return JSONResponse(status_code=429, content={"error": "rate_limited"})

    parsed_body, error_response = await parse_ask_request_body(request)
    if error_response:
        return error_response

    return build_ask_json_response(question=parsed_body["question"], person=parsed_body["person"])


@app.post("/api/ha/ask")
async def api_ha_bridge_ask(request: Request):
    """Home Assistant proxy-friendly bridge endpoint for Siri/Shortcuts.

    This endpoint is intended to be called through:
    /api/hassio/addons/<addon_slug>/proxy/api/ha/ask

    Home Assistant/Nabu Casa protects the outer endpoint with Home Assistant auth.
    The add-on still validates Authorization Bearer when it is supplied.
    """
    authorization_header = request.headers.get("Authorization", "")
    bearer_token = extract_bearer_token(authorization_header)
    if not bearer_token or not validate_ask_token(bearer_token):
        return JSONResponse(status_code=401, content={"error": "unauthorized"})

    parsed_body, error_response = await parse_ask_request_body(request)
    if error_response:
        return error_response

    return build_ask_json_response(question=parsed_body["question"], person=parsed_body["person"])


@app.get("/api/people/{person_name}/calendar.ics")
def api_person_calendar(person_name: str, request: Request, upload_id: Optional[int] = None):
    person_key = sanitize_person_key(person_name)
    if not person_key:
        raise HTTPException(status_code=400, detail="Person name is required")

    selected_upload_id = upload_id or get_latest_upload_id_for_person(person_key)
    if not selected_upload_id:
        raise HTTPException(status_code=404, detail="No shifts found for this person")

    shifts = get_person_shifts(selected_upload_id, person_key)
    if not shifts:
        raise HTTPException(status_code=404, detail="No shifts found for this person in selected upload")

    ingress = ingress_base(request)
    external_base = str(request.base_url).rstrip("/")
    base_url = f"{external_base}{ingress}" if ingress else external_base
    now_utc = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    encoded_person = quote(person_key, safe="")

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Rota Importer//Per Person Calendar//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{icalendar_escape(person_key)} Rota",
    ]

    for row in shifts:
        shift_date = clean_cell(row["shift_date"])
        start_time = clean_cell(row["start_time"])
        end_time = clean_cell(row["end_time"])
        raw_cell = clean_cell(row["raw_cell"])
        shift_start = parse_iso_datetime_local(shift_date, start_time)
        shift_end = parse_iso_datetime_local(shift_date, end_time)
        if not shift_start or not shift_end:
            continue
        if shift_end <= shift_start:
            shift_end += timedelta(days=1)

        uid = f"rota-{selected_upload_id}-{encoded_person}-{shift_date}-{start_time}-{end_time}@rota-importer"
        attach_url = (
            f"{base_url}/api/people/{encoded_person}/charts/{shift_date}.png"
            f"?upload_id={selected_upload_id}"
        )

        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{now_utc}",
                f"DTSTART:{shift_start.strftime('%Y%m%dT%H%M%S')}",
                f"DTEND:{shift_end.strftime('%Y%m%dT%H%M%S')}",
                f"SUMMARY:{icalendar_escape(f'{person_key} shift')}",
                f"DESCRIPTION:{icalendar_escape(raw_cell or f'{start_time}-{end_time}')}",
                f"ATTACH;FMTTYPE=image/png:{attach_url}",
                "END:VEVENT",
            ]
        )

    lines.append("END:VCALENDAR")
    payload = finalize_ical_lines(lines)
    filename = f"{re.sub(r'[^a-zA-Z0-9_-]+', '_', person_key)}.ics"
    headers = {"Content-Disposition": f'inline; filename="{filename}"'}
    return PlainTextResponse(content=payload, media_type="text/calendar; charset=utf-8", headers=headers)


@app.get("/api/people/{person_name}/charts/{shift_date}.png")
def api_person_shift_chart(person_name: str, shift_date: str, upload_id: Optional[int] = None):
    person_key = sanitize_person_key(person_name)
    if not person_key:
        raise HTTPException(status_code=400, detail="Person name is required")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", clean_cell(shift_date)):
        raise HTTPException(status_code=400, detail="shift_date must be YYYY-MM-DD")

    selected_upload_id = upload_id or get_latest_upload_id_for_person(person_key)
    if not selected_upload_id:
        raise HTTPException(status_code=404, detail="No shifts found for this person")

    person_rows = get_person_shifts(selected_upload_id, person_key)
    if not any(clean_cell(row["shift_date"]) == shift_date for row in person_rows):
        raise HTTPException(status_code=404, detail="No shift found for this person/date in selected upload")

    day_rows = get_day_shifts(selected_upload_id, shift_date)
    counts = build_staffing_counts(day_rows)
    image_bytes = render_line_chart_png(counts, f"Team staffing • {shift_date}")
    return Response(content=image_bytes, media_type="image/png")


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
