import csv
import json
import os
import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List

import pdfplumber
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
LOGGANNT_DIR = STATIC_DIR / "loggannt"

CONFIG_DIR = Path("/config")
UPLOAD_DIR = CONFIG_DIR / "uploads"
EXPORT_DIR = CONFIG_DIR / "exports"
DB_PATH = Path(os.environ.get("ROTA_DB_PATH", "/config/rota.db"))

app = FastAPI(title="Rota PDF Importer")

EMPLOYEE_ID_RE = re.compile(r"\((\d+)\)")
DATE_HEADER_RE = re.compile(r"^([A-Za-z]{3})\((\d{2})/(\d{2})\)$")
TIME_RANGE_RE = re.compile(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})")
DAY_ORDER = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
        conn.commit()


@app.on_event("startup")
def startup() -> None:
    init_db()


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


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
        employee = clean_cell(row[0])
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


@app.get("/", response_class=HTMLResponse)
def viewer(request: Request):
    index_path = LOGGANNT_DIR / "index.html"
    html = index_path.read_text(encoding="utf-8")

    base = ingress_base(request)
    injected = (
        f"<script>window.__APP_BASE__={json.dumps(base)};</script>"
        f"<script src=\"{base}/static/loggannt/ha-bridge.js\"></script>"
    )

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

    html = html.replace(
        "client-side",
        "server-backed",
    )

    injected = (
        f"<script>window.__APP_BASE__={json.dumps(base)};</script>"
        f"<script src=\"{base}/static/loggannt/ha-bridge.js\"></script>"
    )

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
        cur = conn.execute(
            "INSERT INTO uploads (original_filename, stored_filename, uploaded_at) VALUES (?, ?, ?)",
            (file.filename, stored_name, datetime.utcnow().isoformat(timespec="seconds")),
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
