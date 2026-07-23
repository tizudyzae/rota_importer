import sqlite3
from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))
import app as app_module


def _build_client(tmp_path):
    app_module.DB_PATH = tmp_path / "rota.db"
    app_module.UPLOAD_DIR = tmp_path / "uploads"
    app_module.EXPORT_DIR = tmp_path / "exports"
    app_module.start_auto_notification_worker = lambda: None
    app_module.stop_auto_notification_worker = lambda: None
    app_module.init_db()
    return TestClient(app_module.app)


def _seed_shifts():
    rows = [
        ("Nathan Udohaya", "215149", "2026-07-01", "wed", "09:00 - 17:00", "09:00", "17:00", "8", 1),
        ("Nathan Other", "999999", "2026-07-01", "wed", "08:00 - 16:00", "08:00", "16:00", "8", 2),
        ("Shop Colleague", "123456", "2026-07-02", "thu", "10:00 - 18:00", "10:00", "18:00", "8", 3),
        ("Nathan Udohaya", "215149", "2026-07-03", "fri", "22:00 - 06:00", "22:00", "06:00", "8", 4),
        ("Nathan Udohaya", "215149", "2026-07-04", "sat", "OFF: 00:00 - 24:00", "", "", "", 5),
        ("Nathan Udohaya", "215149", "2026-07-05", "sun", "", "", "", "", 6),
        ("Nathan Udohaya", "215149", "2026-07-10", "fri", "07:00 - 12:00", "07:00", "12:00", "5", 7),
    ]
    with app_module.get_conn() as conn:
        upload_id = conn.execute(
            "INSERT INTO uploads (original_filename, stored_filename, uploaded_at) VALUES (?, ?, ?)",
            ("private.pdf", "stored.pdf", "2026-07-01T00:00:00"),
        ).lastrowid
        conn.executemany(
            """
            INSERT INTO shifts (
                upload_id, employee, employee_id, shift_date, day_name, day_header,
                raw_cell, start_time, end_time, total_hours, row_index
            ) VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)
            """,
            [(upload_id, *row) for row in rows],
        )
        conn.commit()


def test_endpoint_returns_only_fixed_employee_and_safe_fields(tmp_path):
    client = _build_client(tmp_path)
    _seed_shifts()

    response = client.get(
        "/api/my-wage-shifts", params={"start_date": "2026-07-01", "end_date": "2026-07-10"}
    )

    assert response.status_code == 200
    payload = response.json()
    assert [row["shift_date"] for row in payload] == ["2026-07-01", "2026-07-03", "2026-07-10"]
    assert payload[1]["start_time"] == "22:00"
    assert payload[1]["end_time"] == "06:00"
    assert all(
        set(row)
        == {"shift_id", "shift_date", "day_name", "start_time", "end_time", "raw_cell", "total_hours"}
        for row in payload
    )


def test_endpoint_filters_requested_date_range(tmp_path):
    client = _build_client(tmp_path)
    _seed_shifts()

    response = client.get(
        "/api/my-wage-shifts", params={"start_date": "2026-07-02", "end_date": "2026-07-09"}
    )

    assert response.status_code == 200
    assert [row["shift_date"] for row in response.json()] == ["2026-07-03"]


def test_endpoint_rejects_invalid_reversed_and_excessive_ranges(tmp_path):
    client = _build_client(tmp_path)
    cases = [
        ("not-a-date", "2026-07-02"),
        ("2026-02-30", "2026-03-01"),
        ("2026-07-02", "2026-07-01"),
        ("2026-01-01", "2026-02-12"),
    ]

    for start_date, end_date in cases:
        response = client.get(
            "/api/my-wage-shifts", params={"start_date": start_date, "end_date": end_date}
        )
        assert response.status_code == 400


def test_migration_adds_and_backfills_employee_id_without_deleting_rows(tmp_path):
    db_path = tmp_path / "rota.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_filename TEXT NOT NULL,
                stored_filename TEXT NOT NULL,
                uploaded_at TEXT NOT NULL
            );
            CREATE TABLE shifts (
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
                row_index INTEGER NOT NULL
            );
            INSERT INTO uploads VALUES (1, 'old.pdf', 'old.pdf', '2026-01-01T00:00:00');
            INSERT INTO shifts VALUES
                (1, 1, 'Nathan Udohaya (215149)', 'mon', 'Mon', '2026-01-01', '09:00 - 17:00', '09:00', '17:00', '8', 1),
                (2, 1, 'Unrecoverable Employee', 'mon', 'Mon', '2026-01-01', 'OFF', '', '', '', 2);
            """
        )

    app_module.DB_PATH = db_path
    app_module.UPLOAD_DIR = tmp_path / "uploads"
    app_module.EXPORT_DIR = tmp_path / "exports"
    app_module.start_auto_notification_worker = lambda: None
    app_module.init_db()

    with app_module.get_conn() as conn:
        rows = conn.execute("SELECT employee, employee_id FROM shifts ORDER BY id").fetchall()
    assert len(rows) == 2
    assert rows[0]["employee_id"] == "215149"
    assert rows[1]["employee_id"] is None
