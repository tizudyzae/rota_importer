from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))
import app as app_module


def _client_with_week(tmp_path):
    app_module.DB_PATH = tmp_path / "rota.db"
    app_module.UPLOAD_DIR = tmp_path / "uploads"
    app_module.EXPORT_DIR = tmp_path / "exports"
    app_module.start_auto_notification_worker = lambda: None
    app_module.stop_auto_notification_worker = lambda: None
    app_module.init_db()
    with app_module.get_conn() as conn:
        upload_id = conn.execute(
            "INSERT INTO uploads (original_filename, stored_filename, uploaded_at) VALUES (?, ?, ?)",
            ("rota_2026.pdf", "rota.pdf", "2026-07-20T12:00:00"),
        ).lastrowid
        for index, date in enumerate(("2026-07-19", "2026-07-25"), start=1):
            conn.execute(
                "INSERT INTO shifts (upload_id, employee, employee_id, day_name, day_header, "
                "shift_date, raw_cell, start_time, end_time, total_hours, row_index) "
                "VALUES (?, 'Existing', '100', ?, '', ?, '', '', '', '', ?)",
                (upload_id, "sun" if index == 1 else "sat", date, index),
            )
        conn.commit()
    return TestClient(app_module.app), upload_id


def test_add_manual_shift_persists_pdf_equivalent_fields(tmp_path):
    client, upload_id = _client_with_week(tmp_path)

    response = client.post(
        f"/api/upload/{upload_id}/shifts",
        json={
            "employee": "Jane",
            "employee_id": "123456",
            "shift_date": "2026-07-22",
            "start_time": "22:00",
            "end_time": "06:30",
        },
    )

    assert response.status_code == 201
    with app_module.get_conn() as conn:
        row = conn.execute("SELECT * FROM shifts WHERE id = ?", (response.json()["shift_id"],)).fetchone()
    assert row["employee"] == "Jane"
    assert row["employee_id"] == "123456"
    assert row["day_name"] == "wed"
    assert row["day_header"] == "Wed(07/22)"
    assert row["raw_cell"] == "Whole Shift: 22:00 - 06:30"
    assert row["start_time"] == "22:00"
    assert row["end_time"] == "06:30"
    assert row["total_hours"] == "8.5"


def test_add_manual_shift_rejects_dates_outside_selected_week_and_duplicates(tmp_path):
    client, upload_id = _client_with_week(tmp_path)
    payload = {
        "employee": "Jane",
        "employee_id": "123456",
        "shift_date": "2026-07-22",
        "start_time": "09:00",
        "end_time": "17:00",
    }

    assert client.post(f"/api/upload/{upload_id}/shifts", json=payload).status_code == 201
    duplicate = client.post(f"/api/upload/{upload_id}/shifts", json=payload)
    assert duplicate.status_code == 409
    payload["shift_date"] = "2026-07-26"
    outside = client.post(f"/api/upload/{upload_id}/shifts", json=payload)
    assert outside.status_code == 400
    assert "selected week" in outside.json()["detail"]
