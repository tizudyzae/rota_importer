from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))
import app as app_module


def test_extract_employee_table_finds_header_after_title_rows(monkeypatch, tmp_path):
    class FakePage:
        def extract_tables(self):
            return [
                [
                    ["Wall Schedule Review", "", "", ""],
                    ["5 departments", "", "", ""],
                    ["Employee", "Sun(04/12)", "Mon(04/13)", "Total Hours"],
                    ["Doe, Jane (123456)", "06:00 - 14:30", "", "8.5"],
                ]
            ]

    class FakePdf:
        pages = [FakePage()]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(app_module.pdfplumber, "open", lambda *_args, **_kwargs: FakePdf())

    header, rows = app_module.extract_employee_table(tmp_path / "sample.pdf")

    assert header[0] == "Employee"
    assert header[1] == "Sun(04/12)"
    assert len(rows) == 1
    assert rows[0][0] == "Doe, Jane (123456)"


def test_parse_pdf_to_shift_rows_uses_detected_header(monkeypatch, tmp_path):
    monkeypatch.setattr(
        app_module,
        "extract_employee_table",
        lambda _path: (
            ["Employee", "Sun(04/12)", "Mon(04/13)", "Total Hours"],
            [["Doe, Jane (123456)", "06:00 - 14:30", "", "8.5"]],
        ),
    )

    shifts = app_module.parse_pdf_to_shift_rows(tmp_path / "sample.pdf", "schedule_2026.pdf")

    sunday = next((shift for shift in shifts if shift["day_name"] == "sun"), None)
    assert sunday is not None
    assert sunday["employee"] == "Jane"
    assert sunday["start_time"] == "06:00"
    assert sunday["end_time"] == "14:30"
