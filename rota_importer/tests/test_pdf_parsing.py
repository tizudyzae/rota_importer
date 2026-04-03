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


def test_parse_shift_cell_supports_old_and_new_formats():
    cases = [
        ("Whole Shift: 06:00 - 14:30", "06:00", "14:30", False),
        ("OFF: 00:00 - 24:00", "", "", True),
        ("06:00 - 14:30", "06:00", "14:30", False),
        ("00:00 - 24:00", "", "", True),
        ("Whole Shift:   06:00 - 14:30", "06:00", "14:30", False),
        ("off: 00:00 - 24:00", "", "", True),
        ("Shift A / 06:00 - 14:30", "06:00", "14:30", False),
    ]

    for raw_text, expected_start, expected_end, expected_is_off in cases:
        parsed = app_module.parse_shift_cell(raw_text)
        assert parsed is not None
        assert parsed["raw_cell"] == raw_text
        assert parsed["start_time"] == expected_start
        assert parsed["end_time"] == expected_end
        assert parsed["is_off"] is expected_is_off


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


def test_parse_pdf_to_shift_rows_handles_old_and_new_off_and_working_cells(monkeypatch, tmp_path):
    monkeypatch.setattr(
        app_module,
        "extract_employee_table",
        lambda _path: (
            ["Employee", "Sun(04/12)", "Mon(04/13)", "Tue(04/14)", "Wed(04/15)", "Total Hours"],
            [["Doe, Jane (123456)", "Whole Shift: 06:00 - 14:30", "OFF: 00:00 - 24:00", "14:30 - 23:00", "00:00 - 24:00", "16.0"]],
        ),
    )

    shifts = app_module.parse_pdf_to_shift_rows(tmp_path / "sample.pdf", "schedule_2026.pdf")

    by_day = {shift["day_name"]: shift for shift in shifts}
    assert by_day["sun"]["start_time"] == "06:00"
    assert by_day["sun"]["end_time"] == "14:30"
    assert by_day["mon"]["start_time"] == ""
    assert by_day["mon"]["end_time"] == ""
    assert by_day["tue"]["start_time"] == "14:30"
    assert by_day["tue"]["end_time"] == "23:00"
    assert by_day["wed"]["start_time"] == ""
    assert by_day["wed"]["end_time"] == ""
