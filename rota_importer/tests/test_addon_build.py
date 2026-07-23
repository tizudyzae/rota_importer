from pathlib import Path


ADDON_DIR = Path(__file__).resolve().parents[1]


def test_uvicorn_dependency_avoids_native_standard_extras():
    requirements = {
        line.strip()
        for line in (ADDON_DIR / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert "uvicorn" in requirements
    assert not any(requirement.startswith("uvicorn[") for requirement in requirements)


def test_dockerfile_runs_application_import_smoke_check():
    dockerfile = (ADDON_DIR / "Dockerfile").read_text(encoding="utf-8")

    assert 'python -c "import app"' in dockerfile
