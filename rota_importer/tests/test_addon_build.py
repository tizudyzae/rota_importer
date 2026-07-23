from pathlib import Path
import re

import yaml


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


def test_dockerfile_build_from_has_a_valid_default():
    dockerfile = (ADDON_DIR / "Dockerfile").read_text(encoding="utf-8")

    match = re.search(r"^ARG BUILD_FROM=(\S+)$", dockerfile, re.MULTILINE)
    assert match is not None
    assert match.group(1) == "ghcr.io/home-assistant/amd64-base:latest"


def test_build_yaml_maps_every_configured_architecture():
    config = yaml.safe_load((ADDON_DIR / "config.yaml").read_text(encoding="utf-8"))
    build = yaml.safe_load((ADDON_DIR / "build.yaml").read_text(encoding="utf-8"))

    build_from = build["build_from"]
    assert set(build_from) == set(config["arch"])
    for architecture, image in build_from.items():
        assert image == f"ghcr.io/home-assistant/{architecture}-base:latest"
