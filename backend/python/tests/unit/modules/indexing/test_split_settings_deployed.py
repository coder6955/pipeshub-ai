"""Every setting the split code reads reaches the container: Compose and Helm pass it, with the code's default."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from app.modules.indexing.split_rollback import ROLLBACK_ENV, ROLLBACK_ORG_ENV
from app.modules.indexing.split_seam import _split_timeout_seconds
from app.modules.parsers.pdf.split import config


def _repo_root() -> Path:
    """The checkout root (the directory with ``deployment/``), found by walking up, never by a fixed depth.

    An image that ships only ``backend/python`` has no deployment files: the file-reading tests skip there, and fail in CI,
    where a missing checkout means the check is not running.
    """
    for parent in Path(__file__).resolve().parents:
        if (parent / "deployment" / "docker-compose").is_dir():
            return parent
    if os.getenv("CI"):
        pytest.fail("deployment/ not found above this test: the deployed-settings check cannot run in CI")
    pytest.skip("deployment/ is not part of this image")


@pytest.fixture
def repo() -> Path:
    return _repo_root()


SETTINGS = sorted({
    config.ENABLED_ENV, config.MIN_PAGES_ENV, config.PART_PAGES_ENV, config.MAX_PART_BYTES_ENV, config.OVERLAP_PAGES_ENV,
    ROLLBACK_ENV, ROLLBACK_ORG_ENV, "PDF_SPLIT_TIMEOUT_SECONDS",
})
COMPOSE = [
    "docker-compose.yml", "docker-compose.build.neo4j.yml", "docker-compose.integration.arango.yml",
    "docker-compose.integration.neo4j.yml",
]
DEFAULTS = {config.ENABLED_ENV: "true", config.MIN_PAGES_ENV: "500", config.OVERLAP_PAGES_ENV: "1", "PDF_SPLIT_TIMEOUT_SECONDS": "1800"}


@pytest.mark.parametrize("name", COMPOSE)
@pytest.mark.parametrize("setting", SETTINGS)
def test_a_compose_file_that_runs_indexing_passes_the_setting_with_the_code_default(repo: Path, name: str, setting: str) -> None:
    text = (repo / "deployment" / "docker-compose" / name).read_text()
    assert f"- {setting}=${{{setting}:-{DEFAULTS.get(setting, '')}}}" in text


def test_the_helm_chart_passes_every_setting(repo: Path) -> None:
    template = (repo / "deployment/helm/pipeshub-ai/templates/deployment.yaml").read_text()
    for setting in SETTINGS:
        assert f"- name: {setting}\n" in template, setting


def test_the_env_template_lists_every_setting(repo: Path) -> None:
    text = (repo / "deployment/docker-compose/env.template").read_text()
    for setting in SETTINGS:
        assert f"\n{setting}=" in text, setting


@pytest.mark.parametrize(("raw", "expected"), [(None, 1800.0), ("", 1800.0), ("  ", 1800.0), ("abc", 1800.0), ("0", 1800.0), ("-5", 1800.0), ("90", 90.0)])
def test_an_empty_or_invalid_timeout_is_the_default_never_a_crash(monkeypatch: pytest.MonkeyPatch, raw: str | None, expected: float) -> None:
    if raw is None:
        monkeypatch.delenv("PDF_SPLIT_TIMEOUT_SECONDS", raising=False)
    else:
        monkeypatch.setenv("PDF_SPLIT_TIMEOUT_SECONDS", raw)
    assert _split_timeout_seconds() == expected
