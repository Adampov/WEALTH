"""Tests for safe environment and operating-mode boundaries."""

import pytest
from pydantic import ValidationError

from wealth.domain.events import Environment
from wealth.settings import OperatingMode, RuntimeSettings


def test_runtime_defaults_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WEALTH_ENVIRONMENT", raising=False)
    monkeypatch.delenv("WEALTH_OPERATING_MODE", raising=False)
    monkeypatch.delenv("WEALTH_LOG_LEVEL", raising=False)

    settings = RuntimeSettings()

    assert settings.environment is Environment.DEVELOPMENT
    assert settings.operating_mode is OperatingMode.RESEARCH
    assert settings.log_level == "INFO"


def test_runtime_identity_loads_typed_environment_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEALTH_ENVIRONMENT", "paper")
    monkeypatch.setenv("WEALTH_OPERATING_MODE", "paper")
    monkeypatch.setenv("WEALTH_LOG_LEVEL", "WARNING")

    settings = RuntimeSettings()

    assert settings.environment is Environment.PAPER
    assert settings.operating_mode is OperatingMode.PAPER
    assert settings.log_level == "WARNING"


@pytest.mark.parametrize(
    ("environment", "mode"),
    [
        ("development", "paper"),
        ("test", "automatic"),
        ("research", "semi_automatic"),
        ("paper", "automatic"),
    ],
)
def test_runtime_rejects_mode_escalation(
    monkeypatch: pytest.MonkeyPatch,
    environment: str,
    mode: str,
) -> None:
    monkeypatch.setenv("WEALTH_ENVIRONMENT", environment)
    monkeypatch.setenv("WEALTH_OPERATING_MODE", mode)

    with pytest.raises(ValidationError, match="is not allowed"):
        RuntimeSettings()
