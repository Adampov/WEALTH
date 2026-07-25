"""Tests for safe environment and operating-mode boundaries."""

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsError

from wealth.domain.events import Environment
from wealth.settings import (
    ArchitectureStyle,
    OperatingMode,
    PrimaryMarket,
    RuntimeSettings,
    TradingType,
)

_RUNTIME_ENVIRONMENT_VARIABLES = (
    "WEALTH_ENVIRONMENT",
    "WEALTH_OPERATING_MODE",
    "WEALTH_PRIMARY_MARKET",
    "WEALTH_TRADING_TYPE",
    "WEALTH_SYSTEM_TIMEZONE",
    "WEALTH_USER_TIMEZONE",
    "WEALTH_BASE_CURRENCY",
    "WEALTH_ARCHITECTURE_STYLE",
    "WEALTH_LIVE_TRADING_ENABLED",
    "WEALTH_LEVERAGE_ENABLED",
    "WEALTH_WITHDRAWALS_ENABLED",
    "WEALTH_EXTERNAL_NOTIFICATIONS_ENABLED",
    "WEALTH_AUTONOMOUS_LIVE_EXECUTION_ENABLED",
    "WEALTH_LOG_LEVEL",
)


@pytest.fixture(autouse=True)
def clear_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep host-level runtime variables from changing the tested contract."""

    for variable_name in _RUNTIME_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(variable_name, raising=False)


def test_runtime_defaults_fail_closed() -> None:
    settings = RuntimeSettings()

    assert settings.environment is Environment.DEVELOPMENT
    assert settings.operating_mode is OperatingMode.RESEARCH
    assert settings.primary_market is PrimaryMarket.CRYPTO
    assert settings.trading_type is TradingType.SPOT
    assert settings.system_timezone == "UTC"
    assert settings.user_timezone == "Asia/Jerusalem"
    assert settings.base_currency == "USD"
    assert settings.architecture_style is ArchitectureStyle.MODULAR_MONOLITH
    assert settings.live_trading_enabled is False
    assert settings.leverage_enabled is False
    assert settings.withdrawals_enabled is False
    assert settings.external_notifications_enabled is False
    assert settings.autonomous_live_execution_enabled is False
    assert settings.log_level == "INFO"


def test_runtime_identity_loads_typed_environment_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEALTH_ENVIRONMENT", "paper")
    monkeypatch.setenv("WEALTH_OPERATING_MODE", "paper")
    monkeypatch.setenv("WEALTH_PRIMARY_MARKET", "crypto")
    monkeypatch.setenv("WEALTH_TRADING_TYPE", "spot")
    monkeypatch.setenv("WEALTH_SYSTEM_TIMEZONE", "UTC")
    monkeypatch.setenv("WEALTH_USER_TIMEZONE", "Asia/Jerusalem")
    monkeypatch.setenv("WEALTH_BASE_CURRENCY", "USD")
    monkeypatch.setenv("WEALTH_ARCHITECTURE_STYLE", "modular_monolith")
    monkeypatch.setenv("WEALTH_LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("WEALTH_LEVERAGE_ENABLED", "false")
    monkeypatch.setenv("WEALTH_WITHDRAWALS_ENABLED", "false")
    monkeypatch.setenv("WEALTH_EXTERNAL_NOTIFICATIONS_ENABLED", "false")
    monkeypatch.setenv("WEALTH_AUTONOMOUS_LIVE_EXECUTION_ENABLED", "false")
    monkeypatch.setenv("WEALTH_LOG_LEVEL", "WARNING")

    settings = RuntimeSettings()

    assert settings.environment is Environment.PAPER
    assert settings.operating_mode is OperatingMode.PAPER
    assert settings.primary_market is PrimaryMarket.CRYPTO
    assert settings.trading_type is TradingType.SPOT
    assert settings.architecture_style is ArchitectureStyle.MODULAR_MONOLITH
    assert settings.live_trading_enabled is False
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


def test_runtime_rejects_live_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEALTH_ENVIRONMENT", "live")
    monkeypatch.setenv("WEALTH_OPERATING_MODE", "research")

    with pytest.raises(
        ValidationError,
        match="live environment is unavailable in the current project phase",
    ):
        RuntimeSettings()


@pytest.mark.parametrize(
    ("variable_name", "control_name"),
    [
        ("WEALTH_LIVE_TRADING_ENABLED", "live_trading_enabled"),
        ("WEALTH_LEVERAGE_ENABLED", "leverage_enabled"),
        ("WEALTH_WITHDRAWALS_ENABLED", "withdrawals_enabled"),
        ("WEALTH_EXTERNAL_NOTIFICATIONS_ENABLED", "external_notifications_enabled"),
        (
            "WEALTH_AUTONOMOUS_LIVE_EXECUTION_ENABLED",
            "autonomous_live_execution_enabled",
        ),
    ],
)
def test_runtime_rejects_unavailable_control_escalation(
    monkeypatch: pytest.MonkeyPatch,
    variable_name: str,
    control_name: str,
) -> None:
    monkeypatch.setenv(variable_name, "true")

    with pytest.raises(ValidationError, match=control_name):
        RuntimeSettings()


@pytest.mark.parametrize(
    ("variable_name", "value"),
    [
        ("WEALTH_PRIMARY_MARKET", "equities"),
        ("WEALTH_TRADING_TYPE", "futures"),
        ("WEALTH_SYSTEM_TIMEZONE", "Asia/Jerusalem"),
        ("WEALTH_USER_TIMEZONE", "UTC"),
        ("WEALTH_BASE_CURRENCY", "EUR"),
        ("WEALTH_ARCHITECTURE_STYLE", "microservices"),
    ],
)
def test_runtime_rejects_unapproved_identity_values(
    monkeypatch: pytest.MonkeyPatch,
    variable_name: str,
    value: str,
) -> None:
    monkeypatch.setenv(variable_name, value)

    with pytest.raises((SettingsError, ValidationError)):
        RuntimeSettings()


def test_runtime_forbids_unknown_settings() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        RuntimeSettings.model_validate({"unexpected": True})
