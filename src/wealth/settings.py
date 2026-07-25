"""Validated runtime identity loaded from environment variables."""

from enum import StrEnum
from typing import Literal, Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from wealth.domain.events import Environment


class OperatingMode(StrEnum):
    """Progressive-autonomy modes defined by the project charter."""

    RESEARCH = "research"
    ADVISORY = "advisory"
    SEMI_AUTOMATIC = "semi_automatic"
    PAPER = "paper"
    AUTOMATIC = "automatic"


class PrimaryMarket(StrEnum):
    """Markets currently approved by the project charter."""

    CRYPTO = "crypto"


class TradingType(StrEnum):
    """Trading products currently approved by the project charter."""

    SPOT = "spot"


class ArchitectureStyle(StrEnum):
    """Approved deployment architecture for the current phase."""

    MODULAR_MONOLITH = "modular_monolith"


_ALLOWED_MODES: dict[Environment, frozenset[OperatingMode]] = {
    Environment.DEVELOPMENT: frozenset({OperatingMode.RESEARCH}),
    Environment.TEST: frozenset({OperatingMode.RESEARCH}),
    Environment.RESEARCH: frozenset({OperatingMode.RESEARCH, OperatingMode.ADVISORY}),
    Environment.PAPER: frozenset(
        {OperatingMode.RESEARCH, OperatingMode.ADVISORY, OperatingMode.PAPER}
    ),
    Environment.LIVE: frozenset(
        {
            OperatingMode.RESEARCH,
            OperatingMode.ADVISORY,
            OperatingMode.SEMI_AUTOMATIC,
            OperatingMode.AUTOMATIC,
        }
    ),
}

_UNAVAILABLE_CONTROLS = (
    "live_trading_enabled",
    "leverage_enabled",
    "withdrawals_enabled",
    "external_notifications_enabled",
    "autonomous_live_execution_enabled",
)


class RuntimeSettings(BaseSettings):
    """Describe runtime identity and explicit fail-closed capability controls."""

    model_config = SettingsConfigDict(
        env_prefix="WEALTH_",
        env_file=None,
        extra="forbid",
        frozen=True,
        strict=True,
    )

    environment: Environment = Environment.DEVELOPMENT
    operating_mode: OperatingMode = OperatingMode.RESEARCH
    primary_market: PrimaryMarket = PrimaryMarket.CRYPTO
    trading_type: TradingType = TradingType.SPOT
    system_timezone: Literal["UTC"] = "UTC"
    user_timezone: Literal["Asia/Jerusalem"] = "Asia/Jerusalem"
    base_currency: Literal["USD"] = "USD"
    architecture_style: ArchitectureStyle = ArchitectureStyle.MODULAR_MONOLITH
    live_trading_enabled: bool = False
    leverage_enabled: bool = False
    withdrawals_enabled: bool = False
    external_notifications_enabled: bool = False
    autonomous_live_execution_enabled: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @model_validator(mode="after")
    def runtime_is_authorized(self) -> Self:
        """Reject unavailable environments, modes, and capability escalation."""

        if self.environment is Environment.LIVE:
            raise ValueError("live environment is unavailable in the current project phase")

        allowed = _ALLOWED_MODES[self.environment]
        if self.operating_mode not in allowed:
            raise ValueError(
                f"operating mode {self.operating_mode.value!r} is not allowed "
                f"in environment {self.environment.value!r}"
            )

        enabled_controls = [
            control_name for control_name in _UNAVAILABLE_CONTROLS if getattr(self, control_name)
        ]
        if enabled_controls:
            controls = ", ".join(enabled_controls)
            raise ValueError(
                f"unavailable controls must remain disabled in the current project phase: {controls}"
            )
        return self
