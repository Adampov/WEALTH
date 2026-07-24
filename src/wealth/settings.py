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


class RuntimeSettings(BaseSettings):
    """Describe runtime identity without granting financial permissions."""

    model_config = SettingsConfigDict(
        env_prefix="WEALTH_",
        env_file=None,
        extra="forbid",
        frozen=True,
    )

    environment: Environment = Environment.DEVELOPMENT
    operating_mode: OperatingMode = OperatingMode.RESEARCH
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @model_validator(mode="after")
    def environment_supports_mode(self) -> Self:
        """Reject mode escalation in environments that cannot safely support it."""

        allowed = _ALLOWED_MODES[self.environment]
        if self.operating_mode not in allowed:
            raise ValueError(
                f"operating mode {self.operating_mode.value!r} is not allowed "
                f"in environment {self.environment.value!r}"
            )
        return self
