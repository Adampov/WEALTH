"""Validated, fail-closed snapshot of the repository's canonical project state."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

NonEmptyText = Annotated[str, Field(min_length=1)]


class _StrictStateModel(BaseModel):
    """Shared strictness for every nested project-state contract."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ApprovedArchitecture(_StrictStateModel):
    """Architecture that has passed the repository decision process."""

    style: Literal["modular_monolith"]
    decision_record: Literal["ADR-0002"]


class ProjectConstants(_StrictStateModel):
    """Stable project-wide market, time, currency, and architecture choices."""

    primary_market: Literal["crypto"]
    trading_type: Literal["spot"]
    system_timezone: Literal["UTC"]
    user_timezone: Literal["Asia/Jerusalem"]
    base_currency: Literal["USD"]
    architecture_style: Literal["modular_monolith"]


class ControlFlags(_StrictStateModel):
    """Capabilities that remain impossible until a separately approved state version enables them."""

    live_trading_enabled: Literal[False]
    leverage_enabled: Literal[False]
    withdrawals_enabled: Literal[False]
    external_notifications_enabled: Literal[False]
    autonomous_live_execution_enabled: Literal[False]


class ActiveDataSource(_StrictStateModel):
    """An approved public, read-only market-data boundary."""

    source_id: NonEmptyText
    provider: Literal["binance", "coinbase"]
    interface: Literal["public_rest"]
    access: Literal["public_read_only"]
    datasets: tuple[Literal["candles", "aggregate_trades"], ...]


class OpenTask(_StrictStateModel):
    """A bounded task that may be selected as the canonical next action."""

    task_id: NonEmptyText
    action: NonEmptyText
    status: Literal["ready", "in_progress", "blocked"]
    risk_tier: Annotated[int, Field(ge=0, le=4)]
    requires_human_approval: bool


class DecisionReference(_StrictStateModel):
    """Reference to a durable decision artifact, without duplicating its full contents."""

    decision_id: Annotated[str, Field(pattern=r"^ADR-\d{4}$")]
    summary: NonEmptyText
    artifact: NonEmptyText


class NextAction(_StrictStateModel):
    """Exactly one canonical continuation point for the next work cycle."""

    task_id: NonEmptyText
    action: NonEmptyText


class ProjectState(_StrictStateModel):
    """The compact machine-readable source of truth for the current project state."""

    schema_version: Literal["1.0"]
    project_id: Literal["WEALTH"]
    project_goal: NonEmptyText
    current_phase: Literal["PHASE_2_MARKET_DATA"]
    operating_mode: Literal["research"]
    approved_architecture: ApprovedArchitecture
    project_constants: ProjectConstants
    control_flags: ControlFlags
    active_components: tuple[NonEmptyText, ...]
    active_integrations: tuple[NonEmptyText, ...]
    active_data_sources: tuple[ActiveDataSource, ...]
    active_strategies: tuple[NonEmptyText, ...]
    champion_strategy: NonEmptyText | None
    challenger_strategies: tuple[NonEmptyText, ...]
    risk_policy_version: Literal["1.0"]
    current_risk_state: Literal["NO_TRADING_CAPABILITY"]
    open_positions: tuple[NonEmptyText, ...]
    open_orders: tuple[NonEmptyText, ...]
    open_tasks: tuple[OpenTask, ...]
    blockers: tuple[NonEmptyText, ...]
    known_risks: tuple[Annotated[str, Field(pattern=r"^RISK-\d{3}$")], ...]
    pending_approvals: tuple[NonEmptyText, ...]
    recent_decisions: tuple[DecisionReference, ...]
    next_action: NextAction
    last_updated_utc: AwareDatetime

    @field_validator("last_updated_utc")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        """Reject aware timestamps that are not represented in UTC."""

        if value.utcoffset() != timedelta(0):
            raise ValueError("last_updated_utc must use UTC")
        return value

    @model_validator(mode="after")
    def state_is_consistent_with_current_phase(self) -> Self:
        """Prevent the Phase 2 snapshot from implying unapproved trading capability."""

        if self.active_strategies or self.champion_strategy or self.challenger_strategies:
            raise ValueError("Phase 2 must not declare active strategies")
        if self.open_positions or self.open_orders:
            raise ValueError("Phase 2 must not declare positions or orders")

        unique_collections = {
            "active_components": self.active_components,
            "active_integrations": self.active_integrations,
            "active_strategies": self.active_strategies,
            "challenger_strategies": self.challenger_strategies,
            "known_risks": self.known_risks,
        }
        for field_name, values in unique_collections.items():
            if len(values) != len(set(values)):
                raise ValueError(f"{field_name} must contain unique values")

        source_ids = [source.source_id for source in self.active_data_sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("active data-source identifiers must be unique")
        providers = [source.provider for source in self.active_data_sources]
        if len(providers) != len(set(providers)):
            raise ValueError("active data-source providers must be unique")
        if any(
            len(source.datasets) != len(set(source.datasets)) for source in self.active_data_sources
        ):
            raise ValueError("active data-source datasets must be unique")
        datasets_by_provider = {
            source.provider: set(source.datasets) for source in self.active_data_sources
        }
        expected_datasets = {
            "binance": {"candles", "aggregate_trades"},
            "coinbase": {"candles"},
        }
        if datasets_by_provider != expected_datasets:
            raise ValueError("Phase 2 requires the exact approved public provider datasets")

        task_ids = [task.task_id for task in self.open_tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("open task identifiers must be unique")
        task_by_id = {task.task_id: task for task in self.open_tasks}
        selected_task = task_by_id.get(self.next_action.task_id)
        if selected_task is None:
            raise ValueError("next_action must reference an open task")
        if selected_task.status != "ready":
            raise ValueError("next_action must reference a ready task")
        if selected_task.action != self.next_action.action:
            raise ValueError("next_action must match its open-task action")

        decision_ids = [decision.decision_id for decision in self.recent_decisions]
        if len(decision_ids) != len(set(decision_ids)):
            raise ValueError("recent decision identifiers must be unique")
        return self


def load_project_state(path: Path) -> ProjectState:
    """Load and validate a UTF-8 project-state snapshot from disk."""

    return ProjectState.model_validate_json(path.read_text(encoding="utf-8"))
