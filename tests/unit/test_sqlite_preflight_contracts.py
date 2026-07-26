"""Contract and normalization tests for the unused SQLite preflight foundation."""

import ast
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.adapters.sqlite_preflight import (
    SQLITE_EXPECTED_STORE_IDENTITIES,
    SQLitePreflightError,
    SQLitePreflightErrorCode,
    _normalize_ddl,
)
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_MARKER_COLUMNS,
    SQLiteMarkerRowFingerprint,
    SQLiteMarkerValueFingerprint,
    SQLitePreflightRequest,
    SQLiteStorageClass,
    SQLiteStoreFamily,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PREFLIGHT_ADAPTER_PATH = REPOSITORY_ROOT / "src" / "wealth" / "adapters" / "sqlite_preflight.py"
TIMESTAMP_PARSE_PATH = REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_parse.py"
TIMESTAMP_CANDIDATE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate.py"
)
TIMESTAMP_CANDIDATE_CENSUS_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate_census.py"
)
TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate_census_bundle.py"
)


def valid_request(tmp_path: Path) -> SQLitePreflightRequest:
    """Build one strict request without creating or inspecting its fixture."""

    return SQLitePreflightRequest(
        source_kind="generated_synthetic_fixture",
        fixture_id=UUID("00000000-0000-0000-0000-000000000030"),
        fixture_path=tmp_path / "fixture.sqlite3",
        expected_family=SQLiteStoreFamily.MARKET,
        expected_layout_version=1,
    )


def test_request_contract_is_versioned_strict_frozen_and_synthetic_only(tmp_path: Path) -> None:
    request = valid_request(tmp_path)

    assert request.schema_version == "1.0"
    assert request.source_kind == "generated_synthetic_fixture"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SQLitePreflightRequest.model_validate(
            {
                **request.model_dump(),
                "trusted_digest": "0" * 64,
            }
        )
    with pytest.raises(ValidationError):
        SQLitePreflightRequest.model_validate(
            {
                **request.model_dump(),
                "source_kind": "operator_database",
            }
        )
    with pytest.raises(ValidationError):
        SQLitePreflightRequest.model_validate(
            {
                **request.model_dump(),
                "schema_version": "2.0",
            }
        )
    with pytest.raises(ValidationError):
        SQLitePreflightRequest.model_validate(
            {
                **request.model_dump(),
                "expected_layout_version": 2,
            }
        )
    with pytest.raises(ValidationError):
        request.expected_family = SQLiteStoreFamily.ORDER_FLOW


def test_request_python_boundary_rejects_coercion(tmp_path: Path) -> None:
    request = valid_request(tmp_path)
    payload = request.model_dump()

    with pytest.raises(ValidationError):
        SQLitePreflightRequest.model_validate({**payload, "fixture_path": str(tmp_path)})
    with pytest.raises(ValidationError):
        SQLitePreflightRequest.model_validate({**payload, "fixture_id": str(request.fixture_id)})
    with pytest.raises(ValidationError):
        SQLitePreflightRequest.model_validate(
            {**payload, "expected_family": SQLiteStoreFamily.MARKET.value}
        )


def test_expected_registry_is_complete_unique_and_independent_of_requests() -> None:
    assert tuple(identity.family for identity in SQLITE_EXPECTED_STORE_IDENTITIES) == tuple(
        SQLiteStoreFamily
    )
    assert all(identity.layout_version == 1 for identity in SQLITE_EXPECTED_STORE_IDENTITIES)
    assert all(identity.encoding == "UTF-8" for identity in SQLITE_EXPECTED_STORE_IDENTITIES)
    assert all(identity.application_id == 0 for identity in SQLITE_EXPECTED_STORE_IDENTITIES)
    assert all(identity.user_version == 1 for identity in SQLITE_EXPECTED_STORE_IDENTITIES)
    assert len({identity.store_sha256 for identity in SQLITE_EXPECTED_STORE_IDENTITIES}) == 8
    assert all(identity.store_sha256 != "0" * 64 for identity in SQLITE_EXPECTED_STORE_IDENTITIES)

    marker_families = {
        identity.family: identity.markers
        for identity in SQLITE_EXPECTED_STORE_IDENTITIES
        if identity.markers
    }
    assert set(marker_families) == {
        SQLiteStoreFamily.ORDER_FLOW,
        SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION,
    }
    assert marker_families[SQLiteStoreFamily.ORDER_FLOW][0].rows[0].values[0].blob_hex == (
        b"wealth.order_flow".hex().upper()
    )
    assert (
        marker_families[SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION][0].rows[0].values[0].blob_hex
        == b"wealth.public_trade_collection".hex().upper()
    )


def test_marker_value_contract_rejects_inconsistent_or_fabricated_null_bytes() -> None:
    with pytest.raises(ValidationError, match="byte_length"):
        SQLiteMarkerValueFingerprint(
            storage_class=SQLiteStorageClass.TEXT,
            blob_hex="41",
            byte_length=2,
        )
    with pytest.raises(ValidationError, match="SQLite NULL"):
        SQLiteMarkerValueFingerprint(
            storage_class=SQLiteStorageClass.NULL,
            blob_hex="41",
            byte_length=1,
        )


def test_marker_row_contract_is_bounded() -> None:
    value = SQLiteMarkerValueFingerprint(
        storage_class=SQLiteStorageClass.TEXT,
        blob_hex="41",
        byte_length=1,
    )

    with pytest.raises(ValidationError, match="bounded column count"):
        SQLiteMarkerRowFingerprint(values=(value,) * (MAX_SQLITE_MARKER_COLUMNS + 1))


def test_ddl_normalization_preserves_quoted_bytes_and_avoids_literal_collisions() -> None:
    first = _normalize_ddl(
        """
        CREATE TABLE sample (
            value TEXT CHECK (value = 'A  B'),
            "Quoted  Name" TEXT
        );
        """
    )
    second = _normalize_ddl(
        """
        CREATE  TABLE sample(
            value TEXT CHECK(value='a b'),
            "Quoted Name" TEXT
        )
        """
    )

    assert first != second
    assert "'A  B'" in first
    assert '"Quoted  Name"' in first


def test_ddl_normalization_ignores_only_unquoted_spacing_comments_and_final_semicolon() -> None:
    compact = _normalize_ddl("CREATE TABLE sample(value TEXT)")
    spaced = _normalize_ddl(
        """
        CREATE   TABLE sample /* layout note */
        (
          value TEXT -- column note
        );
        """
    )

    assert spaced == "CREATE TABLE sample ( value TEXT )"
    assert compact == "CREATE TABLE sample(value TEXT)"
    assert not spaced.endswith(";")


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE sample(value TEXT CHECK(value='unterminated))",
        'CREATE TABLE "unterminated(value TEXT)',
        "CREATE TABLE sample(value TEXT /* unterminated)",
        "CREATE TABLE sample(\x00value TEXT)",
    ],
)
def test_ddl_normalization_rejects_malformed_or_nul_input(ddl: str) -> None:
    with pytest.raises(SQLitePreflightError) as captured:
        _normalize_ddl(ddl)

    assert captured.value.code is SQLitePreflightErrorCode.INVALID_SCHEMA


def test_preflight_adapter_never_imports_schema_installing_sqlite_adapters() -> None:
    tree = ast.parse(PREFLIGHT_ADAPTER_PATH.read_text(encoding="utf-8"))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )

    assert "wealth.adapters.sqlite_market" not in imports
    assert "wealth.adapters.sqlite_order_flow" not in imports
    assert all(
        not imported.startswith("wealth.adapters.sqlite_")
        for imported in imports
        if imported != "wealth.adapters.sqlite_preflight"
    )


def test_no_existing_runtime_module_imports_the_preflight_foundation() -> None:
    consumers: list[Path] = []
    for path in (REPOSITORY_ROOT / "src" / "wealth").rglob("*.py"):
        if path in {
            PREFLIGHT_ADAPTER_PATH,
            REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_preflight.py",
            TIMESTAMP_CANDIDATE_PATH,
            TIMESTAMP_CANDIDATE_CENSUS_PATH,
            TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PATH,
            TIMESTAMP_PARSE_PATH,
        }:
            continue
        source = path.read_text(encoding="utf-8")
        if "sqlite_preflight" in source:
            consumers.append(path)

    assert consumers == []
