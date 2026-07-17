"""Tests for the handwritten TypeScript wire-contract duplicate gate."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[5]
SCRIPT = REPO_ROOT / "scripts" / "check-ts-wire-contracts.py"


def _write_openapi(path: Path, *schema_names: str) -> None:
    path.write_text(
        json.dumps(
            {
                "openapi": "3.1.0",
                "info": {"title": "fixture", "version": "1"},
                "paths": {},
                "components": {
                    "schemas": {name: {"type": "object"} for name in schema_names}
                },
            }
        ),
        encoding="utf-8",
    )


def _run_detector(
    tmp_path: Path,
    *,
    contracts: str = "",
    api_clients: str = "",
) -> subprocess.CompletedProcess[str]:
    openapi = tmp_path / "openapi.json"
    contracts_file = tmp_path / "contracts.ts"
    api_clients_file = tmp_path / "api-clients.ts"
    _write_openapi(openapi, "WireResponse")
    contracts_file.write_text(contracts, encoding="utf-8")
    api_clients_file.write_text(api_clients, encoding="utf-8")

    return subprocess.run(
        [
            "python3",
            str(SCRIPT),
            "--openapi",
            str(openapi),
            "--contracts",
            str(contracts_file),
            "--api-clients",
            str(api_clients_file),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_rejects_handwritten_contract_interface_with_openapi_schema_name(
    tmp_path: Path,
) -> None:
    result = _run_detector(
        tmp_path,
        contracts="""
export interface WireResponse {
  value: string;
}
""",
    )

    assert result.returncode == 1
    assert f"{tmp_path / 'contracts.ts'}:2: WireResponse" in result.stderr
    assert "handwritten interface collides with OpenAPI component schema" in result.stderr


def test_allows_contract_alias_to_generated_schema(tmp_path: Path) -> None:
    result = _run_detector(
        tmp_path,
        contracts="""
import type { components as BtApiComponents } from '../generated/bt-api-types';
type BtApiSchemas = BtApiComponents['schemas'];
export type WireResponse = BtApiSchemas['WireResponse'];
""",
    )

    assert result.returncode == 0, result.stderr


def test_allows_derived_indexed_access_type_rooted_in_generated_contract(
    tmp_path: Path,
) -> None:
    result = _run_detector(
        tmp_path,
        contracts="""
import type { components } from '../generated/bt-api-types';
type Schemas = components['schemas'];
type GeneratedEnvelope = Schemas['Envelope'];
export type WireResponse = GeneratedEnvelope['payload'][number];
""",
    )

    assert result.returncode == 0, result.stderr


def test_rejects_generated_alias_extended_with_handwritten_fields(tmp_path: Path) -> None:
    result = _run_detector(
        tmp_path,
        api_clients="""
import type { components } from '../generated/bt-api-types';
type Schemas = components['schemas'];
type GeneratedWireResponse = Schemas['WireResponse'];
export type WireResponse = GeneratedWireResponse & {
  localOnly: string;
};
""",
    )

    assert result.returncode == 1
    assert "WireResponse: handwritten type collides" in result.stderr


def test_allows_distinct_handwritten_ui_model(tmp_path: Path) -> None:
    result = _run_detector(
        tmp_path,
        contracts="""
export interface WireResponseViewModel {
  label: string;
}
""",
    )

    assert result.returncode == 0, result.stderr


def test_rejects_handwritten_api_client_duplicate(tmp_path: Path) -> None:
    result = _run_detector(
        tmp_path,
        api_clients="""
export type WireResponse = {
  value: string;
};
""",
    )

    assert result.returncode == 1
    assert f"{tmp_path / 'api-clients.ts'}:2: WireResponse" in result.stderr
    assert "handwritten type collides with OpenAPI component schema" in result.stderr
