"""Tests for the handwritten TypeScript wire-contract duplicate gate."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest


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


def _run_directory_detector(
    tmp_path: Path,
    files: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    openapi = tmp_path / "openapi.json"
    contracts = tmp_path / "contracts.ts"
    api_clients = tmp_path / "api-clients"
    _write_openapi(openapi, "WireResponse")
    contracts.write_text("", encoding="utf-8")
    for relative_path, source in files.items():
        target = api_clients / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source, encoding="utf-8")
    return subprocess.run(
        [
            "python3",
            str(SCRIPT),
            "--openapi",
            str(openapi),
            "--contracts",
            str(contracts),
            "--api-clients",
            str(api_clients),
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


@pytest.mark.parametrize(
    "declaration",
    [
        "export default interface WireResponse { value: string }",
        "export default type WireResponse = { value: string };",
    ],
)
def test_rejects_default_exported_handwritten_collision(
    tmp_path: Path,
    declaration: str,
) -> None:
    result = _run_detector(tmp_path, contracts=f"\n{declaration}\n")

    assert result.returncode == 1
    assert f"{tmp_path / 'contracts.ts'}:2: WireResponse" in result.stderr


@pytest.mark.parametrize(
    "source",
    [
        """
interface WireResponse { value: string }
export type { WireResponse };
""",
        """
type WireResponse = { value: string };
export { type WireResponse };
""",
    ],
)
def test_rejects_handwritten_declaration_exported_through_list(
    tmp_path: Path,
    source: str,
) -> None:
    result = _run_detector(tmp_path, api_clients=source)

    assert result.returncode == 1
    assert "WireResponse: handwritten" in result.stderr


@pytest.mark.parametrize(
    "body",
    [
        "[GeneratedWireResponse]",
        "Promise<GeneratedWireResponse>",
        "Wrapper<GeneratedWireResponse>",
    ],
)
def test_rejects_generated_contract_laundered_through_wrapper(
    tmp_path: Path,
    body: str,
) -> None:
    result = _run_detector(
        tmp_path,
        contracts=f"""
import type {{ components }} from '../generated/bt-api-types';
type Schemas = components['schemas'];
type GeneratedWireResponse = Schemas['WireResponse'];
export type WireResponse = {body};
""",
    )

    assert result.returncode == 1
    assert "WireResponse: handwritten type collides" in result.stderr


def test_rejects_untrusted_contracts_import_alias_laundering(tmp_path: Path) -> None:
    result = _run_detector(
        tmp_path,
        api_clients="""
import type { UiOnlyModel } from '@trading25/contracts';
export type WireResponse = UiOnlyModel;
""",
    )

    assert result.returncode == 1
    assert "WireResponse: handwritten type collides" in result.stderr


def test_rejects_untrusted_contracts_import_reexported_as_schema_name(
    tmp_path: Path,
) -> None:
    result = _run_detector(
        tmp_path,
        api_clients="""
import type { UiOnlyModel as LocalModel } from '@trading25/contracts';
export type { LocalModel as WireResponse };
""",
    )

    assert result.returncode == 1
    assert f"{tmp_path / 'api-clients.ts'}:3: WireResponse" in result.stderr


def test_allows_endpoint_helper_and_nonnullable_indexed_access(tmp_path: Path) -> None:
    result = _run_detector(
        tmp_path,
        api_clients="""
import type { ApiJsonResponse } from '@trading25/contracts';
import type { components } from '../generated/bt-api-types';
type Schemas = components['schemas'];
type Envelope = Schemas['Envelope'];
export type WireResponse = ApiJsonResponse<'/api/wire', 'get', 200>;
export type DerivedValue = NonNullable<Envelope['payload']>[number];
""",
    )

    assert result.returncode == 0, result.stderr


def test_recursively_scans_nested_ownership_files_with_file_line_diagnostic(
    tmp_path: Path,
) -> None:
    openapi = tmp_path / "openapi.json"
    contracts = tmp_path / "contracts.ts"
    api_clients = tmp_path / "api-clients"
    nested = api_clients / "nested" / "Client.ts"
    ignored_test = api_clients / "nested" / "Client.test.ts"
    ignored_generated = api_clients / "generated" / "wire.ts"
    _write_openapi(openapi, "WireResponse")
    contracts.write_text("", encoding="utf-8")
    nested.parent.mkdir(parents=True)
    nested.write_text(
        "// nested ownership file\n\nexport interface WireResponse {}\n",
        encoding="utf-8",
    )
    ignored_test.write_text("export interface WireResponse {}\n", encoding="utf-8")
    ignored_generated.parent.mkdir(parents=True)
    ignored_generated.write_text("export interface WireResponse {}\n", encoding="utf-8")

    result = subprocess.run(
        [
            "python3",
            str(SCRIPT),
            "--openapi",
            str(openapi),
            "--contracts",
            str(contracts),
            "--api-clients",
            str(api_clients),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert f"{nested}:3: WireResponse" in result.stderr
    assert str(ignored_test) not in result.stderr
    assert str(ignored_generated) not in result.stderr


def test_allows_relative_barrel_reexport_when_target_is_recursively_scanned(
    tmp_path: Path,
) -> None:
    openapi = tmp_path / "openapi.json"
    contracts = tmp_path / "contracts.ts"
    api_clients = tmp_path / "api-clients"
    generated_types = api_clients / "types.ts"
    barrel = api_clients / "index.ts"
    _write_openapi(openapi, "WireResponse")
    contracts.write_text("", encoding="utf-8")
    api_clients.mkdir()
    generated_types.write_text(
        """
import type { components } from '../generated/bt-api-types';
type Schemas = components['schemas'];
export type WireResponse = Schemas['WireResponse'];
""",
        encoding="utf-8",
    )
    barrel.write_text(
        "export type { WireResponse } from './types.js';\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "python3",
            str(SCRIPT),
            "--openapi",
            str(openapi),
            "--contracts",
            str(contracts),
            "--api-clients",
            str(api_clients),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_rejects_renamed_relative_reexport_of_untrusted_symbol(tmp_path: Path) -> None:
    result = _run_directory_detector(
        tmp_path,
        {
            "models.ts": "export interface UiOnlyModel { label: string }\n",
            "index.ts": "export type { UiOnlyModel as WireResponse } from './models.js';\n",
        },
    )

    assert result.returncode == 1
    assert f"{tmp_path / 'api-clients/index.ts'}:1: WireResponse" in result.stderr


def test_allows_renamed_relative_reexport_of_trusted_symbol(tmp_path: Path) -> None:
    result = _run_directory_detector(
        tmp_path,
        {
            "types.ts": """
import type { components } from '../generated/bt-api-types';
type Schemas = components['schemas'];
export type CanonicalWire = Schemas['WireResponse'];
""",
            "index.ts": "export type { CanonicalWire as WireResponse } from './types.js';\n",
        },
    )

    assert result.returncode == 0, result.stderr


def test_allows_generated_namespace_import_indexed_access(tmp_path: Path) -> None:
    result = _run_detector(
        tmp_path,
        contracts="""
import type * as OpenApi from '../generated/bt-api-types';
type Schemas = OpenApi.components['schemas'];
export type WireResponse = Schemas['WireResponse'];
""",
    )

    assert result.returncode == 0, result.stderr


def test_allows_extensionless_relative_reexport_of_trusted_symbol(tmp_path: Path) -> None:
    result = _run_directory_detector(
        tmp_path,
        {
            "types.ts": """
import type { components } from '../generated/bt-api-types';
type Schemas = components['schemas'];
export type WireResponse = Schemas['WireResponse'];
""",
            "index.ts": "export type { WireResponse } from './types';\n",
        },
    )

    assert result.returncode == 0, result.stderr


def test_relative_reexport_cycle_does_not_create_trust_and_is_deterministic(
    tmp_path: Path,
) -> None:
    files = {
        "a.ts": "export type { CycleAlias as WireResponse } from './b';\n",
        "b.ts": "export type { WireResponse as CycleAlias } from './a';\n",
    }

    first = _run_directory_detector(tmp_path, files)
    second = _run_directory_detector(tmp_path, files)

    assert first.returncode == 1
    assert second.returncode == 1
    assert first.stderr == second.stderr
    assert f"{tmp_path / 'api-clients/a.ts'}:1: WireResponse" in first.stderr
