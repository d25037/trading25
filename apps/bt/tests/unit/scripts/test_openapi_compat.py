"""Tests for the standard-library OpenAPI compatibility gate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest


REPO_ROOT = Path(__file__).resolve().parents[5]
SCRIPT = REPO_ROOT / "scripts" / "openapi_compat.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("openapi_compat", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load openapi_compat module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["openapi_compat"] = module
    spec.loader.exec_module(module)
    return module


def _document(
    *,
    paths: dict[str, Any] | None = None,
    schemas: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "openapi": "3.1.0",
        "info": {"title": "fixture", "version": "1"},
        "paths": paths or {},
        "components": {"schemas": schemas or {}},
    }


def _response(schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "description": "ok",
        "content": {"application/json": {"schema": schema}},
    }


def _operation(
    schema: dict[str, Any], *, parameters: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    return {
        "parameters": parameters or [],
        "responses": {"200": _response(schema)},
    }


def _categories(base: dict[str, Any], candidate: dict[str, Any]) -> set[str]:
    return {
        finding.category
        for finding in _load_module().compare_openapi(base, candidate)
    }


def test_compatible_additions_have_no_findings() -> None:
    base = _document(
        paths={"/items": {"get": _operation({"type": "string"})}},
        schemas={"Item": {"type": "object", "properties": {"id": {"type": "string"}}}},
    )
    candidate = _document(
        paths={
            "/items": {
                "get": _operation({"type": "string"}),
                "post": _operation({"type": "string"}),
            },
            "/health": {"get": _operation({"type": "string"})},
        },
        schemas={
            "Item": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "label": {"type": "string"},
                },
            },
            "Extra": {"type": "string"},
        },
    )

    assert _load_module().compare_openapi(base, candidate) == []


@pytest.mark.parametrize(
    ("base", "candidate", "category"),
    [
        (
            _document(paths={"/items": {"get": _operation({"type": "string"})}}),
            _document(),
            "path_removed",
        ),
        (
            _document(paths={"/items": {"get": _operation({"type": "string"})}}),
            _document(paths={"/items": {}}),
            "operation_removed",
        ),
        (
            _document(paths={"/items": {"get": _operation({"type": "string"})}}),
            _document(
                paths={
                    "/items": {
                        "get": {"responses": {"404": _response({"type": "string"})}}
                    }
                }
            ),
            "success_response_removed",
        ),
        (
            _document(schemas={"Item": {"type": "string"}}),
            _document(),
            "schema_removed",
        ),
        (
            _document(
                schemas={
                    "Item": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    }
                }
            ),
            _document(schemas={"Item": {"type": "object", "properties": {}}}),
            "property_removed",
        ),
        (
            _document(
                schemas={
                    "Item": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    }
                }
            ),
            _document(
                schemas={
                    "Item": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                        "required": ["id"],
                    }
                }
            ),
            "required_field_added",
        ),
    ],
)
def test_structural_removals_are_breaking(
    base: dict[str, Any], candidate: dict[str, Any], category: str
) -> None:
    assert category in _categories(base, candidate)


@pytest.mark.parametrize(
    ("before", "after", "category"),
    [
        ({"type": "string"}, {"type": "integer"}, "type_changed"),
        (
            {"type": "string", "format": "date"},
            {"type": "string", "format": "date-time"},
            "format_changed",
        ),
        (
            {"type": ["string", "null"]},
            {"type": "string"},
            "nullability_changed",
        ),
        (
            {"anyOf": [{"type": "string"}, {"type": "null"}]},
            {"type": "string"},
            "nullability_changed",
        ),
        (
            {"$ref": "#/components/schemas/A"},
            {"$ref": "#/components/schemas/B"},
            "reference_changed",
        ),
        (
            {"type": "array", "items": {"type": "string"}},
            {"type": "array", "items": {"type": "integer"}},
            "type_changed",
        ),
        (
            {"type": "string", "enum": ["a", "b"]},
            {"type": "string", "enum": ["a"]},
            "enum_narrowed",
        ),
        (
            {"type": "string"},
            {"type": "string", "enum": ["a", "b"]},
            "enum_narrowed",
        ),
        (
            {"anyOf": [{"type": "string"}, {"type": "null"}]},
            {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            "type_changed",
        ),
    ],
)
def test_schema_constraint_changes_are_breaking(
    before: dict[str, Any], after: dict[str, Any], category: str
) -> None:
    schemas = {
        "A": {"type": "string"},
        "B": {"type": "integer"},
        "Value": before,
    }
    candidate_schemas = schemas | {"Value": after}

    assert category in _categories(
        _document(schemas=schemas), _document(schemas=candidate_schemas)
    )


def test_local_refs_are_resolved_before_comparing_property_constraints() -> None:
    base = _document(
        schemas={
            "Item": {"$ref": "#/components/schemas/ItemBody"},
            "ItemBody": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
            },
        }
    )
    candidate = _document(
        schemas={
            "Item": {"$ref": "#/components/schemas/ItemBody"},
            "ItemBody": {"type": "object", "properties": {}},
        }
    )

    findings = _load_module().compare_openapi(base, candidate)

    assert any(
        finding.category == "property_removed"
        and finding.pointer.endswith("/components/schemas/Item/properties/id")
        for finding in findings
    )


def test_recursive_local_refs_terminate_without_findings() -> None:
    document = _document(
        schemas={
            "Node": {
                "type": "object",
                "properties": {"next": {"$ref": "#/components/schemas/Node"}},
            }
        }
    )

    assert _load_module().compare_openapi(document, document) == []


def test_nullable_anyof_recursively_detects_non_null_reference_change() -> None:
    base = _document(
        schemas={
            "A": {"type": "string"},
            "B": {"type": "string"},
            "Value": {
                "anyOf": [
                    {"$ref": "#/components/schemas/A"},
                    {"type": "null"},
                ]
            },
        }
    )
    candidate = _document(
        schemas={
            "A": {"type": "string"},
            "B": {"type": "string"},
            "Value": {
                "anyOf": [
                    {"type": "null"},
                    {"$ref": "#/components/schemas/B"},
                ]
            },
        }
    )

    findings = _load_module().compare_openapi(base, candidate)

    assert any(
        finding.category == "reference_changed"
        and finding.pointer == "#/components/schemas/Value/anyOf/0/$ref"
        for finding in findings
    )


def test_nullable_oneof_recursively_detects_non_null_format_change() -> None:
    base = _document(
        schemas={
            "Value": {
                "oneOf": [
                    {"type": "string", "format": "date"},
                    {"type": "null"},
                ]
            }
        }
    )
    candidate = _document(
        schemas={
            "Value": {
                "oneOf": [
                    {"type": "null"},
                    {"type": "string", "format": "date-time"},
                ]
            }
        }
    )

    findings = _load_module().compare_openapi(base, candidate)

    assert any(
        finding.category == "format_changed"
        and finding.pointer == "#/components/schemas/Value/oneOf/0/format"
        for finding in findings
    )


def test_nullable_union_type_change_keeps_single_existing_type_finding() -> None:
    base = _document(
        schemas={
            "Value": {
                "anyOf": [{"type": "string"}, {"type": "null"}]
            }
        }
    )
    candidate = _document(
        schemas={
            "Value": {
                "anyOf": [{"type": "integer"}, {"type": "null"}]
            }
        }
    )

    findings = _load_module().compare_openapi(base, candidate)
    type_findings = [item for item in findings if item.category == "type_changed"]

    assert [item.pointer for item in type_findings] == [
        "#/components/schemas/Value/type"
    ]


def _parameter(*, required: bool = False, schema: dict[str, Any] | None = None):
    return {
        "name": "limit",
        "in": "query",
        "required": required,
        "schema": schema or {"type": "integer"},
    }


@pytest.mark.parametrize(
    ("before", "after", "category"),
    [
        ([_parameter()], [], "parameter_removed"),
        ([_parameter()], [_parameter(required=True)], "parameter_required_promoted"),
        (
            [_parameter()],
            [_parameter(schema={"type": "string"})],
            "parameter_changed",
        ),
    ],
)
def test_parameter_breaks_are_detected(
    before: list[dict[str, Any]], after: list[dict[str, Any]], category: str
) -> None:
    base = _document(
        paths={"/items": {"get": _operation({"type": "string"}, parameters=before)}}
    )
    candidate = _document(
        paths={"/items": {"get": _operation({"type": "string"}, parameters=after)}}
    )

    assert category in _categories(base, candidate)


@pytest.mark.parametrize(
    ("before_overrides", "after_overrides"),
    [
        ({"style": "form"}, {"style": "spaceDelimited"}),
        ({"explode": False}, {"explode": True}),
        ({"allowEmptyValue": False}, {"allowEmptyValue": True}),
        (
            {"schema": {"type": "integer"}},
            {
                "schema": None,
                "content": {
                    "application/json": {"schema": {"type": "integer"}}
                },
            },
        ),
    ],
)
def test_parameter_wire_format_changes_are_breaking(
    before_overrides: dict[str, Any], after_overrides: dict[str, Any]
) -> None:
    before = _parameter() | before_overrides
    after = _parameter() | after_overrides
    if after.get("schema") is None:
        after.pop("schema", None)
    base = _document(
        paths={"/items": {"get": _operation({"type": "string"}, parameters=[before])}}
    )
    candidate = _document(
        paths={"/items": {"get": _operation({"type": "string"}, parameters=[after])}}
    )

    assert "parameter_changed" in _categories(base, candidate)


def test_fingerprints_are_stable_for_normalized_finding_payloads() -> None:
    module = _load_module()
    first = module.Finding(
        category="type_changed",
        pointer="#/components/schemas/Value",
        before={"type": "string", "enum": ["b", "a"]},
        after={"enum": ["a"], "type": "string"},
    )
    reordered = module.Finding(
        after={"type": "string", "enum": ["a"]},
        before={"enum": ["b", "a"], "type": "string"},
        pointer="#/components/schemas/Value",
        category="type_changed",
    )

    assert first.fingerprint == reordered.fingerprint
    assert first.fingerprint.startswith("sha256:")
    assert len(first.fingerprint) == len("sha256:") + 64


def test_fingerprints_normalize_enum_order_but_preserve_other_array_order() -> None:
    module = _load_module()
    enum_ab = module.Finding(
        "enum_narrowed", "#/enum", ["a", "b"], ["a"]
    )
    enum_ba = module.Finding(
        "enum_narrowed", "#/enum", ["b", "a"], ["a"]
    )
    ordered_ab = module.Finding(
        "schema_changed", "#/examples", {"examples": ["a", "b"]}, None
    )
    ordered_ba = module.Finding(
        "schema_changed", "#/examples", {"examples": ["b", "a"]}, None
    )

    assert enum_ab.fingerprint == enum_ba.fingerprint
    assert ordered_ab.fingerprint != ordered_ba.fingerprint


def _run_cli(
    tmp_path: Path,
    base: dict[str, Any],
    candidate: dict[str, Any],
    approvals: Any,
) -> subprocess.CompletedProcess[str]:
    files = {
        "base.json": base,
        "candidate.json": candidate,
        "approvals.json": approvals,
    }
    for name, payload in files.items():
        (tmp_path / name).write_text(json.dumps(payload), encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--base",
            str(tmp_path / "base.json"),
            "--candidate",
            str(tmp_path / "candidate.json"),
            "--approvals",
            str(tmp_path / "approvals.json"),
            "--today",
            "2026-07-17",
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_exact_unexpired_approval_allows_a_breaking_finding(tmp_path: Path) -> None:
    module = _load_module()
    base = _document(schemas={"Value": {"type": "string"}})
    candidate = _document(schemas={"Value": {"type": "integer"}})
    finding = module.compare_openapi(base, candidate)[0]
    approvals = {
        "version": 1,
        "approvals": [
            {
                "fingerprint": finding.fingerprint,
                "reason": "intentional v2 migration",
                "expiresOn": "2026-07-18",
            }
        ],
    }

    result = _run_cli(tmp_path, base, candidate, approvals)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "approved" in result.stdout.lower()


@pytest.mark.parametrize(
    ("approvals", "message"),
    [
        ({"version": 2, "approvals": []}, "version"),
        ({"version": True, "approvals": []}, "version"),
        (
            {
                "version": 1,
                "approvals": [
                    {
                        "fingerprint": "sha256:" + "a" * 64,
                        "reason": "first",
                        "expiresOn": "2026-07-18",
                    },
                    {
                        "fingerprint": "sha256:" + "a" * 64,
                        "reason": "second",
                        "expiresOn": "2026-07-18",
                    },
                ],
            },
            "duplicate",
        ),
        (
            {
                "version": 1,
                "approvals": [
                    {
                        "fingerprint": "sha256:" + "a" * 64,
                        "reason": "expired",
                        "expiresOn": "2026-07-16",
                    }
                ],
            },
            "expired",
        ),
        (
            {
                "version": 1,
                "approvals": [
                    {
                        "fingerprint": "sha256:" + "a" * 64,
                        "reason": "not emitted",
                        "expiresOn": "2026-07-18",
                    }
                ],
            },
            "unused",
        ),
    ],
)
def test_invalid_or_unused_approvals_fail_closed(
    tmp_path: Path, approvals: Any, message: str
) -> None:
    result = _run_cli(tmp_path, _document(), _document(), approvals)

    assert result.returncode == 1
    assert message in (result.stdout + result.stderr).lower()


@pytest.mark.parametrize("expires_on", ["20260718", "2026-W29-6"])
def test_approval_expiry_requires_exact_calendar_date_format(
    tmp_path: Path, expires_on: str
) -> None:
    approvals = {
        "version": 1,
        "approvals": [
            {
                "fingerprint": "sha256:" + "a" * 64,
                "reason": "invalid date spelling",
                "expiresOn": expires_on,
            }
        ],
    }

    result = _run_cli(tmp_path, _document(), _document(), approvals)

    assert result.returncode == 1
    assert "malformed approval expiry" in (result.stdout + result.stderr).lower()


def test_unapproved_findings_fail_and_are_sorted_deterministically(tmp_path: Path) -> None:
    base = _document(
        schemas={"Zed": {"type": "string"}, "Alpha": {"type": "string"}}
    )
    candidate = _document(
        schemas={"Zed": {"type": "integer"}, "Alpha": {"type": "integer"}}
    )

    result = _run_cli(
        tmp_path, base, candidate, {"version": 1, "approvals": []}
    )

    assert result.returncode == 1
    assert result.stdout.index("Alpha") < result.stdout.index("Zed")
    assert "sha256:" in result.stdout
