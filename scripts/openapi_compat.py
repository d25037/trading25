#!/usr/bin/env python3
"""Fail closed when an OpenAPI candidate breaks its committed base contract."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any


HTTP_METHODS = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)
FINGERPRINT_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
MISSING = {"__openapi_compat_missing__": True}


def _normalize(value: Any, *, parent_key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize(value[key], parent_key=key) for key in sorted(value)
        }
    if isinstance(value, list):
        normalized = [_normalize(item) for item in value]
        if parent_key == "enum":
            return sorted(
                normalized,
                key=lambda item: json.dumps(
                    item,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            )
        return normalized
    return value


@dataclass(frozen=True)
class Finding:
    category: str
    pointer: str
    before: Any
    after: Any

    @property
    def fingerprint(self) -> str:
        value_key = "enum" if self.category == "enum_narrowed" else None
        payload = {
            "category": self.category,
            "pointer": self.pointer,
            "before": _normalize(self.before, parent_key=value_key),
            "after": _normalize(self.after, parent_key=value_key),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "pointer": self.pointer,
            "before": self.before,
            "after": self.after,
            "fingerprint": self.fingerprint,
        }


def _pointer_token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _resolve_pointer(document: dict[str, Any], reference: str) -> Any:
    if not reference.startswith("#/"):
        return None
    current: Any = document
    for raw_token in reference[2:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or token not in current:
            return None
        current = current[token]
    return current


def _resolved_schema(schema: Any, document: dict[str, Any]) -> Any:
    if not isinstance(schema, dict) or "$ref" not in schema:
        return schema
    target = _resolve_pointer(document, schema["$ref"])
    if not isinstance(target, dict):
        return schema
    siblings = {key: value for key, value in schema.items() if key != "$ref"}
    return target if not siblings else target | siblings


def _nullable(schema: dict[str, Any]) -> bool:
    schema_type = schema.get("type")
    nullable_union = any(
        isinstance(branch, dict) and branch.get("type") == "null"
        for keyword in ("anyOf", "oneOf")
        for branch in (
            schema.get(keyword) if isinstance(schema.get(keyword), list) else []
        )
    )
    return schema.get("nullable") is True or nullable_union or (
        isinstance(schema_type, list) and "null" in schema_type
    )


def _type_without_null(schema: dict[str, Any]) -> Any:
    schema_type = schema.get("type", MISSING)
    if isinstance(schema_type, list):
        non_null = [item for item in schema_type if item != "null"]
        if len(non_null) == 1:
            return non_null[0]
        return non_null
    return schema_type


def _effective_type(schema: dict[str, Any]) -> Any:
    direct_type = _type_without_null(schema)
    if direct_type != MISSING:
        return direct_type
    for keyword in ("anyOf", "oneOf"):
        branches = schema.get(keyword)
        if not isinstance(branches, list):
            continue
        branch_types = [
            _type_without_null(branch)
            for branch in branches
            if isinstance(branch, dict) and branch.get("type") != "null"
        ]
        concrete_types = [item for item in branch_types if item != MISSING]
        if len(concrete_types) == 1:
            return concrete_types[0]
        if concrete_types:
            return sorted(
                concrete_types,
                key=lambda item: json.dumps(item, sort_keys=True),
            )
    return MISSING


def _non_null_branches(
    schema: dict[str, Any], keyword: str, document: dict[str, Any]
) -> list[tuple[int, dict[str, Any]]]:
    branches = schema.get(keyword)
    if not isinstance(branches, list):
        return []
    non_null: list[tuple[int, dict[str, Any]]] = []
    for index, branch in enumerate(branches):
        if not isinstance(branch, dict):
            continue
        resolved = _resolved_schema(branch, document)
        if isinstance(resolved, dict) and resolved.get("type") == "null":
            continue
        non_null.append((index, branch))
    return non_null


def _add(
    findings: list[Finding],
    category: str,
    pointer: str,
    before: Any,
    after: Any,
) -> None:
    findings.append(Finding(category, pointer, before, after))


def _compare_schema(
    base_schema: Any,
    candidate_schema: Any,
    *,
    base_document: dict[str, Any],
    candidate_document: dict[str, Any],
    pointer: str,
    findings: list[Finding],
    visited: set[tuple[int, int]],
    compare_type: bool = True,
) -> None:
    if not isinstance(base_schema, dict) or not isinstance(candidate_schema, dict):
        if base_schema != candidate_schema:
            _add(findings, "schema_changed", pointer, base_schema, candidate_schema)
        return

    base_ref = base_schema.get("$ref")
    candidate_ref = candidate_schema.get("$ref")
    if base_ref != candidate_ref and (base_ref is not None or candidate_ref is not None):
        _add(
            findings,
            "reference_changed",
            f"{pointer}/$ref",
            base_ref if base_ref is not None else MISSING,
            candidate_ref if candidate_ref is not None else MISSING,
        )

    resolved_base = _resolved_schema(base_schema, base_document)
    resolved_candidate = _resolved_schema(candidate_schema, candidate_document)
    if not isinstance(resolved_base, dict) or not isinstance(resolved_candidate, dict):
        return
    visit_key = (id(resolved_base), id(resolved_candidate))
    if visit_key in visited:
        return
    visited.add(visit_key)

    base_type = _effective_type(resolved_base)
    candidate_type = _effective_type(resolved_candidate)
    if compare_type and base_type != candidate_type:
        _add(findings, "type_changed", f"{pointer}/type", base_type, candidate_type)

    if _nullable(resolved_base) != _nullable(resolved_candidate):
        _add(
            findings,
            "nullability_changed",
            f"{pointer}/nullable",
            _nullable(resolved_base),
            _nullable(resolved_candidate),
        )

    for keyword, category in (("format", "format_changed"),):
        before = resolved_base.get(keyword, MISSING)
        after = resolved_candidate.get(keyword, MISSING)
        if before != after:
            _add(findings, category, f"{pointer}/{keyword}", before, after)

    base_enum = resolved_base.get("enum")
    candidate_enum = resolved_candidate.get("enum")
    if isinstance(candidate_enum, list):
        removed = (
            [item for item in base_enum if item not in candidate_enum]
            if isinstance(base_enum, list)
            else [MISSING]
        )
        if removed:
            _add(
                findings,
                "enum_narrowed",
                f"{pointer}/enum",
                base_enum if isinstance(base_enum, list) else MISSING,
                candidate_enum,
            )

    base_required = resolved_base.get("required", [])
    candidate_required = resolved_candidate.get("required", [])
    if isinstance(base_required, list) and isinstance(candidate_required, list):
        for field in sorted(set(candidate_required) - set(base_required)):
            _add(
                findings,
                "required_field_added",
                f"{pointer}/required/{_pointer_token(str(field))}",
                False,
                True,
            )

    base_properties = resolved_base.get("properties", {})
    candidate_properties = resolved_candidate.get("properties", {})
    if isinstance(base_properties, dict) and isinstance(candidate_properties, dict):
        for name in sorted(base_properties):
            property_pointer = f"{pointer}/properties/{_pointer_token(name)}"
            if name not in candidate_properties:
                _add(
                    findings,
                    "property_removed",
                    property_pointer,
                    base_properties[name],
                    MISSING,
                )
                continue
            _compare_schema(
                base_properties[name],
                candidate_properties[name],
                base_document=base_document,
                candidate_document=candidate_document,
                pointer=property_pointer,
                findings=findings,
                visited=visited,
            )

    if "items" in resolved_base:
        items_pointer = f"{pointer}/items"
        if "items" not in resolved_candidate:
            _add(
                findings,
                "array_items_removed",
                items_pointer,
                resolved_base["items"],
                MISSING,
            )
        else:
            _compare_schema(
                resolved_base["items"],
                resolved_candidate["items"],
                base_document=base_document,
                candidate_document=candidate_document,
                pointer=items_pointer,
                findings=findings,
                visited=visited,
            )

    for keyword in ("anyOf", "oneOf"):
        base_branches = _non_null_branches(resolved_base, keyword, base_document)
        candidate_branches = _non_null_branches(
            resolved_candidate, keyword, candidate_document
        )
        if not base_branches or len(base_branches) != len(candidate_branches):
            continue
        for (base_index, base_branch), (_, candidate_branch) in zip(
            base_branches, candidate_branches
        ):
            _compare_schema(
                base_branch,
                candidate_branch,
                base_document=base_document,
                candidate_document=candidate_document,
                pointer=f"{pointer}/{keyword}/{base_index}",
                findings=findings,
                visited=visited,
                compare_type=base_type == candidate_type,
            )


def _parameter_map(parameters: Any) -> dict[tuple[str, str], dict[str, Any]]:
    if not isinstance(parameters, list):
        return {}
    return {
        (parameter["name"], parameter["in"]): parameter
        for parameter in parameters
        if isinstance(parameter, dict)
        and isinstance(parameter.get("name"), str)
        and isinstance(parameter.get("in"), str)
    }


def _compare_parameters(
    base_parameters: Any,
    candidate_parameters: Any,
    *,
    base_document: dict[str, Any],
    candidate_document: dict[str, Any],
    pointer: str,
    findings: list[Finding],
) -> None:
    base_map = _parameter_map(base_parameters)
    candidate_map = _parameter_map(candidate_parameters)
    for key in sorted(base_map):
        name, location = key
        parameter_pointer = (
            f"{pointer}/parameters/{_pointer_token(location)}:{_pointer_token(name)}"
        )
        before = base_map[key]
        after = candidate_map.get(key)
        if after is None:
            _add(findings, "parameter_removed", parameter_pointer, before, MISSING)
            continue
        if before.get("required") is not True and after.get("required") is True:
            _add(
                findings,
                "parameter_required_promoted",
                f"{parameter_pointer}/required",
                before.get("required", False),
                True,
            )
        for keyword in ("style", "explode", "allowEmptyValue"):
            before_value = before.get(keyword, MISSING)
            after_value = after.get(keyword, MISSING)
            if before_value != after_value:
                _add(
                    findings,
                    "parameter_changed",
                    f"{parameter_pointer}/{keyword}",
                    before_value,
                    after_value,
                )

        before_mode = "content" if "content" in before else "schema"
        after_mode = "content" if "content" in after else "schema"
        if before_mode != after_mode:
            _add(
                findings,
                "parameter_changed",
                f"{parameter_pointer}/serialization",
                before_mode,
                after_mode,
            )
            continue

        if before_mode == "content":
            before_content = before.get("content", MISSING)
            after_content = after.get("content", MISSING)
            if before_content != after_content:
                _add(
                    findings,
                    "parameter_changed",
                    f"{parameter_pointer}/content",
                    before_content,
                    after_content,
                )
            continue

        before_schema = before.get("schema", MISSING)
        after_schema = after.get("schema", MISSING)
        if before_schema != after_schema:
            _add(
                findings,
                "parameter_changed",
                f"{parameter_pointer}/schema",
                before_schema,
                after_schema,
            )
            if isinstance(before_schema, dict) and isinstance(after_schema, dict):
                _compare_schema(
                    before_schema,
                    after_schema,
                    base_document=base_document,
                    candidate_document=candidate_document,
                    pointer=f"{parameter_pointer}/schema",
                    findings=findings,
                    visited=set(),
                )


def _success_responses(operation: dict[str, Any]) -> dict[str, Any]:
    responses = operation.get("responses", {})
    if not isinstance(responses, dict):
        return {}
    return {
        status: response
        for status, response in responses.items()
        if isinstance(status, str) and len(status) == 3 and status.startswith("2")
    }


def _response_schemas(response: Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    content = response.get("content", {})
    if not isinstance(content, dict):
        return {}
    return {
        media_type: media["schema"]
        for media_type, media in content.items()
        if isinstance(media, dict) and "schema" in media
    }


def _compare_operation(
    base: dict[str, Any],
    candidate: dict[str, Any],
    *,
    base_document: dict[str, Any],
    candidate_document: dict[str, Any],
    pointer: str,
    findings: list[Finding],
) -> None:
    _compare_parameters(
        base.get("parameters"),
        candidate.get("parameters"),
        base_document=base_document,
        candidate_document=candidate_document,
        pointer=pointer,
        findings=findings,
    )
    base_responses = _success_responses(base)
    candidate_responses = _success_responses(candidate)
    for status in sorted(base_responses):
        response_pointer = f"{pointer}/responses/{status}"
        if status not in candidate_responses:
            _add(
                findings,
                "success_response_removed",
                response_pointer,
                base_responses[status],
                MISSING,
            )
            continue
        base_schemas = _response_schemas(base_responses[status])
        candidate_schemas = _response_schemas(candidate_responses[status])
        for media_type in sorted(base_schemas):
            schema_pointer = (
                f"{response_pointer}/content/{_pointer_token(media_type)}/schema"
            )
            if media_type not in candidate_schemas:
                _add(
                    findings,
                    "success_response_schema_removed",
                    schema_pointer,
                    base_schemas[media_type],
                    MISSING,
                )
                continue
            _compare_schema(
                base_schemas[media_type],
                candidate_schemas[media_type],
                base_document=base_document,
                candidate_document=candidate_document,
                pointer=schema_pointer,
                findings=findings,
                visited=set(),
            )


def compare_openapi(
    base: dict[str, Any], candidate: dict[str, Any]
) -> list[Finding]:
    """Return deterministic breaking-change findings for candidate versus base."""
    findings: list[Finding] = []
    base_paths = base.get("paths", {})
    candidate_paths = candidate.get("paths", {})
    if isinstance(base_paths, dict) and isinstance(candidate_paths, dict):
        for path in sorted(base_paths):
            path_pointer = f"#/paths/{_pointer_token(path)}"
            if path not in candidate_paths:
                _add(findings, "path_removed", path_pointer, base_paths[path], MISSING)
                continue
            base_path = base_paths[path]
            candidate_path = candidate_paths[path]
            if not isinstance(base_path, dict) or not isinstance(candidate_path, dict):
                continue
            _compare_parameters(
                base_path.get("parameters"),
                candidate_path.get("parameters"),
                base_document=base,
                candidate_document=candidate,
                pointer=path_pointer,
                findings=findings,
            )
            for method in sorted(HTTP_METHODS & base_path.keys()):
                operation_pointer = f"{path_pointer}/{method}"
                if method not in candidate_path:
                    _add(
                        findings,
                        "operation_removed",
                        operation_pointer,
                        base_path[method],
                        MISSING,
                    )
                    continue
                if isinstance(base_path[method], dict) and isinstance(
                    candidate_path[method], dict
                ):
                    _compare_operation(
                        base_path[method],
                        candidate_path[method],
                        base_document=base,
                        candidate_document=candidate,
                        pointer=operation_pointer,
                        findings=findings,
                    )

    base_schemas = base.get("components", {}).get("schemas", {})
    candidate_schemas = candidate.get("components", {}).get("schemas", {})
    if isinstance(base_schemas, dict) and isinstance(candidate_schemas, dict):
        for name in sorted(base_schemas):
            pointer = f"#/components/schemas/{_pointer_token(name)}"
            if name not in candidate_schemas:
                _add(
                    findings,
                    "schema_removed",
                    pointer,
                    base_schemas[name],
                    MISSING,
                )
                continue
            _compare_schema(
                base_schemas[name],
                candidate_schemas[name],
                base_document=base,
                candidate_document=candidate,
                pointer=pointer,
                findings=findings,
                visited=set(),
            )

    unique = {
        (finding.category, finding.pointer, finding.fingerprint): finding
        for finding in findings
    }
    return sorted(
        unique.values(),
        key=lambda item: (item.pointer, item.category, item.fingerprint),
    )


def _read_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid {label} JSON at {path}: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid {label}: root must be an object")
    return payload


def _approval_fingerprints(
    registry: dict[str, Any], *, today: date
) -> set[str]:
    if (
        set(registry) != {"version", "approvals"}
        or type(registry.get("version")) is not int
        or registry.get("version") != 1
    ):
        raise ValueError("Invalid approval registry version or shape; expected version 1")
    approvals = registry.get("approvals")
    if not isinstance(approvals, list):
        raise ValueError("Invalid approval registry: approvals must be an array")
    fingerprints: set[str] = set()
    for index, approval in enumerate(approvals):
        if not isinstance(approval, dict) or set(approval) != {
            "fingerprint",
            "reason",
            "expiresOn",
        }:
            raise ValueError(f"Malformed approval at index {index}")
        fingerprint = approval["fingerprint"]
        reason = approval["reason"]
        expires_on = approval["expiresOn"]
        if not isinstance(fingerprint, str) or not FINGERPRINT_RE.fullmatch(fingerprint):
            raise ValueError(f"Malformed approval fingerprint at index {index}")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(f"Malformed approval reason at index {index}")
        if fingerprint in fingerprints:
            raise ValueError(f"Duplicate approval fingerprint: {fingerprint}")
        if not isinstance(expires_on, str) or not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}", expires_on
        ):
            raise ValueError(f"Malformed approval expiry at index {index}")
        try:
            expiry = date.fromisoformat(expires_on)
        except (TypeError, ValueError) as error:
            raise ValueError(f"Malformed approval expiry at index {index}") from error
        if expiry <= today:
            raise ValueError(f"Expired approval fingerprint: {fingerprint}")
        fingerprints.add(fingerprint)
    return fingerprints


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--approvals", required=True, type=Path)
    parser.add_argument("--today", required=True, type=date.fromisoformat)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        base = _read_object(args.base, "base OpenAPI")
        candidate = _read_object(args.candidate, "candidate OpenAPI")
        approvals = _read_object(args.approvals, "approval registry")
        approved = _approval_fingerprints(approvals, today=args.today)
    except ValueError as error:
        print(f"[openapi-compat] ERROR: {error}", file=sys.stderr)
        return 1

    findings = compare_openapi(base, candidate)
    emitted = {finding.fingerprint for finding in findings}
    unused = sorted(approved - emitted)
    if unused:
        print(
            "[openapi-compat] ERROR: unused approval fingerprints: " + ", ".join(unused),
            file=sys.stderr,
        )
        return 1

    for finding in findings:
        status = "approved" if finding.fingerprint in approved else "unapproved"
        print(
            json.dumps(
                finding.as_dict() | {"status": status},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    unapproved = [item for item in findings if item.fingerprint not in approved]
    if unapproved:
        print(
            f"[openapi-compat] FAIL: {len(unapproved)} unapproved breaking change(s)",
            file=sys.stderr,
        )
        return 1
    print(f"[openapi-compat] PASS: {len(findings)} approved breaking change(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
