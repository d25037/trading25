#!/usr/bin/env python3
"""Reject handwritten TypeScript declarations that shadow OpenAPI schemas."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys
from typing import Iterable


DECLARATION_RE = re.compile(
    r"\bexport\s+(?:declare\s+)?(?P<kind>interface|type)\s+"
    r"(?P<name>[A-Za-z_$][\w$]*)"
)
TYPE_ALIAS_RE = re.compile(
    r"\b(?P<export>export\s+)?(?:declare\s+)?type\s+"
    r"(?P<name>[A-Za-z_$][\w$]*)(?:\s*<[^;=]*>)?\s*=\s*(?P<body>.*?);",
    re.DOTALL,
)
IMPORT_RE = re.compile(
    r"\bimport\s+type\s*\{(?P<names>.*?)\}\s*from\s*"
    r"(?P<quote>['\"])(?P<module>.*?)(?P=quote)\s*;",
    re.DOTALL,
)
IDENTIFIER_RE = re.compile(r"\b[A-Za-z_$][\w$]*\b")
GENERATED_MODULE_SUFFIX = "generated/bt-api-types"
ENDPOINT_HELPERS = {"ApiJsonBody", "ApiJsonResponse", "ApiPathParams", "ApiQuery"}


@dataclass(frozen=True, order=True)
class Finding:
    path: Path
    line: int
    name: str
    kind: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reject exported handwritten TypeScript declarations whose names "
            "collide with OpenAPI component schemas."
        )
    )
    parser.add_argument("--openapi", required=True, type=Path)
    parser.add_argument("--contracts", required=True, type=Path, nargs="+")
    parser.add_argument("--api-clients", required=True, type=Path, nargs="+")
    return parser.parse_args()


def _schema_names(openapi_path: Path) -> frozenset[str]:
    with openapi_path.open(encoding="utf-8") as source:
        document = json.load(source)
    schemas = document.get("components", {}).get("schemas", {})
    if not isinstance(schemas, dict):
        raise ValueError("OpenAPI components.schemas must be an object")
    return frozenset(str(name) for name in schemas)


def _typescript_files(paths: Iterable[Path]) -> list[Path]:
    files: set[Path] = set()
    for path in paths:
        if path.is_dir():
            files.update(candidate for candidate in path.rglob("*.ts") if candidate.is_file())
        elif path.is_file():
            files.add(path)
        else:
            raise FileNotFoundError(path)
    return sorted(files, key=lambda candidate: candidate.as_posix())


def _mask_comments(source: str) -> str:
    """Replace comment contents with spaces while preserving offsets/newlines."""
    chars = list(source)
    index = 0
    quote: str | None = None
    while index < len(chars):
        char = chars[index]
        next_char = chars[index + 1] if index + 1 < len(chars) else ""
        if quote is not None:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "/" and next_char == "/":
            chars[index] = chars[index + 1] = " "
            index += 2
            while index < len(chars) and chars[index] not in {"\n", "\r"}:
                chars[index] = " "
                index += 1
            continue
        if char == "/" and next_char == "*":
            chars[index] = chars[index + 1] = " "
            index += 2
            while index < len(chars):
                if index + 1 < len(chars) and chars[index] == "*" and chars[index + 1] == "/":
                    chars[index] = chars[index + 1] = " "
                    index += 2
                    break
                if chars[index] not in {"\n", "\r"}:
                    chars[index] = " "
                index += 1
            continue
        index += 1
    return "".join(chars)


def _imported_name(specifier: str) -> tuple[str, str]:
    parts = re.split(r"\s+as\s+", specifier.strip())
    imported = parts[0].strip()
    local = parts[-1].strip()
    return imported, local


def _trusted_imports(source: str) -> set[str]:
    trusted: set[str] = set()
    for match in IMPORT_RE.finditer(source):
        module = match.group("module")
        generated_module = module.endswith(GENERATED_MODULE_SUFFIX)
        canonical_contract_module = module == "@trading25/contracts" or module.endswith(
            "/endpoint-types"
        )
        for specifier in match.group("names").split(","):
            imported, local = _imported_name(specifier)
            if not imported or not local:
                continue
            if generated_module or (
                canonical_contract_module
                and (imported in ENDPOINT_HELPERS or module == "@trading25/contracts")
            ):
                trusted.add(local)
    return trusted


def _is_alias_or_indexed_access(body: str) -> bool:
    """Exclude handwritten object, union, and intersection composition."""
    quote: str | None = None
    index = 0
    while index < len(body):
        char = body[index]
        if quote is not None:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
        elif char in {"'", '"', "`"}:
            quote = char
        elif char in {"{", "}", "&", "|"}:
            return False
        index += 1
    return True


def _trusted_aliases(source: str) -> set[str]:
    aliases = {
        match.group("name"): (
            match.group("body"),
            frozenset(IDENTIFIER_RE.findall(match.group("body"))),
        )
        for match in TYPE_ALIAS_RE.finditer(source)
    }
    trusted = _trusted_imports(source)
    changed = True
    while changed:
        changed = False
        for name, (body, dependencies) in aliases.items():
            if (
                name not in trusted
                and _is_alias_or_indexed_access(body)
                and dependencies & trusted
            ):
                trusted.add(name)
                changed = True
    return trusted


def _find_collisions(path: Path, schema_names: frozenset[str]) -> list[Finding]:
    source = _mask_comments(path.read_text(encoding="utf-8"))
    trusted_aliases = _trusted_aliases(source)
    findings: list[Finding] = []
    for declaration in DECLARATION_RE.finditer(source):
        name = declaration.group("name")
        kind = declaration.group("kind")
        if name not in schema_names:
            continue
        if kind == "type" and name in trusted_aliases:
            continue
        findings.append(
            Finding(
                path=path,
                line=source.count("\n", 0, declaration.start()) + 1,
                name=name,
                kind=kind,
            )
        )
    return findings


def main() -> int:
    args = _parse_args()
    try:
        schema_names = _schema_names(args.openapi)
        files = _typescript_files([*args.contracts, *args.api_clients])
        findings = sorted(
            finding
            for path in files
            for finding in _find_collisions(path, schema_names)
        )
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError) as error:
        print(f"wire contract check error: {error}", file=sys.stderr)
        return 2

    for finding in findings:
        print(
            f"{finding.path}:{finding.line}: {finding.name}: handwritten "
            f"{finding.kind} collides with OpenAPI component schema",
            file=sys.stderr,
        )
    if findings:
        print(
            "Use a generated schema alias/indexed-access type, or give a distinct "
            "UI/internal model a non-schema name.",
            file=sys.stderr,
        )
        return 1

    print(
        f"[contract] TypeScript wire DTO duplicate check passed "
        f"({len(schema_names)} schemas, {len(files)} files)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
