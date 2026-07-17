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
    r"\b(?P<export>export\s+(?:(?P<default>default)\s+)?)?"
    r"(?:declare\s+)?(?P<kind>interface|type)\s+"
    r"(?P<name>[A-Za-z_$][\w$]*)"
)
TYPE_ALIAS_RE = re.compile(
    r"\b(?P<export>export\s+(?:default\s+)?)?(?:declare\s+)?type\s+"
    r"(?P<name>[A-Za-z_$][\w$]*)(?:\s*<[^;=]*>)?\s*=\s*(?P<body>.*?);",
    re.DOTALL,
)
IMPORT_RE = re.compile(
    r"\bimport\s+type\s*\{(?P<names>.*?)\}\s*from\s*"
    r"(?P<quote>['\"])(?P<module>.*?)(?P=quote)\s*;",
    re.DOTALL,
)
IDENTIFIER_RE = re.compile(r"\b[A-Za-z_$][\w$]*\b")
EXPORT_LIST_RE = re.compile(
    r"\bexport\s+(?P<type_only>type\s+)?\{(?P<names>.*?)\}"
    r"(?:\s+from\s+(?P<quote>['\"])(?P<module>.*?)(?P=quote))?\s*;",
    re.DOTALL,
)
INDEXED_ROOT_RE = re.compile(r"^(?P<root>[A-Za-z_$][\w$]*)(?P<indexes>.*)$", re.DOTALL)
INDEX_CHAIN_RE = re.compile(
    r"(?:\s*\[\s*(?:'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\"|number)\s*\])*\s*"
)
GENERATED_MODULE_SUFFIX = "generated/bt-api-types"
ENDPOINT_HELPERS = {"ApiJsonBody", "ApiJsonResponse", "ApiPathParams", "ApiQuery"}
EXCLUDED_DIRECTORY_SUFFIXES = (".test.ts", ".test-d.ts", ".spec.ts")


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
            files.update(
                candidate
                for candidate in path.rglob("*.ts")
                if candidate.is_file()
                and not candidate.name.endswith(EXCLUDED_DIRECTORY_SUFFIXES)
                and "generated" not in candidate.relative_to(path).parts
            )
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


def _trusted_imports(
    source: str,
    schema_names: frozenset[str],
) -> tuple[set[str], set[str], dict[str, bool]]:
    trusted: set[str] = set()
    helpers: set[str] = set()
    imported_types: dict[str, bool] = {}
    for match in IMPORT_RE.finditer(source):
        module = match.group("module")
        generated_module = module.endswith(GENERATED_MODULE_SUFFIX)
        for specifier in match.group("names").split(","):
            imported, local = _imported_name(specifier)
            if not imported or not local:
                continue
            trusted_schema = module == "@trading25/contracts" and imported in schema_names
            trusted_helper = (
                module == "@trading25/contracts" or module.endswith("/endpoint-types")
            ) and imported in ENDPOINT_HELPERS
            safe = generated_module or trusted_schema or trusted_helper
            imported_types[local] = safe
            if generated_module or trusted_schema:
                trusted.add(local)
            if trusted_helper:
                helpers.add(local)
    return trusted, helpers, imported_types


def _split_outer_generic(body: str, name: str) -> tuple[str, str] | None:
    prefix = f"{name}<"
    if not body.startswith(prefix):
        return None
    depth = 1
    quote: str | None = None
    index = len(prefix)
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
        elif char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
            if depth == 0:
                return body[len(prefix) : index], body[index + 1 :]
        index += 1
    return None


def _is_indexed_derivation(
    body: str,
    trusted: set[str],
    *,
    require_index: bool = False,
) -> bool:
    match = INDEXED_ROOT_RE.fullmatch(body.strip())
    if match is None or match.group("root") not in trusted:
        return False
    indexes = match.group("indexes")
    return (
        (not require_index or bool(indexes.strip()))
        and INDEX_CHAIN_RE.fullmatch(indexes) is not None
    )


def _is_supported_derivation(
    body: str,
    trusted: set[str],
    helpers: set[str],
) -> bool:
    expression = body.strip()
    if _is_indexed_derivation(expression, trusted):
        return True

    nonnullable = _split_outer_generic(expression, "NonNullable")
    if nonnullable is not None:
        inner, suffix = nonnullable
        return (
            _is_indexed_derivation(inner, trusted, require_index=True)
            and INDEX_CHAIN_RE.fullmatch(suffix) is not None
        )

    for helper in helpers:
        helper_call = _split_outer_generic(expression, helper)
        if helper_call is not None and not helper_call[1].strip():
            return True
    return False


def _trusted_aliases(
    source: str,
    schema_names: frozenset[str],
) -> tuple[set[str], dict[str, bool]]:
    aliases = {
        match.group("name"): (
            match.group("body"),
            frozenset(IDENTIFIER_RE.findall(match.group("body"))),
        )
        for match in TYPE_ALIAS_RE.finditer(source)
    }
    trusted, helpers, imported_types = _trusted_imports(source, schema_names)
    changed = True
    while changed:
        changed = False
        for name, (body, dependencies) in aliases.items():
            if (
                name not in trusted
                and _is_supported_derivation(body, trusted, helpers)
                and dependencies & (trusted | helpers)
            ):
                trusted.add(name)
                changed = True
    return trusted, imported_types


def _exported_names(specifiers: str) -> list[tuple[str, str]]:
    exported: list[tuple[str, str]] = []
    for specifier in specifiers.split(","):
        cleaned = re.sub(r"^\s*type\s+", "", specifier.strip())
        if not cleaned:
            continue
        parts = re.split(r"\s+as\s+", cleaned)
        local = parts[0].strip()
        public = parts[-1].strip()
        if IDENTIFIER_RE.fullmatch(local) and IDENTIFIER_RE.fullmatch(public):
            exported.append((local, public))
    return exported


def _is_owned_relative_reexport(
    path: Path,
    module: str | None,
    owned_files: frozenset[Path],
) -> bool:
    if module is None or not module.startswith("."):
        return False
    target = path.parent / module
    candidates = [target]
    if target.suffix in {".js", ".jsx", ".mjs"}:
        candidates.append(target.with_suffix(".tsx" if target.suffix == ".jsx" else ".ts"))
    candidates.extend((target / "index.ts", target / "index.tsx"))
    return any(candidate.resolve() in owned_files for candidate in candidates)


def _find_collisions(
    path: Path,
    schema_names: frozenset[str],
    owned_files: frozenset[Path],
) -> list[Finding]:
    source = _mask_comments(path.read_text(encoding="utf-8"))
    trusted_aliases, imported_types = _trusted_aliases(source, schema_names)
    declarations = {
        declaration.group("name"): declaration
        for declaration in DECLARATION_RE.finditer(source)
    }
    findings: list[Finding] = []
    for declaration in declarations.values():
        if declaration.group("export") is None:
            continue
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

    for export_list in EXPORT_LIST_RE.finditer(source):
        module = export_list.group("module")
        for local, public in _exported_names(export_list.group("names")):
            if public not in schema_names:
                continue
            declaration = declarations.get(local)
            if declaration is not None:
                kind = declaration.group("kind")
                if kind == "type" and local in trusted_aliases:
                    continue
                offset = declaration.start()
            else:
                direct_contract_reexport = (
                    module == "@trading25/contracts" and local in schema_names
                )
                if (
                    direct_contract_reexport
                    or imported_types.get(local, False)
                    or _is_owned_relative_reexport(path, module, owned_files)
                ):
                    continue
                kind = "type"
                offset = export_list.start()
            findings.append(
                Finding(
                    path=path,
                    line=source.count("\n", 0, offset) + 1,
                    name=public,
                    kind=kind,
                )
            )
    return sorted(set(findings))


def main() -> int:
    args = _parse_args()
    try:
        schema_names = _schema_names(args.openapi)
        files = _typescript_files([*args.contracts, *args.api_clients])
        owned_files = frozenset(path.resolve() for path in files)
        findings = sorted(
            finding
            for path in files
            for finding in _find_collisions(path, schema_names, owned_files)
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
