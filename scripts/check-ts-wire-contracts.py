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
NAMESPACE_IMPORT_RE = re.compile(
    r"\bimport\s+type\s+\*\s+as\s+(?P<name>[A-Za-z_$][\w$]*)\s+from\s*"
    r"(?P<quote>['\"])(?P<module>.*?)(?P=quote)\s*;"
)
IDENTIFIER_RE = re.compile(r"\b[A-Za-z_$][\w$]*\b")
EXPORT_LIST_RE = re.compile(
    r"\bexport\s+(?P<type_only>type\s+)?\{(?P<names>.*?)\}"
    r"(?:\s+from\s+(?P<quote>['\"])(?P<module>.*?)(?P=quote))?\s*;",
    re.DOTALL,
)
INDEXED_ROOT_RE = re.compile(r"^(?P<root>[A-Za-z_$][\w$]*)(?P<indexes>.*)$", re.DOTALL)
NAMESPACE_INDEXED_ROOT_RE = re.compile(
    r"^(?P<root>[A-Za-z_$][\w$]*)\.components(?P<indexes>.*)$",
    re.DOTALL,
)
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
    path: Path,
    owned_files: frozenset[Path],
    trusted_exports: frozenset[tuple[Path, str]],
) -> tuple[set[str], set[str], dict[str, bool], set[str]]:
    trusted: set[str] = set()
    helpers: set[str] = set()
    imported_types: dict[str, bool] = {}
    namespaces = {
        match.group("name")
        for match in NAMESPACE_IMPORT_RE.finditer(source)
        if match.group("module").endswith(GENERATED_MODULE_SUFFIX)
    }
    for match in IMPORT_RE.finditer(source):
        module = match.group("module")
        generated_module = module.endswith(GENERATED_MODULE_SUFFIX)
        relative_target = _resolve_owned_module(path, module, owned_files)
        for specifier in match.group("names").split(","):
            imported, local = _imported_name(specifier)
            if not imported or not local:
                continue
            trusted_schema = module == "@trading25/contracts" and imported in schema_names
            trusted_helper = (
                module == "@trading25/contracts" or module.endswith("/endpoint-types")
            ) and imported in ENDPOINT_HELPERS
            trusted_relative = (
                relative_target is not None
                and (relative_target, imported) in trusted_exports
            )
            safe = generated_module or trusted_schema or trusted_helper or trusted_relative
            imported_types[local] = safe
            if generated_module or trusted_schema or trusted_relative:
                trusted.add(local)
            if trusted_helper:
                helpers.add(local)
    return trusted, helpers, imported_types, namespaces


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
    namespaces: set[str],
    *,
    require_index: bool = False,
) -> bool:
    expression = body.strip()
    match = INDEXED_ROOT_RE.fullmatch(expression)
    namespace_match = NAMESPACE_INDEXED_ROOT_RE.fullmatch(expression)
    if match is not None and match.group("root") in trusted:
        indexes = match.group("indexes")
    elif namespace_match is not None and namespace_match.group("root") in namespaces:
        indexes = namespace_match.group("indexes")
    else:
        return False
    return (
        (not require_index or bool(indexes.strip()))
        and INDEX_CHAIN_RE.fullmatch(indexes) is not None
    )


def _is_supported_derivation(
    body: str,
    trusted: set[str],
    helpers: set[str],
    namespaces: set[str],
) -> bool:
    expression = body.strip()
    if _is_indexed_derivation(expression, trusted, namespaces):
        return True

    nonnullable = _split_outer_generic(expression, "NonNullable")
    if nonnullable is not None:
        inner, suffix = nonnullable
        return (
            _is_indexed_derivation(
                inner,
                trusted,
                namespaces,
                require_index=True,
            )
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
    path: Path,
    owned_files: frozenset[Path],
    trusted_exports: frozenset[tuple[Path, str]],
) -> tuple[set[str], dict[str, bool]]:
    aliases = {
        match.group("name"): (
            match.group("body"),
            frozenset(IDENTIFIER_RE.findall(match.group("body"))),
        )
        for match in TYPE_ALIAS_RE.finditer(source)
    }
    trusted, helpers, imported_types, namespaces = _trusted_imports(
        source,
        schema_names,
        path,
        owned_files,
        trusted_exports,
    )
    changed = True
    while changed:
        changed = False
        for name, (body, dependencies) in aliases.items():
            if (
                name not in trusted
                and _is_supported_derivation(body, trusted, helpers, namespaces)
                and dependencies & (trusted | helpers | namespaces)
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


def _resolve_owned_module(
    path: Path,
    module: str | None,
    owned_files: frozenset[Path],
) -> Path | None:
    if module is None or not module.startswith("."):
        return None
    target = path.parent / module
    candidates = [target]
    if target.suffix in {".js", ".jsx", ".mjs"}:
        candidates.append(target.with_suffix(".tsx" if target.suffix == ".jsx" else ".ts"))
    elif not target.suffix:
        candidates.extend((target.with_suffix(".ts"), target.with_suffix(".tsx")))
    candidates.extend((target / "index.ts", target / "index.tsx"))
    return next(
        (candidate.resolve() for candidate in candidates if candidate.resolve() in owned_files),
        None,
    )


def _compute_trusted_exports(
    sources: dict[Path, str],
    schema_names: frozenset[str],
    owned_files: frozenset[Path],
) -> frozenset[tuple[Path, str]]:
    trusted_exports: set[tuple[Path, str]] = set()
    changed = True
    while changed:
        changed = False
        snapshot = frozenset(trusted_exports)
        for path in sorted(sources, key=lambda candidate: candidate.as_posix()):
            source = sources[path]
            trusted_aliases, imported_types = _trusted_aliases(
                source,
                schema_names,
                path,
                owned_files,
                snapshot,
            )
            declarations = {
                declaration.group("name"): declaration
                for declaration in DECLARATION_RE.finditer(source)
            }
            for declaration in declarations.values():
                name = declaration.group("name")
                key = (path.resolve(), name)
                if (
                    declaration.group("export") is not None
                    and declaration.group("kind") == "type"
                    and name in trusted_aliases
                    and key not in trusted_exports
                ):
                    trusted_exports.add(key)
                    changed = True

            for export_list in EXPORT_LIST_RE.finditer(source):
                module = export_list.group("module")
                relative_target = _resolve_owned_module(path, module, owned_files)
                for local, public in _exported_names(export_list.group("names")):
                    direct_contract_reexport = (
                        module == "@trading25/contracts" and local in schema_names
                    )
                    trusted_relative = (
                        relative_target is not None
                        and (relative_target, local) in snapshot
                    )
                    trusted_local = (
                        module is None
                        and (local in trusted_aliases or imported_types.get(local, False))
                    )
                    key = (path.resolve(), public)
                    if (
                        (direct_contract_reexport or trusted_relative or trusted_local)
                        and key not in trusted_exports
                    ):
                        trusted_exports.add(key)
                        changed = True
    return frozenset(trusted_exports)


def _find_collisions(
    path: Path,
    source: str,
    schema_names: frozenset[str],
    trusted_exports: frozenset[tuple[Path, str]],
) -> list[Finding]:
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
        if (path.resolve(), name) in trusted_exports:
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
        for local, public in _exported_names(export_list.group("names")):
            if public not in schema_names:
                continue
            declaration = declarations.get(local)
            if (path.resolve(), public) in trusted_exports:
                continue
            if declaration is not None:
                kind = declaration.group("kind")
                offset = declaration.start()
            else:
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
        sources = {
            path: _mask_comments(path.read_text(encoding="utf-8")) for path in files
        }
        trusted_exports = _compute_trusted_exports(
            sources,
            schema_names,
            owned_files,
        )
        findings = sorted(
            finding
            for path in files
            for finding in _find_collisions(
                path,
                sources[path],
                schema_names,
                trusted_exports,
            )
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
