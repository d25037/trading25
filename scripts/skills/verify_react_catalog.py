#!/usr/bin/env python3
"""Refresh and verify the vendored React best-practices catalog."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

PACKAGE_NAME = "build-web-apps"
PACKAGE_VERSION = "0.1.2"
SOURCE_SKILL_PATH = "skills/react-best-practices"
CATALOG_VERSION = "1.0.0"
EXPECTED_RULE_COUNT = 64
LOCAL_SKILL_PATH = Path(".codex/skills/ts-vercel-react-best-practices")
MANIFEST_NAME = "react-catalog-provenance.json"
NORMALIZATION = (
    "Decode UTF-8; convert CRLF and CR to LF; remove trailing spaces and tabs "
    "from every line; remove all trailing blank lines; append exactly one LF."
)
AGGREGATE_DIGEST = (
    "For files sorted by basename, update SHA-256 with UTF-8 basename, NUL, "
    "normalized UTF-8 content, NUL."
)


def normalize_content(raw: bytes) -> bytes:
    text = raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(line.rstrip(" \t") for line in text.split("\n"))
    return (normalized.rstrip("\n") + "\n").encode("utf-8")


def normalized_file_digest(path: Path) -> str:
    return hashlib.sha256(normalize_content(path.read_bytes())).hexdigest()


def normalized_catalog_digest(files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda candidate: candidate.name):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(normalize_content(path.read_bytes()))
        digest.update(b"\0")
    return digest.hexdigest()


def source_rule_files(source_root: Path) -> list[Path]:
    return sorted(
        path
        for path in (source_root / "rules").glob("*.md")
        if not path.name.startswith("_")
    )


def _local_paths(repo_root: Path) -> tuple[Path, Path, Path, Path]:
    skill_root = repo_root / LOCAL_SKILL_PATH
    return (
        skill_root,
        skill_root / "AGENTS.md",
        skill_root / "rules",
        skill_root / MANIFEST_NAME,
    )


def _source_manifest(source_root: Path) -> dict[str, object]:
    metadata_path = source_root / "metadata.json"
    if not metadata_path.exists():
        raise ValueError(f"Installed React catalog metadata is missing: {metadata_path}")
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("version") != CATALOG_VERSION:
        raise ValueError(
            "Installed React catalog version mismatch: "
            f"expected {CATALOG_VERSION}, got {metadata.get('version')!r}"
        )

    agents_file = source_root / "AGENTS.md"
    rules = source_rule_files(source_root)
    if not agents_file.exists():
        raise ValueError(f"Installed React catalog handbook is missing: {agents_file}")
    if len(rules) != EXPECTED_RULE_COUNT:
        raise ValueError(
            f"Installed React catalog must contain {EXPECTED_RULE_COUNT} rules; got {len(rules)}"
        )

    return {
        "aggregateDigestAlgorithm": AGGREGATE_DIGEST,
        "agentsSha256": normalized_file_digest(agents_file),
        "catalogVersion": CATALOG_VERSION,
        "normalization": NORMALIZATION,
        "package": PACKAGE_NAME,
        "packageVersion": PACKAGE_VERSION,
        "ruleFiles": [path.name for path in rules],
        "rulesSha256": normalized_catalog_digest(rules),
        "sourceSkillPath": SOURCE_SKILL_PATH,
    }


def _load_manifest(manifest_file: Path) -> dict[str, object]:
    try:
        manifest = json.loads(manifest_file.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise ValueError(f"React catalog provenance manifest is invalid: {manifest_file}") from exc
    return manifest


def validate_local_catalog(repo_root: Path) -> list[str]:
    errors: list[str] = []
    _, agents_file, rules_root, manifest_file = _local_paths(repo_root)
    try:
        manifest = _load_manifest(manifest_file)
    except ValueError as exc:
        return [str(exc)]

    expected_provenance = {
        "aggregateDigestAlgorithm": AGGREGATE_DIGEST,
        "catalogVersion": CATALOG_VERSION,
        "normalization": NORMALIZATION,
        "package": PACKAGE_NAME,
        "packageVersion": PACKAGE_VERSION,
        "sourceSkillPath": SOURCE_SKILL_PATH,
    }
    for key, expected in expected_provenance.items():
        if manifest.get(key) != expected:
            errors.append(
                f"React catalog provenance {key!r} must be {expected!r}; got {manifest.get(key)!r}"
            )

    expected_inventory = manifest.get("ruleFiles")
    if not isinstance(expected_inventory, list) or not all(
        isinstance(name, str) for name in expected_inventory
    ):
        errors.append("React catalog provenance ruleFiles must be a string list")
        return errors
    if len(expected_inventory) != EXPECTED_RULE_COUNT:
        errors.append(
            f"React catalog provenance must list {EXPECTED_RULE_COUNT} rules; "
            f"got {len(expected_inventory)}"
        )

    local_rules = sorted(rules_root.glob("*.md"))
    actual_inventory = [path.name for path in local_rules]
    if actual_inventory != sorted(expected_inventory):
        errors.append("React catalog local rule inventory differs from pinned provenance")

    if not agents_file.exists():
        errors.append(f"React catalog handbook is missing: {agents_file}")
    else:
        if agents_file.read_bytes() != normalize_content(agents_file.read_bytes()):
            errors.append(f"React catalog handbook is not normalized: {agents_file}")
        if normalized_file_digest(agents_file) != manifest.get("agentsSha256"):
            errors.append(f"React catalog handbook digest differs from pinned provenance: {agents_file}")

    if actual_inventory == sorted(expected_inventory):
        for path in local_rules:
            if path.read_bytes() != normalize_content(path.read_bytes()):
                errors.append(f"React catalog rule is not normalized: {path}")
        if normalized_catalog_digest(local_rules) != manifest.get("rulesSha256"):
            errors.append(f"React catalog rules digest differs from pinned provenance: {rules_root}")

    return errors


def verify_catalog(repo_root: Path, source_root: Path) -> list[str]:
    errors = validate_local_catalog(repo_root)
    try:
        source_manifest = _source_manifest(source_root)
    except ValueError as exc:
        return [*errors, str(exc)]

    _, local_agents, local_rules_root, manifest_file = _local_paths(repo_root)
    try:
        local_manifest = _load_manifest(manifest_file)
    except ValueError:
        return errors

    for key in ("agentsSha256", "ruleFiles", "rulesSha256"):
        if local_manifest.get(key) != source_manifest.get(key):
            errors.append(f"React catalog {key} differs from installed source: {source_root}")

    source_agents = source_root / "AGENTS.md"
    if local_agents.exists() and local_agents.read_bytes() != normalize_content(source_agents.read_bytes()):
        errors.append(f"React catalog handbook differs from installed source: {source_agents}")

    source_rules = source_rule_files(source_root)
    local_inventory = {path.name for path in local_rules_root.glob("*.md")}
    for source_rule in source_rules:
        local_rule = local_rules_root / source_rule.name
        if source_rule.name in local_inventory and local_rule.read_bytes() != normalize_content(
            source_rule.read_bytes()
        ):
            errors.append(f"React catalog rule differs from installed source: {source_rule.name}")

    return errors


def refresh_catalog(repo_root: Path, source_root: Path) -> None:
    manifest = _source_manifest(source_root)
    skill_root, agents_file, rules_root, manifest_file = _local_paths(repo_root)
    skill_root.mkdir(parents=True, exist_ok=True)
    rules_root.mkdir(parents=True, exist_ok=True)

    agents_file.write_bytes(normalize_content((source_root / "AGENTS.md").read_bytes()))
    expected_names = set(manifest["ruleFiles"])
    for existing in rules_root.glob("*.md"):
        if existing.name not in expected_names:
            existing.unlink()
    for source_rule in source_rule_files(source_root):
        (rules_root / source_rule.name).write_bytes(normalize_content(source_rule.read_bytes()))
    manifest_file.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def locate_installed_source() -> Path:
    relative = Path(
        f"plugins/cache/openai-curated-remote/{PACKAGE_NAME}/{PACKAGE_VERSION}/{SOURCE_SKILL_PATH}"
    )
    candidates: list[Path] = []
    configured_root = os.environ.get("CODEX_HOME")
    if configured_root:
        candidates.append(Path(configured_root) / relative)
    candidates.append(Path.home() / ".codex" / relative)
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    searched = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(
        "Installed build-web-apps@0.1.2 React catalog was not found. "
        f"Searched: {searched}. Pass --source or use --offline for local-only verification."
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, help="Explicit installed react-best-practices root")
    parser.add_argument("--offline", action="store_true", help="Validate only vendored inventory/digests")
    parser.add_argument("--refresh", action="store_true", help="Refresh vendored files from installed source")
    args = parser.parse_args()
    if args.offline and (args.source is not None or args.refresh):
        parser.error("--offline cannot be combined with --source or --refresh")

    repo_root = Path(__file__).resolve().parents[2]
    if args.offline:
        errors = validate_local_catalog(repo_root)
        mode = "offline local-inventory"
    else:
        try:
            source_root = args.source.resolve() if args.source is not None else locate_installed_source()
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if args.refresh:
            refresh_catalog(repo_root, source_root)
        errors = verify_catalog(repo_root, source_root)
        mode = "installed-source"

    if errors:
        print(f"React catalog {mode} verification failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"React catalog {mode} verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
