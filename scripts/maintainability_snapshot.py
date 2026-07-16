#!/usr/bin/env python3
"""Collect lightweight maintainability metrics for refactor planning.

The report is intentionally dependency-free so it can run before package setup.
It measures tracked source files only and uses AST metrics for Python plus
conservative text heuristics for TypeScript/TSX.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import re
import subprocess
import sys
import tokenize
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Sequence


SOURCE_ROOTS = (
    "apps/bt/src/",
    "apps/bt/scripts/",
    "apps/ts/packages/api-clients/src/",
    "apps/ts/packages/utils/src/",
    "apps/ts/packages/web/src/",
    "scripts/",
)
SOURCE_EXTENSIONS = {".py", ".ts", ".tsx"}
EXCLUDED_PATH_PARTS = {
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "node_modules",
    "dist",
    "build",
}
EXCLUDED_SUFFIXES = (
    ".test.py",
    ".test.ts",
    ".test.tsx",
    ".spec.py",
    ".spec.ts",
    ".spec.tsx",
)
COMMENT_PREFIXES = {
    ".py": ("#",),
    ".ts": ("//",),
    ".tsx": ("//",),
}
BRANCH_TOKENS = re.compile(
    r"\b(if|else\s+if|for|while|case|catch|switch|try|except|elif|match|with)\b|&&|\|\||\?"
)
TS_BLOCK_START = re.compile(
    r"^\s*(?:export\s+)?(?:async\s+)?(?:function\s+\w+|"
    r"(?:const|let|var)\s+\w+\s*=\s*(?:async\s*)?(?:\([^)]*\)|\w+)\s*=>|"
    r"\w+\s*:\s*(?:async\s*)?(?:\([^)]*\)|\w+)\s*=>|"
    r"(?:public|private|protected)?\s*(?:async\s+)?\w+\s*\([^)]*\)\s*\{)"
)
MINIMUM_PYTHON = (3, 12)
PYTHON_RECOVERY_COMMAND = (
    "uv run --project apps/bt python scripts/maintainability_snapshot.py ..."
)


@dataclass(frozen=True)
class FunctionMetric:
    path: str
    name: str
    language: str
    start_line: int
    end_line: int
    lines: int
    branch_score: int
    max_nesting: int


@dataclass(frozen=True)
class FileMetric:
    path: str
    language: str
    total_lines: int
    code_lines: int
    branch_score: int
    max_nesting: int
    function_count: int
    max_function_lines: int
    hotspot_score: float


@dataclass(frozen=True)
class Thresholds:
    file_warn_lines: int = 500
    file_high_lines: int = 800
    file_critical_lines: int = 1000
    function_warn_lines: int = 80
    function_high_lines: int = 120
    function_critical_lines: int = 180
    branch_warn_score: int = 30
    branch_high_score: int = 50
    nesting_warn_depth: int = 5


@dataclass(frozen=True)
class Snapshot:
    roots: tuple[str, ...]
    thresholds: Thresholds
    totals: dict[str, int]
    languages: dict[str, dict[str, int]]
    file_buckets: dict[str, int]
    function_buckets: dict[str, int]
    top_files: list[FileMetric]
    top_functions: list[FunctionMetric]
    branch_hotspots: list[FileMetric]
    nesting_hotspots: list[FileMetric]
    target_metrics: dict[str, int]
    notes: list[str] = field(default_factory=list)


def require_supported_python(
    version_info: Sequence[int] | None = None,
) -> None:
    current = (
        (sys.version_info.major, sys.version_info.minor)
        if version_info is None
        else tuple(version_info[:2])
    )
    if current >= MINIMUM_PYTHON:
        return
    print(
        "maintainability_snapshot.py requires Python >=3.12. "
        f"Run: {PYTHON_RECOVERY_COMMAND}",
        file=sys.stderr,
    )
    raise SystemExit(2)


def git_tracked_files(repo_root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=repo_root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return [repo_root / line for line in result.stdout.splitlines() if line]


def is_source_file(repo_root: Path, path: Path, roots: tuple[str, ...]) -> bool:
    relative = path.relative_to(repo_root).as_posix()
    if path.suffix not in SOURCE_EXTENSIONS:
        return False
    if path.name.endswith(EXCLUDED_SUFFIXES):
        return False
    if any(part in EXCLUDED_PATH_PARTS for part in path.parts):
        return False
    return any(relative.startswith(root) for root in roots)


def language_for(path: Path) -> str:
    if path.suffix == ".py":
        return "python"
    if path.suffix == ".tsx":
        return "tsx"
    return "typescript"


def count_code_lines(path: Path, lines: list[str]) -> int:
    if path.suffix == ".py":
        return len(python_effective_code_lines("\n".join(lines)))
    if path.suffix == ".tsx":
        return tsx_logic_line_count(lines)

    prefixes = COMMENT_PREFIXES.get(path.suffix, ())
    in_block_comment = False
    count = 0
    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            continue
        if path.suffix in {".ts", ".tsx"}:
            if in_block_comment:
                if "*/" in stripped:
                    in_block_comment = False
                continue
            if stripped.startswith("/*"):
                if "*/" not in stripped:
                    in_block_comment = True
                continue
        if stripped.startswith(prefixes):
            continue
        count += 1
    return count


def branch_score_for_text(text: str) -> int:
    return len(BRANCH_TOKENS.findall(text))


def python_branch_score(node: ast.AST) -> int:
    branch_nodes = (
        ast.If,
        ast.For,
        ast.AsyncFor,
        ast.While,
        ast.Try,
        ast.ExceptHandler,
        ast.IfExp,
        ast.BoolOp,
        ast.Match,
        ast.comprehension,
    )
    return sum(1 for child in ast.walk(node) if isinstance(child, branch_nodes))


def python_max_nesting(node: ast.AST) -> int:
    nesting_nodes = (
        ast.If,
        ast.For,
        ast.AsyncFor,
        ast.While,
        ast.Try,
        ast.With,
        ast.AsyncWith,
        ast.Match,
    )

    def visit(child: ast.AST, depth: int) -> int:
        next_depth = depth + 1 if isinstance(child, nesting_nodes) else depth
        nested = [visit(grandchild, next_depth) for grandchild in ast.iter_child_nodes(child)]
        return max([next_depth, *nested])

    return visit(node, 0)


def python_effective_code_lines(text: str) -> set[int]:
    """Return code-bearing lines, excluding multi-line literal payloads."""
    effective_lines: set[int] = set()
    ignored_tokens = {
        tokenize.COMMENT,
        tokenize.NL,
        tokenize.NEWLINE,
        tokenize.INDENT,
        tokenize.DEDENT,
        tokenize.ENDMARKER,
        tokenize.ENCODING,
    }
    try:
        tokens = tokenize.generate_tokens(io.StringIO(text).readline)
        for token in tokens:
            if token.type in ignored_tokens:
                continue
            if token.type == tokenize.STRING and token.start[0] != token.end[0]:
                continue
            effective_lines.add(token.start[0])
    except tokenize.TokenError:
        return set(range(1, len(text.splitlines()) + 1))
    return effective_lines


def collect_python_functions(path: Path, relative: str, text: str) -> list[FunctionMetric]:
    tree = ast.parse(
        text,
        filename=relative,
        feature_version=(3, 12),
    )
    effective_lines = python_effective_code_lines(text)
    metrics: list[FunctionMetric] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.end_lineno is None:
            continue
        lines = sum(1 for line_number in range(node.lineno, node.end_lineno + 1) if line_number in effective_lines)
        metrics.append(
            FunctionMetric(
                path=relative,
                name=node.name,
                language="python",
                start_line=node.lineno,
                end_line=node.end_lineno,
                lines=lines,
                branch_score=python_branch_score(node),
                max_nesting=python_max_nesting(node),
            )
        )
    return metrics


def ts_max_brace_depth(lines: list[str]) -> int:
    depth = 0
    max_depth = 0
    for line in lines:
        stripped = re.sub(r"//.*", "", line)
        for char in stripped:
            if char == "{":
                depth += 1
                max_depth = max(max_depth, depth)
            elif char == "}":
                depth = max(0, depth - 1)
    return max_depth


def is_tsx_layout_only_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if stripped in {"(", ")", ");", "{", "}", "};", "<>", "</>"}:
        return True
    if stripped.startswith(("<", "</")) and not any(token in stripped for token in ("=>", "?", "&&", "||")):
        return True
    if re.match(r"^[A-Za-z][A-Za-z0-9_-]*=", stripped):
        return True
    return False


def tsx_logic_line_count(lines: list[str]) -> int:
    return sum(1 for line in lines if not is_tsx_layout_only_line(line) and not line.strip().startswith("//"))


def tsx_logic_text(lines: list[str]) -> str:
    return "\n".join(line for line in lines if not is_tsx_layout_only_line(line) and not line.strip().startswith("//"))


def collect_ts_functions(relative: str, lines: list[str]) -> list[FunctionMetric]:
    metrics: list[FunctionMetric] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not TS_BLOCK_START.search(line):
            index += 1
            continue
        name_match = re.search(r"(?:function\s+|(?:const|let|var)\s+|^|\s)([A-Za-z0-9_]+)", line)
        name = name_match.group(1) if name_match else "<anonymous>"
        body_start = index
        if re.match(r"^\s*(?:export\s+)?(?:async\s+)?function\s+\w+\(\{\s*$", line):
            for probe_index in range(index + 1, len(lines)):
                if re.match(r"^\s*}\)\s*\{\s*$", lines[probe_index]):
                    body_start = probe_index
                    break
        depth = 0
        started = False
        start_index = index
        end_index = body_start
        while end_index < len(lines):
            stripped = re.sub(r"//.*", "", lines[end_index])
            for char in stripped:
                if char == "{":
                    depth += 1
                    started = True
                elif char == "}":
                    depth -= 1
            if started and depth <= 0:
                break
            end_index += 1
        block = lines[start_index : end_index + 1]
        line_count = tsx_logic_line_count(block) if relative.endswith(".tsx") else end_index - start_index + 1
        branch_text = tsx_logic_text(block) if relative.endswith(".tsx") else "\n".join(block)
        metrics.append(
            FunctionMetric(
                path=relative,
                name=name,
                language="typescript" if relative.endswith(".ts") else "tsx",
                start_line=start_index + 1,
                end_line=end_index + 1,
                lines=line_count,
                branch_score=branch_score_for_text(branch_text),
                max_nesting=ts_max_brace_depth(block),
            )
        )
        index = max(end_index + 1, index + 1)
    return metrics


def hotspot_score(code_lines: int, branch_score: int, max_nesting: int, max_function_lines: int) -> float:
    return round(
        code_lines
        + branch_score * 18
        + max_nesting * 25
        + max(0, max_function_lines - 80) * 3,
        2,
    )


def collect_metrics(repo_root: Path, roots: tuple[str, ...]) -> tuple[list[FileMetric], list[FunctionMetric]]:
    files: list[FileMetric] = []
    functions: list[FunctionMetric] = []
    for path in git_tracked_files(repo_root):
        if not is_source_file(repo_root, path, roots):
            continue
        relative = path.relative_to(repo_root).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as error:
            error.add_note(f"while reading tracked source {relative}")
            raise
        lines = text.splitlines()
        language = language_for(path)
        if path.suffix == ".py":
            file_functions = collect_python_functions(path, relative, text)
            max_nesting = max((metric.max_nesting for metric in file_functions), default=0)
            branch_score = sum(metric.branch_score for metric in file_functions)
        else:
            file_functions = collect_ts_functions(relative, lines)
            max_nesting = ts_max_brace_depth(lines)
            branch_score = branch_score_for_text(tsx_logic_text(lines) if path.suffix == ".tsx" else text)
        functions.extend(file_functions)
        max_function_lines = max((metric.lines for metric in file_functions), default=0)
        code_lines = count_code_lines(path, lines)
        files.append(
            FileMetric(
                path=relative,
                language=language,
                total_lines=len(lines),
                code_lines=code_lines,
                branch_score=branch_score,
                max_nesting=max_nesting,
                function_count=len(file_functions),
                max_function_lines=max_function_lines,
                hotspot_score=hotspot_score(code_lines, branch_score, max_nesting, max_function_lines),
            )
        )
    return files, functions


def bucket_files(files: Iterable[FileMetric], thresholds: Thresholds) -> dict[str, int]:
    return {
        "critical_1000_plus": sum(1 for item in files if item.total_lines >= thresholds.file_critical_lines),
        "high_800_plus": sum(1 for item in files if item.total_lines >= thresholds.file_high_lines),
        "warn_500_plus": sum(1 for item in files if item.total_lines >= thresholds.file_warn_lines),
    }


def bucket_functions(functions: Iterable[FunctionMetric], thresholds: Thresholds) -> dict[str, int]:
    return {
        "critical_180_plus": sum(1 for item in functions if item.lines >= thresholds.function_critical_lines),
        "high_120_plus": sum(1 for item in functions if item.lines >= thresholds.function_high_lines),
        "warn_80_plus": sum(1 for item in functions if item.lines >= thresholds.function_warn_lines),
        "branch_50_plus": sum(1 for item in functions if item.branch_score >= thresholds.branch_high_score),
    }


def summarize_languages(files: list[FileMetric]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for item in files:
        language = summary.setdefault(item.language, {"files": 0, "total_lines": 0, "code_lines": 0})
        language["files"] += 1
        language["total_lines"] += item.total_lines
        language["code_lines"] += item.code_lines
    return dict(sorted(summary.items()))


def target_metrics(file_buckets: dict[str, int], function_buckets: dict[str, int]) -> dict[str, int]:
    return {
        "critical_1000_plus_files": max(0, min(file_buckets["critical_1000_plus"], 10)),
        "high_800_plus_files": max(0, min(file_buckets["high_800_plus"], 25)),
        "warn_500_plus_files": max(0, min(file_buckets["warn_500_plus"], 75)),
        "critical_180_plus_functions": max(0, min(function_buckets["critical_180_plus"], 5)),
        "high_120_plus_functions": max(0, min(function_buckets["high_120_plus"], 25)),
        "branch_50_plus_functions": 0,
    }


def make_snapshot(repo_root: Path, roots: tuple[str, ...]) -> Snapshot:
    thresholds = Thresholds()
    files, functions = collect_metrics(repo_root, roots)
    file_buckets = bucket_files(files, thresholds)
    function_buckets = bucket_functions(functions, thresholds)
    totals = {
        "files": len(files),
        "functions": len(functions),
        "total_lines": sum(item.total_lines for item in files),
        "code_lines": sum(item.code_lines for item in files),
    }
    return Snapshot(
        roots=roots,
        thresholds=thresholds,
        totals=totals,
        languages=summarize_languages(files),
        file_buckets=file_buckets,
        function_buckets=function_buckets,
        top_files=sorted(files, key=lambda item: item.hotspot_score, reverse=True)[:25],
        top_functions=sorted(functions, key=lambda item: (item.lines, item.branch_score), reverse=True)[:25],
        branch_hotspots=sorted(files, key=lambda item: item.branch_score, reverse=True)[:15],
        nesting_hotspots=sorted(files, key=lambda item: item.max_nesting, reverse=True)[:15],
        target_metrics=target_metrics(file_buckets, function_buckets),
        notes=[
            "Python function metrics use ast.FunctionDef spans and AST branch nodes.",
            "Python function line counts are effective code lines; multi-line SQL, Markdown, and string payloads are not counted as executable code.",
            "TSX function and code-line metrics count logic-bearing lines and avoid JSX-only layout inflation.",
            "TypeScript/TSX function metrics are heuristic because this script has no TypeScript parser dependency.",
            "Generated contracts, test files, and docs are excluded; the scope is maintainable production/tool source.",
        ],
    )


def markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        output.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(output)


def render_markdown(snapshot: Snapshot) -> str:
    top_file_rows = [
        [
            item.path,
            item.total_lines,
            item.code_lines,
            item.max_function_lines,
            item.branch_score,
            item.max_nesting,
            item.hotspot_score,
        ]
        for item in snapshot.top_files[:15]
    ]
    top_function_rows = [
        [
            item.path,
            item.name,
            item.lines,
            item.branch_score,
            item.max_nesting,
        ]
        for item in snapshot.top_functions[:15]
    ]
    language_rows = [
        [language, values["files"], values["total_lines"], values["code_lines"]]
        for language, values in snapshot.languages.items()
    ]
    target_rows = [
        [metric, current, snapshot.target_metrics[target]]
        for metric, current, target in [
            (
                "files >= 1000 lines",
                snapshot.file_buckets["critical_1000_plus"],
                "critical_1000_plus_files",
            ),
            (
                "files >= 800 lines",
                snapshot.file_buckets["high_800_plus"],
                "high_800_plus_files",
            ),
            (
                "files >= 500 lines",
                snapshot.file_buckets["warn_500_plus"],
                "warn_500_plus_files",
            ),
            (
                "functions >= 180 lines",
                snapshot.function_buckets["critical_180_plus"],
                "critical_180_plus_functions",
            ),
            (
                "functions >= 120 lines",
                snapshot.function_buckets["high_120_plus"],
                "high_120_plus_functions",
            ),
            (
                "functions branch score >= 50",
                snapshot.function_buckets["branch_50_plus"],
                "branch_50_plus_functions",
            ),
        ]
    ]
    return "\n".join(
        [
            "# Maintainability Snapshot",
            "",
            "This is a quantitative baseline for staged spaghetti-code reduction.",
            "The numbers are not quality by themselves; they identify where focused, behavior-preserving refactor slices should start.",
            "",
            "## Scope",
            "",
            "Measured tracked source under:",
            "",
            *[f"- `{root}`" for root in snapshot.roots],
            "",
            "## Summary",
            "",
            markdown_table(
                ["metric", "value"],
                [
                    ["files", snapshot.totals["files"]],
                    ["functions/blocks", snapshot.totals["functions"]],
                    ["total lines", snapshot.totals["total_lines"]],
                    ["code lines", snapshot.totals["code_lines"]],
                ],
            ),
            "",
            "## Language Split",
            "",
            markdown_table(["language", "files", "total lines", "code lines"], language_rows),
            "",
            "## Baseline To Target",
            "",
            markdown_table(["metric", "current", "target"], target_rows),
            "",
            "## Top File Hotspots",
            "",
            markdown_table(
                [
                    "path",
                    "lines",
                    "code",
                    "max block code lines",
                    "branch score",
                    "nesting",
                    "hotspot score",
                ],
                top_file_rows,
            ),
            "",
            "## Top Function/Block Hotspots",
            "",
            markdown_table(["path", "name", "code lines", "branch score", "nesting"], top_function_rows),
            "",
            "## Interpretation Rules",
            "",
            "- Reduce large orchestrators by extracting responsibility-specific helpers only when tests can characterize the existing behavior.",
            "- Do not treat low reference count as dead code without proving current runtime/API/workflow reachability.",
            "- Prefer lowering per-file and per-function concentration over raw total LOC reduction; module splits can temporarily increase family LOC.",
            "- Re-run this script after every cleanup slice and compare the baseline-to-target table.",
            "",
            "## Notes",
            "",
            *[f"- {note}" for note in snapshot.notes],
            "",
        ]
    )


def write_json(path: Path, snapshot: Snapshot) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_json(snapshot), encoding="utf-8")


def write_markdown(path: Path, snapshot: Snapshot) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_markdown(snapshot), encoding="utf-8")


def render_json(snapshot: Snapshot) -> str:
    return json.dumps(asdict(snapshot), ensure_ascii=False, indent=2) + "\n"


def check_outputs(
    snapshot: Snapshot,
    *,
    json_path: Path | None,
    markdown_path: Path | None,
) -> bool:
    expected = []
    if json_path is not None:
        expected.append((json_path, render_json(snapshot)))
    if markdown_path is not None:
        expected.append((markdown_path, render_markdown(snapshot)))

    drifted: list[Path] = []
    for path, content in expected:
        try:
            existing = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            drifted.append(path)
            continue
        if existing != content:
            drifted.append(path)

    if not drifted:
        return True
    print(
        "maintainability snapshot drift: "
        + ", ".join(str(path) for path in drifted)
        + ". Regenerate with "
        + PYTHON_RECOVERY_COMMAND.replace("...", ""),
        file=sys.stderr,
    )
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Repository root. Defaults to the current directory.",
    )
    parser.add_argument("--json-out", type=Path, help="Write snapshot JSON to this path.")
    parser.add_argument("--md-out", type=Path, help="Write snapshot Markdown to this path.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Compare with existing outputs without rewriting them.",
    )
    parser.add_argument(
        "--roots",
        nargs="*",
        default=SOURCE_ROOTS,
        help="Tracked source roots to include.",
    )
    args = parser.parse_args()
    if args.check and args.json_out is None:
        parser.error("--check requires --json-out to compare the JSON artifact")
    return args


def main() -> int:
    require_supported_python()
    args = parse_args()
    repo_root = args.root.resolve()
    roots = tuple(root if root.endswith("/") else f"{root}/" for root in args.roots)
    snapshot = make_snapshot(repo_root, roots)
    if args.check:
        return (
            0
            if check_outputs(
                snapshot,
                json_path=args.json_out,
                markdown_path=args.md_out,
            )
            else 1
        )
    if args.json_out:
        write_json(args.json_out, snapshot)
    if args.md_out:
        write_markdown(args.md_out, snapshot)
    if not args.json_out and not args.md_out:
        print(render_markdown(snapshot))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
