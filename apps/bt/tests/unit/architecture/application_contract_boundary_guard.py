"""Focused static guard for application-owned contracts.

Non-goals are arbitrary attribute access, dynamic imports, and dataflow inference.
HTTP ownership keeps migrated contract names absent from transport modules at runtime.
"""

from __future__ import annotations

import ast
from collections.abc import Callable, Iterable
from pathlib import Path


APPLICATION_HTTP_SCHEMA_PREFIX = "src.entrypoints.http.schemas"
SIGNAL_REFERENCE_HTTP_CONTRACT_NAMES = frozenset(
    {
        "SignalFieldTypeValue",
        "SignalExecutionSemantics",
        "FieldConstraints",
        "SignalFieldSchema",
        "SignalChartCapability",
        "SignalReferenceSchema",
        "SignalAvailabilityProfile",
        "SignalCategorySchema",
        "SignalReferenceResponse",
    }
)
FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES = {
    "AnalyticsSourceKind",
    "ResponseDiagnostics",
    "DataProvenance",
    "JobStatus",
    "JobProgress",
    "JobEvent",
    "SSEJobEvent",
    "BacktestResultSummary",
    "SignalAttributionMetrics",
    "SignalAttributionLooResult",
    "SignalAttributionShapleyResult",
    "SignalAttributionSignalResult",
    "SignalAttributionTopNScore",
    "SignalAttributionTopNSelection",
    "SignalAttributionTiming",
    "SignalAttributionShapleyMeta",
    "SignalAttributionResult",
    "MatchedStrategyItem",
    "ScreeningResultItem",
    "ScreeningSummary",
    "MarketScreeningResponse",
    "ScreeningJobRequest",
    "ScreeningJobPayload",
    "EntryDecidability",
    "ScreeningSupport",
    "ScreeningSortBy",
    "SortOrder",
} | SIGNAL_REFERENCE_HTTP_CONTRACT_NAMES

ImportFromResolver = Callable[[Path, ast.ImportFrom], str | None]


def _is_http_schema_module(module_name: str) -> bool:
    return module_name == APPLICATION_HTTP_SCHEMA_PREFIX or module_name.startswith(
        f"{APPLICATION_HTTP_SCHEMA_PREFIX}."
    )


def _python_files(root: Path) -> Iterable[Path]:
    if root.is_file():
        if root.suffix == ".py":
            yield root
        return
    for py_file in root.rglob("*.py"):
        relative_parts = py_file.relative_to(root).parts
        if not any(part.startswith(".") or part == "__pycache__" for part in relative_parts):
            yield py_file


def _bound_target_names(node: ast.AST) -> set[str]:
    names = {
        child.id
        for child in ast.walk(node)
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store)
    }
    for child in ast.walk(node):
        if isinstance(child, (ast.MatchAs, ast.MatchStar)) and child.name is not None:
            names.add(child.name)
        elif isinstance(child, ast.MatchMapping) and child.rest is not None:
            names.add(child.rest)
    return names


def _module_scope_nodes(tree: ast.Module) -> Iterable[ast.AST]:
    """Walk module control flow without entering function or class bodies."""

    def definition_expressions(node: ast.AST) -> Iterable[ast.expr]:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            arguments = node.args
            yield from arguments.defaults
            yield from (value for value in arguments.kw_defaults if value is not None)
            for argument in (
                *arguments.posonlyargs,
                *arguments.args,
                *arguments.kwonlyargs,
                arguments.vararg,
                arguments.kwarg,
            ):
                if argument is not None and argument.annotation is not None:
                    yield argument.annotation
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield from node.decorator_list
            if node.returns is not None:
                yield node.returns
        elif isinstance(node, ast.ClassDef):
            yield from node.decorator_list
            yield from node.bases
            yield from (keyword.value for keyword in node.keywords)

    def walk(node: ast.AST) -> Iterable[ast.AST]:
        yield node
        if isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
        ):
            for expression in definition_expressions(node):
                yield from walk(expression)
            return
        for child in ast.iter_child_nodes(node):
            yield from walk(child)

    for statement in tree.body:
        yield from walk(statement)


def _binding_names(node: ast.AST) -> set[str]:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {node.name}
    if isinstance(node, ast.Assign):
        return set().union(*(_bound_target_names(target) for target in node.targets))
    if isinstance(node, ast.TypeAlias):
        return _bound_target_names(node.name)
    if isinstance(node, (ast.AnnAssign, ast.AugAssign, ast.NamedExpr)):
        return _bound_target_names(node.target)
    if isinstance(node, (ast.For, ast.AsyncFor)):
        return _bound_target_names(node.target)
    if isinstance(node, (ast.With, ast.AsyncWith)):
        return {
            name
            for item in node.items
            if item.optional_vars is not None
            for name in _bound_target_names(item.optional_vars)
        }
    if isinstance(node, ast.ExceptHandler):
        return {node.name} if node.name is not None else set()
    if isinstance(node, ast.Import):
        return {alias.asname or alias.name.split(".")[0] for alias in node.names}
    if isinstance(node, ast.ImportFrom):
        imported = {alias.name for alias in node.names if alias.name != "*"}
        bound = {alias.asname or alias.name for alias in node.names if alias.name != "*"}
        return imported | bound
    if isinstance(node, (ast.MatchAs, ast.MatchStar, ast.MatchMapping)):
        return _bound_target_names(node)
    return set()


def _literal_strings(node: ast.expr) -> set[str]:
    try:
        value = ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError):
        return set()
    if isinstance(value, str):
        return {value}
    if isinstance(value, (list, tuple, set, frozenset)):
        return {item for item in value if isinstance(item, str)}
    return set()


def _all_export_names(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Assign):
        targets = set().union(*(_bound_target_names(target) for target in node.targets))
        return _literal_strings(node.value) if "__all__" in targets else set()
    if isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        if "__all__" in _bound_target_names(node.target) and node.value is not None:
            return _literal_strings(node.value)
        return set()
    if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
        return set()
    call = node.value
    if (
        isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "__all__"
        and call.func.attr in {"append", "extend"}
        and call.args
    ):
        return _literal_strings(call.args[0])
    return set()


def _direct_import_violations(
    py_file: Path,
    tree: ast.Module,
    project_root: Path,
    resolve_import_from_module: ImportFromResolver,
) -> list[str]:
    relative = py_file.relative_to(project_root)
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        module_name = resolve_import_from_module(py_file, node)
        if module_name is None or not _is_http_schema_module(module_name):
            continue
        imported = (
            {alias.name for alias in node.names}
            & FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES
        )
        if imported:
            violations.append(
                f"{relative}:{node.lineno} imports forbidden HTTP application contracts "
                f"{sorted(imported)} from {module_name}"
            )
    return violations


def _http_ownership_violations(
    py_file: Path,
    tree: ast.Module,
    project_root: Path,
    forbidden_names: set[str] | frozenset[str],
) -> list[str]:
    relative = py_file.relative_to(project_root)
    violations: list[str] = []
    for node in _module_scope_nodes(tree):
        bindings = _binding_names(node) & forbidden_names
        if bindings:
            violations.append(
                f"{relative}:{node.lineno} binds forbidden HTTP application contracts "
                f"{sorted(bindings)}"
            )
        exports = _all_export_names(node) & forbidden_names
        if exports:
            violations.append(
                f"{relative}:{node.lineno} exports forbidden HTTP application contracts "
                f"{sorted(exports)} via __all__"
            )
    return violations


def forbidden_http_application_contract_references(
    *roots: Path,
    project_root: Path,
    resolve_import_from_module: ImportFromResolver,
) -> list[str]:
    """Return direct-import and HTTP ownership violations."""

    violations: list[str] = []
    http_root = project_root / "src" / "entrypoints" / "http"
    schema_root = http_root / "schemas"
    for root in roots:
        for py_file in _python_files(root):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            violations.extend(
                _direct_import_violations(
                    py_file, tree, project_root, resolve_import_from_module
                )
            )
            if py_file.is_relative_to(http_root):
                forbidden_names = (
                    FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES
                    if py_file.is_relative_to(schema_root)
                    else SIGNAL_REFERENCE_HTTP_CONTRACT_NAMES
                )
                violations.extend(
                    _http_ownership_violations(
                        py_file,
                        tree,
                        project_root,
                        forbidden_names,
                    )
                )
    return sorted(violations)
