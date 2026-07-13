"""AST guard for application-owned job contracts at the HTTP schema boundary."""

from __future__ import annotations

import ast
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path


APPLICATION_HTTP_SCHEMA_PREFIX = "src.entrypoints.http.schemas"
FORBIDDEN_HTTP_JOB_CONTRACT_NAMES = {
    "JobStatus",
    "JobProgress",
    "JobEvent",
    "SSEJobEvent",
}

ImportFromResolver = Callable[[Path, ast.ImportFrom], str | None]

_IMPORTLIB_MODULE = "<importlib-module>"
_IMPORT_MODULE_FUNCTION = "<import-module-function>"
_SCHEMA_PACKAGE_ROOT = "<schema-package-root>"


def _is_http_schema_module(module_name: str) -> bool:
    return module_name == APPLICATION_HTTP_SCHEMA_PREFIX or module_name.startswith(
        f"{APPLICATION_HTTP_SCHEMA_PREFIX}."
    )


def _attribute_path(node: ast.expr) -> str | None:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    parts.append(current.id)
    return ".".join(reversed(parts))


def _bound_names(target: ast.expr) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    if isinstance(target, (ast.Tuple, ast.List)):
        return {
            name
            for element in target.elts
            for name in _bound_names(element)
        }
    return set()


class _LocalBindingCollector(ast.NodeVisitor):
    """Collect names owned by one lexical scope without entering child scopes."""

    def __init__(self) -> None:
        self.names: set[str] = set()

    def collect(self, statements: list[ast.stmt]) -> set[str]:
        for statement in statements:
            self.visit(statement)
        return self.names

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self.names.update(_bound_names(target))
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.names.update(_bound_names(node.target))
        if node.value is not None:
            self.visit(node.value)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.names.update(_bound_names(node.target))
        self.visit(node.value)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.names.update(_bound_names(node.target))
        self.visit(node.value)

    def visit_Import(self, node: ast.Import) -> None:
        self.names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self.names.update(
            alias.asname or alias.name for alias in node.names if alias.name != "*"
        )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.names.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.names.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def visit_ListComp(self, node: ast.ListComp) -> None:
        return

    def visit_SetComp(self, node: ast.SetComp) -> None:
        return

    def visit_DictComp(self, node: ast.DictComp) -> None:
        return

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        return

    def visit_For(self, node: ast.For) -> None:
        self.names.update(_bound_names(node.target))
        self.generic_visit(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self.names.update(_bound_names(node.target))
        self.generic_visit(node)

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> None:
        for item in node.items:
            if item.optional_vars is not None:
                self.names.update(_bound_names(item.optional_vars))
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        self._visit_with(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        self._visit_with(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name is not None:
            self.names.add(node.name)
        self.generic_visit(node)


def _argument_names(arguments: ast.arguments) -> set[str]:
    return {
        argument.arg
        for argument in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
            *([arguments.vararg] if arguments.vararg is not None else []),
            *([arguments.kwarg] if arguments.kwarg is not None else []),
        )
    }


def _argument_annotations(arguments: ast.arguments) -> tuple[ast.expr, ...]:
    return tuple(
        argument.annotation
        for argument in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
            *([arguments.vararg] if arguments.vararg is not None else []),
            *([arguments.kwarg] if arguments.kwarg is not None else []),
        )
        if argument.annotation is not None
    )


@dataclass
class _Scope:
    parent: _Scope | None
    kind: str
    local_names: set[str]
    bindings: dict[str, str] = field(default_factory=dict)

    def resolve(self, name: str) -> str | None:
        if name in self.bindings:
            return self.bindings[name]
        if name in self.local_names:
            return None
        if self.parent is not None:
            return self.parent.resolve(name)
        return None

    def shadows(self, name: str) -> bool:
        if name in self.local_names:
            return True
        return self.parent.shadows(name) if self.parent is not None else False

    def bind(self, name: str, value: str | None) -> None:
        if value is None:
            self.bindings.pop(name, None)
        else:
            self.bindings[name] = value


class _ContractAccessVisitor(ast.NodeVisitor):
    def __init__(
        self,
        py_file: Path,
        tree: ast.Module,
        project_root: Path,
        resolve_import_from_module: ImportFromResolver,
    ) -> None:
        self.py_file = py_file
        self.relative = py_file.relative_to(project_root)
        self.resolve_import_from_module = resolve_import_from_module
        self.scope = _Scope(
            parent=None,
            kind="module",
            local_names=_LocalBindingCollector().collect(tree.body),
        )
        self.violations: list[str] = []

    def _record(self, node: ast.AST, message: str) -> None:
        self.violations.append(f"{self.relative}:{node.lineno} {message}")

    def _child_parent(self) -> _Scope | None:
        if self.scope.kind == "class":
            return self.scope.parent
        return self.scope

    def _module_reference(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            value = self.scope.resolve(node.id)
            return value if value is not None and _is_http_schema_module(value) else None

        path = _attribute_path(node)
        if path is not None and _is_http_schema_module(path):
            root_name = path.split(".", maxsplit=1)[0]
            if self.scope.resolve(root_name) == _SCHEMA_PACKAGE_ROOT:
                return path

        return self._literal_dynamic_module(node)

    def _literal_dynamic_module(self, node: ast.expr) -> str | None:
        if not isinstance(node, ast.Call) or not node.args:
            return None

        is_loader = False
        if isinstance(node.func, ast.Name):
            if node.func.id == "__import__" and not self.scope.shadows("__import__"):
                is_loader = True
            elif self.scope.resolve(node.func.id) == _IMPORT_MODULE_FUNCTION:
                is_loader = True
        elif (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.attr == "import_module"
            and self.scope.resolve(node.func.value.id) == _IMPORTLIB_MODULE
        ):
            is_loader = True
        if not is_loader:
            return None

        module_arg = node.args[0]
        if (
            isinstance(module_arg, ast.Constant)
            and isinstance(module_arg.value, str)
            and _is_http_schema_module(module_arg.value)
        ):
            return module_arg.value
        return None

    def _bind_targets(self, targets: list[ast.expr], value: str | None) -> None:
        for target in targets:
            for name in _bound_names(target):
                self.scope.bind(name, value)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            bound_name = alias.asname or alias.name.split(".")[0]
            if alias.name == "importlib":
                self.scope.bind(bound_name, _IMPORTLIB_MODULE)
            elif _is_http_schema_module(alias.name):
                self.scope.bind(
                    bound_name,
                    alias.name if alias.asname else _SCHEMA_PACKAGE_ROOT,
                )
            else:
                self.scope.bind(bound_name, None)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module_name = self.resolve_import_from_module(self.py_file, node)
        if module_name == "importlib":
            for alias in node.names:
                bound_name = alias.asname or alias.name
                value = _IMPORT_MODULE_FUNCTION if alias.name == "import_module" else None
                self.scope.bind(bound_name, value)
            return

        if module_name is None or not _is_http_schema_module(module_name):
            for alias in node.names:
                if alias.name != "*":
                    self.scope.bind(alias.asname or alias.name, None)
            return

        imported = {
            name
            for alias in node.names
            for name in (alias.name, alias.asname)
            if name in FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        }
        if imported:
            self._record(
                node,
                "imports forbidden HTTP job contracts "
                f"{sorted(imported)} from {module_name}",
            )
        for alias in node.names:
            if alias.name != "*":
                self.scope.bind(
                    alias.asname or alias.name,
                    f"{module_name}.{alias.name}",
                )

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        self._bind_targets(node.targets, self._module_reference(node.value))

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.visit(node.annotation)
        if node.value is not None:
            self.visit(node.value)
        value = self._module_reference(node.value) if node.value is not None else None
        self._bind_targets([node.target], value)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.visit(node.target)
        self.visit(node.value)
        self._bind_targets([node.target], None)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self._bind_targets([node.target], self._module_reference(node.value))

    def _visit_for(self, node: ast.For | ast.AsyncFor) -> None:
        self.visit(node.iter)
        self._bind_targets([node.target], None)
        for statement in (*node.body, *node.orelse):
            self.visit(statement)

    def visit_For(self, node: ast.For) -> None:
        self._visit_for(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self._visit_for(node)

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self._bind_targets([item.optional_vars], None)
        for statement in node.body:
            self.visit(statement)

    def visit_With(self, node: ast.With) -> None:
        self._visit_with(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        self._visit_with(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.type is not None:
            self.visit(node.type)
        if node.name is not None:
            self.scope.bind(node.name, None)
        for statement in node.body:
            self.visit(statement)

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        for expression in (
            *node.decorator_list,
            *node.args.defaults,
            *(default for default in node.args.kw_defaults if default is not None),
            *_argument_annotations(node.args),
            *([node.returns] if node.returns is not None else []),
        ):
            self.visit(expression)
        self.scope.bind(node.name, None)

        local_names = _LocalBindingCollector().collect(node.body)
        local_names.update(_argument_names(node.args))
        previous_scope = self.scope
        self.scope = _Scope(self._child_parent(), "function", local_names)
        for statement in node.body:
            self.visit(statement)
        self.scope = previous_scope

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for expression in (*node.decorator_list, *node.bases, *node.keywords):
            self.visit(expression if isinstance(expression, ast.expr) else expression.value)
        self.scope.bind(node.name, None)

        previous_scope = self.scope
        self.scope = _Scope(
            parent=previous_scope,
            kind="class",
            local_names=_LocalBindingCollector().collect(node.body),
        )
        for statement in node.body:
            self.visit(statement)
        self.scope = previous_scope

    def visit_Lambda(self, node: ast.Lambda) -> None:
        for expression in (
            *node.args.defaults,
            *(default for default in node.args.kw_defaults if default is not None),
        ):
            self.visit(expression)
        previous_scope = self.scope
        self.scope = _Scope(
            self._child_parent(),
            "lambda",
            _argument_names(node.args),
        )
        self.visit(node.body)
        self.scope = previous_scope

    def _visit_comprehension(
        self,
        generators: list[ast.comprehension],
        result_expressions: tuple[ast.expr, ...],
    ) -> None:
        self.visit(generators[0].iter)
        local_names = {
            name
            for generator in generators
            for name in _bound_names(generator.target)
        }
        previous_scope = self.scope
        self.scope = _Scope(self._child_parent(), "comprehension", local_names)
        for index, generator in enumerate(generators):
            if index > 0:
                self.visit(generator.iter)
            self._bind_targets([generator.target], None)
            for condition in generator.ifs:
                self.visit(condition)
        for expression in result_expressions:
            self.visit(expression)
        self.scope = previous_scope

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node.generators, (node.key, node.value))

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in FORBIDDEN_HTTP_JOB_CONTRACT_NAMES:
            module_name = self._module_reference(node.value)
            if module_name is not None:
                self._record(
                    node,
                    "accesses forbidden HTTP job contract "
                    f"{node.attr} from {module_name}",
                )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        ):
            module_name = self._module_reference(node.args[0])
            if module_name is not None:
                self._record(
                    node,
                    "dynamically accesses forbidden HTTP job contract "
                    f"{node.args[1].value} from {module_name}",
                )
        self.generic_visit(node)


def _module_scope_nodes(tree: ast.Module) -> list[ast.stmt]:
    nodes: list[ast.stmt] = []

    def visit(statement: ast.stmt) -> None:
        nodes.append(statement)
        if isinstance(statement, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            return
        for child in ast.iter_child_nodes(statement):
            if isinstance(child, ast.stmt):
                visit(child)

    for statement in tree.body:
        visit(statement)
    return nodes


def _literal_strings(node: ast.AST) -> set[str]:
    return {
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    }


def _http_schema_top_level_contracts(
    py_file: Path,
    tree: ast.Module,
    project_root: Path,
) -> list[str]:
    violations: list[str] = []
    relative = py_file.relative_to(project_root)

    for node in _module_scope_nodes(tree):
        forbidden_bindings: set[str] = set()
        if isinstance(node, ast.ClassDef):
            forbidden_bindings = {node.name} & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            forbidden_bindings = (
                set().union(*(_bound_names(target) for target in targets))
                & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
            )
        elif isinstance(node, ast.Import):
            bound = {alias.asname or alias.name.split(".")[0] for alias in node.names}
            forbidden_bindings = bound & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        elif isinstance(node, ast.ImportFrom):
            imported = {alias.name for alias in node.names}
            bound = {alias.asname or alias.name for alias in node.names}
            forbidden_bindings = (imported | bound) & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES

        if forbidden_bindings:
            violations.append(
                f"{relative}:{node.lineno} binds forbidden HTTP job contracts "
                f"{sorted(forbidden_bindings)}"
            )

        forbidden_exports: set[str] = set()
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if "__all__" in set().union(*(_bound_names(target) for target in targets)):
                value = node.value
                if value is not None:
                    forbidden_exports = (
                        _literal_strings(value) & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
                    )
        elif (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and isinstance(node.value.func.value, ast.Name)
            and node.value.func.value.id == "__all__"
            and node.value.func.attr in {"append", "extend"}
            and node.value.args
        ):
            forbidden_exports = (
                _literal_strings(node.value.args[0])
                & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
            )
        if forbidden_exports:
            violations.append(
                f"{relative}:{node.lineno} exports forbidden HTTP job contracts "
                f"{sorted(forbidden_exports)} via __all__"
            )

    return violations


def forbidden_http_job_contract_references(
    *roots: Path,
    project_root: Path,
    resolve_import_from_module: ImportFromResolver,
) -> list[str]:
    """Return forbidden HTTP job-contract references in source and schema ownership."""

    violations: list[str] = []
    schema_root = project_root / "src" / "entrypoints" / "http" / "schemas"
    for root in roots:
        for py_file in root.rglob("*.py"):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            access_visitor = _ContractAccessVisitor(
                py_file,
                tree,
                project_root,
                resolve_import_from_module,
            )
            access_visitor.visit(tree)
            violations.extend(access_visitor.violations)
            if py_file.is_relative_to(schema_root):
                violations.extend(
                    _http_schema_top_level_contracts(py_file, tree, project_root)
                )
    return sorted(violations)
