"""Research artifact bundle helpers for analytics studies."""

from __future__ import annotations

import json
import math
import subprocess
from dataclasses import dataclass, fields, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import UnionType
from typing import Any, Iterable, Literal, TypeVar, Union, get_args, get_origin, get_type_hints

import duckdb
import pandas as pd

from src.shared.paths.resolver import get_data_dir

MANIFEST_FILENAME = "manifest.json"
RESULTS_DB_FILENAME = "results.duckdb"
SUMMARY_FILENAME = "summary.md"
PUBLISHED_SUMMARY_FILENAME = "summary.json"
DEFAULT_RESEARCH_ROOT_NAME = "research"
BundleResultT = TypeVar("BundleResultT")


@dataclass(frozen=True)
class ResearchBundleInfo:
    experiment_id: str
    run_id: str
    created_at: str
    git_commit: str | None
    git_dirty: bool
    module: str
    function: str
    params: dict[str, Any]
    db_path: str
    db_fingerprint: dict[str, Any]
    analysis_start_date: str | None
    analysis_end_date: str | None
    output_tables: tuple[str, ...]
    result_metadata: dict[str, Any]
    notes: str | None
    bundle_dir: Path
    manifest_path: Path
    results_db_path: Path
    summary_path: Path
    published_summary_path: Path

    def to_payload(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "run_id": self.run_id,
            "created_at": self.created_at,
            "git_commit": self.git_commit,
            "git_dirty": self.git_dirty,
            "module": self.module,
            "function": self.function,
            "params": _sanitize_json_payload(self.params),
            "db_path": self.db_path,
            "db_fingerprint": _sanitize_json_payload(self.db_fingerprint),
            "analysis_start_date": self.analysis_start_date,
            "analysis_end_date": self.analysis_end_date,
            "output_tables": list(self.output_tables),
            "result_metadata": _sanitize_json_payload(self.result_metadata),
            "notes": self.notes,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any], bundle_dir: Path) -> ResearchBundleInfo:
        return cls(
            experiment_id=str(payload["experiment_id"]),
            run_id=str(payload["run_id"]),
            created_at=str(payload["created_at"]),
            git_commit=payload.get("git_commit"),
            git_dirty=bool(payload.get("git_dirty", False)),
            module=str(payload["module"]),
            function=str(payload["function"]),
            params=dict(payload.get("params", {})),
            db_path=str(payload["db_path"]),
            db_fingerprint=dict(payload.get("db_fingerprint", {})),
            analysis_start_date=payload.get("analysis_start_date"),
            analysis_end_date=payload.get("analysis_end_date"),
            output_tables=tuple(str(name) for name in payload.get("output_tables", [])),
            result_metadata=dict(payload.get("result_metadata", {})),
            notes=payload.get("notes"),
            bundle_dir=bundle_dir,
            manifest_path=bundle_dir / MANIFEST_FILENAME,
            results_db_path=bundle_dir / RESULTS_DB_FILENAME,
            summary_path=bundle_dir / SUMMARY_FILENAME,
            published_summary_path=bundle_dir / PUBLISHED_SUMMARY_FILENAME,
        )


def get_research_root_dir(output_root: str | Path | None = None) -> Path:
    if output_root is not None:
        return Path(output_root).expanduser()
    return get_data_dir() / DEFAULT_RESEARCH_ROOT_NAME


def get_research_experiment_dir(
    experiment_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_root_dir(output_root) / experiment_id


def get_research_bundle_dir(
    experiment_id: str,
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_experiment_dir(experiment_id, output_root=output_root) / run_id


def find_latest_research_bundle_path(
    experiment_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    experiment_dir = get_research_experiment_dir(experiment_id, output_root=output_root)
    if not experiment_dir.exists():
        return None
    candidates = [
        path
        for path in experiment_dir.iterdir()
        if path.is_dir() and (path / MANIFEST_FILENAME).exists()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime_ns, path.name))


def build_research_run_id(
    *,
    now: datetime | None = None,
    git_commit: str | None = None,
) -> str:
    effective_now = now or datetime.now(UTC)
    timestamp = effective_now.astimezone().strftime("%Y%m%d_%H%M%S")
    short_commit = (git_commit or _get_git_commit() or "nogit")[:8]
    return f"{timestamp}_{short_commit}"


def write_research_bundle(
    *,
    experiment_id: str,
    module: str,
    function: str,
    params: dict[str, Any],
    db_path: str,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    result_metadata: dict[str, Any],
    result_tables: dict[str, pd.DataFrame],
    summary_markdown: str,
    published_summary: dict[str, Any] | None = None,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    git_commit = _get_git_commit()
    git_dirty = _is_git_dirty()
    resolved_run_id = run_id or build_research_run_id(git_commit=git_commit)
    resolved_run_id, bundle_dir = _reserve_bundle_dir(
        experiment_id,
        resolved_run_id,
        output_root=output_root,
        explicit_run_id=run_id is not None,
    )

    info = ResearchBundleInfo(
        experiment_id=experiment_id,
        run_id=resolved_run_id,
        created_at=datetime.now(UTC).isoformat(),
        git_commit=git_commit,
        git_dirty=git_dirty,
        module=module,
        function=function,
        params=params,
        db_path=db_path,
        db_fingerprint=_fingerprint_file(Path(db_path).expanduser()),
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        output_tables=tuple(result_tables.keys()),
        result_metadata=result_metadata,
        notes=notes,
        bundle_dir=bundle_dir,
        manifest_path=bundle_dir / MANIFEST_FILENAME,
        results_db_path=bundle_dir / RESULTS_DB_FILENAME,
        summary_path=bundle_dir / SUMMARY_FILENAME,
        published_summary_path=bundle_dir / PUBLISHED_SUMMARY_FILENAME,
    )
    _write_results_db(info.results_db_path, result_tables)
    info.manifest_path.write_text(
        json.dumps(info.to_payload(), ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    info.summary_path.write_text(summary_markdown, encoding="utf-8")
    if published_summary is not None:
        info.published_summary_path.write_text(
            json.dumps(
                _sanitize_json_payload(published_summary),
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            ),
            encoding="utf-8",
        )
    return info


def write_dataclass_research_bundle(
    *,
    experiment_id: str,
    module: str,
    function: str,
    params: dict[str, Any],
    result: Any,
    table_field_names: Iterable[str],
    summary_markdown: str,
    published_summary: dict[str, Any] | None = None,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_dataclass_bundle_payload(
        result,
        table_field_names=table_field_names,
    )
    return write_research_bundle(
        experiment_id=experiment_id,
        module=module,
        function=function,
        params=params,
        db_path=str(result_metadata["db_path"]),
        analysis_start_date=_coerce_optional_str(
            result_metadata.get("analysis_start_date")
        ),
        analysis_end_date=_coerce_optional_str(
            result_metadata.get("analysis_end_date")
        ),
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=summary_markdown,
        published_summary=published_summary,
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_research_bundle_info(bundle_path: str | Path) -> ResearchBundleInfo:
    bundle_dir = _resolve_bundle_dir(bundle_path)
    manifest_path = bundle_dir / MANIFEST_FILENAME
    if not manifest_path.exists():
        raise FileNotFoundError(f"Research bundle manifest was not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return ResearchBundleInfo.from_payload(payload, bundle_dir)


def load_research_bundle_tables(
    bundle_path: str | Path,
    *,
    table_names: Iterable[str] | None = None,
) -> dict[str, pd.DataFrame]:
    info = load_research_bundle_info(bundle_path)
    requested_names = tuple(table_names or info.output_tables)
    tables: dict[str, pd.DataFrame] = {}
    conn = duckdb.connect(str(info.results_db_path), read_only=True)
    try:
        for table_name in requested_names:
            _validate_table_name(table_name)
            tables[table_name] = conn.execute(
                f'SELECT * FROM "{table_name}"'
            ).fetchdf()
    finally:
        conn.close()
    return tables


def load_research_bundle_published_summary(
    bundle_path: str | Path,
) -> dict[str, Any] | None:
    info = load_research_bundle_info(bundle_path)
    if not info.published_summary_path.exists():
        return None
    payload = json.loads(info.published_summary_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(
            "Research bundle published summary must be a JSON object: "
            f"{info.published_summary_path}"
        )
    return payload


def load_dataclass_research_bundle(
    bundle_path: str | Path,
    *,
    result_type: type[BundleResultT],
    table_field_names: Iterable[str],
) -> BundleResultT:
    if not is_dataclass(result_type):
        raise TypeError("result_type must be a dataclass type")

    table_names = tuple(table_field_names)
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path, table_names=table_names)
    metadata = dict(info.result_metadata)
    type_hints = get_type_hints(result_type)
    table_name_set = set(table_names)

    kwargs: dict[str, Any] = {}
    for field in fields(result_type):
        if field.name in table_name_set:
            kwargs[field.name] = tables[field.name]
            continue
        if field.name not in metadata:
            raise KeyError(
                f"Research bundle metadata was missing required field '{field.name}'"
            )
        kwargs[field.name] = _coerce_bundle_metadata_value(
            metadata[field.name],
            type_hints.get(field.name, Any),
        )
    return result_type(**kwargs)


def list_research_bundle_infos(
    *,
    output_root: str | Path | None = None,
) -> tuple[ResearchBundleInfo, ...]:
    research_root = get_research_root_dir(output_root)
    if not research_root.exists():
        return ()

    manifests = sorted(
        research_root.rglob(MANIFEST_FILENAME),
        key=lambda path: (path.stat().st_mtime_ns, str(path)),
        reverse=True,
    )
    return tuple(load_research_bundle_info(path.parent) for path in manifests)


def _resolve_bundle_dir(bundle_path: str | Path) -> Path:
    path = Path(bundle_path).expanduser()
    if path.is_dir():
        return path
    if path.name in {
        MANIFEST_FILENAME,
        RESULTS_DB_FILENAME,
        SUMMARY_FILENAME,
        PUBLISHED_SUMMARY_FILENAME,
    }:
        return path.parent
    raise FileNotFoundError(
        "Research bundle path must point to a bundle directory or one of its "
        f"artifact files. Received: {path}"
    )


def _write_results_db(results_db_path: Path, result_tables: dict[str, pd.DataFrame]) -> None:
    conn = duckdb.connect(str(results_db_path))
    try:
        for table_name, dataframe in result_tables.items():
            _validate_table_name(table_name)
            conn.register("_bundle_frame", dataframe)
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM _bundle_frame')
            conn.unregister("_bundle_frame")
    finally:
        conn.close()


def _validate_table_name(table_name: str) -> None:
    if not table_name.replace("_", "").isalnum():
        raise ValueError(f"Unsupported research bundle table name: {table_name}")


def _reserve_bundle_dir(
    experiment_id: str,
    run_id: str,
    *,
    output_root: str | Path | None,
    explicit_run_id: bool,
) -> tuple[str, Path]:
    candidate_run_id = run_id
    duplicate_index = 2
    while True:
        bundle_dir = get_research_bundle_dir(
            experiment_id,
            candidate_run_id,
            output_root=output_root,
        )
        try:
            bundle_dir.mkdir(parents=True, exist_ok=False)
            return candidate_run_id, bundle_dir
        except FileExistsError:
            if explicit_run_id:
                raise
            candidate_run_id = f"{run_id}_{duplicate_index:02d}"
            duplicate_index += 1


def _fingerprint_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False}
    stat = path.stat()
    return {
        "exists": True,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _sanitize_json_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _split_dataclass_bundle_payload(
    result: Any,
    *,
    table_field_names: Iterable[str],
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    if not is_dataclass(result):
        raise TypeError("result must be a dataclass instance")

    table_name_set = set(table_field_names)
    metadata: dict[str, Any] = {}
    tables: dict[str, pd.DataFrame] = {}

    for field in fields(result):
        value = getattr(result, field.name)
        if field.name in table_name_set:
            if not isinstance(value, pd.DataFrame):
                raise TypeError(
                    "Research bundle table fields must be pandas DataFrames: "
                    f"{field.name}"
                )
            tables[field.name] = value
            continue
        metadata[field.name] = _sanitize_json_payload(value)

    missing_table_names = table_name_set - set(tables)
    if missing_table_names:
        missing = ", ".join(sorted(missing_table_names))
        raise KeyError(f"Research bundle table fields were missing: {missing}")
    if "db_path" not in metadata:
        raise KeyError("Research bundle dataclass result must define db_path")
    return metadata, tables


def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _coerce_bundle_metadata_value(value: Any, annotation: Any) -> Any:
    if annotation is Any:
        return value
    if value is None:
        return None

    origin = get_origin(annotation)
    if origin in (list, tuple):
        args = get_args(annotation)
        if origin is list:
            element_type = args[0] if args else Any
            return [
                _coerce_bundle_metadata_value(element, element_type)
                for element in value
            ]
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(
                _coerce_bundle_metadata_value(element, args[0])
                for element in value
            )
        return tuple(
            _coerce_bundle_metadata_value(element, args[index])
            for index, element in enumerate(value)
        )
    if origin is dict:
        key_type, value_type = get_args(annotation) or (Any, Any)
        return {
            _coerce_bundle_metadata_value(key, key_type): _coerce_bundle_metadata_value(
                nested_value,
                value_type,
            )
            for key, nested_value in dict(value).items()
        }
    if origin in (UnionType, Union):
        non_none_args = [
            option for option in get_args(annotation) if option is not type(None)
        ]
        if len(non_none_args) == 1:
            return _coerce_bundle_metadata_value(value, non_none_args[0])
        return value
    if origin is Literal:
        literal_args = get_args(annotation)
        if not literal_args:
            return value
        base_type = type(literal_args[0])
        return base_type(value)

    if annotation in (str, int, float, bool):
        return annotation(value)
    if annotation is Path:
        return Path(value)
    return value


def _find_repo_root(start: Path) -> Path:
    for current in (start, *start.parents):
        if (current / ".git").exists():
            return current
    raise RuntimeError(f"Repository root not found from {start}")


def _get_git_commit() -> str | None:
    try:
        repo_root = _find_repo_root(Path(__file__).resolve())
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def _is_git_dirty() -> bool:
    try:
        repo_root = _find_repo_root(Path(__file__).resolve())
        result = subprocess.run(
            ["git", "status", "--short"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
        return bool(result.stdout.strip())
    except Exception:
        return False
