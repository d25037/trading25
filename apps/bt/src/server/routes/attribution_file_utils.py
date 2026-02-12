"""
Attribution File Utilities

backtest attribution artifact JSON の一覧・読み込み共通ロジック
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from loguru import logger

ATTRIBUTION_FILENAME_PATTERN = re.compile(r"^attribution_(\d{8})_(\d{6})_(.+)\.json$")
VALID_JSON_FILENAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+\.json$")
VALID_STRATEGY_SEGMENT_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+$")


def parse_attribution_filename(filename: str) -> tuple[str | None, datetime | None]:
    """
    attributionファイル名から job_id と作成日時をパース

    形式: attribution_{YYYYMMDD}_{HHMMSS}_{job_id}.json
    """
    match = ATTRIBUTION_FILENAME_PATTERN.match(filename)
    if not match:
        return None, None

    date_str = match.group(1)
    time_str = match.group(2)
    job_id = match.group(3)
    try:
        created_at = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        return job_id, created_at
    except ValueError:
        return job_id, None


def _ensure_within(base_dir: Path, target: Path, param_name: str) -> None:
    base_resolved = base_dir.resolve()
    target_resolved = target.resolve()
    if target_resolved != base_resolved and base_resolved not in target_resolved.parents:
        raise HTTPException(status_code=400, detail=f"不正な{param_name}です")


def validate_attribution_strategy_param(strategy: str) -> str:
    """
    attribution strategy クエリパラメータを検証

    strategy は "experimental/range_break_v18" のような階層を許可。
    """
    if not strategy:
        raise HTTPException(status_code=400, detail="不正な戦略名です")
    if strategy.startswith("/") or strategy.startswith("\\"):
        raise HTTPException(status_code=400, detail="不正な戦略名です")
    if "\0" in strategy or "\\" in strategy:
        raise HTTPException(status_code=400, detail="不正な戦略名です")

    normalized = strategy.strip("/")
    if not normalized:
        raise HTTPException(status_code=400, detail="不正な戦略名です")

    segments = normalized.split("/")
    for segment in segments:
        if segment in ("", ".", ".."):
            raise HTTPException(status_code=400, detail="不正な戦略名です")
        if not VALID_STRATEGY_SEGMENT_PATTERN.match(segment):
            raise HTTPException(status_code=400, detail="不正な戦略名です")

    return "/".join(segments)


def _resolve_strategy_dir(results_dir: Path, strategy: str) -> Path:
    normalized = validate_attribution_strategy_param(strategy)
    strategy_dir = (results_dir / normalized).resolve()
    _ensure_within(results_dir, strategy_dir, "戦略名")
    return strategy_dir


def validate_attribution_filename(filename: str) -> None:
    """attribution JSON ファイル名を検証"""
    if not filename:
        raise HTTPException(status_code=400, detail="不正なファイル名です")
    if ".." in filename or "/" in filename or "\\" in filename or "\0" in filename:
        raise HTTPException(status_code=400, detail="不正なファイル名です")
    if not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="ファイル名は .json で終わる必要があります")
    if not VALID_JSON_FILENAME_PATTERN.match(filename):
        raise HTTPException(
            status_code=400,
            detail="ファイル名は英数字・アンダースコア・ハイフン・ピリオドのみ使用可能です",
        )


def list_attribution_files_in_dir(
    results_dir: Path,
    strategy: str | None = None,
    limit: int = 100,
) -> tuple[list[dict[str, Any]], int]:
    """
    attribution結果ディレクトリからJSONファイル一覧を取得
    """
    files: list[dict[str, Any]] = []

    if not results_dir.exists():
        return [], 0

    if strategy:
        target_dir = _resolve_strategy_dir(results_dir, strategy)
        if not target_dir.exists():
            return [], 0
        candidates = target_dir.rglob("*.json")
    else:
        candidates = results_dir.rglob("*.json")

    for artifact_file in candidates:
        if not artifact_file.is_file():
            continue

        strategy_name = artifact_file.parent.relative_to(results_dir).as_posix()
        if strategy_name in ("", "."):
            continue

        job_id, created_at = parse_attribution_filename(artifact_file.name)
        if created_at is None:
            mtime = os.path.getmtime(artifact_file)
            created_at = datetime.fromtimestamp(mtime)

        files.append({
            "strategy_name": strategy_name,
            "filename": artifact_file.name,
            "created_at": created_at,
            "size_bytes": artifact_file.stat().st_size,
            "job_id": job_id,
        })

    files.sort(key=lambda item: item["created_at"], reverse=True)
    total = len(files)
    return files[:limit], total


def read_attribution_file(results_dir: Path, strategy: str, filename: str) -> dict[str, Any]:
    """
    attribution JSON ファイルを読み込み
    """
    strategy_dir = _resolve_strategy_dir(results_dir, strategy)
    validate_attribution_filename(filename)

    artifact_path = (strategy_dir / filename).resolve()
    _ensure_within(strategy_dir, artifact_path, "ファイル名")

    if not artifact_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"attributionファイルが見つかりません: {strategy}/{filename}",
        )
    if not artifact_path.is_file() or artifact_path.suffix != ".json":
        raise HTTPException(status_code=400, detail="無効なファイルパス")

    try:
        raw = artifact_path.read_text(encoding="utf-8")
    except OSError as e:
        logger.error(f"attributionファイル読み込みエラー: {e}")
        raise HTTPException(status_code=500, detail=f"ファイル読み込みエラー: {e}") from e

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"attribution JSONデコードエラー: {artifact_path}: {e}")
        raise HTTPException(status_code=500, detail=f"JSONデコードエラー: {e}") from e

    if not isinstance(parsed, dict):
        raise HTTPException(status_code=500, detail="JSONファイル形式が不正です")

    return parsed
