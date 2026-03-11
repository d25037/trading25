"""
HTML File Utilities

backtest/optimize ルート間で共通のHTMLファイル操作ロジック
"""

import base64
import os
import re
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException
from loguru import logger

from src.entrypoints.http.routes.utils import validate_path_param

# HTMLファイル名パターン: {dataset}_{YYYYMMDD}_{HHMMSS}.html
HTML_FILENAME_PATTERN = re.compile(r"^(.+)_(\d{8})_(\d{6})\.html$")

# リネーム時の有効ファイル名パターン（英数字・アンダースコア・ハイフン・ピリオドのみ）
VALID_FILENAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+\.html$")
HTML_BUNDLE_SUFFIXES = (".metrics.json", ".manifest.json", ".report.json")


def _bundle_paths_for_html(html_path: Path) -> list[Path]:
    return [html_path, *[html_path.with_suffix(suffix) for suffix in HTML_BUNDLE_SUFFIXES]]


def _anchor_html_name_for_bundle_path(path: Path) -> str | None:
    if path.suffix == ".html":
        return path.name
    for suffix in HTML_BUNDLE_SUFFIXES:
        if path.name.endswith(suffix):
            return f"{path.name[: -len(suffix)]}.html"
    return None


def _existing_bundle_paths(html_path: Path) -> list[Path]:
    return [path for path in _bundle_paths_for_html(html_path) if path.exists()]


def _primary_bundle_path(html_path: Path, existing_paths: list[Path]) -> Path:
    for path in _bundle_paths_for_html(html_path):
        if path in existing_paths:
            return path
    return existing_paths[0]


def _rollback_renames(renamed_pairs: list[tuple[Path, Path]]) -> None:
    for original_path, renamed_path in reversed(renamed_pairs):
        try:
            if renamed_path.exists() and not original_path.exists():
                renamed_path.rename(original_path)
        except OSError:
            pass


def parse_html_filename(filename: str) -> tuple[str, datetime | None]:
    """
    HTMLファイル名からデータセット名と作成日時をパース

    ファイル名形式: {dataset_name}_{YYYYMMDD}_{HHMMSS}.html

    Returns:
        (dataset_name, created_at or None)
    """
    match = HTML_FILENAME_PATTERN.match(filename)

    if match:
        dataset_name = match.group(1)
        date_str = match.group(2)
        time_str = match.group(3)
        try:
            created_at = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
            return dataset_name, created_at
        except ValueError:
            pass

    # パースできない場合はファイル名をデータセット名として返す
    # datetime.now()ではなくNoneを返し、呼び出し元でmtimeを使用
    return filename.replace(".html", ""), None


def list_html_files_in_dir(
    results_dir: Path,
    strategy: str | None = None,
    limit: int = 100,
) -> tuple[list[dict], int]:
    """
    結果ディレクトリからHTMLファイル一覧を取得

    Args:
        results_dir: 結果ベースディレクトリ
        strategy: 戦略名でフィルタ（オプション）
        limit: 取得件数上限

    Returns:
        (ファイル情報リスト, 総ファイル数)
    """
    if strategy:
        validate_path_param(strategy, "戦略名")

    files: list[dict] = []

    if not results_dir.exists():
        return [], 0

    # 戦略ディレクトリを走査
    strategy_dirs = [results_dir / strategy] if strategy else list(results_dir.iterdir())

    for strategy_dir in strategy_dirs:
        if not strategy_dir.is_dir():
            continue

        strategy_name = strategy_dir.name
        bundle_names: set[str] = set()
        for pattern in ("*.html", "*.metrics.json", "*.manifest.json", "*.report.json"):
            for artifact_path in strategy_dir.glob(pattern):
                anchor_name = _anchor_html_name_for_bundle_path(artifact_path)
                if anchor_name is not None:
                    bundle_names.add(anchor_name)

        for bundle_name in bundle_names:
            html_path = strategy_dir / bundle_name
            existing_paths = _existing_bundle_paths(html_path)
            if not existing_paths:
                continue

            dataset_name, created_at = parse_html_filename(bundle_name)
            if created_at is None:
                primary_path = _primary_bundle_path(html_path, existing_paths)
                mtime = os.path.getmtime(primary_path)
                created_at = datetime.fromtimestamp(mtime)
            primary_path = _primary_bundle_path(html_path, existing_paths)
            files.append({
                "strategy_name": strategy_name,
                "filename": bundle_name,
                "dataset_name": dataset_name,
                "created_at": created_at,
                "size_bytes": primary_path.stat().st_size,
                "html_available": html_path.exists(),
            })

    # 作成日時で降順ソート（新しいファイルが先頭）
    files.sort(key=lambda f: f["created_at"], reverse=True)

    total = len(files)
    files = files[:limit]

    return files, total


def read_html_file(results_dir: Path, strategy: str, filename: str) -> str:
    """
    HTMLファイルをbase64エンコードして読み込み

    Args:
        results_dir: 結果ベースディレクトリ
        strategy: 戦略名
        filename: ファイル名

    Returns:
        base64エンコードされたHTMLコンテンツ

    Raises:
        HTTPException: ファイルが見つからない、無効、読み込みエラー
    """
    validate_path_param(strategy, "戦略名")
    validate_path_param(filename, "ファイル名")

    html_path = results_dir / strategy / filename

    if not html_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"HTMLファイルが見つかりません: {strategy}/{filename}",
        )

    if not html_path.is_file() or html_path.suffix != ".html":
        raise HTTPException(
            status_code=400,
            detail="無効なファイルパス",
        )

    try:
        with open(html_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except OSError as e:
        logger.error(f"HTMLファイル読み込みエラー: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"ファイル読み込みエラー: {e}",
        ) from e


def rename_html_file(
    results_dir: Path,
    strategy: str,
    filename: str,
    new_filename: str,
    log_prefix: str = "",
) -> None:
    """
    HTMLファイルをリネーム

    Args:
        results_dir: 結果ベースディレクトリ
        strategy: 戦略名
        filename: 現在のファイル名
        new_filename: 新しいファイル名
        log_prefix: ログメッセージの接頭辞

    Raises:
        HTTPException: バリデーションエラー、ファイル操作エラー
    """
    validate_path_param(strategy, "戦略名")
    validate_path_param(filename, "ファイル名")

    if not filename.endswith(".html"):
        raise HTTPException(status_code=400, detail="無効なファイルパス")

    if not new_filename.endswith(".html"):
        raise HTTPException(
            status_code=400, detail="ファイル名は .html で終わる必要があります"
        )

    # ファイル名パターン検証（英数字・アンダースコア・ハイフン・ピリオドのみ）
    # この正規表現は ".." や "/" や "\\" も排除するため、パストラバーサル対策を兼ねる
    if not VALID_FILENAME_PATTERN.match(new_filename):
        raise HTTPException(
            status_code=400,
            detail="ファイル名は英数字・アンダースコア・ハイフン・ピリオドのみ使用可能です",
        )

    current_path = results_dir / strategy / filename
    new_path = current_path.parent / new_filename

    # 変更がない場合は何もしない
    if new_path == current_path:
        return

    existing_paths = _existing_bundle_paths(current_path)
    if not existing_paths:
        raise HTTPException(
            status_code=404,
            detail=f"HTMLファイルが見つかりません: {strategy}/{filename}",
        )

    # 既存ファイル上書き防止（POSIX rename()はサイレント上書きするため）
    if _existing_bundle_paths(new_path):
        raise HTTPException(
            status_code=409,
            detail=f"ファイル名が既に存在します: {new_filename}",
        )

    bundle_pairs: list[tuple[Path, Path]] = []
    for source_path in existing_paths:
        if source_path == current_path:
            target_path = new_path
        else:
            matching_suffix = next(
                suffix
                for suffix in HTML_BUNDLE_SUFFIXES
                if source_path.name.endswith(suffix)
            )
            target_path = new_path.with_suffix(matching_suffix)
        bundle_pairs.append((source_path, target_path))

    # リネーム実行
    renamed_pairs: list[tuple[Path, Path]] = []
    try:
        for source_path, target_path in bundle_pairs:
            source_path.rename(target_path)
            renamed_pairs.append((source_path, target_path))
        logger.info(
            f"{log_prefix}HTML成果物リネーム: {strategy}/{filename} -> {new_filename}"
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"HTMLファイルが見つかりません: {strategy}/{filename}",
        )
    except PermissionError as e:
        _rollback_renames(renamed_pairs)
        logger.error(f"ファイルリネーム権限エラー: {e}")
        raise HTTPException(
            status_code=403, detail="ファイルのリネーム権限がありません"
        ) from e
    except OSError as e:
        _rollback_renames(renamed_pairs)
        logger.error(f"ファイルリネームエラー: {e}")
        raise HTTPException(status_code=500, detail=f"リネームエラー: {e}") from e


def delete_html_file(
    results_dir: Path,
    strategy: str,
    filename: str,
    log_prefix: str = "",
) -> None:
    """
    HTMLファイルを削除

    Args:
        results_dir: 結果ベースディレクトリ
        strategy: 戦略名
        filename: ファイル名
        log_prefix: ログメッセージの接頭辞

    Raises:
        HTTPException: ファイル操作エラー
    """
    validate_path_param(strategy, "戦略名")
    validate_path_param(filename, "ファイル名")

    if not filename.endswith(".html"):
        raise HTTPException(status_code=400, detail="無効なファイルパス")

    html_path = results_dir / strategy / filename
    bundle_paths = _existing_bundle_paths(html_path)
    if not bundle_paths:
        raise HTTPException(
            status_code=404,
            detail=f"HTMLファイルが見つかりません: {strategy}/{filename}",
        )

    try:
        for path in bundle_paths:
            path.unlink()
        logger.info(f"{log_prefix}HTML成果物削除: {strategy}/{filename}")
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"HTMLファイルが見つかりません: {strategy}/{filename}",
        )
    except PermissionError as e:
        logger.error(f"ファイル削除権限エラー: {e}")
        raise HTTPException(
            status_code=403, detail="ファイルの削除権限がありません"
        ) from e
    except OSError as e:
        logger.error(f"ファイル削除エラー: {e}")
        raise HTTPException(status_code=500, detail=f"削除エラー: {e}") from e
