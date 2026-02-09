"""
パラメータ最適化結果の可視化Notebook自動生成

最適化結果から可視化Notebookを生成します（Marimo実行方式）
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger


def generate_optimization_notebook(
    results: List[Dict[str, Any]],
    output_path: str,
    strategy_name: str,
    parameter_ranges: Dict[str, Any],
    scoring_weights: Dict[str, float],
    n_combinations: int,
    _skip_path_validation: bool = False,  # テスト用（非公開パラメータ）
) -> str:
    """
    最適化結果から可視化Notebookを自動生成（Marimo実行方式）

    Args:
        results: 最適化結果リスト（スコア順ソート済み）
        output_path: 出力Notebookパス
        strategy_name: 戦略名
        parameter_ranges: パラメータ範囲定義
        scoring_weights: スコアリング重み
        n_combinations: 組み合わせ総数

    Returns:
        str: 生成されたHTMLファイルのパス

    実装方式:
        1. 結果データをJSONファイルとして保存
        2. テンプレートnotebookをMarimoで実行
        3. 実行済みHTMLを生成（データはJSONから読み込み）

    生成される可視化:
        - 複合スコアランキング表（上位20件）
        - パラメータ感度分析（2D散布図）
        - 指標別分布図（Sharpe/Calmar/Return）
        - パラメータ相関行列（ヒートマップ）
        - 最適パラメータ詳細表
    """
    # パス検証とセキュリティチェック（テスト時はスキップ可能）
    if _skip_path_validation:
        output_path_obj = Path(output_path).resolve()
    else:
        output_path_obj = _validate_output_path(output_path)

    # 出力ディレクトリ作成
    output_dir = output_path_obj.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. 結果データをJSONファイルとして保存
    json_path = _save_results_as_json(results, str(output_dir))

    # 出力ファイル名を指定して実行
    output_filename = os.path.basename(output_path)

    logger.info(f"最適化結果Notebook生成開始: {strategy_name}")
    logger.debug(f"出力先: {output_dir}/{output_filename}")
    logger.debug(f"結果データJSON: {json_path}")

    # Marimo実行（HTML出力）
    # output_dir は既にstrategy別ディレクトリを含む完全パス
    # （get_optimization_results_dir(strategy_name) で構築済み）
    return _generate_with_marimo(
        json_path=json_path,
        strategy_name=strategy_name,
        output_dir=str(output_dir),
        output_filename=output_filename,
        parameter_ranges=parameter_ranges,
        scoring_weights=scoring_weights,
        n_combinations=n_combinations,
    )


def _generate_with_marimo(
    json_path: str,
    strategy_name: str,
    output_dir: str,
    output_filename: str,
    parameter_ranges: Dict[str, Any],
    scoring_weights: Dict[str, float],
    n_combinations: int,
) -> str:
    """
    Marimoを使用して最適化結果Notebookを生成

    Returns:
        html_path - 生成されたHTMLファイルのパス
    """
    from src.lib.backtest_core.marimo_executor import MarimoExecutor

    template_path = "notebooks/templates/marimo/optimization_analysis.py"

    # パラメータ準備
    parameters = {
        "results_json_path": json_path,
        "strategy_name": strategy_name,
        "parameter_ranges": parameter_ranges,
        "scoring_weights": scoring_weights,
        "n_combinations": n_combinations,
    }

    # output_dir は既にstrategy別ディレクトリを含む完全パスのため、
    # execute_notebook に strategy_name=None を渡してサブディレクトリ作成をスキップ
    executor = MarimoExecutor(output_dir=output_dir)

    try:
        # HTML出力ファイル名
        html_filename = Path(output_filename).stem

        logger.debug(f"Marimoテンプレート: {template_path}")

        html_path = executor.execute_notebook(
            template_path=template_path,
            parameters=parameters,
            strategy_name=None,
            output_filename=html_filename,
        )

        logger.info(f"最適化結果Notebook生成完了 (Marimo): {html_path}")
        return str(html_path)

    except Exception as e:
        logger.error(f"Marimo Notebook生成エラー: {e}")
        raise

    finally:
        # Notebook生成後はJSONファイル不要なので削除
        if os.path.exists(json_path):
            os.remove(json_path)
            logger.debug(f"一時JSONファイル削除: {json_path}")


def _save_results_as_json(results: List[Dict[str, Any]], output_dir: str) -> str:
    """
    最適化結果をJSONファイルとして保存

    Args:
        results: 最適化結果リスト
        output_dir: 出力ディレクトリ

    Returns:
        str: 保存されたJSONファイルのパス
    """
    # portfolioオブジェクトは除外（シリアライズ不可）
    serializable_results = []
    for r in results:
        serializable_results.append(
            {
                "params": r["params"],
                "score": r["score"],
                "metric_values": r["metric_values"],
                "normalized_metrics": r.get("normalized_metrics", {}),
            }
        )

    # JSONファイルパス生成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"data_{timestamp}.json"
    json_path = os.path.join(output_dir, json_filename)

    # JSON保存
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(serializable_results, f, indent=2, ensure_ascii=False)

    logger.debug(f"結果データJSONを保存: {json_path} ({len(serializable_results)} 件)")

    return json_path


def _is_subpath(path: Path, parent: Path) -> bool:
    """pathがparentのサブパスかどうかを判定する"""
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _validate_output_path(output_path: str) -> Path:
    """
    出力パスの安全性を検証（パストラバーサル攻撃対策）

    Args:
        output_path: ユーザー指定の出力パス

    Returns:
        Path: 検証済み出力パス（Pathオブジェクト）

    Raises:
        ValueError: 不正なパスの場合

    セキュリティチェック:
        1. パス正規化とパストラバーサル検出
        2. 許可ディレクトリ内かチェック
        3. ファイル名の妥当性検証（.ipynb/.html拡張子）
    """
    # 1. パス正規化
    output_path_obj = Path(output_path).resolve()

    # 2. 許可ディレクトリ内かチェック
    from src.paths import get_backtest_results_dir, get_optimization_results_dir

    allowed_dirs = [
        get_backtest_results_dir().resolve(),
        get_optimization_results_dir().resolve(),
    ]

    is_allowed = any(
        _is_subpath(output_path_obj, allowed_dir)
        for allowed_dir in allowed_dirs
    )
    if not is_allowed:
        raise ValueError(
            f"出力パスが許可されたディレクトリ外です: {output_path}\n"
            f"許可ディレクトリ: {[str(d) for d in allowed_dirs]}"
        )

    # 3. ファイル名検証
    filename = output_path_obj.name
    if not re.match(r"^[a-zA-Z0-9._-]+\.(ipynb|html)$", filename):
        raise ValueError(f"不正なファイル名です（英数字・記号・.ipynb/.html拡張子のみ許可）: {filename}")

    # 4. パストラバーサル文字列の追加チェック
    dangerous_patterns = ["..", "~", "//"]
    if any(pattern in str(output_path) for pattern in dangerous_patterns):
        raise ValueError(f"不正な文字が含まれています: {output_path}")

    return output_path_obj
