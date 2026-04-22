"""
パラメータ最適化結果の可視化HTML生成

最適化結果から静的HTMLを生成します。
"""

import json
import os
import re
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger


def generate_optimization_report(
    results: List[Dict[str, Any]],
    output_path: str,
    strategy_name: str,
    parameter_ranges: Dict[str, Any],
    scoring_weights: Dict[str, float],
    n_combinations: int,
    _skip_path_validation: bool = False,  # テスト用（非公開パラメータ）
) -> str:
    """
    最適化結果から静的HTMLを生成

    Args:
        results: 最適化結果リスト（スコア順ソート済み）
        output_path: 出力HTMLパス
        strategy_name: 戦略名
        parameter_ranges: パラメータ範囲定義
        scoring_weights: スコアリング重み
        n_combinations: 組み合わせ総数

    Returns:
        str: 生成されたHTMLファイルのパス

    実装方式:
        1. 結果データをJSONファイルとして保存
        2. JSON と最適化メタデータから静的HTMLを生成
        3. 一時JSONを削除

    生成される可視化:
        - 複合スコアランキング表（上位20件）
        - scoring weights / parameter ranges
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

    logger.info(f"最適化結果HTML生成開始: {strategy_name}")
    logger.debug(f"出力先: {output_path_obj}")
    logger.debug(f"結果データJSON: {json_path}")

    try:
        html = _build_optimization_html(
            results=results,
            strategy_name=strategy_name,
            parameter_ranges=parameter_ranges,
            scoring_weights=scoring_weights,
            n_combinations=n_combinations,
            json_path=json_path,
        )
        output_path_obj.write_text(html, encoding="utf-8")
        logger.info(f"最適化結果HTML生成完了: {output_path_obj}")
        return str(output_path_obj)

    except Exception as e:
        logger.error(f"最適化結果HTML生成エラー: {e}")
        raise

    finally:
        # HTML生成後はJSONファイル不要なので削除
        if os.path.exists(json_path):
            os.remove(json_path)
            logger.debug(f"一時JSONファイル削除: {json_path}")


def _build_optimization_html(
    *,
    results: List[Dict[str, Any]],
    strategy_name: str,
    parameter_ranges: Dict[str, Any],
    scoring_weights: Dict[str, float],
    n_combinations: int,
    json_path: str,
) -> str:
    generated_at = datetime.now().isoformat(timespec="seconds")
    best_result = results[0] if results else {}
    payload = {
        "results": results,
        "strategy_name": strategy_name,
        "parameter_ranges": parameter_ranges,
        "scoring_weights": scoring_weights,
        "n_combinations": n_combinations,
        "generated_at": generated_at,
    }
    raw_json = json.dumps(payload, ensure_ascii=False, default=str).replace("</", "<\\/")

    return f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Optimization Analysis - {escape(strategy_name)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #667085;
      --border: #d9dee7;
      --accent: #0f766e;
    }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
    }}
    main {{
      width: min(1180px, calc(100vw - 40px));
      margin: 0 auto;
      padding: 28px 0 40px;
    }}
    header {{
      margin-bottom: 20px;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 28px;
      letter-spacing: 0;
    }}
    h2 {{
      margin: 0 0 12px;
      font-size: 18px;
      letter-spacing: 0;
    }}
    .meta {{
      color: var(--muted);
      font-size: 13px;
    }}
    section {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      margin: 14px 0;
      padding: 16px;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
    }}
    .stat {{
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 10px 12px;
      background: #fbfcfd;
    }}
    .label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }}
    .value {{
      font-size: 20px;
      font-weight: 650;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border-bottom: 1px solid var(--border);
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      background: #fbfcfd;
    }}
    code, pre {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
    }}
    pre {{
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      margin: 0;
    }}
    .score {{
      color: var(--accent);
      font-weight: 700;
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Optimization Analysis</h1>
      <div class="meta">Strategy: {escape(strategy_name)} | Generated: {escape(generated_at)}</div>
    </header>
    <section class="stats">
      {_stat_card("Results", len(results))}
      {_stat_card("Combinations", n_combinations)}
      {_stat_card("Best Score", best_result.get("score"))}
      {_stat_card("Temp Data", Path(json_path).name)}
    </section>
    <section>
      <h2>Top Results</h2>
      {_results_table(results)}
    </section>
    <section>
      <h2>Best Parameters</h2>
      <pre>{_pretty_json(best_result.get("params", {}))}</pre>
    </section>
    <section>
      <h2>Scoring Weights</h2>
      {_mapping_table(scoring_weights)}
    </section>
    <section>
      <h2>Parameter Ranges</h2>
      <pre>{_pretty_json(parameter_ranges)}</pre>
    </section>
  </main>
  <script type="application/json" id="optimization-data">{raw_json}</script>
</body>
</html>
"""


def _format_scalar(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.4f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _stat_card(label: str, value: Any) -> str:
    return (
        '<div class="stat">'
        f'<div class="label">{escape(label)}</div>'
        f'<div class="value">{escape(_format_scalar(value))}</div>'
        "</div>"
    )


def _pretty_json(value: Any) -> str:
    return escape(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def _mapping_table(values: Dict[str, Any]) -> str:
    if not values:
        return "<p>No values.</p>"
    rows = "\n".join(
        "<tr>"
        f"<th>{escape(str(key))}</th>"
        f"<td>{escape(_format_scalar(value))}</td>"
        "</tr>"
        for key, value in values.items()
    )
    return f"<table>{rows}</table>"


def _results_table(results: List[Dict[str, Any]]) -> str:
    if not results:
        return "<p>No optimization results.</p>"
    rows = []
    for rank, result in enumerate(results[:20], start=1):
        rows.append(
            "<tr>"
            f"<td>{rank}</td>"
            f'<td class="score">{escape(_format_scalar(result.get("score")))}</td>'
            f"<td><pre>{_pretty_json(result.get('params', {}))}</pre></td>"
            f"<td><pre>{_pretty_json(result.get('metric_values', {}))}</pre></td>"
            f"<td><pre>{_pretty_json(result.get('normalized_metrics', {}))}</pre></td>"
            "</tr>"
        )
    return (
        "<table>"
        "<thead><tr>"
        "<th>Rank</th><th>Score</th><th>Params</th><th>Metrics</th><th>Normalized</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


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
        3. ファイル名の妥当性検証（.html拡張子）
    """
    # 1. パス正規化
    output_path_obj = Path(output_path).resolve()

    # 2. 許可ディレクトリ内かチェック
    from src.shared.paths import get_backtest_results_dir, get_optimization_results_dir

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
    if not re.match(r"^[a-zA-Z0-9._-]+\.html$", filename):
        raise ValueError(f"不正なファイル名です（英数字・記号・.html拡張子のみ許可）: {filename}")

    # 4. パストラバーサル文字列の追加チェック
    dangerous_patterns = ["..", "~", "//"]
    if any(pattern in str(output_path) for pattern in dangerous_patterns):
        raise ValueError(f"不正な文字が含まれています: {output_path}")

    return output_path_obj
