"""
Marimo Notebook Executor

Marimoを使用したNotebook実行・HTML出力ラッパー
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from src.domains.backtest.core.artifacts import BacktestArtifactPaths, BacktestArtifactWriter
from src.shared.utils.snapshot_ids import canonicalize_dataset_snapshot_id


class MarimoExecutor:
    """
    Marimo Notebook実行ラッパー

    戦略パラメータを使用してMarimo notebookを実行し、
    静的HTMLとして結果を保存する
    """

    def __init__(self, output_dir: str | None = None):
        """
        実行ラッパーの初期化

        Args:
            output_dir: 出力ディレクトリ（Noneで外部ディレクトリを使用）
        """
        if output_dir is None:
            from src.shared.paths import get_backtest_results_dir

            self.output_dir = get_backtest_results_dir()
        else:
            self._validate_output_directory(output_dir)
            self.output_dir = Path(output_dir)

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _validate_output_directory(self, output_dir: str) -> None:
        """
        出力ディレクトリの安全性を検証

        プロジェクト内と外部データディレクトリ（~/.local/share/trading25）を許可

        Args:
            output_dir: 出力ディレクトリパス

        Raises:
            ValueError: 不正なディレクトリパスの場合
        """
        dangerous_patterns = [
            "../",
            "..\\",
            "/etc",
            "/var/log",
            "/usr",
            "/bin",
            "/sbin",
        ]
        if any(pattern in output_dir for pattern in dangerous_patterns):
            raise ValueError(f"不正な出力ディレクトリパス: {output_dir}")

        try:
            from src.shared.paths import get_data_dir

            output_path = Path(output_dir).expanduser().resolve()
            current_dir = Path.cwd().resolve()
            data_dir = get_data_dir().resolve()

            if "/tmp" in str(output_path) or "tmp" in str(output_path).lower():
                return

            if str(output_path).startswith(str(current_dir)):
                return
            if str(output_path).startswith(str(data_dir)):
                return

            raise ValueError(
                f"出力ディレクトリはプロジェクト内または~/.local/share/trading25に制限されています: {output_dir}"
            )

        except ValueError:
            raise
        except Exception as e:
            if "/tmp" in output_dir or "tmp" in output_dir.lower():
                return
            raise ValueError(f"出力ディレクトリパス検証エラー: {e}")

    def _validate_filename(self, filename: str, extension: str = ".html") -> None:
        """
        ファイル名の安全性を検証

        Args:
            filename: ファイル名
            extension: 期待する拡張子

        Raises:
            ValueError: 不正なファイル名の場合
        """
        pattern = rf"^[a-zA-Z0-9._-]+\{extension}$"
        if not re.match(pattern, filename):
            raise ValueError(
                f"不正なファイル名: {filename} ({extension}ファイルで英数字、アンダースコア、ハイフン、ドットのみ許可)"
            )

        dangerous_patterns = ["..", "/", "\\", "~", ":", "*", "?", '"', "<", ">", "|"]
        if any(pattern in filename for pattern in dangerous_patterns):
            raise ValueError(f"ファイル名に不正な文字が含まれています: {filename}")

        if len(filename) > 100:
            raise ValueError(f"ファイル名が長すぎます: {filename} (最大100文字)")

    def _serialize_params_to_json(self, parameters: dict[str, Any]) -> str:
        """
        複雑なネスト辞書パラメータをJSONファイルとして保存

        Marimo CLI引数は文字列ベースのため、複雑なネスト辞書は
        JSONファイル参照を経由して渡す。

        Args:
            parameters: パラメータ辞書

        Returns:
            保存されたJSONファイルのパス
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        json_filename = f"params_{timestamp}.json"
        json_path = self.output_dir / json_filename

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(parameters, f, ensure_ascii=False, indent=2)

        logger.debug(f"パラメータJSONを保存: {json_path}")
        return str(json_path)

    def execute_notebook(
        self,
        template_path: str,
        parameters: dict[str, Any],
        strategy_name: str | None = None,
        output_filename: str | None = None,
        timeout: int = 600,
        extra_env: dict[str, str] | None = None,
    ) -> Path:
        """
        Marimo notebookを実行してHTML出力

        Args:
            template_path: Marimoテンプレート(.py)のパス
            parameters: 実行パラメータ
            strategy_name: 戦略名
            output_filename: 出力ファイル名（拡張子なし）
            timeout: タイムアウト秒数
            extra_env: marimo subprocess に追加注入する環境変数

        Returns:
            html_path - 出力HTMLのパス
        """
        template_path_obj = Path(template_path)

        if not template_path_obj.exists():
            raise FileNotFoundError(f"テンプレートが見つかりません: {template_path}")

        artifact_paths = self.resolve_output_paths(
            parameters,
            strategy_name=strategy_name,
            output_filename=output_filename,
        )
        html_path = artifact_paths.html_path

        try:
            html_path_resolved = html_path.resolve()
            output_dir_resolved = self.output_dir.resolve()

            if not str(html_path_resolved).startswith(str(output_dir_resolved)):
                raise ValueError(f"出力ファイルパスが出力ディレクトリ外です: {html_path.name}")
        except Exception as e:
            logger.error(f"出力パス検証エラー: {e}")
            raise ValueError(f"不正な出力パス: {html_path.name}")

        parameters_with_meta = dict(parameters)
        execution_meta = parameters_with_meta.get("_execution")
        if not isinstance(execution_meta, dict):
            execution_meta = {}
        parameters_with_meta["_execution"] = {
            **execution_meta,
            "html_path": str(html_path),
        }
        params_json_path = self._serialize_params_to_json(parameters_with_meta)

        try:
            relative_output = (
                str(html_path.relative_to(self.output_dir))
                if html_path.is_absolute()
                else str(html_path)
            )
            logger.info(f"Marimo notebook実行開始: {template_path_obj.name}")
            logger.debug(f"出力先: {relative_output}")
            print(f"  Executing: {template_path_obj.name}")
            print(f"  Output: {html_path}")

            cmd = [
                sys.executable,
                "-m",
                "marimo",
                "export",
                "html",
                "--no-include-code",
                str(template_path_obj),
                "-o",
                str(html_path),
                "--",
                "--params-json",
                params_json_path,
            ]

            logger.debug(f"Marimo実行コマンド: {' '.join(cmd)}")
            print("  Running marimo export...")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                stdin=subprocess.DEVNULL,
                env={**os.environ, **(extra_env or {})},
            )

            logger.debug(
                f"Marimo stdout: {result.stdout[:500] if result.stdout else '(empty)'}"
            )
            logger.debug(
                f"Marimo stderr: {result.stderr[:500] if result.stderr else '(empty)'}"
            )
            logger.debug(f"Marimo return code: {result.returncode}")

            if result.returncode != 0:
                logger.error(f"Marimo実行エラー: {result.stderr}")
                print(f"  Error: {result.stderr[:500]}")
                raise RuntimeError(f"Marimo execution failed: {result.stderr}")

            if not html_path.exists():
                logger.error(f"HTMLファイルが生成されませんでした: {html_path}")
                print(f"  Warning: HTML file not created at {html_path}")
                raise RuntimeError(f"HTML file was not created: {html_path}")

            file_size = html_path.stat().st_size
            logger.info(
                f"HTML出力完了: {relative_output} ({file_size} bytes)"
            )
            print(f"  HTML generated: {html_path} ({file_size} bytes)")

            return html_path

        except subprocess.TimeoutExpired:
            logger.error(f"Marimo実行タイムアウト: {timeout}秒")
            raise RuntimeError(f"Marimo execution timed out after {timeout} seconds")

        finally:
            if Path(params_json_path).exists():
                Path(params_json_path).unlink()
                logger.debug(f"一時JSONファイル削除: {params_json_path}")

    def resolve_output_paths(
        self,
        parameters: dict[str, Any],
        *,
        strategy_name: str | None = None,
        output_filename: str | None = None,
    ) -> BacktestArtifactPaths:
        """Resolve the expected artifact locations before rendering starts."""

        if output_filename is None:
            strategy_dir_path, base_filename = self._generate_output_filename(
                parameters, strategy_name
            )
        else:
            if strategy_name:
                strategy_dir_path = f"backtest/{strategy_name}"
            else:
                strategy_dir_path = ""
            base_filename = output_filename.replace(".html", "")

        strategy_output_dir = self.output_dir / strategy_dir_path
        strategy_output_dir.mkdir(parents=True, exist_ok=True)

        html_filename = f"{base_filename}.html"
        self._validate_filename(html_filename, ".html")
        html_path = strategy_output_dir / html_filename
        return BacktestArtifactWriter.artifact_paths_for_html(html_path)

    def _generate_output_filename(
        self, parameters: dict[str, Any], strategy_name: str | None = None
    ) -> tuple[str, str]:
        """
        出力ファイル名と戦略ディレクトリパスを生成

        Args:
            parameters: 実行パラメータ
            strategy_name: 戦略名（Noneの場合は"unknown"を使用）

        Returns:
            (戦略ディレクトリパス, ベースファイル名) のタプル
        """
        strategy_name = strategy_name or "unknown"
        shared_config = parameters.get("shared_config", {})
        dataset = shared_config.get("dataset", "")
        dataset_name = canonicalize_dataset_snapshot_id(dataset) or "unknown"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        from src.shared.paths import get_backtest_results_dir

        external_dir = get_backtest_results_dir()
        if str(self.output_dir).startswith(str(external_dir)):
            strategy_dir_path = strategy_name
        else:
            strategy_dir_path = f"backtest/{strategy_name}"

        return strategy_dir_path, f"{dataset_name}_{timestamp}"

    def get_execution_summary(self, html_path: Path) -> dict[str, Any]:
        """
        実行結果のサマリーを取得

        Args:
            html_path: 出力HTMLのパス

        Returns:
            実行サマリー
        """
        try:
            summary = {
                "html_path": str(html_path.relative_to(self.output_dir)),
                "file_size": html_path.stat().st_size if html_path.exists() else 0,
                "generated_at": datetime.now().isoformat(),
            }
            return summary
        except Exception as e:
            logger.error(f"実行サマリー取得エラー: {e}")
            return {"error": str(e)}


__all__ = ["MarimoExecutor"]
