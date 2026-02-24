"""
Loguruベースのログ設定モジュール

戦略とCLI用の統一ログシステムを提供します。
環境変数やフラグによるログレベル制御に対応。
"""

import sys
import os
import re
from typing import Optional
from loguru import logger

from src.shared.config.settings import reload_settings


def sanitize_sensitive_info(message: str) -> str:
    """
    ログメッセージから機密情報を除去

    Args:
        message: 元のログメッセージ

    Returns:
        サニタイズされたログメッセージ
    """
    # ファイルパスの一部をマスク（プロジェクトルート以下のみ表示）
    message = re.sub(r"/Users/[^/]+/[^/]+/", ".../", message)
    message = re.sub(r"C:\\Users\\[^\\]+\\[^\\]+\\", r"...\\", message)

    # 一般的なファイルシステムパスをマスク
    message = re.sub(
        r"/(?:home|root|var|etc|usr|bin|sbin)/[^\s]+", "[SYSTEM_PATH]", message
    )

    # パスワードやキーらしき文字列をマスク
    message = re.sub(
        r"(password|passwd|pwd|key|token|secret)[=:\s]+[^\s]+",
        r"\1=***",
        message,
        flags=re.IGNORECASE,
    )

    # SQLite接続文字列の詳細をマスク
    message = re.sub(r"sqlite:///([^/]+/)*([^/\s]+\.db)", r"sqlite:///***\2", message)

    return message


def setup_logger(
    verbose: bool = False, quiet: bool = False, level_override: Optional[str] = None
) -> None:
    """
    戦略用のlogger設定

    Args:
        verbose: 詳細ログを有効化（DEBUGレベル）
        quiet: エラーログのみ表示（ERRORレベル）
        level_override: ログレベルの直接指定（INFO/DEBUG/WARNING/ERROR）
    """
    # 既存のハンドラーを削除
    logger.remove()

    # ログレベルを決定
    if level_override:
        level = level_override.upper()
    elif quiet:
        level = "ERROR"
    elif verbose:
        level = "DEBUG"
    else:
        # 環境変数から取得、デフォルトはWARNING（本番用）
        level = reload_settings().log_level.upper()

    # セキュアなメッセージフィルター関数
    def secure_message_filter(record):
        """ログレコードの機密情報をサニタイズ"""
        if "message" in record:
            record["message"] = sanitize_sensitive_info(str(record["message"]))
        return True

    # フォーマットを設定
    format_string = (
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    # コンソール出力を設定（セキュリティフィルター付き）
    logger.add(
        sys.stderr,
        level=level,
        format=format_string,
        colorize=True,
        backtrace=True,
        diagnose=True,
        filter=secure_message_filter,
    )

    logger.info(f"Logger initialized - Level: {level}")


def get_logger(name: str = "strategy"):
    """
    戦略用のloggerインスタンスを取得

    Args:
        name: ロガー名（モジュール名など）

    Returns:
        logger: 設定済みのloggerインスタンス
    """
    # loguruは単一のloggerインスタンスを使用
    return logger.bind(name=name)


def setup_quiet_logger():
    """
    Quiet モード用の最小限ログ設定
    ERRORレベル以上のメッセージのみ表示
    """
    setup_logger(quiet=True)


def setup_verbose_logger():
    """
    Verbose モード用の詳細ログ設定
    DEBUGレベル以上のメッセージを全て表示
    """
    setup_logger(verbose=True)


def setup_production_logger():
    """
    本番環境用のログ設定
    INFOレベル以上のメッセージを表示
    """
    setup_logger(verbose=False, quiet=False)


class Logger:
    """
    戦略用のロガークラス

    既存のprintlogパターンと互換性を保ちつつ、
    loguruベースのログシステムを提供します。
    """

    def __init__(self, name: str = "strategy", printlog: bool = True):
        """
        初期化

        Args:
            name: ロガー名
            printlog: ログ出力の有効/無効フラグ（既存コードとの互換性用）
        """
        self.name = name
        self.printlog = printlog

        # printlog設定に基づいてグローバルロガーを設定
        # NOTE: グローバルロガーは一度設定されると全Loggerインスタンスに影響する
        if not printlog:
            # printlog=Falseの場合、ERRORレベル以上のみ出力
            current_level = os.getenv("LOG_LEVEL", "").upper()
            if current_level not in ["ERROR", "CRITICAL"]:
                os.environ["LOG_LEVEL"] = "ERROR"
                # グローバルロガーのハンドラーが存在しない、またはデフォルトの場合のみ再設定
                if len(logger._core.handlers) == 0 or len(logger._core.handlers) == 1:
                    setup_logger(level_override="ERROR")
        else:
            # printlog=Trueの場合、WARNINGレベル以上を出力（本番デフォルト）
            # ただし、既にERRORレベルに設定されている場合は何もしない（execute_strategy_with_configが制御）
            current_level = os.getenv("LOG_LEVEL", "").upper()
            if not current_level or current_level == "NOTSET":
                # 環境変数が設定されていない場合のみデフォルト設定
                os.environ["LOG_LEVEL"] = "WARNING"
                if len(logger._core.handlers) == 0:
                    setup_logger(level_override="WARNING")

        self.logger = get_logger(name)

    def debug(self, message: str) -> None:
        """デバッグメッセージ（printlog=False時は完全抑制）"""
        if self.printlog:
            self.logger.debug(f"[{self.name}] {message}")

    def info(self, message: str) -> None:
        """情報メッセージ（printlog=False時は完全抑制）"""
        if self.printlog:
            self.logger.info(f"[{self.name}] {message}")

    def success(self, message: str) -> None:
        """成功メッセージ（printlog=False時は完全抑制）"""
        if self.printlog:
            self.logger.success(f"[{self.name}] {message}")

    def warning(self, message: str) -> None:
        """警告メッセージ（printlog=False時は完全抑制）"""
        if self.printlog:
            self.logger.warning(f"[{self.name}] {message}")

    def error(self, message: str) -> None:
        """エラーメッセージ（printlogに関わらず常に表示）"""
        self.logger.error(f"[{self.name}] {message}")

    def critical(self, message: str) -> None:
        """重要エラーメッセージ（printlogに関わらず常に表示）"""
        self.logger.critical(f"[{self.name}] {message}")

    # 既存コードとの互換性用のエイリアス
    def log(self, message: str) -> None:
        """汎用ログメッセージ（INFOレベル）"""
        self.info(message)


# グローバルのデフォルトロガー（後方互換性）
default_logger = Logger()


def log_info(message: str) -> None:
    """グローバル情報ログ関数"""
    default_logger.info(message)


def log_debug(message: str) -> None:
    """グローバルデバッグログ関数"""
    default_logger.debug(message)


def log_error(message: str) -> None:
    """グローバルエラーログ関数"""
    default_logger.error(message)


def log_warning(message: str) -> None:
    """グローバル警告ログ関数"""
    default_logger.warning(message)


# 後方互換性のためのエイリアス
VBTLogger = Logger
setup_vbt_logger = setup_logger
get_vbt_logger = get_logger
