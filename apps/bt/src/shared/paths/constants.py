"""
パス関連の定数定義

XDG Base Directory Specification準拠の環境変数とデフォルト値
"""

from pathlib import Path

# 環境変数名
ENV_DATA_DIR = "TRADING25_DATA_DIR"
ENV_STRATEGIES_DIR = "TRADING25_STRATEGIES_DIR"
ENV_BACKTEST_DIR = "TRADING25_BACKTEST_DIR"

# デフォルトデータディレクトリ（XDG Base Directory Specification準拠）
DEFAULT_DATA_DIR = Path.home() / ".local" / "share" / "trading25"

# 戦略カテゴリ定義
STRATEGY_CATEGORIES = [
    "experimental",  # 実験的戦略（外部保存）
    "production",    # 本番戦略（プロジェクト内）
    "reference",     # リファレンス（プロジェクト内）
    "legacy",        # レガシー（プロジェクト内）
]

# 外部ディレクトリに保存するカテゴリ
EXTERNAL_CATEGORIES = ["experimental"]

# プロジェクト内に残すカテゴリ
PROJECT_CATEGORIES = ["production", "reference", "legacy"]

# デフォルトの検索順序（experimental を最優先）
SEARCH_ORDER = ["experimental", "production", "reference", "legacy"]

# プロジェクト内の設定ディレクトリ
PROJECT_CONFIG_DIR = Path("config")
PROJECT_STRATEGIES_DIR = PROJECT_CONFIG_DIR / "strategies"
PROJECT_OPTIMIZATION_DIR = PROJECT_CONFIG_DIR / "optimization"
