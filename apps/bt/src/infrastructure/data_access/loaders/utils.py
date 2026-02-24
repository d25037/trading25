"""
データローダー共通ユーティリティ

各ローダーモジュールで共通して使用する関数群
"""

from pathlib import Path


def extract_dataset_name(dataset: str) -> str:
    """データセット名を抽出する.

    Args:
        dataset: データセット名 (e.g., "sampleA") または旧形式パス (e.g., "dataset/sampleA.db")

    Returns:
        str: データセット名 (e.g., "sampleA")
    """
    path = Path(dataset)
    return path.stem
