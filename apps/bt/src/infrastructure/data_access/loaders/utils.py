"""
データローダー共通ユーティリティ

各ローダーモジュールで共通して使用する関数群
"""

from src.shared.utils.snapshot_ids import canonicalize_dataset_snapshot_id


def extract_dataset_name(dataset: str) -> str:
    """データセット名を抽出する.

    Args:
        dataset: データセット名 (e.g., "sampleA") または
            legacy 互換パス表現 (e.g., "dataset/sampleA.db")

    Returns:
        str: データセット名 (e.g., "sampleA")
    """
    dataset_name = canonicalize_dataset_snapshot_id(dataset)
    if dataset_name is None:
        raise ValueError(f"Invalid dataset name: {dataset}")
    return dataset_name
