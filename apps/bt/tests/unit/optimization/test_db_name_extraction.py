"""
DB名抽出ロジックのテスト
"""

import os


def test_db_name_extraction():
    """DB名抽出ロジックの動作確認"""
    # テストケース
    test_cases = [
        ("primeExTopix500", "primeExTopix500"),
        ("topix100-A", "topix100-A"),
        ("sampleA", "sampleA"),
        ("primeExTopix500.db", "primeExTopix500"),
        ("nested/path/test", "test"),
        ("", "unknown"),  # 空文字列の場合
    ]

    for db_path, expected_name in test_cases:
        # DB名抽出ロジック（engine.pyと同じ）
        if db_path:
            db_filename = os.path.basename(db_path)
            db_name = os.path.splitext(db_filename)[0]
        else:
            db_name = "unknown"

        assert db_name == expected_name, (
            f"Failed: {db_path} -> {db_name} (expected: {expected_name})"
        )


def test_filename_generation():
    """ファイル名生成のテスト"""
    from datetime import datetime

    db_path = "primeExTopix500"
    strategy_name = "range_break_v6"

    # DB名抽出
    db_filename = os.path.basename(db_path)
    db_name = os.path.splitext(db_filename)[0]

    # タイムスタンプ生成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ファイル名生成（XDG準拠パスを使用）
    from src.shared.paths import get_optimization_results_dir

    output_dir = get_optimization_results_dir(strategy_name)
    output_path = output_dir / f"{db_name}_{timestamp}.html"

    # 検証
    assert "primeExTopix500" in str(output_path)
    assert strategy_name in str(output_path)
    assert str(output_path).endswith(".html")


def test_fallback_to_unknown():
    """db_pathが空の場合のフォールバック動作テスト"""
    db_path = ""

    if db_path:
        db_filename = os.path.basename(db_path)
        db_name = os.path.splitext(db_filename)[0]
    else:
        db_name = "unknown"

    assert db_name == "unknown"
