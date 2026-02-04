"""
Shared Route Utilities
"""

from fastapi import HTTPException


def validate_path_param(value: str, param_name: str) -> None:
    """パストラバーサル攻撃を防止するパスパラメータ検証

    Args:
        value: 検証する値
        param_name: パラメータ名（エラーメッセージ用）

    Raises:
        HTTPException: 不正な文字列が含まれる場合
    """
    if ".." in value or "/" in value or "\\" in value or "\0" in value:
        raise HTTPException(
            status_code=400,
            detail=f"不正な{param_name}です",
        )
