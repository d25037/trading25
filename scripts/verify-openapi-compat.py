#!/usr/bin/env python3
"""
OpenAPI 互換性検証スクリプト

contracts/hono-openapi-baseline.json と FastAPI /openapi.json を比較し、
Phase 3 移行の互換性を検証する。

使用方法:
    # FastAPI サーバーが起動中の場合
    python3 scripts/verify-openapi-compat.py

    # ファイルから検証（サーバー不要）
    python3 scripts/verify-openapi-compat.py --fastapi-file /path/to/openapi.json
"""

import argparse
import json
import sys
from pathlib import Path

BASELINE_PATH = Path(__file__).parent.parent / "contracts" / "hono-openapi-baseline.json"
FASTAPI_URL = "http://localhost:3002/openapi.json"


def load_baseline() -> dict:
    """Hono baseline を読み込む"""
    with open(BASELINE_PATH) as f:
        return json.load(f)


def load_fastapi(file_path: str | None = None) -> dict:
    """FastAPI OpenAPI スキーマを読み込む"""
    if file_path:
        with open(file_path) as f:
            return json.load(f)

    import urllib.request

    try:
        with urllib.request.urlopen(FASTAPI_URL, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"ERROR: FastAPI サーバーに接続できません ({FASTAPI_URL}): {e}")
        print("       `uv run bt server --port 3002` でサーバーを起動するか、")
        print("       --fastapi-file オプションでファイルを指定してください。")
        sys.exit(1)


def collect_operation_tags(schema: dict) -> set[str]:
    """paths 内の全 operation から使われているタグを収集"""
    tags: set[str] = set()
    for methods in schema.get("paths", {}).values():
        for operation in methods.values():
            if isinstance(operation, dict) and "tags" in operation:
                tags.update(operation["tags"])
    return tags


def verify_operation_tags(baseline: dict, fastapi: dict) -> list[str]:
    """operation tags の互換性を検証"""
    errors: list[str] = []
    baseline_tags = collect_operation_tags(baseline)
    fastapi_top_tags = {t["name"] for t in fastapi.get("tags", [])}

    missing = baseline_tags - fastapi_top_tags
    if missing:
        errors.append(f"Hono operation tags が FastAPI top-level tags に不足: {missing}")

    return errors


def verify_info(baseline: dict, fastapi: dict) -> list[str]:
    """info セクションの互換性を検証"""
    errors: list[str] = []
    bi = baseline.get("info", {})
    fi = fastapi.get("info", {})

    if fi.get("title") != bi.get("title"):
        errors.append(f"info.title 不一致: baseline={bi.get('title')!r}, fastapi={fi.get('title')!r}")

    if "contact" not in fi:
        errors.append("info.contact が未設定")

    if "license" not in fi:
        errors.append("info.license が未設定")

    return errors


def verify_servers(fastapi: dict) -> list[str]:
    """servers に FastAPI エントリがあるか検証"""
    errors: list[str] = []
    urls = [s.get("url", "") for s in fastapi.get("servers", [])]
    if not any(":3002" in u for u in urls):
        errors.append("servers に :3002 エントリが存在しない")
    return errors


def verify_error_response(fastapi: dict) -> list[str]:
    """ErrorResponse スキーマの存在を検証"""
    errors: list[str] = []
    schemas = fastapi.get("components", {}).get("schemas", {})
    if "ErrorResponse" not in schemas:
        errors.append("components/schemas/ErrorResponse が存在しない")
    return errors


def verify_error_responses_in_paths(fastapi: dict) -> list[str]:
    """各エンドポイントに 400/500 エラーレスポンスがあるか検証"""
    errors: list[str] = []
    for path, methods in fastapi.get("paths", {}).items():
        for method, operation in methods.items():
            if not isinstance(operation, dict) or "responses" not in operation:
                continue
            for code in ["400", "500"]:
                if code not in operation["responses"]:
                    errors.append(f"{method.upper()} {path}: {code} レスポンスが未定義")
    return errors


def track_migration_status(baseline: dict, fastapi: dict) -> dict[str, str]:
    """パス単位の移行ステータスを追跡"""
    baseline_paths = set(baseline.get("paths", {}).keys())
    fastapi_paths = set(fastapi.get("paths", {}).keys())

    status: dict[str, str] = {}
    for p in sorted(baseline_paths):
        status[p] = "migrated" if p in fastapi_paths else "pending"
    for p in sorted(fastapi_paths - baseline_paths):
        status[p] = "bt-only"
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description="OpenAPI 互換性検証")
    parser.add_argument("--fastapi-file", help="FastAPI OpenAPI JSON ファイルパス（サーバー不要）")
    args = parser.parse_args()

    print("=" * 60)
    print("OpenAPI 互換性検証")
    print("=" * 60)

    baseline = load_baseline()
    fastapi = load_fastapi(args.fastapi_file)

    all_errors: list[str] = []

    # 1. Operation tags
    print("\n[1] Operation Tags 互換性")
    errs = verify_operation_tags(baseline, fastapi)
    all_errors.extend(errs)
    if errs:
        for e in errs:
            print(f"  FAIL: {e}")
    else:
        baseline_tags = collect_operation_tags(baseline)
        print(f"  OK: Hono {len(baseline_tags)} operation tags が全て定義済み")

    # 2. Info
    print("\n[2] Info セクション")
    errs = verify_info(baseline, fastapi)
    all_errors.extend(errs)
    if errs:
        for e in errs:
            print(f"  FAIL: {e}")
    else:
        print(f"  OK: title={fastapi['info']['title']!r}")

    # 3. Servers
    print("\n[3] Servers")
    errs = verify_servers(fastapi)
    all_errors.extend(errs)
    if errs:
        for e in errs:
            print(f"  FAIL: {e}")
    else:
        print("  OK: :3002 エントリ存在")

    # 4. ErrorResponse
    print("\n[4] ErrorResponse スキーマ")
    errs = verify_error_response(fastapi)
    all_errors.extend(errs)
    if errs:
        for e in errs:
            print(f"  FAIL: {e}")
    else:
        print("  OK: ErrorResponse 定義済み")

    # 5. Error responses in paths
    print("\n[5] エンドポイント別エラーレスポンス")
    errs = verify_error_responses_in_paths(fastapi)
    all_errors.extend(errs)
    if errs:
        print(f"  FAIL: {len(errs)} エンドポイントでエラーレスポンス不足")
        for e in errs[:5]:
            print(f"    - {e}")
        if len(errs) > 5:
            print(f"    ... 他 {len(errs) - 5} 件")
    else:
        paths_count = sum(
            1 for methods in fastapi.get("paths", {}).values() for op in methods.values() if isinstance(op, dict) and "responses" in op
        )
        print(f"  OK: {paths_count} エンドポイント全てに 400/500 定義済み")

    # 6. Migration status
    print("\n[6] パス移行ステータス")
    status = track_migration_status(baseline, fastapi)
    migrated = sum(1 for s in status.values() if s == "migrated")
    pending = sum(1 for s in status.values() if s == "pending")
    bt_only = sum(1 for s in status.values() if s == "bt-only")
    print(f"  移行済み: {migrated}, 未移行: {pending}, bt固有: {bt_only}")

    # Summary
    print("\n" + "=" * 60)
    if all_errors:
        print(f"RESULT: FAIL ({len(all_errors)} errors)")
        sys.exit(1)
    else:
        print("RESULT: PASS")
        sys.exit(0)


if __name__ == "__main__":
    main()
