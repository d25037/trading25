"""
Phase 2.5 レイテンシ計測スクリプト

apps/bt/ APIサーバーに対しHTTPリクエストを送信し、インジケーター計算のレイテンシとエラー率を計測する。

使用方法:
  1. apps/bt/ サーバー起動: uv run bt server --port 3002
  2. 計測実行: uv run python scripts/measure_indicator_latency.py

判定基準:
  - P95 レイテンシ < 800ms
  - エラー率 < 1%
"""

from __future__ import annotations

import statistics
import sys
import time

import httpx


BASE_URL = "http://localhost:3002"
STOCK_CODE = "7203"  # Toyota

# 計測シナリオ
SCENARIOS = {
    "4_indicators": {
        "description": "4インジケータ一括 (SMA, RSI, MACD, Bollinger)",
        "iterations": 100,
        "payload": {
            "stock_code": STOCK_CODE,
            "source": "market",
            "timeframe": "daily",
            "indicators": [
                {"type": "sma", "params": {"period": 20}},
                {"type": "rsi", "params": {"period": 14}},
                {"type": "macd", "params": {"fast_period": 12, "slow_period": 26, "signal_period": 9}},
                {"type": "bollinger", "params": {"period": 20, "std_dev": 2.0}},
            ],
        },
    },
    "11_indicators": {
        "description": "全11インジケータ一括",
        "iterations": 10,
        "payload": {
            "stock_code": STOCK_CODE,
            "source": "market",
            "timeframe": "daily",
            "indicators": [
                {"type": "sma", "params": {"period": 20}},
                {"type": "ema", "params": {"period": 20}},
                {"type": "rsi", "params": {"period": 14}},
                {"type": "macd", "params": {"fast_period": 12, "slow_period": 26, "signal_period": 9}},
                {"type": "ppo", "params": {"fast_period": 12, "slow_period": 26, "signal_period": 9}},
                {"type": "bollinger", "params": {"period": 20, "std_dev": 2.0}},
                {"type": "atr", "params": {"period": 14}},
                {"type": "atr_support", "params": {"lookback_period": 20, "atr_multiplier": 2.0}},
                {"type": "nbar_support", "params": {"period": 20}},
                {"type": "volume_comparison", "params": {"short_period": 20, "long_period": 100}},
                {"type": "trading_value_ma", "params": {"period": 20}},
            ],
        },
    },
}


def measure_scenario(
    client: httpx.Client,
    name: str,
    scenario: dict,
) -> dict[str, float | int | str]:
    """1シナリオの計測を実行"""
    description = scenario["description"]
    iterations = scenario["iterations"]
    payload = scenario["payload"]
    url = f"{BASE_URL}/api/indicators/compute"

    latencies: list[float] = []
    errors = 0

    print(f"\n--- {description} ({iterations} iterations) ---")

    for i in range(iterations):
        start = time.perf_counter()
        try:
            resp = client.post(url, json=payload, timeout=10.0)
            elapsed_ms = (time.perf_counter() - start) * 1000
            if resp.status_code == 200:
                latencies.append(elapsed_ms)
            else:
                errors += 1
                if i == 0:
                    print(f"  Error on first request: {resp.status_code} {resp.text[:200]}")
        except httpx.RequestError as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            errors += 1
            if i == 0:
                print(f"  Connection error: {e}")

        # Progress
        if (i + 1) % 25 == 0 or i == 0:
            print(f"  [{i+1}/{iterations}] ", end="")
            if latencies:
                print(f"last={latencies[-1]:.0f}ms")
            else:
                print("no successful requests yet")

    if not latencies:
        return {
            "scenario": name,
            "description": description,
            "total": iterations,
            "errors": errors,
            "error_rate": 100.0,
            "p50": 0,
            "p95": 0,
            "p99": 0,
        }

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95_idx = int(len(latencies) * 0.95)
    p99_idx = int(len(latencies) * 0.99)
    p95 = latencies[min(p95_idx, len(latencies) - 1)]
    p99 = latencies[min(p99_idx, len(latencies) - 1)]
    error_rate = errors / iterations * 100

    return {
        "scenario": name,
        "description": description,
        "total": iterations,
        "errors": errors,
        "error_rate": error_rate,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "mean": statistics.mean(latencies),
        "min": min(latencies),
        "max": max(latencies),
    }


def print_report(results: list[dict]) -> None:
    """計測結果レポートを出力"""
    print("\n" + "=" * 60)
    print("  apps/bt/ Indicator API Latency Report")
    print("=" * 60)

    all_pass = True
    for r in results:
        print(f"\n  [{r['scenario']}] {r['description']}")
        print(f"    Requests: {r['total']} | Errors: {r['errors']} ({r['error_rate']:.1f}%)")
        if r["p50"] > 0:
            print(f"    P50: {r['p50']:.0f}ms | P95: {r['p95']:.0f}ms | P99: {r['p99']:.0f}ms")
            print(f"    Mean: {r['mean']:.0f}ms | Min: {r['min']:.0f}ms | Max: {r['max']:.0f}ms")
        else:
            print("    No successful requests")

        # 判定
        if r["error_rate"] >= 1.0:
            print("    ** FAIL: Error rate >= 1% **")
            all_pass = False
        if r["p95"] >= 800:
            print("    ** FAIL: P95 >= 800ms **")
            all_pass = False

    print("\n" + "-" * 60)
    if all_pass:
        print("  RESULT: PASS - All criteria met")
    else:
        print("  RESULT: FAIL - Some criteria not met")
    print("=" * 60)


def main() -> None:
    # Health check
    try:
        with httpx.Client() as client:
            resp = client.get(f"{BASE_URL}/api/health", timeout=5.0)
            if resp.status_code != 200:
                print(f"Server health check failed: {resp.status_code}")
                sys.exit(1)
    except httpx.RequestError as e:
        print(f"Cannot connect to apps/bt/ server at {BASE_URL}: {e}")
        print("Start the server first: uv run bt server --port 3002")
        sys.exit(1)

    print(f"Connected to apps/bt/ server at {BASE_URL}")

    results: list[dict] = []
    with httpx.Client() as client:
        for name, scenario in SCENARIOS.items():
            result = measure_scenario(client, name, scenario)
            results.append(result)

    print_report(results)


if __name__ == "__main__":
    main()
