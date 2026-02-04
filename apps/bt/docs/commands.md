# CLIコマンドリファレンス

このプロジェクトのCLIは `bt` / `portfolio` の2系統です。
実行は `uv run` 経由を推奨します。

## bt（バックテスト）

```bash
# 戦略一覧
uv run bt list

# バックテスト実行
uv run bt backtest range_break_v5

# パラメータ最適化付きバックテスト
uv run bt backtest range_break_v6 --optimize

# 設定検証
uv run bt validate range_break_v5

# 古い結果のクリーンアップ
uv run bt cleanup --days 7

# APIサーバー起動
uv run bt server
uv run bt server --port 3002 --reload
```

## portfolio（ポートフォリオ分析）

```bash
uv run portfolio --help
```

## 出力先（XDG準拠）

バックテスト結果はXDG Base Directory仕様に基づき、デフォルトで以下に保存されます。

```
~/.local/share/trading25/backtest/results/{strategy_name}/
```

以下の環境変数でベースパスを変更できます。

- `TRADING25_DATA_DIR`
- `TRADING25_BACKTEST_DIR`
- `TRADING25_STRATEGIES_DIR`

## ヘルプ

各コマンドの詳細は `--help` を参照してください。

```bash
uv run bt --help
uv run portfolio --help
```
