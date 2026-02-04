# CLIコマンド詳細

## bt コマンド（バックテストシステム）

### bt backtest

バックテスト実行コマンド

#### 通常モード
```bash
# カテゴリ付き（明示的指定）
uv run bt backtest production/buy_and_hold
uv run bt backtest experimental/range_break_v5

# カテゴリ省略（自動推測: experimental → production → reference → legacy）
uv run bt backtest buy_and_hold
uv run bt backtest range_break_v5
```

#### 最適化モード
```bash
# グリッド設定は config/optimization/{strategy_name}_grid.yaml を自動検索
uv run bt backtest range_break_v6 --optimize
uv run bt backtest range_break_v6 -O  # 短縮形

# 詳細ログ出力（最適化モード時のみ有効）
uv run bt backtest range_break_v6 --optimize --verbose
uv run bt backtest range_break_v6 -O -v  # 短縮形
```

#### オプション
- `--optimize` / `-O`: グリッドサーチによるパラメータ最適化モード
- `--verbose` / `-v`: 詳細ログ出力（最適化モード時のみ有効）

### bt list

利用可能戦略一覧表示

```bash
uv run bt list
```

### bt validate

設定検証

```bash
# カテゴリ省略可能
uv run bt validate range_break_v5

# カテゴリ明示も可能
uv run bt validate production/range_break_v5
```

### bt cleanup

古いNotebookクリーンアップ

```bash
# デフォルト: 7日以上前のNotebookを削除
uv run bt cleanup

# カスタム期間指定
uv run bt cleanup --days 14
```

#### オプション
- `--days`: 削除対象の日数（デフォルト: 7）

## portfolio コマンド（ポートフォリオ管理）

**詳細**: `src/cli_portfolio/`配下のサブコマンド実装参照

```bash
uv run portfolio --help
```

## 戦略名自動推測機能

カテゴリ省略時は以下の順で自動探索：
1. `experimental/`
2. `production/`
3. `reference/`
4. `legacy/`

実装箇所: `ConfigLoader._infer_strategy_path()`

## Rich対応の美しいCLI表示

- カラフルなテーブル表示
- プログレスバー
- スピナー
- エラーメッセージの強調表示

## 関連ファイル

- `src/cli_bt/`: btコマンド実装
- `src/cli_portfolio/`: portfolioコマンド実装
- `pyproject.toml`: コマンドエントリーポイント定義
