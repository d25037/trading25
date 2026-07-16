# 戦略パラメータ最適化ガイド

## 現行契約

最適化仕様の唯一の実行時SoTは、対象strategy YAMLのトップレベル
`optimization` blockです。最適化エンジン、CLI、API、Web editorは同じ仕様を
読みます。

```yaml
entry_filter_params:
  period_extrema_break:
    enabled: true
    period: 100

optimization:
  description: "breakout period search"
  parameter_ranges:
    entry_filter_params:
      period_extrema_break:
        period: [30, 50, 100, 200]
```

`parameter_ranges`には候補値だけを記載し、signalの有効化や固定値は同じstrategy
YAMLの通常設定に置きます。

## 操作

Webでは `Backtest > Strategies > Optimize` からdraft生成、検証、保存、実行を
行います。保存済み仕様がない場合は `Generate Draft from Strategy` を使います。

CLI実行:

```bash
uv run bt backtest production/example --optimize
```

移行が必要な古い設定は `uv run bt migrate-optimization-specs` で一度だけ
strategy YAMLへ取り込みます。移行後の実行エンジンは外部設定ファイルを探索・
読込しません。

## 検証

保存時と実行時に次を検証します。

- `optimization.parameter_ranges` がmappingであること
- 候補値が空でないこと
- 対象parameter pathがstrategyのsignal構造と整合すること
- 依存制約（long > short、slow > fast、max > min）を満たすこと
- 組み合わせ数とengine policyが実行可能であること

## 実行結果

結果はbest/worst parameter、score、closed trade count、HTML reportを持ちます。
`fast_then_verify`ではvectorbt上位候補をNautilus child backtestで検証し、親jobは
verification完了までrunningを維持します。

実装境界は
[`parameter-optimization-system.md`](parameter-optimization-system.md)を参照して
ください。
