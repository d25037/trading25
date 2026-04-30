---
name: bt-research-workflow
description: apps/bt の research runner / bundle workflow を扱うスキル。研究定義を src/domains に実装し、vectorbt fast path・Nautilus verification・canonical docs で運用するときに使用する。
---

# bt-research-workflow

## When to use

- runner-first research script、bundle、canonical experiment docs を追加・改修するとき。
- analytics research の定義を `src/domains` に残し、再現可能な実行を runner に寄せたいとき。
- notebook runtime に依存せず、runner / bundle / docs を SoT として扱いたいとき。

## Source of Truth

- `apps/bt/scripts/research`
- `apps/bt/scripts/research/common.py`
- `apps/bt/src/domains`
- `apps/bt/src/shared/utils/pit_guard.py`
- `apps/bt/tests/unit`
- `apps/bt/docs/experiments`

## Workflow

1. 変更したい計算ロジックを `apps/bt/src/domains` の既存ドメインに実装する。
2. 追加・変更したロジックの unit test を `apps/bt/tests/unit` に実装する。
3. `apps/bt/scripts/research` に runner script を追加・更新し、`manifest.json + results.duckdb + summary.md` の bundle を保存できるようにする。publication SoT は canonical README の `## Published Readout` とする。`summary.json` は bundle-local structured fallback / compatibility artifact であり、新規 research では原則として個別 module の `_build_published_summary()` を増やさない。
4. snapshot / universe / fundamentals / ranking join は必ず `as_of_date` 基準で切り、`slice_frame_as_of` / `latest_rows_per_group_as_of` / `filter_records_as_of` を優先利用する。
5. research 内の高速 backtest は `vectorbt` adapter を使い、追加の custom execution engine を増やさない。上位候補の authoritative check が必要な場合だけ `Nautilus` verification を使う。
6. 長く残す研究は `apps/bt/docs/experiments/*/*/README.md` の canonical note にし、runner と bundle 出力から辿れるようにする。README の先頭付近に `## Published Readout` を置き、Codex closeout で説明した判断・数値・解釈をチャットだけでなく source md に保存する。`Published Readout` の本文は日本語で書く（runner 名、table 名、metric 名、code path などの識別子は英語のまま残してよい）。`Main Findings` は key takeaway の平文羅列ではなく、原則として `#### 結論` の小見出しと、その直下の根拠 table で構成する。単一の補足だけなら短い prose でもよいが、複数 horizon、複数 bucket、複数 metric が並ぶ根拠は pipe table にする。
7. 結果確認は canonical README の `## Published Readout` と、runner が出力する `summary.md` / `results.duckdb` を使う。`summary.json` は docs readout が未整備の bundle や過去 bundle の fallback としてだけ扱う。

## Guardrails

- notebook runtime を repo の必須導線に戻さない。再現可能な run は runner script と bundle を SoT にする。
- future leak / point-in-time contamination は P0 として扱う。`latest per group` は必ず as-of filtering の後に取る。
- 新しい research pipeline では PIT stability test を追加し、discovery / validation / walk-forward を跨いだ future-derived bucket や summary を使わない。
- execution semantics の会計は `vectorbt` fast path に寄せ、`Nautilus` は verification 用に限定する。
- experiment README には削除済み notebook path や notebook runtime command を戻さない。
- Research closeout は source md への publication を完了条件にする。`## Published Readout` には `Decision` / `Main Findings` / `Interpretation` / `Production Implication` / `Caveats` / `Source Artifacts` を必ず含め、本文は日本語で書く。`Main Findings` は `#### 結論` + 直下の根拠 table を基本形にし、複数 horizon / bucket / metric を prose で横並びにしない。
- Research UI の SoT は `Published Readout` / `readoutSections` のみ。`purpose` / `method` / `resultHeadline` / `resultBullets` / `considerations` など旧 digest フィールドから Research detail の本文を合成しない。
- `summary.json` / `published_summary=` は publication SoT ではない。新規・変更 research code で残す場合は、docs readout がない bundle 単体利用や過去互換などの理由を `# bundle-structured-fallback: ...` コメントで明示する。
- `scripts/check-research-guardrails.py` で runner / bundle / docs surface の退行を検出する。

## Verification

- `uv run pytest <affected tests>`
- `uv run --project apps/bt python apps/bt/scripts/research/<runner>.py --help`
- `python3 scripts/check-research-guardrails.py`
- `python3 scripts/skills/audit_skills.py --strict-legacy`
