# Trading25 Shikiho Bridge

Company Shikiho Online の認証済み銘柄ページから、画面に表示済みの許可項目だけを自動取得し、Trading25 の Symbol Workbench に表示するローカル専用 Atlas 拡張機能です。

## ビルドと Atlas へのインストール

リポジトリルートから拡張機能をビルドします。

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension build
```

Atlas で次の順に開きます。

```text
Atlas -> Settings -> Web browsing -> Extensions -> Manage extensions
Enable Developer mode -> Load unpacked
Select apps/ts/packages/shikiho-extension/dist
```

`dist` はリポジトリ内の `apps/ts/packages/shikiho-extension/dist` を選択してください。インストール後、既に開いていた Trading25 と四季報オンラインのタブを再読み込みします。

## 使い方

1. Trading25 を `http://localhost:5173` で起動し、`/symbol-workbench?symbol=7203` など対象銘柄の Symbol Workbench を開きます。
2. Workbench の既存の四季報リンクから、同じ銘柄の Company Shikiho Online ページを開きます。
3. 必要なら四季報オンラインにログインし、ページの表示が落ち着くまで待ちます。
4. 手動の取得ボタンはありません。拡張機能が DOM の変化を監視し、表示が安定した時点で自動取得します。Workbench の `Company Shikiho` パネルも自動更新されます。

四季報オンライン内で別銘柄へ移動した場合も、その銘柄として個別に保存されます。Workbench は URL の選択銘柄と一致するスナップショットだけを表示します。

## 許可範囲とプライバシー

- 四季報側の対象は `https://shikiho.toyokeizai.net/stocks/*` だけです。
- Trading25 側は `http://localhost:5173/*`、`http://127.0.0.1:5173/*` と、Vite preview の同等な `4173` origin だけでブリッジが動作します。manifest は localhost/127.0.0.1 のページへ content script を注入しますが、実行時に port を検証し、それ以外では停止します。
- 権限は `storage` だけで、`cookies` 権限は要求しません。cookie、認証 header、local-storage の認証情報、raw HTML は取得しません。
- 開いているページの表示済み DOM だけを読みます。追加の Shikiho `fetch`/XHR、ログイン操作、隠れたタブのクリック、bulk crawl は行いません。
- スナップショットは Atlas profile の `chrome.storage.local` にだけ保存されます。FastAPI、DuckDB、dataset、`portfolio.db`、remote service へ送信しません。telemetry もありません。
- 銘柄ごとの最新の正常スナップショットを最大 200 銘柄分保持し、上限超過時は取得時刻が最も古い銘柄から削除します。同じ内容は重複保存しません。
- 取得失敗の診断は正常スナップショットと別に保存されるため、新しい失敗が前回の正常データを上書きすることはありません。

個人のローカル利用専用です。取得内容の転載、再配布、remote sync、定期巡回には使用しないでください。

## 変更後の再ビルド

拡張機能の source、manifest、または contract を変更したら再ビルドします。

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension build
```

その後 Atlas の `Manage extensions` で `Trading25 Shikiho Bridge` の Reload を実行し、Trading25 と四季報オンラインの対象タブも再読み込みしてください。`dist` を直接編集しないでください。

## 状態表示とトラブルシューティング

### Extension unavailable

Workbench が拡張機能から 1 秒以内に応答を受け取れない状態です。

- `Trading25 Shikiho Bridge` が Atlas に読み込まれ、有効になっているか確認します。
- 読み込んだディレクトリが package root ではなく、ビルド済みの `apps/ts/packages/shikiho-extension/dist` であることを確認します。
- 変更後は拡張機能を Reload し、Workbench タブを再読み込みします。
- Trading25 の URL が `http://localhost:5173` / `http://127.0.0.1:5173`、または preview port `4173` であることを確認します。

### Login required

四季報ページがログイン画面または認証切れを示しています。Atlas の同じ profile で四季報オンラインへログインし、対象銘柄ページを再読み込みしてください。前回の正常スナップショットがある場合は削除されず、古いデータとして保持されます。

### Page changed

対象銘柄や必須 section を安全に特定できず、現在の DOM を取得しなかった状態です。URL が `/stocks/{4桁コード}` であり、ページ内の銘柄コードと一致することを確認して再読み込みしてください。解消しない場合は四季報オンラインの DOM 変更に extractor が未対応の可能性があります。前回の正常スナップショットは stale として保持されます。

### Partial capture

必須情報は確認できたものの、一部の任意項目が表示されていない状態です。取得済み section はそのまま表示されます。四季報ページの読み込み完了を待つか、ユーザーが閲覧を許可された必要な section を通常の UI で開いてください。拡張機能が DOM 変化を検知して再取得します。拡張機能自身が隠れた section をクリックすることはありません。

いずれの状態でも、Workbench の source link、取得時刻、edition/update 情報、status を確認して、表示内容がどのページ・時点のものか判断してください。
