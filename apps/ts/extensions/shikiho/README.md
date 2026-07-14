# Trading25 Shikiho Bridge

Company Shikiho Online の認証済み銘柄ページから、画面に表示済みの許可項目だけを自動取得し、Trading25 の Symbol Workbench に表示するローカル専用 Chrome 拡張機能です。

## ビルドと Chrome へのインストール

リポジトリルートから拡張機能をビルドします。

```bash
bun run --filter @trading25/shikiho-extension build
```

Chrome で `chrome://extensions` を開き、次の手順で読み込みます。

1. `デベロッパー モード` を有効にします。
2. `パッケージ化されていない拡張機能を読み込む` を選びます。
3. `apps/ts/extensions/shikiho/dist` を指定します。

インストール後、既に開いていた Trading25 と四季報オンラインのタブを再読み込みします。Chrome の通常プロファイルで Company Shikiho Online にログインし、そのプロファイルで Trading25 を開いてください。

## 使い方

1. Trading25 を `http://localhost:5173` で起動し、`/symbol-workbench?symbol=7203` など対象銘柄の Symbol Workbench を開きます。
2. 銘柄を選ぶと、記事スナップショットは 24 時間、当日株価は 15 分を TTL として判定します。記事が新鮮でも、当日株価が未取得・JST 日付違い・取得から 15 分以上なら、選択中の銘柄だけを inactive な background tab で自動更新します。
3. 同じ銘柄を表示済みの四季報タブがある場合は、その DOM を再取得します。そのタブを遷移・再読み込み・閉じることはありません。表示済みタブが無い場合、拡張機能が作成した inactive tab を取得成功後 3 分間だけ再利用し、生成から 5 分を上限として閉じます。ユーザーがその tab を開いた場合は以後ユーザー所有として扱い、自動遷移・自動終了しません。
4. 24 時間以内でも取り直したい場合は、`Company Shikiho` パネルの `更新` を押して強制更新します。取得中も前回のスナップショットは表示されたままで、認識できた新しい項目は順次表示されます。

四季報オンライン内で別銘柄へ移動した場合も、その銘柄として個別に保存されます。Workbench は URL の選択銘柄と一致するスナップショットだけを表示します。source link から通常タブで四季報ページを確認することもできます。

自動更新は Workbench での銘柄選択時など、選択中銘柄の解決要求に応じて 1 回だけ行います。timer polling、scheduled refresh、複数銘柄の巡回取得は行いません。

## 初回表示が遅い場合の再現と取得診断

四季報ページの初回ナビゲーションを含む遅さを計測するときは、新鮮なスナップショットがなく、同じ銘柄の既存タブも拡張機能が再利用できる所有 tab もない銘柄を Symbol Workbench で開きます。同一銘柄の表示済みタブまたは再利用可能な所有 tab がある場合、その DOM を使うため初回ナビゲーションの再現にはなりません。

`Company Shikiho` パネルの `更新` は、選択銘柄の取得 pipeline と途中表示を再確認するための強制更新です。既存タブの表示済み DOM を再取得する場合があるため、これだけではページの初回表示遅延を再現したことになりません。拡張機能による四季報タブの自動 Reload は行いません。

取得中はパネルのステータスに現在の段階と経過時間が表示されます。`取得診断` を開くと、DevTools を使わずに次を確認できます。

- `Tab探索` / `Tab準備` / `Receiver待ち` / `DOM観測` / `保存`: 1つの background attempt 開始時刻を基準にした pipeline の実測値です。合計時間と各項目の初回認識時刻も同じ基準なので、どの段階が支配的かを比較できます。処理が発生しなかった段階や時計の分解能未満で完了した段階は `0ms` になり得ます。
- `Receiver待ち`: content script が要求を受け取れるまでの時間と送信試行回数です。ここが長ければ、Chrome が content script を実行可能にするまでが主な待ち時間です。
- `responseStart` / `DOM interactive` / `DOMContentLoaded` / `load`: Chrome の Navigation Timing をページの `navigationStart` からの相対時間で示します。既存タブでは capture 開始より前のナビゲーション値になるため、capture の経過時間として直接比較しません。初回ナビゲーションを新しく発生させた取得でこれらが遅ければ、ページ応答または初期ナビゲーションが主因です。
- 各項目の時刻: 銘柄、株価、特色、連結事業、コメントなどを DOM で最初に認識した時刻です。銘柄と株価は記事本文が揃う前でも独立して計測します。必要な項目の時刻が遅ければ、四季報ページ側の項目表示待ちが主因です。
- `DOM観測`: `DOM更新` は mutation batch 数、`有効変化` は取得対象フィールドが実際に変化した回数です。DOM 更新だけが多く有効変化が少なければ、広告や周辺 widget など取得項目と無関係な DOM churn が多い状態です。
- `DOM抽出` / `抽出処理`: sample ごとに1回だけ行う DOM inspection の回数と、全処理時間の合計・最大です。最大または合計が大きければ extractor 自体の処理時間が支配的です。
- `終了理由`: `項目が安定`、`期限到達`、`ログイン要求を確認`、`ページ遷移を検知`、`応答形式エラー`、`取得エラー` のいずれで終了したかを示します。`期限到達` なら、その時点までの項目時刻と各計測値を比較して支配的な待ち段階を判断します。

診断 trace は schema version、銘柄コード、attempt ID、capture mode、開始・更新時刻、段階、outcome、終了理由、document readyState、Navigation Timing、認識済み・未認識フィールド名、各フィールドの初回認識時刻、receiver・DOM mutation・有効変化・sample・抽出の回数と処理時間、pipeline timing を保持します。記事本文、株価値、raw HTML、URL、selector、例外文は含みません。URL と表示銘柄の一致を確認できた後は、特色・連結事業・株価・スコアなど対応項目を1つでも認識すると途中候補として順次表示します。途中候補はメモリ内だけで扱い、正式なスナップショット、TTL、チャート overlay、診断抑制には使用しません。

## 許可範囲とプライバシー

- 四季報側の対象は `https://shikiho.toyokeizai.net/stocks/*` だけです。
- Trading25 側は `http://localhost:5173/*`、`http://127.0.0.1:5173/*` と、Vite preview の同等な `4173` origin だけでブリッジが動作します。manifest は localhost/127.0.0.1 のページへ content script を注入しますが、実行時に port を検証し、それ以外では停止します。
- 権限は `storage` と `alarms` だけです。`alarms` は一時的な拡張機能所有 tab を期限後に終了するためだけに使用します。`cookies` 権限は要求せず、cookie、認証 header、local-storage の認証情報、raw HTML は取得しません。
- 通常タブまたは拡張機能が生成した inactive tab の表示済み DOM だけを読みます。追加の Shikiho `fetch`/XHR、ログイン操作、ページ内の自動クリック、bulk crawl は行いません。
- 正常スナップショット、失敗診断、metadata-only trace は Chrome プロファイルの `chrome.storage.local` にだけ保存されます。途中表示の候補データは保存されません。FastAPI、DuckDB、dataset、`portfolio.db`、remote service へ送信せず、telemetry もありません。
- 銘柄ごとの最新の正常スナップショットと trace を最大 200 銘柄分保持し、上限超過時は取得時刻が最も古い銘柄から削除します。同じ内容は重複保存しません。
- 取得失敗の診断は正常スナップショットと別に保存されるため、新しい失敗が前回の正常データを上書きすることはありません。

個人のローカル利用専用です。取得内容の転載、再配布、remote sync、定期巡回には使用しないでください。

## 変更後の再ビルドと Reload

拡張機能の source、manifest、または contract を変更したら再ビルドします。

```bash
bun run --filter @trading25/shikiho-extension build
```

その後 `chrome://extensions` で `Trading25 Shikiho Bridge` の Reload ボタンを押し、Trading25 と四季報オンラインの対象タブも再読み込みしてください。`dist` を直接編集しないでください。

## 状態表示とトラブルシューティング

### Extension unavailable

Workbench が拡張機能から 1 秒以内に応答を受け取れない状態です。

- `Trading25 Shikiho Bridge` が `chrome://extensions` に読み込まれ、有効になっているか確認します。
- 読み込んだディレクトリが package root ではなく、ビルド済みの `apps/ts/extensions/shikiho/dist` であることを確認します。
- 変更後は拡張機能を Reload し、Workbench タブを再読み込みします。
- Trading25 の URL が `http://localhost:5173` / `http://127.0.0.1:5173`、または preview port `4173` であることを確認します。

### Login required

四季報ページがログイン画面または認証切れを示しています。同じ Chrome プロファイルで四季報オンラインへログインし、Workbench の `更新` を押してください。前回の正常スナップショットがある場合は削除されず、古いデータとして保持されます。

### Page changed

対象銘柄や必須 section を安全に特定できず、現在の DOM を取得しなかった状態です。URL が `/stocks/{4桁コード}` であり、ページ内の銘柄コードと一致することを source link で確認してから `更新` を押してください。解消しない場合は四季報オンラインの DOM 変更に extractor が未対応の可能性があります。前回の正常スナップショットは stale として保持されます。

### Partial capture

必須情報は確認できたものの、一部の任意項目が表示されていない状態です。取得済み section はそのまま表示されます。四季報ページの読み込み完了を待つか、ユーザーが閲覧を許可された必要な section を通常の UI で開いてください。拡張機能が DOM 変化を検知して再取得します。拡張機能自身が隠れた section をクリックすることはありません。

いずれの状態でも、Workbench の source link、取得時刻、edition/update 情報、status、`取得診断` を確認して、表示内容がどのページ・時点のものか判断してください。
