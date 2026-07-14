# M1レビュー指摘の修正

このExecPlanはliving documentであり、`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective`を作業中に更新する。

## Purpose / Big Picture

M1 Gatewayがproducerの再接続、WAL障害、終了処理、SQLite再構築を安全に扱えるようにする。

修正後は、期限切れ前後のproducer接続が同時にbatchを受理せず、WAL書き込み後の不確実な状態を再利用せず、内部I/O障害をretryableとしてproducerへ返し、終了中の新規handlerを確実に停止し、SQLiteの論理状態をWALと照合できる。

## Progress

- [x] (2026-07-15 01:00 UTC+8) レビュー指摘5件を実装コード、M1契約、既存テストと照合した。
- [x] (2026-07-15 01:05 UTC+8) session lease、WAL failure、ErrorV1分類、shutdown、journal validationを修正対象と決定した。
- [x] (2026-07-15 01:45 UTC+8) session generationを導入し、期限切れ旧接続を拒否する回帰テストを追加した。
- [x] (2026-07-15 01:50 UTC+8) WAL failure後のStore poison、retryable ErrorV1、shutdown、journal再構築の回帰テストを追加した。
- [x] (2026-07-15 01:58 UTC+8) `mise run check`、`go vet ./...`、`go test -count=1 ./...`を実行した。
- [x] (2026-07-15 01:58 UTC+8) race testはCGO無効のため実行できないことを記録した。
- [x] (2026-07-15 01:58 UTC+8) 修正結果と残存リスクを`Outcomes & Retrospective`へ記録した。

## Surprises & Discoveries

- Observation: `session_lease_timeout_ms`の既定値は30秒で、`heartbeat_idle_timeout_ms`は60秒である。
  Evidence: `internal/ingest/config.go`の既定値と`local/tick-gateway.toml.example`の設定値を確認した。

- Observation: `acceptBatch`は接続開始時のlease文字列だけを検証し、Gatewayが現在所有する接続世代を検証していない。
  Evidence: `internal/ingest/gateway.go`の`startSession`、`acceptBatch`、`touchSession`を照合した。

- Observation: WALのwrite後に`Sync`または`Stat`が失敗しても、Storeは次のappendを受け付ける。
  Evidence: `internal/wal/wal.go`の`Append`が内部状態を更新する前にエラーを返し、Storeを停止状態にしていない。

- Observation: 内部エラーを`ErrorCodeOf`へ渡すと`INVALID_FIELD`になり、MQL5 producerは非retryable応答として停止する。
  Evidence: `internal/protocol/protocol.go`、`internal/ingest/gateway.go`、`producers/mt5/TickCaptureService.mq5`のエラー経路を追跡した。

## Decision Log

- Decision: lease文字列の決定性を維持しつつ、接続ごとのgeneration tokenを追加する。
  Rationale: 同一sessionの再接続では同じin-flight frameを同じleaseで再送する必要があるが、期限切れ旧接続と新接続を区別する必要がある。
  Date/Author: 2026-07-15 / Codex

- Decision: WALのwrite開始後に失敗したStoreをpoisonし、再openまでappendを拒否する。
  Rationale: 書き込み済みbyteの耐久性とfile offsetをメモリ状態から確定できないため、同じsequenceを継続利用するとWAL chainを壊す。
  Date/Author: 2026-07-15 / Codex

- Decision: ProtocolErrorはwrapped errorを含めて元のcodeを保持し、その他のエラーを`INTERNAL_RETRYABLE`へ分類する。
  Rationale: 入力不正と一時的なI/O障害を分け、producerが未ACKのbatchを保持して再接続できるようにする。
  Date/Author: 2026-07-15 / Codex

- Decision: SQLiteのcountとlast hashだけでなく、WALから期待するbatch inventoryとcursor stateを検証し、差分があればjournalを再構築する。
  Rationale: SQLiteはraw truthではなくWALから再生成できるindexであり、cursor directiveだけが破損した場合も誤ったResumeを返してはならない。
  Date/Author: 2026-07-15 / Codex

## Outcomes & Retrospective

5件のレビュー指摘をすべて妥当と判断し、session generation、WAL poison、retryable error分類、shutdown registration、WAL照合付きjournal rebuildを実装した。

回帰テストは期限切れ接続の拒否、旧接続終了後の新lease維持、WAL sync失敗後のappend拒否、cursor state破損の再構築、AcceptとCloseの競合を検証する。

`mise run check`、`mise exec -- go vet ./...`、`mise exec -- go test -count=1 ./...`は成功した。

`mise exec -- go test -race ./internal/ingest ./internal/wal`は、環境のCGOが無効で`-race requires cgo`となるため実行できなかった。

最終実行では`mise run check`、`mise exec -- go vet ./...`、`mise exec -- go test -count=1 ./...`を再実行し、すべて成功した。

残存リスクは、Windows実機での突然の電源断、MetaEditorの実機compile、CGO有効環境でのrace testがこの作業環境では未検証であることである。

## Context and Orientation

このrepositoryはGo Gateway、MQL5 producer、Protocol V1、append-only WAL、SQLite journalを同じmonorepoで管理する。

`internal/ingest/gateway.go`はHello/Resume、session lease、Batch受理、journal commit後のACK、listener lifecycleを実装する。

`internal/wal/wal.go`は受信したBatchFrameV1をactive WALへwriteし、file sync後にメモリ上のentry inventoryを更新する。

`internal/journal/journal.go`はWALから再構築できるbatch inventoryとcursor stateをSQLiteへ保存する。

`internal/protocol/protocol.go`はProtocol V1のエラー分類とwire codecを所有する。

`producers/mt5/TickCaptureService.mq5`はGo Gatewayから受け取ったErrorV1のretryable flagで再接続または停止を選ぶ。

## Plan of Work

`internal/ingest/gateway.go`へ接続世代tokenと終了状態を追加し、session開始、batch受理、lease更新、lease解放を同じgenerationに限定する。

`internal/ingest/config.go`のlease設定は現行の互換性を保ち、接続世代の有効期限を使って期限切れ旧接続だけを拒否する。

`internal/wal/wal.go`へStoreのunusable状態を追加し、write開始後のwrite、sync、stat失敗で状態をpoisonし、以後のappendを再open要求として拒否する。

`internal/protocol/protocol.go`の`ErrorCodeOf`をwrapped ProtocolErrorに対応させ、未知の通常エラーを`ErrInternalRetryable`へ分類する。

`internal/journal/journal.go`のResetがgateway instance IDを設定し、state rowの存在を保証するようにする。

`internal/ingest/gateway.go`のstartup reconcileは各WAL entryを順に再生して期待するbatch rowと最終stateを計算し、SQLiteとの差分があればReset後に再構築する。

`internal/ingest/gateway_test.go`、`internal/wal/wal_test.go`、`internal/protocol/runtime_contract_test.go`へ、各修正が失敗前に検出できる回帰ケースを追加する。

## Concrete Steps

作業ディレクトリは`C:\projects\utaki-tick-data-platform`とする。

最初に対象ファイルを編集し、次のコマンドで整形とテストを実行する。

    mise run check

次に静的検証を実行する。

    mise exec -- go vet ./...

race testはCGO対応のGo環境で次を実行する。

    mise exec -- go test -race ./internal/ingest ./internal/wal

CGOが無効で実行できない場合は、エラーを成果物へ記録し、通常の`go test ./...`結果と区別する。

## Validation and Acceptance

同一sessionの旧接続がlease timeout後にbatchを送ると、Gatewayは`SESSION_LEASE_CONFLICT`を返し、新接続のbatchだけをWALへ追加する。

同一sessionの新接続が旧接続を置き換えた後に旧接続が終了しても、新接続のleaseは解除されない。

WALのwrite後に失敗したStoreは、後続batchを同じsequenceでappendせず、再openまで内部retryable errorを返す。

journalのcursor、boundary digest、next directiveだけを変更してGatewayを再openした場合、WALから期待状態を再構築し、Resumeは変更前の正しい状態を返す。

CloseとAcceptが競合する場合、Close完了後に新規handlerがStoreへアクセスせず、受け付け途中のconnectionを閉じる。

`mise run check`はformat、fixture、Python test、Go testをすべて成功させ、`go vet ./...`も成功する。

## Idempotence and Recovery

ExecPlanの編集とテストは既存のruntime dataを削除せず、各テストは`testing.T.TempDir`を使うため繰り返し実行できる。

修正途中でテストが失敗した場合は、失敗したテストと対象コードを確認してから次の修正へ進み、無関係な既存変更を戻さない。

WALをpoisonしたStoreは同一process内で再利用せず、Gateway processを終了してWALとjournalを再openする。

## Artifacts and Notes

最終的な成果物は、修正済みGoとMQL5コード、回帰テスト、検証ログ、更新済みExecPlanである。

実装では、Protocol V1のwire layoutと既存のdeterministic session lease IDを変更しない。

## Interfaces and Dependencies

session handlerはconnectionごとのgeneration tokenを保持し、`startSession`、`acceptBatch`、`touchSession`、`releaseSession`へ同じtokenを渡す。

WAL Storeはwrite開始後の不確実な状態を表すerrorを返し、Gatewayはそれを`ErrInternalRetryable`としてErrorV1へ変換する。

journal reconcileは`wal.Entry`、`protocol.BatchFrameV1`、`journal.State`、`journal.Batch`を比較し、cursor outcomeは既存の`outcomeForBatch`を使って算出する。

### 変更履歴

2026-07-15に、レビュー指摘5件の再評価結果と修正方針を追加した。

2026-07-15に、実装、回帰テスト、最終検証結果を追記した。
