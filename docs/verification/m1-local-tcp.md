# M1 Local TCP captureとdurable ACKの検証記録

実施日は2026-07-15です。

対象は、1つのproducer instance、1つのcampaign、1つのbroker feed、1つのexact symbolを固定したGo GatewayとMT5 Serviceのlocalhost TCP経路です。

R2、Parquet、HTTP delivery、local pruning、複数producer、24時間soakは対象外です。

## Gateway runtime

`internal/ingest`は127.0.0.1のTCP listener、bounded frame reader、Hello/Resume handshake、session lease、Batch/Ack処理、cursor directive、status metricsを提供します。

`internal/wal`は`protocol/v1/wal-layout.md`のactive WALへ受信済みBatchFrameV1を先にappendし、file syncを完了します。

`internal/journal`はSQLiteへbatch inventoryとcursor stateをtransactionで保存します。

GatewayはWAL syncとSQLite commitの後に限りAckV1を送信します。

SQLiteを削除した場合はactive WALをscanし、同じbatch inventory、cursor、boundary digest、chain root相当のlast entry hashを再構築します。

## Go integration test

実行コマンドは次のとおりです。

```powershell
mise exec -- go test ./internal/ingest ./internal/wal ./internal/protocol ./producers/fake
```

終了ステータスは0です。

テストは実TCP listenerを使い、fake producerのHello、Resume、Batch、Ackを送受信しました。

テストはaccepted batch、same-byte duplicate、same identity/different bytes、source error付きshort response、dense boundary continuation、dense hard cap、partial frame、WAL sync後の再送、journal commit後のACK loss、journal deletion後のrebuildを含みます。

WAL unit testはpartial tailを最後のvalid commit markerまで回復し、committed entryのhash mutationではintegrity errorを返すことを確認しました。

## MQL5 compile

compile対象は`producers/mt5/TickCaptureService.mq5`です。

compilerは`C:\Program Files\MetaTrader 5 IC Markets Global\MetaEditor64.exe`です。

生成logの結果は次のとおりです。

```text
Result: 0 errors, 0 warnings, 612 ms elapsed, cpu='X64 Regular'
```

Serviceは`CopyTicks(COPY_TICKS_ALL)`、built-in TCP socket、Hello/Resume、in-flight frame保持、部分read、ACK判定、再接続、再送を実装します。

実brokerへのlive captureはこの検証では実行していません。

## CLI smoke

設定例からGatewayのWALとSQLite journalを初期化し、statusを取得しました。

```powershell
mise exec -- go run ./cmd/tick-gateway init --config local/tick-gateway.toml.example
mise exec -- go run ./cmd/tick-gateway status --config local/tick-gateway.toml.example
```

statusはWAL entry数0、journal batch数0、initial cursor 0、next requested count 128を返しました。

runtime生成物は`.gitignore`対象であり、repositoryへ追加しません。

## 未実施境界

MT5 terminalから実brokerのTickを取得するlive run、MT5 terminal再起動、forced reboot、disk full、slow storage、24時間soakはこのmilestoneの検証記録へ含めません。

M1の実装とsynthetic TCP fault injectionは完了しましたが、live source provenanceの確認はMetaTrader環境で別途実施する必要があります。
