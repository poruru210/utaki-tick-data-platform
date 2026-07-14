# MT5 producerの責務

MT5 producerは、MetaTrader 5のMQL5環境からティックデータを取得し、Go Gatewayへ送ります。

## データ取得

ティックデータは`CopyTicks(COPY_TICKS_ALL)`で取得します。

取得したMQL5固有の値は、`mt5.mqltick.v1`のsource schemaとして送ります。

`time_msc`、flags、volume、買い気配、売り気配などのMQL5固有フィールドは、共通transportのmessageへ直接追加しません。

## 通信

```text
CopyTicks -> mt5.mqltick.v1 -> Protocol V1 frame -> localhost TCP -> Gateway
```

producerはHello、Resume、Batch、Ackの手順に従います。

ACKを受信できない場合は、cursorに基づいて再送します。

`TickCaptureService.mq5`には、`EncodeHelloFrameV1`と`EncodeBatchFrameV1`を実装しています。

MQL5 compilerを使ったコンパイル結果は、このrepositoryの自動検証には含めません。

MetaEditorのあるWindows環境で、MQL5のcompiler buildとともに別途確認します。
接続が切れた場合は再接続し、Gatewayの応答に基づいて送信位置を復元します。

## 責務の境界

MT5 producerは取得、source schemaへの変換、frame生成、接続状態、再送を担当します。

WAL、SQLite、Parquet、R2、catalog、trading executionはGatewayまたは別のサービスの責務です。

MT5 producerの実機確認にはMetaTrader 5のWindows環境が必要です。

接続確認を行う前に、symbol、terminal、localhost TCPのport、時刻設定を確認します。
