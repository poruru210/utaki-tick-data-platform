# MT5 producerの責務

MT5 producerは、MetaTrader 5のMQL5環境からティックデータを取得し、Go Gatewayへ送ります。

**exact symbol**は、broker terminal上で収集対象として固定した一つのsymbol名です。

## データ取得

ティックデータは`CopyTicks(COPY_TICKS_ALL)`で取得します。

`CopyTicks`の戻り値が`-1`の場合はsource errorとして`BatchFrameV1.copy_ticks_error`へ保持し、Gateway側でcursorを進めません。

取得したMQL5固有の値は、`mt5.mqltick.v1`のsource schemaとして送ります。

`time_msc`、flags、volume、買い気配、売り気配などのMQL5固有フィールドは、共通transportのmessageへ直接追加しません。

## 通信

```text
CopyTicks -> mt5.mqltick.v1 -> Protocol V1 frame -> localhost TCP -> Gateway
```

producerは`HelloV1 -> ResumeV1 -> BatchFrameV1 -> AckV1`の手順に従います。

TCPの部分readをframe lengthまで累積してからCRCを検証します。

ACKを受信できない場合は、ACKを受け取っていないin-flight frameを破棄せず、同じbytesを再送します。

processを再起動した場合は新しいproducer sessionでHelloを送り、ResumeV1のcommitted cursorからinclusiveに再取得します。

`TickCaptureService.mq5`は、CopyTicks、built-in TCP socket、Hello/Resume、Batch送信、Ack判定、再接続、再送を一つのService control flowとして実装しています。

ServiceのinputでGateway endpoint、producer identity、source、broker fingerprint、exact symbol、初期cursor、batch上限を固定します。

MQL5 compilerを使ったコンパイル結果は、このrepositoryのGo/Python自動検証には含めません。

MetaEditorのあるWindows環境で、MQL5のcompiler buildとともに別途確認します。
接続が切れた場合は再接続し、Gatewayの応答に基づいて送信位置を復元します。

## 責務の境界

MT5 producerは取得、source schemaへの変換、frame生成、接続状態、再送を担当します。

WAL、SQLite、Parquet、R2、catalog、trading executionはGatewayまたは別のサービスの責務です。

MT5 producerの実機確認にはMetaTrader 5のWindows環境が必要です。

MetaEditorで`TickCaptureService.mq5`をcompileし、生成logの`Result: 0 errors, 0 warnings`を確認します。

接続確認を行う前に、symbol、terminal、localhost TCPのport、時刻設定を確認します。
