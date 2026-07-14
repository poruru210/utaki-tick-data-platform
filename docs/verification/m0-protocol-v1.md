# M0 Protocol V1検証記録

実施日は2026-07-14です。

対象はProtocol V1の契約、golden fixture、Go decoder、独立Python verifier、fake producer、MQL5 encoderです。

TCP実通信、MQL5実機fixture出力、live MT5 collection、R2、Parquet、SQLite journal runtimeは対象外です。

## mise gate

実行コマンドは次のとおりです。

```powershell
mise run check
```

終了ステータスは0です。

fixture verifierは18 fixtureを検証しました。

Python testは13件を成功させました。

Go testは全packageで成功しました。

Go testはWALのcommit marker、entry CRC、batch hash、entry hash、file hash、trailer CRCも検証しました。

Ruff、gofmt、Ruff format、git diff --checkを成功させました。

## MetaEditor compile

compiler pathは次のとおりです。

```text
C:\Program Files\MetaTrader 5 IC Markets Global\MetaEditor64.exe
```

compiler file versionは5.0.0.5836です。

compile対象はproducers/mt5/TickCaptureService.mq5です。

MetaEditor logの結果は次のとおりです。

```text
Result: 0 errors, 0 warnings, 390 ms elapsed, cpu='X64 Regular'
```

Start-Processで取得したMetaEditor launcherの終了ステータスは1です。

MetaEditorはGUI launcherのため、compile判定はlauncher終了ステータスではなく、生成されたlogのResult行で判定しました。

compileで生成したex5とlogはrepositoryへ残していません。

## Fixture coverage

正常系はHello、Resume、Batch、Ack、Errorを含みます。

BatchはRawMqlTickV1の全field、IEEE-754 bit pattern、source payload hash、observation hash、batch hashを検証します。

manifestはraw-day-manifest-v1とreplay-day-manifest-v1のrequired key、JSON type、canonical JSON、専用digestを検証します。

WALはfile header、entry length、batch frame、batch hash、entry hash、commit marker、entry CRC、trailer、file hashを検証します。

wire異常系はtruncation、CRC mutation、unknown version、unknown message、oversized frameを含みます。

stateful scenarioはduplicate retransmission、ACK loss retry、WAL recoveryを含みます。

source境界はshort response with errorとdense boundaryをvalid BatchFrameV1として検証します。

## 未実施

MQL5 encoderの実機fixture出力は実行していません。

したがって、MQL5実行時の出力bytesとGo、Python、fake producerのbytesが一致することは、compiler結果と共通wire規則によって確認した範囲に限定されます。

実機出力のcross-language runtime証明は、M0後のMT5環境検証で実施します。
