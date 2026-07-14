# Fake producerの役割

Fake producerは、実際のブローカーやMT5端末を使わずに、producerとGatewayの接続を検証するための実装です。

## 目的

Fake producerは、同じ入力に対して同じbytesと同じhashを生成します。

Fake producerは、fixtureで定義した正常系と異常系を再現します。

再現対象には、ACK欠落、切断、再接続、重複、短いresponse、範囲境界を含めます。

Fake producerは本番データや実際のbroker接続を必要としません。

## 契約

送信するwireとmessageは`protocol/v1/`に従います。

期待するbytes、hash、ACK結果は`testdata/tickdata/`のgolden fixtureと比較します。

新しいシナリオを追加するときは、fixture、conformance文書、検証テストを同時に更新します。

Fake producerの成功は、MT5 producerの実機接続を保証しません。

MT5固有の取得結果は、別途MT5 producerとMetaTrader 5環境で確認します。
