# Producerの責務

Producerは、データ源からティックデータを取得し、`protocol/v1/`のIFへ変換してGatewayへ送ります。

Producerはデータ源ごとに独立したディレクトリへ置きます。

```text
producers/
  mt5/         MT5向けMQL5 producer
  fake/        決定的なテストproducer
```

## 共通の責務

Producerはsource schemaの値を正しく生成します。

Producerはwire framing、message type、cursor、ACKの規則に従います。

Producerは切断、再接続、再送の状態を管理します。

ProducerはGateway内部のWAL、SQLite、Parquet、R2へ直接アクセスしません。

## Producerの追加

新しいデータ源は`producers/<name>/`に追加します。

共通のtransportを変更せず、データ源固有の値はsource schemaに定義します。

追加時は、schema、golden fixture、conformance case、実行手順を同時に用意します。

データ源のSDKや端末に依存する処理はproducer内に閉じ込め、Gatewayへ漏らしません。
