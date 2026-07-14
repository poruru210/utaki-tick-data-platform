# Protocol V1のmessage

Protocol V1のmessageは、共通transportの状態とデータ交換の単位を表します。

## 共通message

**`HelloV1`**：producerがprotocol version、source、能力をGatewayへ通知するmessageです。

**`ResumeV1`**：producerが再接続後の送信位置をGatewayへ提示するmessageです。

**`BatchFrameV1`**：source payloadを順序付きのBatchとして運ぶmessageです。

**`AckV1`**：Gatewayが受理した範囲と次に期待するcursorを返すmessageです。

**`ErrorV1`**：Gatewayまたはproducerが処理を継続できない理由を返すmessageです。

message type、frame length、header length、CRCはwire layoutの規則に従います。

## Source schema

共通messageは、source固有の値の意味を定義しません。

MT5の値は`mt5.mqltick.v1`として定義します。

`CopyTicks`、`RawMqlTickV1`、`time_msc`などのMT5固有概念を共通messageへ追加しません。

新しいproducerは、新しいsource schemaとfixtureを追加し、共通messageの意味を変えずに接続します。

## 互換性

必須フィールド、未知message、unknown version、短いframe、CRC不一致の扱いを実装間で一致させます。

互換性を変更する場合は、wire、schema、fixture、conformanceを同じ変更単位で更新します。
