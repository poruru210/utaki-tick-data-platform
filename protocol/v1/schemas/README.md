# Source schema

Source schemaは、データ源固有のpayloadと取得状態を定義します。

共通transportとsource schemaを分けることで、新しいproducerを追加しても共通messageの意味を変えずに済みます。

## 識別子

**`source_schema_id`**：payloadの構造と解釈を識別する安定した名前です。

初期のMT5 producerは`mt5.mqltick.v1`を使います。

schema identifierはmanifest、fixture、Batch metadataで同じ値を使います。

## MT5 schemaの範囲

MT5 schemaには、`MqlTick`に由来する価格、volume、flags、`time_msc`などのsource固有フィールドを定義します。

MT5 schemaには、cursor、取得時刻、取得エラーなど、source payloadの解釈に必要な状態も定義します。

MT5固有の型名や`CopyTicks`の呼び出し方法は、共通transportの契約にしません。

## 追加規則

新しいproducerは、`<source>.<format>.v<version>`形式のschema identifierを検討します。

新しいschemaを追加するときは、payload例、canonical JSON、golden bytes、hash、conformance caseを用意します。

既存schemaの意味を変更するときは、versionを更新し、既存fixtureとの互換性を明示します。
