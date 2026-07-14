# Source schema

**Source schema**：データ源固有のpayloadと取得状態を定義する識別された形式です。

共通transportはsource schemaのfield意味を解釈しません。

## mt5.mqltick.v1

`source_schema_id`の値`mt5.mqltick.v1`は、BatchFrameV1のrecordsが次の固定record形式であることを示します。

RawMqlTickV1のfieldは次の順序で、すべてlittle-endianで連結します。

| field | type | unit or meaning |
| --- | --- | --- |
| time | I64 | signed Unix seconds |
| bid_bits | U64 | IEEE-754 binary64 bit pattern |
| ask_bits | U64 | IEEE-754 binary64 bit pattern |
| last_bits | U64 | IEEE-754 binary64 bit pattern |
| volume | U64 | unsigned source volume |
| time_msc | I64 | signed Unix milliseconds |
| flags | U32 | MqlTick flags bitset |
| volume_real_bits | U64 | IEEE-754 binary64 bit pattern |
| capture_sequence | U64 | producer-session-scoped ordinal |

RawMqlTickV1のrecord widthは68 bytesです。

価格とvolume_realは数値へ変換せず、IEEE-754 binary64のbit patternを保持します。

`capture_sequence`はproducer session内だけで有効であり、source event IDとして扱いません。

## Batchのsource状態

BatchFrameV1の`copy_ticks_error`は、CopyTicksの直後に一度だけ取得したsource error codeです。

`returned_count`はCopyTicksの戻り値をI32で保持します。

`record_count`は実際にencodingしたRawMqlTickV1の数です。

`source_status_flags`は次のbitを使います。

| bit | name | meaning |
| ---: | --- | --- |
| `0x00000001` | COPY_TICKS_ERROR_NONZERO | copy_ticks_errorが0ではない |
| `0x00000002` | SHORT_RESPONSE | returned_countがrequested_count未満 |
| `0x00000004` | EMPTY_RESPONSE | record_countが0 |
| `0x00000008` | DENSE_BOUNDARY_CANDIDATE | 境界解決の再取得が必要 |

MQL5 producerはCopyTicksの直前にResetLastErrorを呼び、直後にGetLastErrorを一度だけ呼びます。

copy_ticks_errorが0でないBatchはraw evidenceとして有効ですが、cursor advanceの根拠にはなりません。

timestamp regression、crossed quote、zero price、同値repeatをproducerが除去または補正してはなりません。

## Schemaの追加

新しいproducerは`<source>.<format>.v<version>`形式のsource_schema_idを追加します。

既存schemaの意味を変更するときはversionを更新し、既存fixtureとの非互換性を記録します。

新しいschemaにはpayload例、canonical JSON、golden bytes、hash、conformance caseを同時に追加します。
