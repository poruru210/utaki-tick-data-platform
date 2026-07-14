# Protocol V1のhash domain

Hashは、domain prefixと固定された入力bytesから計算します。

すべてのdigestはSHA-256の32 raw bytesで保持し、公開時はlowercase hexadecimalへ変換します。

## 共通encoding

`LP(value)`は、UTF-8 bytesの長さをU16で書き、その直後にbytesを書きます。

`LP`の長さは0以上255以下です。

`U32`、`U64`、`I64`はlittle-endianです。

`H32`は32 bytesのdigestをそのまま書きます。

domain prefixに続くfieldは、ここに記載した順序で連結します。

## source_payload_fingerprint

**`source_payload_fingerprint`**：source payloadの内容だけを識別するdigestです。

入力bytesは次の順序です。

```text
"tick-data-platform/source-payload/v1\0"
LP("mt5.mqltick.v1")
I64(time)
U64(bid_bits)
U64(ask_bits)
U64(last_bits)
U64(volume)
I64(time_msc)
U32(flags)
U64(volume_real_bits)
```

capture_sequence、producer session、batch sequence、取得時刻は入力に含めません。

## observation_hash

**`observation_hash`**：同じpayloadが別の取得occurrenceで現れたことを区別するdigestです。

入力bytesは次の順序です。

```text
"tick-data-platform/observation/v1\0"
LP(producer_instance_id)
LP(producer_session_id)
U64(batch_sequence)
U32(record_ordinal)
U64(capture_sequence)
H32(source_payload_fingerprint)
```

## gateway_batch_sha256

**`gateway_batch_sha256`**：Gatewayが受理したBatchFrameV1全体を識別するdigestです。

入力bytesは、prefixの直後にCRCを含むBatchFrameV1の全frame bytesを置いたものです。

```text
"tick-data-platform/batch/v1\0" || batch_frame_bytes
```

## wal_entry_hash

**`wal_entry_hash`**：WAL chain内のentry順序とentry内容を識別するdigestです。

入力bytesは次の順序です。

```text
"tick-data-platform/wal-entry/v1\0"
U64(gateway_ingest_sequence)
H32(previous_entry_hash)
I64(receive_wall_s)
U64(receive_monotonic_us)
H32(gateway_batch_sha256)
batch_frame_bytes
```

commit markerとentry checksumはhash入力に含めません。

## boundary_digest

**`boundary_digest`**：Gatewayがcommitted cursorの同一millisecond境界について、観測したordered source payloadのsequenceとmultiplicityを識別するdigestです。

同じcursorへ後続batchを追加する場合は、直前のdigestを`previous_boundary_digest`として入力します。

```text
"tick-data-platform/boundary/v1\0"
I64(committed_cursor_msc)
H32(previous_boundary_digest)
U32(boundary_record_count)
H32(source_payload_fingerprint) repeated in observed order
```

cursorが新しいmillisecondへ進む場合、`previous_boundary_digest`は全zero bytesです。

## Manifest digest

raw-day manifestのdigestは次のbytesから計算します。

```text
"tick-data-platform/raw-day-manifest/v1\0"
canonical_json_bytes
```

replay-day manifestのdigestは次のbytesから計算します。

```text
"tick-data-platform/replay-day-manifest/v1\0"
canonical_json_bytes
```

## Canonical JSON

canonical JSONは、UTF-8、BOMなし、空白なし、改行なしで出力します。

object keyはUTF-8 byte列の昇順で並べ、arrayの順序は入力の順序を保持します。

numberは整数だけを許可し、先頭の0、`+`、指数表記、`-0`を使いません。

stringはJSONの引用規則を使い、制御文字、引用符、backslashをescapeします。

ASCII以外のcode pointはlowercase hexadecimalの`\u`形式へ変換し、補助平面はUTF-16 surrogate pairで表します。

同じ入力から異なるcanonical JSONが生成される場合、その実装はProtocol V1に適合しません。

## 変更規則

domain prefix、field順序、encoding、canonical JSONを変更すると、既存digestとの互換性が失われます。

変更時はwire、schema、manifest、golden fixture、conformance caseを同じ変更単位で更新します。
