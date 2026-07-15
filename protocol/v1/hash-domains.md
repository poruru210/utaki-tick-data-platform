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

raw-day manifestのcanonical bytesにはday-selected `objects`とchain-complete `chain_objects`の両方を含めます。

manifest digest自身と外部のpublication receiptはmanifest canonical bytesへ含めません。

## raw_set_root

`raw_set_root`は、manifestが選択したraw WAL objectのcontent hashとinclusive coordinate rangeを識別するdigestです。

object keyは入力に含めず、異なるkeyで同じcontent hashとrangeを参照しても同じrootになります。

入力bytesは次の順序です。

```text
"tick-data-platform/raw-set/v1\0"
U32(element_count)
H32(object_sha256)
U64(object_bytes)
U64(start_ingest_sequence)
U64(end_ingest_sequence)
U32(first_record_ordinal)
U32(last_record_ordinal)
repeated for each ordered range
```

各rangeは`(start_ingest_sequence, first_record_ordinal)`から`(end_ingest_sequence, last_record_ordinal)`までのinclusive coordinateです。

rangeはemptyでなく、manifest内で厳密昇順かつnon-overlapでなければなりません。

## raw WAL object key

raw WAL objectのcampaign-relative keyは次のASCII文字列です。

```text
objects/raw/wal-<64 lowercase sha256>.rtw
```

`<64 lowercase sha256>`はsealed WAL object全体のSHA-256です。

object keyは`raw_set_root`には含めませんが、manifestの各rangeと`chain_objects`ではcontent hashと一対一に一致しなければなりません。

## Canonical JSON

`canonical-json-v1`は、UTF-8、BOMなし、空白なし、末尾改行なしで出力します。

object keyはUTF-8 byte列の昇順で並べ、arrayの順序は入力の順序を保持します。

numberは整数だけを許可し、Protocol V1のsignedまたはunsigned 64-bit rangeに収まるdecimal表記だけを使います。

stringはJSONの引用規則を使い、制御文字、引用符、backslashをescapeします。

ASCII以外のcode pointはlowercase hexadecimalの`\u`形式へ変換し、補助平面はUTF-16 surrogate pairで表します。

strict decoderはunknown key、duplicate key、float、noncanonical integer、invalid UTF-8、noncanonical bytes、schemaで定義されたrange違反を拒否します。

同じ入力から異なるcanonical JSONが生成される場合、その実装はProtocol V1に適合しません。

canonical JSONのdigestはcanonical bytesに対して計算し、digest自身をcanonical bytesへ再帰的に埋め込みません。

archive configのdigestは次のbytesから計算します。

```text
"tick-data-platform/archive-config/v1\0"
canonical_config_json_bytes
```

## 変更規則

domain prefix、field順序、encoding、canonical JSONを変更すると、既存digestとの互換性が失われます。

変更時はwire、schema、manifest、golden fixture、conformance caseを同じ変更単位で更新します。
