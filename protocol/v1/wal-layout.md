# Protocol V1のGateway WAL layout

**Gateway WAL**：Gatewayが受理したBatchFrameV1を、ACK前後の復旧検証に使う順序付きentryとして記録する形式です。

M0ではWALのbytesとhash入力だけを固定します。

WAL writer、fsync、SQLite journal、rotation、crash recoveryはM1以降の実装です。

## File header

| offset | width | field | encoding |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | ASCII `TWAL`, bytes `54 57 41 4C` |
| 4 | 2 | wal_schema_version | U16, value `1` |
| 6 | 2 | header_length | U16, `30 + gateway_instance_id byte length` |
| 8 | 8 | segment_start_sequence | U64 |
| 16 | 8 | segment_created_wall_s | I64 |
| 24 | 4 | flags | U32, value `0` in v1 |
| 28 | 2 | gateway_instance_id_length | U16 |
| 30 | variable | gateway_instance_id | UTF-8 bytes |

gateway_instance_id_lengthは255以下です。

header_lengthはoffset 0から最初のentry_length直前までのbytesです。

## Entry

entryは次の順序で連結します。

| relative offset | width | field |
| ---: | ---: | --- |
| 0 | 4 | entry_length | total entry bytes |
| 4 | 2 | entry_version | U16, value `1` |
| 6 | 2 | flags | U16, value `0` in v1 |
| 8 | 8 | gateway_ingest_sequence | U64 |
| 16 | 8 | receive_wall_s | I64 |
| 24 | 8 | receive_monotonic_us | U64 |
| 32 | 32 | previous_entry_hash | H32 |
| 64 | 4 | batch_frame_length | U32 |
| 68 | variable | batch_frame_bytes | complete BatchFrameV1 frame |
| 68 + batch_frame_length | 32 | gateway_batch_sha256 | H32 |
| 100 + batch_frame_length | 32 | wal_entry_hash | H32 |
| 132 + batch_frame_length | 4 | commit_marker | U32, value `0x434F4D4D` |
| 136 + batch_frame_length | 4 | entry_crc32c | CRC32C over bytes before entry_crc32c |

entry_lengthは`140 + batch_frame_length`です。

batch_frame_lengthは20以上MAX_FRAME_BYTES以下です。

entryのgateway_batch_sha256とwal_entry_hashはhash-domains.mdの規則で計算します。

commit_markerまたはentry_crc32cが不一致のentryはvalid entryではありません。

## Sealed trailer

sealed segmentの末尾には次のtrailerを一つだけ置きます。

| field | width | encoding |
| --- | ---: | --- |
| magic | 4 | ASCII `TWTR`, bytes `54 57 54 52` |
| trailer_version | 2 | U16, value `1` |
| trailer_length | 2 | U16, value `96` |
| first_sequence | 8 | U64 |
| last_sequence | 8 | U64 |
| entry_count | 4 | U32 |
| chain_root | 32 | H32 |
| file_sha256 | 32 | SHA-256 of complete file before trailer |
| trailer_crc32c | 4 | CRC32C over trailer bytes before this field |

trailerのfile_sha256は、file headerと全valid entryのbytesを対象にします。

active segmentはtrailerを持ちません。

## 検証失敗

entry_lengthが短すぎる、batch frameがwire上限を超える、entry hashが不一致、commit markerが不一致、entry CRCが不一致の場合はentryを受理しません。

WAL layoutの検証失敗は、部分entryをBatchFrameV1やmanifestの入力にしません。

この文書はWAL runtimeの耐久性を保証しません。
