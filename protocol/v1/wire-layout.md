# Protocol V1のwire layout

**Wire layout**：TCPのstreamから一つのframeを復元するためのバイト配置です。

すべてのmessageは、同じ16 bytesのheaderと末尾4 bytesのCRC32Cを持ちます。

## Envelopeの配置

| offset | width | field | encoding |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | ASCII `TICK`, bytes `54 49 43 4B` |
| 4 | 2 | protocol_version | unsigned 16-bit little-endian, value `1` |
| 6 | 2 | message_type | unsigned 16-bit little-endian |
| 8 | 4 | frame_length | unsigned 32-bit little-endian, total frame bytes |
| 12 | 4 | header_length | unsigned 32-bit little-endian, fixed value `16` |
| 16 | variable | payload | message-specific bytes |
| `frame_length - 4` | 4 | crc32c | CRC32C Castagnoli, unsigned 32-bit little-endian |

`frame_length`はheader、payload、CRCを含みます。

`frame_length`は20以上`MAX_FRAME_BYTES`以下でなければなりません。

`MAX_FRAME_BYTES`は`1_048_576` bytesです。

`header_length`は常に16であり、別の値を受理しません。

payloadの長さは`frame_length - 20`です。

CRC32Cの入力範囲は、offset 0から`frame_length - 5`までです。

CRC32Cの出力は、入力範囲の直後にある4 bytesと比較します。

## Message type

| value | message | direction |
| ---: | --- | --- |
| `1` | HelloV1 | producer to Gateway |
| `2` | ResumeV1 | Gateway to producer |
| `3` | BatchFrameV1 | producer to Gateway |
| `4` | AckV1 | Gateway to producer |
| `5` | ErrorV1 | either direction |

値0と6以上は未知messageです。

未知messageはpayloadを解釈せず、`UNKNOWN_MESSAGE_TYPE`として拒否します。

## 共通の上限

**`MAX_RECORDS`**：BatchFrameV1が含められるRawMqlTickV1の最大数で、`4096`です。

**`MAX_STRING_BYTES`**：UTF-8 stringの最大長で、length prefixを除いて`255` bytesです。

**`MAX_BYTES_FIELD`**：length-prefixed bytes fieldの最大長で、`1_048_556` bytesです。

BatchFrameV1の固定payloadとrecord bytesが上限を超える場合は、frameを作成せず拒否します。

## 受理順序と失敗

decoderは、headerを20 bytesまで読み、frame_lengthを確認してから残りのbytesを読みます。

frame_lengthが20未満、MAX_FRAME_BYTES超過、header_length不一致、magic不一致の場合はpayloadを割り当てず拒否します。

protocol_versionが1以外の場合は`UNSUPPORTED_PROTOCOL_VERSION`として拒否します。

frameが短い場合は`TRUNCATED_FRAME`として扱い、部分payloadを返しません。

CRC32Cが一致しない場合は`CRC_MISMATCH`として扱います。

検証に失敗したframeはWAL、hash、ACKの入力にしません。

## Source payloadとの境界

wire envelopeはデータ源に依存しません。

MT5のpayloadは`mt5.mqltick.v1`としてBatchFrameV1のpayloadへ入ります。

共通wireや共通Batchへ`CopyTicks`、`RawMqlTickV1`、MT5固有のcursorを追加しません。
