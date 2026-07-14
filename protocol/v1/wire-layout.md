# Protocol V1のwire layout

Wire layoutは、TCPで交換するframeのバイト配置を定義します。

実装は、messageの意味だけでなく、ここで定義した順序、幅、長さ、CRCを一致させます。

## Envelope

現在のframeは次の順序を基本形とします。

```text
magic[4]
protocol_version u16
message_type u16
frame_length u32
header_length u32
message-specific bytes
crc32c u32
```

`magic`はframeの開始を識別します。

`protocol_version`はwire契約のversionです。

`message_type`はHello、Resume、Batch、Ack、Errorなどのmessageを識別します。

`frame_length`はframe全体の長さを表します。

`header_length`はmessage-specific bytesの開始位置を表します。

`crc32c`は、Protocol V1で定義した範囲の整合性を検証します。

## M0で固定する項目

各fieldのoffset、整数のbyte order、width、最小値、最大値、最大frame長を固定します。

unknown version、未知message、短いframe、長いframe、CRC不一致の扱いを固定します。

固定した内容はgolden bytesとconformance caseで検証します。

## Source payloadとの境界

wire envelopeはデータ源に依存しません。

MT5のpayloadは`mt5.mqltick.v1`のsource schemaとしてmessage-specific bytesに入ります。

共通wireや共通Batchへ`CopyTicks`、`RawMqlTickV1`、MT5固有のcursorを追加しません。
