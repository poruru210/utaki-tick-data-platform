# Protocol V1の契約

`protocol/v1/`は、producerとGatewayが共有するIFの正規仕様です。

**正規仕様**：実装より優先して参照する、固定済みのwire、message、schema、hash、manifestの規則です。

## M0の範囲

M0では、Protocol V1の契約とcross-language conformanceに必要な入力を固定します。

M0の対象は、wire envelope、5種類のmessage、`mt5.mqltick.v1`、Gateway WAL entry、canonical JSON、hash domain、raw-day manifest、replay-day manifestです。

`part-manifest-v1`はParquetのday-local part chainに属するため、M3へ延期します。

TCP runtime、live MT5 collection、R2、Parquet、SQLite journal runtime、crash injection、production operationはM0の対象外です。

## 固定値

wireの固定値は[wire-layout.md](wire-layout.md)に定義します。

messageのfield順序とenum値は[messages.md](messages.md)に定義します。

MT5固有のpayloadは[schemas/README.md](schemas/README.md)に定義します。

Gateway WALのbytesは[wal-layout.md](wal-layout.md)に定義します。

fixtureとconformanceの形式は[fixtures/README.md](fixtures/README.md)と[conformance/README.md](conformance/README.md)に定義します。

hashの入力bytesは[hash-domains.md](hash-domains.md)に定義します。

manifestのcanonical JSONは[manifests.md](manifests.md)に定義します。

## 実装境界

Go Gatewayはこの仕様をdecoder、validator、encoderへ実装します。

PythonはGo実装から独立した検証decoderとして同じbytes、hash、failureを検証します。

MQL5 encoderは`mt5.mqltick.v1`を生成します。

fake producerは、ネットワークを使わずに決定的なBatchFrameV1を生成するGo製test producer/packageです。

共通messageへ`CopyTicks`、`RawMqlTickV1`、MT5固有のcursorを追加しません。

## 変更規則

固定値を変更するときは、該当する仕様、fixture、Go検証、Python検証、conformance caseを同じ変更単位で更新します。

実装が仕様と異なる場合は、実装を修正し、仕様を実装へ合わせる変更を先に行いません。
