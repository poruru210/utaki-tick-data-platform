# Protocol V1の契約

`protocol/v1/`は、producerとGatewayが共有するIFの正規仕様です。

**正規仕様**：実装より優先して参照する、固定済みのwire、message、schema、hash、manifestの規則です。

## M0の範囲

M0では、Protocol V1の契約とcross-language conformanceに必要な入力を固定します。

M0の対象は、wire envelope、5種類のmessage、`mt5.mqltick.v1`、Gateway WAL entry、canonical JSON、hash domain、raw-day manifest、M0 replay-day manifest互換形です。

`part-manifest-v1`、canonical replay row、marker、row-chain、part_set_rootはM3-1で凍結したProtocol V1 derivative contractです。

`replay-publication-bundle-v1`と`replay-publication-final-observation-v1`はM3-3A-R1で凍結したpublication contractです。

M2だけがpublisher claimを作成し、M3はbundleへbindした既存claimのExactだけを受理します。

bundle、final observation、10個のresource limit、failure classificationは`hash-domains.md`と`fixtures/replay-publication-v1-conformance.json`で固定します。

Final observationの各replay edgeはstrict canonical replay-day manifest bytes、trusted full key、part countを保持し、empty dayをmanifest証明なしで推定しません。

part manifestはexact ReplayScope、raw-day manifest key+domain digest、ConversionTuple、part predecessor、previous row-chain hashをcanonical JSONへ含めます。

part setは同一scope/raw binding/conversionだけを受理し、最後のpartの`last_row_chain_hash`とreplay-day manifestの`canonical_stream_row_chain_root`を一致させます。

M3 derivativeの物理keyは、exact UTF-8 bytesへSHA-256を適用するProtocol V1の一元`ExactIdentityPathKey`から導出するcampaign-relative date-local keyです。

baseは`derivatives/stream=<sha256(replay_contract_id)>/format=ticks-parquet-v1/conversion=<sha256(conversion_id)>/day-definition=<sha256(day_definition_id)>/date=YYYY-MM-DD`であり、hour partitionは持ちません。

Parquet、part manifest、replay-day manifestのkeyは`manifests.md`の形式だけを受理し、`objects/replay`、`manifests/replay`、`snapshots/replay`のgeneric keyとaliasを拒否します。

trusted `r2.Layout`は検証済みrelative keyへimmutable rootとcampaign prefixを一度だけprependします。

M0のempty-parts replay fixtureは読み取り専用互換形として残し、M3の新規manifestはraw manifest key+SHA bindingとrevision predecessorを必須にします。

TCP runtime、live MT5 collection、R2、Parquet、SQLite journal runtime、crash injection、production operationはM0の対象外です。

## M1 runtime binding

M1では、この契約を変更せずに`internal/ingest`、`internal/wal`、`internal/journal`がlocalhost TCP runtimeへ実装します。

Gatewayはframeを完全に受信してProtocol V1の検証を終えた後、`protocol/v1/wal-layout.md`のactive WALへappendし、file syncとSQLite transaction commitを完了してからAckV1を返します。

同一producer sessionとbatch sequenceの同じbytesは`DUPLICATE`として再利用し、異なるbytesは`SOURCE_STATE_CONFLICT`として受理しません。

M1のruntimeはR2、Parquet、HTTP delivery、local pruningを呼び出しません。

## 固定値

wireの固定値は[wire-layout.md](wire-layout.md)に定義します。

messageのfield順序とenum値は[messages.md](messages.md)に定義します。

MT5固有のpayloadは[schemas/README.md](schemas/README.md)に定義します。

Gateway WALのbytesは[wal-layout.md](wal-layout.md)に定義します。

fixtureとconformanceの形式は[fixtures/README.md](fixtures/README.md)と[conformance/README.md](conformance/README.md)に定義します。

hashの入力bytesは[hash-domains.md](hash-domains.md)に定義します。

manifestのcanonical JSONは[manifests.md](manifests.md)に定義します。

M4のhandover、retention、prune checkpoint、resource limit contractは[operations.md](operations.md)に定義します。

## 実装境界

Go Gatewayはこの仕様をdecoder、validator、encoderへ実装します。

PythonはGo実装から独立した検証decoderとして同じbytes、hash、failureを検証します。

MQL5 encoderは`mt5.mqltick.v1`を生成します。

fake producerは、ネットワークを使わずに決定的なBatchFrameV1を生成するGo製test producer/packageです。

共通messageへ`CopyTicks`、`RawMqlTickV1`、MT5固有のcursorを追加しません。

## 変更規則

固定値を変更するときは、該当する仕様、fixture、Go検証、Python検証、conformance caseを同じ変更単位で更新します。

実装が仕様と異なる場合は、実装を修正し、仕様を実装へ合わせる変更を先に行いません。
