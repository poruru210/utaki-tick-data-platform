# Raw-dayとReplay-dayのmanifest

Manifestは、保存範囲、schema、件数、hash、immutable objectをcanonical JSONで記録します。

Manifestのobject keyとhashは、実装型やprivate Go、MQL5 typeを参照しません。

## Canonical JSONの共通規則

manifestは、hash-domains.mdのcanonical JSON規則でUTF-8 bytesへ変換します。

全fieldを固定順序のobject keyとして出力し、未定義の追加keyを受理しません。

digestはlowercase hexadecimalの64文字で表します。

dateはUTCの`YYYY-MM-DD`です。

count、sequence、epoch、timestampはJSON integerです。

## raw-day-manifest-v1

**`raw-day-manifest-v1`**：一つのdataset、campaign、日付に対応するraw保存範囲を記録するmanifestです。

top-level objectは次のrequired keyを持ちます。

| key | JSON type | rule |
| --- | --- | --- |
| manifest_version | string | `raw-day-manifest-v1` |
| manifest_id | string | stable identifier |
| dataset_id | string | non-empty |
| campaign_id | string | non-empty |
| day_definition_id | string | non-empty |
| date | string | UTC date |
| revision | integer | `>= 1`; genesis is `1` |
| publisher_id | string | non-empty |
| publisher_epoch | integer | non-negative |
| config_hash | string | 64 lowercase hex |
| protocol_version | integer | `1` |
| source_schema_id | string | `mt5.mqltick.v1` for initial source |
| wal_schema_id | string | `gateway-wal-v1` |
| observed_through_source_msc | integer | signed Unix milliseconds |
| observed_through_capture_sequence | integer | non-negative |
| terminal_sync_status | string | source-reported state |
| settle_policy | string | policy identifier |
| completeness_status | string | one of the fixed statuses |
| objects | array | ordered inclusive coordinate ranges in verified raw WAL objects |
| chain_objects | array | minimal ordered complete sealed WAL object chain covering the selected slice |
| accepted_record_count | integer | non-negative |
| error_count | integer | non-negative |
| chain_slice_start_sequence | integer | non-negative |
| chain_slice_start_root | string | 64 lowercase hex |
| chain_slice_end_sequence | integer | non-negative |
| chain_slice_end_root | string | 64 lowercase hex |
| raw_set_root | string | 64 lowercase hex |
| previous_manifest_sha256 | string or null | previous revision digest |
| logical_close_time_s | integer | signed Unix seconds |

`objects`の各要素は`key`、`sha256`、`bytes`、`start_ingest_sequence`、`end_ingest_sequence`、`first_record_ordinal`、`last_record_ordinal`を持ちます。

`chain_objects`の各要素は`key`、`sha256`、`bytes`、`start_ingest_sequence`、`end_ingest_sequence`を持ちます。

raw WAL objectのkeyはcampaign-relativeなASCII文字列`objects/raw/wal-<64 lowercase sha256>.rtw`であり、`sha256`から機械的に導出します。

`chain_objects`は、最初のselected WAL entryから最後のselected WAL entryまでのinclusive chain sliceに交差するsealed WAL objectの最小完全集合です。

`chain_objects`は重複せず、WAL sequenceの厳密昇順で隣接し、sequence gapとoverlapを持ちません。

最初のchain objectはchain start sequenceを含み、最後のchain objectはchain end sequenceを含み、全chain objectはchain sliceと交差します。

`objects`の各rangeは、同じkey、sha256、bytesを持つchain objectに正確に一つだけbindし、そのobjectのsequence boundsとchain sliceの範囲内にあります。

同じchain objectを複数のdisjointな`objects` rangeが参照できます。

chain sliceが空の場合、`objects`と`chain_objects`は空配列であり、chain sequenceとchain rootはzero値です。

各要素は同じraw WAL object内のinclusive coordinate range `(gateway_ingest_sequence, record_ordinal)`です。

同じ`key`と`sha256`を持つ複数rangeを許しますが、全rangeを厳密昇順かつnon-overlapにします。

non-empty batchのrecord ordinalは`0..N-1`です。

zero-record batchはordinal `0`のsentinel rangeを持ち、`RequestedFromMSC`のUTC dayへ割り当てます。

`accepted_record_count`は選択されたrecord数です。

`error_count`は選択されたBatchFrameV1のうち`CopyTicksError != 0`であるbatch数です。

chain sliceのstart rootは最初のselected WAL entryの`PreviousEntryHash`であり、end rootは最後のselected WAL entryの`EntryHash`です。

campaign chainがsegment間で不連続な場合はmanifestをrejectします。

objects配列はcampaign chainの順序で並べます。

completeness_statusは`provisional`、`settled_snapshot`、`incomplete_source_error`、`incomplete_sync`、`incomplete_gateway_outage`のいずれかです。

`settled_snapshot`は指定watermark時点のsnapshotであり、将来のlate historyがないことを保証しません。

## replay-day-manifest-v1

**`replay-day-manifest-v1`**：raw-day manifestから生成するreplay用データの入力と変換を記録するmanifestです。

top-level objectは次のrequired keyを持ちます。

| key | JSON type | rule |
| --- | --- | --- |
| manifest_version | string | `replay-day-manifest-v1` |
| manifest_id | string | stable identifier |
| dataset_id | string | non-empty |
| campaign_id | string | non-empty |
| day_definition_id | string | non-empty |
| date | string | UTC date |
| raw_day_manifest_sha256 | string | 64 lowercase hex |
| replay_contract_id | string | non-empty |
| format_id | string | `ticks-parquet-v1` |
| conversion_id | string | non-empty |
| converter_build_id | string | non-empty |
| dependency_lock_hash | string | 64 lowercase hex |
| writer_configuration_hash | string | 64 lowercase hex |
| target_platform_contract | string | non-empty |
| completeness_status | string | `provisional` or `settled_snapshot` |
| part_manifest_keys | array | empty in M0, populated by M3 |
| part_set_root | string or null | null in M0 |
| canonical_stream_row_chain_root | string or null | null in M0 |

`part_manifest_keys`のobject形式とpart chainはM3で定義します。

M0のreplay-day fixtureでは、part_manifest_keysを空配列、part_set_rootとcanonical_stream_row_chain_rootをnullにします。

## 不変性

manifestが参照するobjectは、digestを検証できるimmutable objectとして扱います。

同じkeyを異なるbytesで上書きしません。

manifest revisionは新しいmanifest_idとprevious_manifest_sha256で表し、既存manifestを変更しません。

genesis revisionは`1`かつ`previous_manifest_sha256`は`null`です。

successor revisionは直前revisionに`1`を加え、直前digestを`previous_manifest_sha256`へ入れます。

successorのobjectsとchain_objectsは直前revisionの配列をprefixとして累積します。

successorはscopeとpublisher epochを変更せず、chain startを保持し、chain endは同じか前方へ延長します。

successorのaccepted count、error count、source watermark、capture watermarkは減少しません。

raw-day manifest digestとreplay-day manifest digestは、hash-domains.mdの専用domainで計算します。

`raw_set_root`は、同じ文書のordered rangeをhash-domains.mdの専用domainで計算します。
