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
| objects | array | ordered raw object references |
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

raw-day manifest digestとreplay-day manifest digestは、hash-domains.mdの専用domainで計算します。
