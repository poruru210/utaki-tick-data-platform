# Protocol V1のhash domain

Hashは、domain prefixと固定された入力bytesから計算します。

すべてのdigestはSHA-256の32 raw bytesで保持し、公開時はlowercase hexadecimalへ変換します。

## 共通encoding

`LP(value)`は、UTF-8 bytesの長さをU16 little-endianで書き、その直後にbytesを書きます。

`LP`の長さは0以上255以下です。

`U8`は符号なし1 byteです。

`U32`、`U64`、`I32`、`I64`はlittle-endianです。

`I32`は符号付き32-bit整数をtwo's complementで書きます。

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

## session lease ID

**`session_lease_id`**：M2 Gatewayが一つのproducer sessionへ割り当てる既存lease identifierです。

M3 replayはこの値をproducer instance identityとして扱いません。

leaseの入力bytesは、M2とのwire互換性を保つためLPではなく、次の順序でUTF-8 bytesとU+0000 separatorを連結します。

```text
"tick-data-platform/lease/v1" || U+0000
producer_instance_id || U+0000
producer_session_id || U+0000
campaign_id || U+0000
provider_id || U+0000
stable_feed_id || U+0000
broker_server_fingerprint || U+0000
exact_source_symbol
```

入力全体をSHA-256し、先頭16 raw bytesをlowercase hexadecimalへ変換して`lease-<32 hex>`を生成します。

このfield順、separator、prefix、切り出すdigest長は既存M2 algorithmの一部であり、変更時にはwire fixtureを同時に更新します。

## publisher claim digest

M2の`PublisherClaim`は、campaignとpublisher epochのidentityをcanonical JSONへ記録する既存のconditional-create objectです。

publisher claim digestは次のbytesから計算します。

```text
"tick-data-platform/publisher-claim/v1\0"
publisher_claim_canonical_json_bytes
```

M2 raw publicationだけがこのclaimを`If-None-Match: *`で作成します。

M3 replay publicationはclaimを作成せず、exact claim key、claim canonical bytes、上記domain digestをremote observationで検証します。

claimがAbsent、Different、Ambiguous、Oversized、Unavailableの場合、M3 replayはderivative actionとreceipt保存を許可しません。

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

## replay publication bundle digest

`publication_bundle_digest`は、公開対象のlocal verified factsとtrusted Layout-derived keyを一つのimmutable logical contractへ束ねるdigestです。

digestは次のbytesから計算します。

```text
"tick-data-platform/replay-publication-bundle/v1\0"
replay_publication_bundle_canonical_json_bytes
```

bundle canonical JSONのtop-level key集合は次のとおりです。

```text
bundle_version
canonical_stream_row_chain_root
claim
conversion
limits
parquet_objects
part_manifests
part_set_root
raw_manifest
raw_objects
replay_manifest
rclone_identity
scope
```

`claim`は`canonical_json`、`domain_digest`、`full_key`を持ちます。

`scope`は`broker_server_fingerprint`、`campaign_id`、`dataset_id`、`date`、`day_definition_id`、`exact_source_symbol`、`immutable_prefix`、`provider_id`、`publisher_epoch`、`publisher_id`、`rclone_prefix`、`scope_config_hash`、`scope_key`、`settle_policy`、`stable_feed_id`を持ちます。

`conversion`は`replay_contract_id`、`format_id`、`conversion_id`、`converter_build_id`、`dependency_lock_hash`、`writer_configuration_hash`、`target_platform_contract`、`max_rows_per_part`、`max_canonical_bytes_per_part`、`max_rows_per_row_group`を持ちます。

`limits`は`max_metadata_object_bytes`、`max_total_metadata_bytes`、`max_parquet_object_bytes`、`max_total_parquet_bytes`、`max_list_objects`、`max_graph_nodes`、`max_parts`、`max_observation_bytes`、`max_observation_requests`、`max_publication_rounds`を持ちます。

`raw_manifest`は`bytes`、`domain_digest`、`full_key`、`relative_key`、`rclone_key`、`revision`を持ちます。

`raw_objects`の各要素は`bytes`、`full_key`、`relative_key`、`rclone_key`、`sha256`を持ちます。

`parquet_objects`の各要素は`bytes`、`first_stream_sequence`、`full_key`、`last_stream_sequence`、`object_id`、`relative_key`、`rclone_key`、`sha256`を持ちます。

`part_manifests`の各要素は`bytes`、`domain_digest`、`full_key`、`object_id`、`part_sequence`、`relative_key`、`rclone_key`を持ちます。

`replay_manifest`は`bytes`、`domain_digest`、`full_key`、`relative_key`、`rclone_key`、`revision`を持ちます。

`rclone_identity`は`binary_sha256`、`goarch`、`goos`、`version`を持ちます。

全SHA-256とdomain digestは64文字のlowercase hexadecimalで表します。

`canonical_json`はpublisher claimのexact canonical JSONをJSON stringとして保持します。

bundle canonical JSONは、local path、local file handle、timestamp、credential、endpoint secret、journal row、event、retry error、digest自身を含みません。

全required digestは全zeroを拒否し、keyはProtocol helperとtrusted `r2.Layout`から機械的に導出します。

bundle canonical JSONのobject keyはbytewise lexicographic orderで一度だけ現れ、arrayはProtocol V1が定める順序を保持します。

## replay publication final observation digest

`final_observation_digest`は、bundleと一致する完全なremote observationを一回のbounded passで識別するdigestです。

digestは次のbytesから計算します。

```text
"tick-data-platform/replay-publication-final-observation/v1\0"
replay_publication_final_observation_canonical_json_bytes
```

final observation canonical JSONのtop-level key集合は次のとおりです。

```text
bundle_digest
claim
complete
derivative_objects
observation_bytes
observation_requests
observation_version
raw_manifest
raw_objects
replay_edges
```

`claim`はbundleと同じ`canonical_json`、`domain_digest`、`full_key`を持ちます。

`raw_manifest`は`bytes`、`domain_digest`、`full_key`を持ちます。

`raw_objects`の各要素は`bytes`、`full_key`、`sha256`を持ちます。

`derivative_objects`の各要素は`bytes`、`digest`、`digest_domain`、`full_key`、`kind`を持ちます。

`digest_domain`はParquet objectの`sha256`、part manifestの`part-manifest-v1`、replay manifestの`replay-day-manifest-v1`のいずれかです。

`replay_edges`の各要素は`canonical_json`、`canonical_stream_row_chain_root`、`full_key`、`manifest_digest`、`part_count`、`part_set_root`、nullableな`previous_manifest_digest`、`revision`を持ちます。

`canonical_json`はstrict M3 replay-day manifestのcanonical bytesをJSON stringとして保持し、decoderは再encodeしたbytesとの完全一致、replay manifest domain digest、Protocol key、trusted Layout full key、revision、predecessor、scope、ConversionTuple、roots、part countをedgeと照合します。

Empty terminalまたはempty predecessorは、当該edgeのcanonical manifestが空の`part_manifest_keys`と二つのzero rootsを証明する場合だけ受理します。

Partを一つでも持つmanifestのzero root、二つのrootの片方だけがzeroであるmanifest、canonical manifestを欠く以前のzero-root edgeは拒否します。

`complete`は常に`true`です。

`observation_bytes`はclaim canonical bytes、全観測object bytes、全edgeの`full_key`と`canonical_json`のUTF-8 bytes、JSON escape後のfinal observation canonical bytesをoverflow-safeに合算したcounterであり、`observation_requests`は当該観測へ消費したremote request数です。

Exact以外の観測結果はこのschemaへencodeしません。

derivative namespace inventoryはParquet、part manifest、replay manifestを含み、unknown object、duplicate descriptor、conflicting size、branch、missing predecessorをcomplete observationへ含めません。

canonical JSONはclock、retry/error string、ETag、local path、credential、journal state、digest自身を含みません。

Absent、Different、Ambiguous、Oversized、Unavailable、欠落、異なるbytes、曖昧なgraph、requestまたはbyte limit超過の状態からfinal observation digestを構成しません。

final observation digestと`verification_complete=true`をbindしないreceiptは、M3 replay verification receiptとして受理しません。

## replay publication resource limits

Protocol V1実装上限は、`max_graph_nodes=50000`、`max_list_objects=50000`、`max_metadata_object_bytes=16777216`、`max_observation_bytes=70368744177664`、`max_observation_requests=100000`、`max_parquet_object_bytes=1099511627776`、`max_parts=10000`、`max_publication_rounds=20002`、`max_total_metadata_bytes=268435456`、`max_total_parquet_bytes=17592186044416`です。

publicationは一つのapproved actionごとにfresh observationを作るため、part数を`N`とすると必要なround数は`2N+2`です。
この上限は`max_parts=10000`のbundleを受理できる値であり、bundle sealerとPython verifierは実際のpart数に対して必要round数をlock取得前に検査します。

bundleは10個のlimitを全てnonzeroで保持し、実装上限超過、単一object上限とaggregate上限の逆転、`next > limit`または`total > limit - next`となる加算を拒否します。

Sealerは全replay edgeを含むcomplete final observationの保守的なcanonical byte見積りをbundle digest計算とcampaign lock取得の前に行い、budget不足を拒否します。

M2 raw publisherだけがpublisher claimを作成し、M3はdomain digest、exact key、canonical claimの全scope fieldがbundleの`scope`と一致する場合だけExactとして受理します。

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

## continuity segment ID

continuity segment IDは次のbytesのSHA-256をlowercase hexadecimalで表した64文字です。

```text
"tick-data-platform/continuity-segment/v1\0"
LP(dataset_id)
LP(campaign_id)
LP(day_definition_id)
LP(date)
LP(replay_contract_id)
LP(conversion_id)
LP(raw_day_manifest_key)
H32(raw_day_manifest_sha256)
U64(start_gateway_ingest_sequence)
U32(start_record_ordinal)
LP(start_marker_code)
H32(predecessor_row_chain_hash)
```

乱数、実行時刻、mapのiteration順は入力に含めません。

## canonical replay row

canonical replay rowはrow-chainのhash入力となる唯一のbinary encodingです。

data rowのbytesは次の順序です。

```text
"tick-data-platform/replay-row/v1\0"
U8(1)
U64(stream_sequence)
LP(dataset_id)
LP(campaign_id)
LP(day_definition_id)
LP(date)
LP(replay_contract_id)
LP(conversion_id)
LP(continuity_segment_id)
H32(raw_day_manifest_sha256)
LP(raw_object_key)
H32(raw_object_sha256)
U64(gateway_ingest_sequence)
LP(producer_instance_id)
LP(producer_session_id)
U64(batch_sequence)
U32(record_ordinal)
U64(capture_sequence)
I64(time)
U64(bid_bits)
U64(ask_bits)
U64(last_bits)
U64(volume)
I64(time_msc)
U32(flags)
U64(volume_real_bits)
H32(source_payload_fingerprint)
H32(observation_hash)
I64(fetch_wall_start_s)
I64(fetch_wall_end_s)
U64(fetch_monotonic_start_us)
U64(fetch_monotonic_end_us)
I32(copy_ticks_error)
U32(source_status_flags)
```

marker rowはcommon fieldsの後に次を続けます。

```text
"tick-data-platform/replay-marker/v1\0"
LP(marker_code)
LP(reason)
LP(marker_detail)
U64(reference_gateway_ingest_sequence)
U32(reference_record_ordinal)
H32(predecessor_row_chain_hash)
H32(continuity_segment_start_hash)
```

marker rowはsource Tick fieldを持たず、zero値で埋めません。

`row_kind=1`はdata row、`row_kind=2`はmarker rowだけを受理し、未知のkindを拒否します。

marker codeとreasonの組は次の固定表だけを受理します。

| marker_code | reason |
| --- | --- |
| `SEGMENT_START` | `INITIAL` |
| `AMBIGUOUS_OVERLAP` | `NO_UNIQUE_OVERLAP` |
| `SOURCE_HISTORY_CHANGED` | `SAME_POSITION_PAYLOAD_CHANGED` |
| `SOURCE_ERROR` | `SOURCE_REPORTED_ERROR` |
| `GAP` | `WAL_SEQUENCE_GAP` |
| `TIMESTAMP_REGRESSION` | `TIME_MSC_REGRESSION` |
| `CAMPAIGN_BOUNDARY` | `CAMPAIGN_CHANGED` |

raw evidenceがないmarkerでは`raw_object_key`を空LP、`raw_object_sha256`を全zero H32とします。

raw evidenceがあるmarkerではkeyとhashの両方を必須にし、片方だけの入力を拒否します。

`capture_sequence`と`time_msc`はmt5.mqltick.v1のstable source identityではありません。

`SOURCE_HISTORY_CHANGED`は、stable source IDが保存されている場合、または保持済みtailのsuffixとincoming batchのprefixの境界を三つ以上のfingerprintで一意に対応付けられる場合だけ使用します。

境界の最長alignmentは、両端を除く一つのpayloadだけが異なり、その前後のfingerprintが完全一致しなければなりません。

同じ最長alignmentが保持tail内の複数位置へwildcard-compatibleに対応する場合、またはsuffixとprefixの境界を証明できない内部subsequenceしかない場合は`AMBIGUOUS_OVERLAP`を出します。

短いnested alignmentが同じ変更座標を示すだけでは、最長alignmentを曖昧とは扱いません。

session restart、繰り返しpayload、tail容量または位置の不確実性では`AMBIGUOUS_OVERLAP`を出し、incoming occurrenceを削除しません。

## row-chain

empty row streamのrootは32 bytesの全zeroです。

rowを追加するhashは次のbytesのSHA-256です。

```text
"tick-data-platform/replay-row-chain/v1\0"
U64(stream_sequence)
H32(previous_row_hash)
U32(canonical_row_bytes_length)
canonical_row_bytes
```

最初の`previous_row_hash`は全zeroです。

stream sequenceは0から1ずつ増え、row bytesの順序とmarkerを含めてrootへ反映します。

## part-manifest-v1とpart_set_root

この`part-manifest-v1`のfield追加は、branchが未mergeでM3 V1をまだreleaseしていない段階の契約修正です。

既存のM3 V1をversion splitせず、M3 writerがまだ出力していない不完全なpart manifestを受理対象から外し、同じ`part-manifest-v1`のcanonical contractを修正します。

`part-manifest-v1`のcanonical JSONは次のkey集合だけを持ちます。

```text
campaign_id
canonical_row_bytes
conversion_id
converter_build_id
dataset_id
date
day_definition_id
dependency_lock_hash
first_row_chain_hash
first_stream_sequence
format_id
last_row_chain_hash
last_stream_sequence
manifest_version
part_bytes
part_key
part_sequence
part_sha256
previous_manifest_sha256
previous_row_chain_hash
raw_day_manifest_key
raw_day_manifest_sha256
replay_contract_id
row_count
target_platform_contract
writer_configuration_hash
```

上の列はcanonical JSONのUTF-8 object key順、すなわちbytewise lexicographic orderを示します。

canonical JSONのobject keyはこの順序で一度だけ現れ、不要な空白を持たず、stringはJSON UTF-8、integerはnon-negative decimal JSON number、hashは64文字lowercase hex stringで表します。

`dataset_id`、`campaign_id`、`day_definition_id`、`date`、`replay_contract_id`、`conversion_id`、`raw_day_manifest_key`、`raw_day_manifest_sha256`は、partが属する正確な`ReplayScope`とraw-day manifest revisionをbindします。

`format_id`、`converter_build_id`、`dependency_lock_hash`、`writer_configuration_hash`、`target_platform_contract`は、partの完全な`ConversionTuple`をbindします。

identityとcontrol stringはUTF-8で255 bytes以下、relative physical keyは1024 bytes以下、`date`はUTCの`YYYY-MM-DD`、hashはlowercase hexadecimalの64文字、hash domainは対応するProtocol V1 domainです。

`part_key`は、exact UTF-8 bytesへnormalizationやcase foldingを行わずSHA-256を適用したcampaign-relative baseに、`first_stream_sequence`、`last_stream_sequence`、`part_sha256`を連結した`<base>/parquet/<first>-<last>-<part_sha256>.parquet`です。

baseは`derivatives/stream=<sha256(exact replay_contract_id)>/format=ticks-parquet-v1/conversion=<sha256(exact conversion_id)>/day-definition=<sha256(exact day_definition_id)>/date=YYYY-MM-DD`です。

このbaseはcampaign-relativeであり、dataset、provider、feed、symbol、campaignはtrusted Layoutのcampaign prefixとcanonical manifest bindingで閉じます。

part chainはdate-localに閉じ、hour partitionを持ちません。

marker rowへsyntheticなhourを割り当てません。

part sequenceはday-localに0から始まり、0のpredecessorはnullかつ`previous_row_chain_hash`は全zero、1以降は直前part manifest digestと直前partの`last_row_chain_hash`です。

part 0は`first_stream_sequence=0`でなければなりません。

successorの`first_stream_sequence`は直前partの`last_stream_sequence+1`でなければなりません。

`part_bytes`は1以上でなければならず、`part_sha256`、`first_row_chain_hash`、`last_row_chain_hash`は全zeroを拒否します。

`previous_row_chain_hash`はpart 0だけが全zeroを持ち、part 1以降は全zeroでない直前partの`last_row_chain_hash`と一致しなければなりません。

part manifest digestは`"tick-data-platform/part-manifest/v1\0" || canonical_json_bytes`のSHA-256です。

raw-day bindingの`raw_day_manifest_sha256`はplain SHA-256ではなく、M2の`"tick-data-platform/raw-day-manifest/v1\0" || raw_day_manifest_canonical_bytes`のdomain digestです。

part manifest digestのcanonical bytesにはscope、raw binding、ConversionTuple、`previous_row_chain_hash`が含まれるため、同じpart objectでもこれらを変更したmanifestは別digestと別manifest keyになり、part setのbinding検証で混在を拒否します。

`part_manifest_key`は`<base>/manifests/part-<8桁zero-padded sequence>-<part_manifest_digest>.json`です。

`part_set_root`は次のbytesのSHA-256です。

```text
"tick-data-platform/part-set/v1\0"
U32(part_count)
U32(path_bytes_length) || path_bytes(part_manifest_key)
H32(part_manifest_digest)
U64(first_stream_sequence)
U64(last_stream_sequence)
```

上記のpart要素をpart sequence順に繰り返します。

各part manifest digestはcanonical JSON内のscope、raw binding、ConversionTuple、row-chain anchorを含むため、`part_set_root`はそれら全てを間接的に含みます。

part set内の全partはscope、raw binding、ConversionTupleが完全一致しなければなりません。

`part_set_root`の検証では、part 0の`previous_row_chain_hash`が全zeroであり、successorの同値が直前partの`last_row_chain_hash`であることを確認します。

partが空の場合の`part_set_root`は32 bytesの全zeroであり、空part manifestは作りません。

part object key、part manifest key、part predecessor、row range、scope、raw binding、ConversionTuple、row-chain predecessorが一致しない入力を拒否します。

## replay-day manifest M3 form

M3の`replay-day-manifest-v1`は、既存のM0 empty-parts formとは異なる次のkey集合を持ちます。

```text
manifest_version
manifest_id
dataset_id
campaign_id
day_definition_id
date
revision
raw_day_manifest_key
raw_day_manifest_sha256
replay_contract_id
format_id
conversion_id
converter_build_id
dependency_lock_hash
writer_configuration_hash
target_platform_contract
completeness_status
part_manifest_keys
part_set_root
canonical_stream_row_chain_root
previous_manifest_sha256
```

`raw_day_manifest_key`と`raw_day_manifest_sha256`は同一raw manifest objectのkeyとcanonical bytes digestをbindし、keyまたはhashだけのselectorを受理しません。

replay manifest keyは`<base>/replay-day-<revision>-<replay_manifest_digest>.json`です。

part manifest keyとreplay manifest keyはbase、date、scope、conversion identityへbindし、`objects/replay`、`manifests/replay`、`snapshots/replay`のgeneric keyとaliasを拒否します。

revision 1のpredecessorはnullです。

revision 2以降は同じdataset、campaign、day definition、date、replay contract、conversion tupleを保ち、直前digestをpredecessorに入れます。

raw inputのrevisionが進んだ場合だけ同じconversion tupleのsuccessorを作り、converter、dependency、writer、platformのいずれかが変わった場合は新しいconversion tupleのrevision 1を作ります。

M0 compatibility formは、旧key集合、`part_manifest_keys=[]`、`part_set_root=null`、`canonical_stream_row_chain_root=null`のときだけ受理します。

M0 compatibility formは既存fixtureの読み取り専用互換であり、M3 publicationのraw bindingを満たすmanifestとして扱いません。

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
