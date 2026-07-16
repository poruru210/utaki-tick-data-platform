# Raw-dayとReplay-dayのmanifest

Manifestは、保存範囲、schema、件数、hash、immutable objectをcanonical JSONで記録します。

Manifestのobject keyとhashは、実装型やprivate Go、MQL5 typeを参照しません。

## Canonical JSONの共通規則

manifestは、hash-domains.mdのcanonical JSON規則でUTF-8 bytesへ変換します。

全fieldを固定順序のobject keyとして出力し、未定義の追加keyを受理しません。

digestはlowercase hexadecimalの64文字で表します。

dateはUTCの`YYYY-MM-DD`です。

count、sequence、epoch、timestampはJSON integerです。

## M3 derivative physical keys

M3 V1の物理key修正は、branchが未mergeでM3 V1をまだreleaseしていないため、version splitではなく同じProtocol V1へのin-place correctionです。

`ExactIdentityPathKey`は、入力identityのexact UTF-8 bytesだけへSHA-256を適用し、normalization、case folding、trimを行いません。

全てのM3 derivative keyは次のcampaign-relative baseを使います。

```text
derivatives/stream=<ExactIdentityPathKey(replay_contract_id)>/format=ticks-parquet-v1/conversion=<ExactIdentityPathKey(conversion_id)>/day-definition=<ExactIdentityPathKey(day_definition_id)>/date=YYYY-MM-DD
```

part chainは日付単位で閉じます。

hour partitionは存在せず、marker rowへsyntheticなhourを割り当てません。

Parquet object keyは`<base>/parquet/<first_stream_sequence>-<last_stream_sequence>-<part_sha256>.parquet`です。

part manifest keyは`<base>/manifests/part-<8桁zero-padded part_sequence>-<part_manifest_digest>.json`です。

replay-day manifest keyは`<base>/replay-day-<revision>-<replay_manifest_digest>.json`です。

`objects/replay`、`manifests/replay`、`snapshots/replay`のgeneric keyと、別scopeへaliasするkeyはM3 derivative keyとして拒否します。

Protocol helperがrelative keyを検証して導出し、trusted `r2.Layout`はそのrelative keyへimmutable rootとcampaign prefixを一度だけprependします。

relative keyはcampaign-relativeであるため、dataset、provider、feed、symbol、campaignのscope bindingはcanonical manifestとtrusted `r2.Layout`のcampaign prefixで閉じます。

relative keyだけを別campaignへ移すことはfull remote keyの同一性を満たさず、Layout verifierが拒否します。

relative physical keyの最大長は1024 bytesです。

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

M3のtop-level objectは次のrequired keyを持ちます。

| key | JSON type | rule |
| --- | --- | --- |
| manifest_version | string | `replay-day-manifest-v1` |
| manifest_id | string | stable identifier |
| dataset_id | string | non-empty |
| campaign_id | string | non-empty |
| day_definition_id | string | non-empty |
| date | string | UTC date |
| revision | integer | `>= 1`; same conversion tupleのsuccessor axis |
| raw_day_manifest_key | string | immutable raw manifest object key; hashと同時に必須 |
| raw_day_manifest_sha256 | string | 64 lowercase hex |
| replay_contract_id | string | non-empty |
| format_id | string | `ticks-parquet-v1` |
| conversion_id | string | non-empty |
| converter_build_id | string | non-empty |
| dependency_lock_hash | string | 64 lowercase hex |
| writer_configuration_hash | string | 64 lowercase hex |
| target_platform_contract | string | non-empty |
| completeness_status | string | `provisional` or `settled_snapshot` |
| part_manifest_keys | array | day-local part manifest keyをpart sequence順に並べる |
| part_set_root | string | partが空なら32 bytes全zeroのhex |
| canonical_stream_row_chain_root | string | rowが空なら32 bytes全zeroのhex |
| previous_manifest_sha256 | string or null | revision 1はnull、successorは直前digest |

part manifest keysが空なら`part_set_root`と`canonical_stream_row_chain_root`は全zeroです。

part manifest keysが空でなければ、ordered part setを検証した`part_set_root`と、最後のpartの`last_row_chain_hash`が`canonical_stream_row_chain_root`へ完全一致しなければなりません。

任意のnonzero row-chain rootをmanifest bytesだけから受理せず、verified part chainまたはverified streaming summaryと一致しないrootを拒否します。

`raw_day_manifest_key`で取得したobjectのcanonical bytesに`"tick-data-platform/raw-day-manifest/v1\0"`を前置してSHA-256を計算し、`raw_day_manifest_sha256`と一致することを確認してからreplayを受理します。

canonical bytesへのplain SHA-256だけを`raw_day_manifest_sha256`として提示する入力は拒否します。

raw manifest keyは、scope prefixを除くcampaign-relativeな`raw-day-<revision>-<domain digest>.json`へ機械的に導出し、keyとdomain digestを同じmanifestへbindします。

replay source APIの`ManifestRelativeKey`は、このcampaign-relative keyとの完全一致だけを受理します。

immutable rootを前置したfull remote keyの検証は、trusted `r2.Layout`が相対keyを検証した後にだけ行う責務であり、archive replay sourceは任意rootを受理しません。

`objects`のcompact rangeを実装が受理する場合、`(start_ingest_sequence, first_record_ordinal)`から`(end_ingest_sequence, last_record_ordinal)`までを、first entryはfirstからend、middle entryは全record、last entryは0からlastのper-entry coordinateへ展開します。

same-entry rangeはfirstからlastのinclusive coordinateです。

zero-record batchはProtocol V1のrequested-day sentinel `(ordinal=0)`として展開し、source-error batchはそのbatchの全てのcanonical selected coordinateと`error_count`を保持します。

展開後の`objects`、`accepted_record_count`、`error_count`、両watermark、chain sliceのsequence/root、`chain_objects`、`raw_set_root`は、verified sealed WALから再導出したM2 canonical full-day selectionと完全一致しなければなりません。

したがって、7-of-9のようなpartial selectionはcompact rangeの形がvalidでも拒否されます。

compact表現を使わないM2 canonical formは、同一entryのper-entry inclusive rangeを`objects`へ並べた形式です。

### replay source resource limits

各`replay_contract_id`は、実装が`ReplaySourceInput`へ渡す必須の`ReplayResourceLimits` profileを持ちます。

profileは`MaxChainObjects`、`MaxObjectBytes`、`MaxChainBytes`のU64値を持ち、全てnon-zeroかつ有限でなければなりません。

archive verifierはsealed objectを開く前に、chain object数、各descriptorのbytes、descriptor bytesの合計をoverflow-safeに検証します。

各objectの再検証でもfile bytesを検査し、`MaxObjectBytes`は一つの`VerifySealedSegment` parseのpeak、`MaxChainBytes`と`MaxChainObjects`はfull-chain in-memory verificationの上限です。

上限超過、整数overflow、descriptorとverified bytesの不一致はfail closedし、RowSinkへ一行も渡しません。

M0の既存fixtureは、旧key集合、`part_manifest_keys=[]`、`part_set_root=null`、`canonical_stream_row_chain_root=null`を持つ読み取り専用互換形として受理します。

M0互換形はM3 publicationのbinding-complete manifestではなく、M3の新規manifest writerはこの形を出力しません。

## part-manifest-v1

**`part-manifest-v1`**：一つのimmutable Parquet partと、そのpartがreplay row streamのどの範囲を持つかを記録するmanifestです。

このfield追加は、branchが未mergeでM3 V1をまだreleaseしていない段階の契約修正です。

既存のM3 V1をversion splitせず、M3 writerがまだ出力していない旧形式part manifestを受理対象から外し、同じ`part-manifest-v1`のcanonical contractを修正します。

top-level objectは、scope、raw binding、ConversionTuple、part summary、row-chain predecessorを含む次のkeyだけを持ちます。

`dataset_id`、`campaign_id`、`day_definition_id`、`date`、`replay_contract_id`、`conversion_id`、`raw_day_manifest_key`、`raw_day_manifest_sha256`は、partの正確なReplayScopeとraw-day manifest revisionをbindします。

`format_id`、`converter_build_id`、`dependency_lock_hash`、`writer_configuration_hash`、`target_platform_contract`は、partのConversionTupleをbindします。

canonical JSONのobject keyは、UTF-8 bytewise lexicographic orderで一度だけ現れます。

canonical field orderは`campaign_id`、`canonical_row_bytes`、`conversion_id`、`converter_build_id`、`dataset_id`、`date`、`day_definition_id`、`dependency_lock_hash`、`first_row_chain_hash`、`first_stream_sequence`、`format_id`、`last_row_chain_hash`、`last_stream_sequence`、`manifest_version`、`part_bytes`、`part_key`、`part_sequence`、`part_sha256`、`previous_manifest_sha256`、`previous_row_chain_hash`、`raw_day_manifest_key`、`raw_day_manifest_sha256`、`replay_contract_id`、`row_count`、`target_platform_contract`、`writer_configuration_hash`です。

part manifest digestは`tick-data-platform/part-manifest/v1\0` domainとこのcanonical JSON bytesを連結してSHA-256を計算します。

`raw_day_manifest_sha256`はplain SHA-256ではなく、M2の`tick-data-platform/raw-day-manifest/v1\0` domain digestです。

| key | JSON type | rule |
| --- | --- | --- |
| manifest_version | string | `part-manifest-v1` |
| dataset_id | string | ReplayScopeのdataset |
| campaign_id | string | ReplayScopeのcampaign |
| day_definition_id | string | ReplayScopeのday definition |
| date | string | UTC `YYYY-MM-DD` |
| replay_contract_id | string | ReplayScopeのreplay contract |
| format_id | string | `ticks-parquet-v1` |
| conversion_id | string | ConversionTupleのconversion |
| converter_build_id | string | converter build identity |
| dependency_lock_hash | string | 64 lowercase hex、nonzero |
| writer_configuration_hash | string | 64 lowercase hex、nonzero |
| target_platform_contract | string | writer platform contract |
| raw_day_manifest_key | string | exact campaign-relative raw manifest key |
| raw_day_manifest_sha256 | string | raw-day manifest domain digest、64 lowercase hex、nonzero |
| part_sequence | integer | `0..2^32-1`; day-local連番 |
| part_key | string | `<base>/parquet/<first_stream_sequence>-<last_stream_sequence>-<part_sha256>.parquet`; exact scope、conversion、date、range、hashから導出 |
| part_sha256 | string | 64 lowercase hex、全zero禁止 |
| part_bytes | integer | U64、`>= 1` |
| row_count | integer | `>= 1` |
| canonical_row_bytes | integer | `>= 1` |
| first_stream_sequence | integer | non-negative U64 |
| last_stream_sequence | integer | `>= first_stream_sequence` |
| previous_row_chain_hash | string | part 0は全zero、successorは直前partの`last_row_chain_hash` |
| first_row_chain_hash | string | 64 lowercase hex、全zero禁止 |
| last_row_chain_hash | string | 64 lowercase hex、全zero禁止 |
| previous_manifest_sha256 | string or null | part 0はnull、successorは直前digest |

`part_sequence`は一つのreplay day内で0から始まる連番です。

`part_key`は上記のdate-local base、inclusiveなfirst/last stream sequence、part bytesのSHA-256から機械的に導出します。

part 0のpredecessorはnullです。

part 1以降のpredecessorは直前part manifestのcanonical digestでなければなりません。

part 1以降の`previous_row_chain_hash`は直前partの`last_row_chain_hash`でなければなりません。

part 0の`previous_row_chain_hash`は全zeroであり、`first_stream_sequence`は0でなければなりません。

part 0だけが`previous_row_chain_hash`の全zeroを許し、part 1以降は全zeroでない直前partの`last_row_chain_hash`を要求します。

`part_bytes`は1以上でなければならず、`part_sha256`、`first_row_chain_hash`、`last_row_chain_hash`は全zeroを拒否します。

partの`last_stream_sequence - first_stream_sequence + 1`は`row_count`と一致しなければなりません。

同じpart set内の全partはdataset、campaign、day definition、date、replay contract、raw manifest key+digest、ConversionTupleが完全一致しなければなりません。

`PartManifestInput`は、close、sync、hash、reopen、独立検証済みのParquet `PartArtifact`と、検証済みのexact scopeおよびConversionTupleからだけ構成します。

M3 writerは、scope、raw binding、ConversionTuple、`previous_row_chain_hash`を持たない旧形式のpart manifestを出力しません。

empty replay dayはpart manifestを作らず、`part_set_root`を全zeroへ設定します。

`part_manifest_keys`はpart sequence順のkey arrayであり、重複、sequenceの欠落、別dayのpart keyを受理しません。

## 不変性

manifestが参照するobjectは、digestを検証できるimmutable objectとして扱います。

同じkeyを異なるbytesで上書きしません。

manifest revisionは新しいmanifest_idとprevious_manifest_sha256で表し、既存manifestを変更しません。

genesis revisionは`1`かつ`previous_manifest_sha256`は`null`です。

successor revisionは直前revisionに`1`を加え、直前digestを`previous_manifest_sha256`へ入れます。

raw-day successorのobjectsとchain_objectsは直前revisionの配列をprefixとして累積します。

replay-day successorは同じscopeとconversion identityを維持し、新しいraw manifest keyとdomain digestへbindする場合だけ同じconversion tupleのrevisionを進めます。

converter build、dependency lock、writer configuration、target platformのいずれかが変わる場合はrevisionを進めず、別のconversion tupleのrevision 1へ分けます。

replay manifestのpublication journalとreceiptはraw-day manifestのjournal stateを再利用せず、derivative-specific identityで管理します。

M3-3A-R0で、旧`intent`、`claimed`、`raw_verified`、`parquet_copied`、`parquet_verified`、`part_manifests_copied`、`part_manifests_verified`、`replay_manifest_copied`、`replay_manifest_verified`、`receipt_saved`の固定stage列とSQLite transitionをProtocol authorityから外します。

旧stage実装とそのtest countは、失敗経路を保持するbehavior/failure inventoryであり、M3-3Aの受入証拠ではありません。

新しいpublicationは、verified local inputを`ReplayPublicationBundle`へsealし、bounded remote observationを`ObservationClass`とcomplete graphへ変換し、pure reconcilerがbundle object IDだけのactionを返し、executorがactionを実行します。

`ReplayPublicationBundle`の正確なtop-level key、nested object key、二つのdomain digest、10個のresource limitは`hash-domains.md`を正本とします。

bundleはM2 claim、scope、ConversionTuple、raw manifestとraw object、Parquet object、part manifest、replay manifest、part set root、canonical row-chain rootをcanonical JSONへ含めます。

bundleはlocal path、file handle、clock、credential、endpoint secret、journal、event、retry state、自身のdigestを含めません。

complete final observationはbundle digest、Exact claim、Exact raw inventory、full keyで整列した完全なderivative inventory、canonical replay-day manifest bytesとtrusted full keyを持つ連続replay revision edge、request counter、byte counterを含めます。

各edgeはcanonical manifestをstrict decodeして再encodeし、domain digest、Protocol key、trusted Layout full key、revision、predecessor、scope、ConversionTuple、roots、part countを照合します。

Empty terminalとempty predecessorはcanonical manifestが空partsとzero rootsを証明する場合だけ受理し、証明のないzero root、non-empty zero root、mixed rootを拒否します。

incompleteまたはExactでない観測からcanonical final observationとdigestを構成しません。

event journalは`BundleRegistered`、`ObservationCompleted`、`ActionPlanned`、`ActionStarted`、`ActionFinished`、`ReceiptSaved`のappend-only記録を持ちますが、eventの存在、欠落、順序、monotonic値だけでuploadやreceiptを許可しません。

restartはSQLite stageをauthorityにせず、immutable bundleとfresh remote observationから再計画します。

callerはfull remote keyを渡しません。

Protocol V1 helperがcampaign-relative keyを導出し、trusted `r2.Layout`がimmutable root、dataset、provider、feed、symbol、campaign prefixを一度だけprependします。

publication receipt v1は、M2が作成したpublisher claimのexact key、canonical bytes、claim domain digest、final observation digest、bundle digest、exact raw manifest relative/full keyとdomain digest、検証済みraw objectのfull key、全Parquet objectのfull key、hash、bytes、inclusive range、part manifestのfull keyとdomain digest、replay manifestのfull keyとdomain digest、完全なConversionTuple、全resource limit、`part_set_root`、canonical row-chain root、`verification_complete=true`をbindします。

receiptはcompleteなfinal observationからだけ構成し、journal intent hash、stage順位、retry文字列、ETag、local path、credential、future remote stateをauthorityとして使いません。

M2 raw publicationがpublisher claimを`If-None-Match: *`で作成するため、M3 replayはclaimを作成せず、既存claimがExactであることをraw manifestと全raw objectのExact検証より先に確認します。

claimがAbsent、Different、Ambiguous、Oversized、Unavailableの場合、M3はderivative uploadとreceipt保存を行いません。

claim、raw manifest、参照raw objectのAbsentは、M2で存在が証明済みの必須dependencyが失われたintegrity stopです。

candidate Parquet、part manifest、replay manifestのAbsentだけが、直前barrierを全てExactで満たした場合のupload候補です。

M3の観測分類は`Absent`、`Exact`、`Different`、`Ambiguous`、`Oversized`、`Unavailable`です。

`Unavailable`はretry可能ですがactionを0件にし、`Different`と`Ambiguous`はintegrity stop、`Oversized`はresource-limit stopにします。

list、GET、stream、`check --download`のfailureを`Absent`へ変換せず、candidate-only exact Parquetだけをpart manifest未公開時のresumable objectとして扱います。

raw manifestと全参照raw objectがExactでない限り、Parquet、part manifest、replay manifestのactionを計画しません。

part manifestはExactなParquet objectの後にだけ現れ、replay manifestはExactでcompleteなpart chainの後にだけ現れます。

final observation digestはpoint-in-time evidenceであり、future mutation、admin-resistant WORM、failover、handover、remote transactionを証明しません。

receiptへcredential、environment secret、access tokenを保存しません。

successorはscopeとpublisher epochを変更せず、chain startを保持し、chain endは同じか前方へ延長します。

successorのaccepted count、error count、source watermark、capture watermarkは減少しません。

raw-day manifest digestとreplay-day manifest digestは、hash-domains.mdの専用domainで計算します。

`raw_set_root`は、同じ文書のordered rangeをhash-domains.mdの専用domainで計算します。
