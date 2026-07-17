# Protocol V1のmessage

message payloadは、wire envelopeのpayload領域へfield順序どおりに連結します。

## 共通field encoding

`U8`、`U16`、`U32`、`U64`はunsigned little-endianです。

`I32`と`I64`はtwo's complementのsigned little-endianです。

`Hash256`は32 bytesのraw SHA-256 digestです。

`String`は、U16 lengthとUTF-8 bytesを連結します。

String lengthはprefixを含まず、0以上255以下です。

UTF-8でないbytes、length上限を超えるString、途中に終端のないfieldは拒否します。

## HelloV1

**`HelloV1`**：producerが接続対象のsourceと能力をGatewayへ通知するmessageです。

payload fieldsは次の順序で並べます。

| field | type |
| --- | --- |
| producer_instance_id | String |
| producer_session_id | String |
| producer_build_id | String |
| mql_compiler_build | String |
| terminal_build | String |
| os_contract | String |
| clock_api_id | String |
| provider_id | String |
| stable_feed_id | String |
| broker_server_fingerprint | String |
| exact_source_symbol | String |
| source_schema_id | String |
| acquisition_mode | U8 |
| initial_from_msc | I64 |
| capability_flags | U32 |

acquisition_modeの`1`はlive_follow、`2`はhistorical_backfillです。

未定義のacquisition_modeは拒否します。

## ResumeV1

**`ResumeV1`**：Gatewayが接続を受理した後に、producerへ再開位置とpoll上限を返すmessageです。

payload fieldsは次の順序で並べます。

| field | type |
| --- | --- |
| accepted_protocol_version | U16 |
| gateway_instance_id | String |
| session_lease_id | String |
| committed_cursor_msc | I64 |
| committed_boundary_digest | Hash256 |
| last_durable_batch_sequence | U64 |
| last_durable_batch_hash | Hash256 |
| next_from_msc | I64 |
| next_requested_count | U32 |
| maximum_frame_bytes | U32 |
| maximum_records | U32 |
| heartbeat_idle_timeout_ms | U32 |

`accepted_protocol_version`は1でなければなりません。

## BatchFrameV1

**`BatchFrameV1`**：一回のCopyTicks responseと、そのresponseに含まれるordered recordsを運ぶmessageです。

payload fieldsは次の順序で並べます。

| field | type |
| --- | --- |
| session_lease_id | String |
| producer_session_id | String |
| batch_sequence | U64 |
| requested_from_msc | I64 |
| requested_count | U32 |
| fetch_wall_start_s | I64 |
| fetch_wall_end_s | I64 |
| fetch_monotonic_start_us | U64 |
| fetch_monotonic_end_us | U64 |
| returned_count | I32 |
| copy_ticks_error | I32 |
| source_status_flags | U32 |
| source_schema_id | String |
| record_count | U32 |
| records | record_count repetitions of RawMqlTickV1 |

`returned_count`が負の場合もBatchFrameV1は有効であり、record_countは0でなければなりません。

`returned_count`が0以上の場合は、record_countはreturned_countと一致しなければなりません。

record_countはMAX_RECORDS以下でなければなりません。

## AckV1

**`AckV1`**：GatewayがBatchFrameV1をWALへ受理した結果と、次のpoll directiveを返すmessageです。

payload fieldsは次の順序で並べます。

| field | type |
| --- | --- |
| producer_session_id | String |
| batch_sequence | U64 |
| gateway_batch_sha256 | Hash256 |
| gateway_ingest_sequence | U64 |
| status | U8 |
| committed_cursor_msc | I64 |
| committed_boundary_digest | Hash256 |
| next_from_msc | I64 |
| next_requested_count | U32 |
| retry_after_ms | U32 |

statusの値は、1 ACCEPTED_ADVANCED、2 ACCEPTED_NO_ADVANCE、3 DUPLICATE、4 DENSE_BOUNDARY_CONTINUE、5 DENSE_BOUNDARY_UNRESOLVED、6 RETRYABLE_GATEWAY_ERROR、7 FATAL_PROTOCOL_ERROR、8 SOURCE_STATE_CONFLICT、9 SESSION_LEASE_CONFLICTです。

未知のstatusはAckV1全体を拒否します。

## ErrorV1

**`ErrorV1`**：frameまたはmessageを受理できない理由を返すmessageです。

payload fieldsは次の順序で並べます。

| field | type |
| --- | --- |
| code | U16 |
| retryable | U8 |
| related_message_type | U16 |
| related_batch_sequence | U64 |
| message | String |

codeの値は、1 INVALID_FRAME、2 UNSUPPORTED_PROTOCOL_VERSION、3 UNKNOWN_MESSAGE_TYPE、4 TRUNCATED_FRAME、5 OVERSIZED_FRAME、6 CRC_MISMATCH、7 INVALID_FIELD、8 SOURCE_STATE_CONFLICT、9 SESSION_LEASE_CONFLICT、10 INTERNAL_RETRYABLEです。

`retryable`は0または1です。

ErrorV1を返しても、失敗したframeをWALへ追加したことにはなりません。

## 互換性

message type、field順序、field type、enum値、失敗コードを変更するとProtocol V1の互換性が失われます。

変更時はwire、schema、fixture、conformanceを同じ変更単位で更新します。
