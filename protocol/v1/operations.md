# M4 Production Operation Contracts

この文書はM4-1で固定する、M3 derivative contractの外側にある運用契約です。
canonical JSONはProtocol V1と同じUTF-8、重複key拒否、整数のみ、辞書順key、末尾byteなしの規則を使います。

## Publisher handover artifact

handover artifactのcanonical objectは次の12 keyを必須とします。

```text
campaign_id
dataset_id
expected_next_claim_domain_digest
expected_next_claim_key
handover_version
next_publisher_epoch
operator_evidence_digest
prior_claim_domain_digest
prior_claim_key
prior_publisher_epoch
scope_key
transition_key
```

`handover_version`は`publisher-handover-v1`、digest domainは
`tick-data-platform/publisher-handover/v1\0`です。全digestはzeroでないlowercase SHA-256
hex、epochはU64かつ`next_publisher_epoch > prior_publisher_epoch`でなければなりません。

trusted immutable campaign prefixを`P`とすると、物理keyは次のように導出します。

```text
handover artifact: P/handover/next-epoch=N.json
prior claim:       P/publisher-claims/epoch=E.json
next claim:        P/publisher-claims/epoch=N.json
transition:        P/handover-transitions/next-epoch=N.json
```

bodyにはcredential、endpoint、local path、自由形式secretを含めません。decoderはunknown key、
missing key、duplicate key、non-canonical bytes、wrong suffix、zero digestをfail closedします。
artifact digestは`SHA256(domain || canonical_bytes)`です。

## Operator confirmation record

Typed process-stop and credential-revocation evidence is not an operator
approval. Before a remote action, the operator supplies a separate
`operator-handover-confirmation-v1` record with these fields:

```text
confirmation_version
confirmed
confirmed_at_unix_ms
operator_id_digest
prior_epoch
scope_key
seal_digest
```

The confirmation binds the exact handover seal digest, trusted scope key, and
prior epoch. `confirmed` must be true; the operator identity is represented by
a nonzero digest only. The record is checked again by the conditional
executor immediately before its bounded fresh observation and remote write.

## Retention proof

retention proofはProtocol wire messageではなく、local `internal/retention`が所有する
`retention-proof-v1`です。必須keyは次の通りです。

```text
artifact_kind, bytes, content_sha256, covering_manifest_digest,
covering_manifest_key, grace_not_before_unix_ms, limits,
observed_wall_time_unix_ms, proof_version, remote_observation,
replay_identity, scope_config_hash, trusted_relative_path, verification_report_digest,
wal_range
```

`wal_range`または`replay_identity`の一方だけを持ちます。remote observationは
`class=Exact`、full key、size、content digestを同時にbindします。`Different`、`Ambiguous`、
`Absent`、`Oversized`、`Unavailable`はproofになりません。`grace_not_before_unix_ms`は
観測時刻より前にできません。proof limitsは`max_proof_objects`、`max_proof_bytes`、
`max_manifest_nodes`を持ち、全てzeroまたは実装上限超過を拒否します。

WAL identityはstart/end sequenceとchain root、replay identityはdataset、campaign、UTC date、
manifest key/digest、part-set root、canonical row-chain rootを持ちます。
`scope_config_hash`はpublisher epoch、settle policy、protocol limitsを含むtrusted scopeの
canonical hashであり、recovered raw completionを別scopeへ再利用しません。

## Prune checkpoint

prune checkpointは`prune-checkpoint-v1`のappend-only local chainです。

```text
checkpoint_version
end_sequence
last_segment_sha256
previous_checkpoint_digest
retained_chain_root
retention_proof_digest
retention_proof
```

`end_sequence`とcurrent identity digestsはzeroを拒否します。前のcheckpoint digestは
genesisだけzeroを許可します。`retention_proof`はcanonicalな`retention-proof-v1`本体であり、
`retention_proof_digest`、checkpointed WAL range、last segment digestへbindします。executorは
checkpoint publish前のcrashでsegmentを削除しません。

## Operational resource limits

resource contractは`m4-operational-limits-v1`で、次のkeyを必須とします。

```text
limits_version
max_api_request_bytes
max_api_response_items
max_concurrent_requests
max_handover_observation_bytes
max_handover_observation_requests
max_manifest_nodes
max_proof_bytes
max_proof_objects
max_prune_candidates
request_timeout_ms
```

全値は正のU64で、実装上限、`max_proof_bytes <= max_handover_observation_bytes`、
`request_timeout_ms`のGo duration変換可能性を検査します。limit超過、overflow、unknown field、
incomplete objectはfail closedです。
