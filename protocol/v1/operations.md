# M4 Production Operation Contracts

この文書はM4-1で固定する、M3 derivative contractの外側にある運用契約です。
canonical JSONはProtocol V1と同じUTF-8、重複key拒否、整数のみ、辞書順key、末尾byteなしの規則を使います。

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
max_manifest_nodes
max_proof_bytes
max_proof_objects
max_prune_candidates
request_timeout_ms
```

全値は正のU64で、実装上限、`request_timeout_ms`のGo duration変換可能性を検査します。limit超過、overflow、unknown field、
incomplete objectはfail closedです。
