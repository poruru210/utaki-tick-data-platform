# M4 external evidence template

これは外部環境で取得した証跡のsecret-free summary templateです。原文のlog、R2
object、MT5 data、credentialはrepository外のoperator-controlled
storageへ保存し、このrecordにはdigest、時刻、tool version、保存期限だけを転記します。
このtemplateを埋めただけではpassになりません。各項目のraw artifactを独立に確認し、
`status: pass`と`final_audit.required_action_count: 0`を満たした場合だけM4 acceptanceへ
進めます。

## Evidence rules

- digestはlowercase SHA-256、値は改行なしで記録する。
- credential value、token、endpoint、bucket名、account ID、local
  absolute path、実データ、WAL、Parquet本文は記録しない。
- scope、prefix、run identity、credential ID、process identity、operator identityは、
  raw valueではなくstable redacted digestを記録する。
- 外部artifactにはcommit SHA、実行OS、architecture、tool version、開始/終了時刻、
  artifact retention期限を含める。
- `skip`、短縮run、fault未注入、write credentialだけを使ったverification、operator入力
  だけの自己申告はpass evidenceではない。
- write probeが成功した、証跡が欠落した、またはunknown outcomeを説明できない場合は、
  objectを削除して後始末せず`fail`として隔離し、`delivery_status: incomplete`を維持する。

## Summary record

次のYAMLを外部recordからredactして転記する。未実施項目は空欄にせず、`not_run`または
`skipped`と理由を明記する。

```yaml
record_version: m4-external-evidence-v1
delivery_status: incomplete
commit_sha: "<40 lowercase hex>"
run_id_digest: "<sha256>"
scope_digest: "<sha256>"
operator_id_digest: "<sha256>"
started_at_utc: "<RFC3339>"
finished_at_utc: "<RFC3339>"
toolchain:
  os: "<redacted runner/host class>"
  arch: "<GOARCH or host architecture>"
  go: "<go version>"
  r2_sdk: "<AWS SDK for Go v2 module versions or build digest>"
  mt5: "<version, or not_applicable>"
config_digest: "<sha256>"
artifact_store:
  location_ref: "<external store reference, no absolute path>"
  retention_until_utc: "<RFC3339>"
  summary_digest: "<sha256>"

race:
  status: not_run
  status_reason: "<required when status is not_run, skipped, or fail>"
  artifact_name: ""
  artifact_digest: ""
  workflow_run_id: ""
  package_set_digest: ""
  json_pass: false
  environment_artifact_digest: ""

real_r2:
  raw_smoke:
    status: not_run
    status_reason: "<required when status is not_run, skipped, or fail>"
    run_id_digest: ""
    prefix_digest: ""
    report_digest: ""
    fresh_read_report_digest: ""
    same_content_retry: false
    different_content_rejected: false
  prune:
    status: not_run
    status_reason: "<required when status is not_run, skipped, or fail>"
    dry_run_report_digest: ""
    plan_digest: ""
    plan_current_wall_time_unix_ms: 0
    scope_config_digest: ""
    retention_proof_digest: ""
    execute_report_digest: ""
    recovery_report_digest: ""
    read_only_observation_digest: ""

mt5_soak:
  status: not_run
  status_reason: "<required when status is not_run, skipped, or fail>"
  run_id_digest: ""
  duration_seconds: 0
  exact_symbol_digest: ""
  broker_scope_digest: ""
  baseline_report_digest: ""
  event_schedule_digest: ""
  fault_event_report_digest: ""
  restart_recovery_report_digest: ""
  forced_reboot_report_digest: ""
  resource_summary_digest: ""
  started_at_utc: ""
  finished_at_utc: ""

independent_verification:
  status: not_run
  status_reason: "<required when status is not_run, skipped, or fail>"
  empty_cache_digest: ""
  read_only_credential_id_digest: ""
  raw_day_report_digest: ""
  sealed_wal_report_digest: ""
  scope_report_digest: ""
  replay_report_digest: ""
  part_report_digest: ""
  parquet_report_digest: ""
  api_fetch_plan_report_digest: ""

final_audit:
  status: pending
  status_reason: "<required while pending or when status is fail>"
  required_action_count: 7
  secret_scan_report_digest: ""
  scope_exclusion_report_digest: ""
  artifact_retention_report_digest: ""
  runbook_execution_report_digest: ""
  reviewer_id_digest: ""
```

## Acceptance mapping

| Record section | Required observation | Pass condition |
| --- | --- | --- |
| `race` | `linux-race-<run_id>` or `windows-race-<run_id>` artifact from the corresponding workflow | All M4 packages pass; JSON and toolchain metadata match the commit |
| `real_r2.raw_smoke` | Isolated synthetic prefix, fresh read, immutable retry and collision behavior | No production `v1/` scope; read verification and negative cases pass |
| `real_r2.prune` | Strict config, complete scope binding, bounded read-only observation, frozen plan time | dry-run and execute bind to the same plan/proof; recovery is verified |
| `mt5_soak` | One broker, one exact symbol, one Gateway, all scheduled faults, at least 24 hours | Every injected event has expected/observed/recovery evidence; no unexplained gap or loss |
| `independent_verification` | Empty cache and separate read-only credential after soak | All raw, WAL, scope, replay, part, Parquet, and API checks pass |
| `final_audit` | Secret/scope/retention/runbook re-audit | every required field is evidenced and `required_action_count: 0` |

The external gate record should link this summary by `summary_digest` and should remain
`delivery_status: incomplete` until the final audit conditions are met.
