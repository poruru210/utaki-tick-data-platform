# M4 external gate record

このrecordは2026-07-16 (Asia/Tokyo)時点のM4-7以降の受入境界を固定する。

```yaml
delivery_status: incomplete
final_audit: pending
real_r2_raw_smoke: skipped
real_r2_handover: not_run
read_only_credential_verification: not_run
mt5_24h_soak: not_run
local_race: pass
```

## Current evidence

- 外部実行時は [`m4-external-evidence-template.md`](m4-external-evidence-template.md) を
  redacted summaryの固定様式として使う。raw artifactはrepository外へ保存し、tracked record
  にはdigest、時刻、tool version、retention期限だけを記録する。
- M4-7 network-free repository gate、focused tests、vet、format、diffはpassした。
- 2026-07-16のWAL branch anchor、scope-bound retention proof、filesystem child-directory hardening後も、権限付き`mise run check`、Go全package、Python 35件、fixture、ruff、gofmt、diff、vetを再実行してpassした。
- 10x load recordはfull requested durationをpacedで実行し、`pass: true`を記録した。
- local Linux-equivalent raceはloopback許可付きで`CGO_ENABLED=1`、GCC 15.2.0を使って全対象packageを実行し、exit code 0、failなし、`DATA RACE`なしでpassした。raw JSON digestは`b427d385ea1eb39bfb2c4f3ec488c954b626c3731bdc55f6b7ac966793641c94`で、M4-9で外部retentionを再確認する。
- `go test -tags real_r2_smoke ./internal/delivery -run TestOptionalRealR2Smoke -count=1 -v`
  はexplicit opt-inと実R2 credentialsがないためskipした。
- `internal/r2/real_m4_smoke_test.go`にphase分離したreal-R2 handover harnessを追加し、
  `tick-gateway prune-local`はstrict retention config、完全なgateway scope binding、read-only R2
  observer、durable proof、frozen plan timeを使う経路を持つが、
  real R2でのoperator handover phaseと24時間運用証跡はまだ実施していない。
- MT5 Windows、実broker、forced reboot host、24時間のexternal artifactは存在しない。
- 秘密値を表示しない環境監査でも、real-R2用env、Race compiler、MT5証跡はこのworkspaceに存在しないことを確認した。
- `.github/workflows/linux-race.yml`と`.github/workflows/windows-race.yml`はrace JSON、toolchain情報、
  commit/refをそれぞれ`linux-race-<run_id>`、`windows-race-<run_id>` artifactへ保存するため、
  外部実行後にM4-7の再利用可能な証跡を取得できる。Linux workflowはrunner上で`build-essential`
  を導入してC compiler不足を解消する。

## Required before M4-8/M4-9 acceptance

1. M4-7で取得したRace raw JSONを外部artifact storeへ保存し、digest、compiler/toolchain、retention期限をfinal auditへ添付する。Linux/Windows workflow artifactはこの保存先の代替経路である。
2. isolated synthetic R2 prefixでraw publication/read verificationを実行する。
3. `m4_real_r2_smoke`の`prepare`/`verify`をisolated synthetic prefixで実行し、M4 handoverの
   operator evidenceを取得する。old process stop、old credential revoke、old write denied、
   new write accepted、read-only write deniedを別々に確認する。
4. 1 broker・1 exact symbolのMT5 runを24時間以上継続し、runbook記載のfaultを注入する。
5. 空の別cacheとread-only credentialでraw/replay/campaign/part/Parquet/API fetch planを再検証する。
6. production prune CLIをreal R2の隔離scopeで実行し、bounded remote observation、manifest coverage、
   canonical retention proof、frozen plan time、dry-run/executeの実削除経路を再検証する。replay
   outbox/cacheは対応するproofがない限り削除対象に含めない。
7. redacted artifact digest、時刻、tool version、config digest、recovery resultを外部保存し、
   final auditのrequired actionをzeroにする。

skip、短縮、read/write credentialを使った自己検証、fault未注入はM4 completion evidenceではない。
