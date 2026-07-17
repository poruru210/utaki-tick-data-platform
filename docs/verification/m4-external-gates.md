# M4 external gate record

このrecordは2026-07-16 (Asia/Tokyo)時点のM4-7以降の受入境界を固定する。

```yaml
delivery_status: incomplete
final_audit: pending
real_r2_raw_smoke: pass
real_r2_raw_smoke_evidence_retention: pending
real_r2_source_symbol_layout_smoke: failed_permission_denied
real_r2_symbol_hash_smoke: blocked_by_same_permission_denied
read_only_credential_verification: not_run
mt5_24h_soak: not_run
local_race: pass
ci_verification: pass
ci_verification_commit: 1ebecf724fb588eaeff556e664f6bb92e331eae2
ci_verification_pr: 5
```

## Current evidence

- 外部実行時は [`m4-external-evidence-template.md`](m4-external-evidence-template.md) を
  redacted summaryの固定様式として使う。raw artifactはrepository外へ保存し、tracked record
  にはdigest、時刻、tool version、retention期限だけを記録する。
- M4-7 network-free repository gate、focused tests、vet、format、diffはpassした。
- 2026-07-16のWAL branch anchor、scope-bound retention proof、filesystem child-directory hardening後も、権限付き`mise run check`、Go全package、Python 35件、fixture、ruff、gofmt、diff、vetを再実行してpassした。
- 10x load recordはfull requested durationをpacedで実行し、`pass: true`を記録した。
- local Linux-equivalent raceはloopback許可付きで`CGO_ENABLED=1`、GCC 15.2.0を使って全対象packageを実行し、exit code 0、failなし、`DATA RACE`なしでpassした。raw JSON digestは`b427d385ea1eb39bfb2c4f3ec488c954b626c3731bdc55f6b7ac966793641c94`で、M4-9で外部retentionを再確認する。
- PR #5のcommit `1ebecf724fb588eaeff556e664f6bb92e331eae2`では、pushイベントとpull_requestイベントのRepository check、Linux Race、Windows Raceが6実行すべてsuccessだった。push runは[Repository check](https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29464847156)、[Linux Race](https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29464847151)、[Windows Race](https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29464847154)、PR runは[Repository check](https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29464861491)、[Linux Race](https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29464861398)、[Windows Race](https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29464861412)である。Race JSON、compiler/toolchain metadataのcaptureとartifact uploadもsuccessだった。workflow artifactの保存期限は14日であり、raw evidenceの長期保存は未完了である。
- 2026-07-17にignoredなrepository-local `env.local`からR2環境変数を読み込み、`go test -tags r2_smoke ./internal/delivery -run TestR2Smoke -count=1 -json`を実行した。
  初回はbucket設定不一致によりサンドボックス外で`remote permission denied`だったが、bucket修正後の再実行は
  `2026-07-17T15:42:03+09:00`開始、`2026-07-17T15:42:08+09:00`終了でpassした。
  その後、二重run階層削除と短いsmoke専用scopeへの変更を行い、現在コードを`smoke/gateway=<gateway-id>/run=<UTC-based-run-id>`階層で
  `2026-07-17T15:47:41+09:00`開始、`2026-07-17T15:47:45+09:00`終了で再実行してpassした。
  v1のR2物理階層を`source=<source>/symbol=<symbol>`へ変更した後の再実行は、
  `2026-07-17T16:11:38+09:00`に`remote permission denied`で失敗した。
  `EURUSD.pro#`による`#`保持検証も`2026-07-17T16:23:35+09:00`に同じ`remote permission denied`で失敗し、
  通常の`EURUSD`も`2026-07-17T16:23:47+09:00`に同じ失敗だったため、`#`固有のR2 key問題ではなく
  現在のbucket/token scope問題である。
  bucket/token scope確認後に再実行が必要である。
  Gateway IDは実行主体として固定し、run IDはUTC時刻ベースで毎回一意にする。
  credential value、endpoint、bucket名、object keyは記録していない。
  raw JSONのrepository外長期保存とdigest記録はfinal auditまでに実施する。
- `tick-gateway prune-local`はstrict retention config、完全なgateway scope binding、read-only R2
  observer、durable proof、frozen plan timeを使う経路を持つが、real R2でのprune証跡はまだ実施していない。
- MT5 Windows、実broker、forced reboot host、24時間のexternal artifactは存在しない。
- 秘密値を表示しない環境監査では、R2 smoke用env key、`smoke` immutable root、Gateway ID、endpoint shape、credential shapeを確認した。
  credential value、endpoint、bucket名はtracked artifactへ記録しない。
- `.github/workflows/linux-race.yml`と`.github/workflows/windows-race.yml`はrace JSON、toolchain情報、
  commit/refをそれぞれ`linux-race-<run_id>`、`windows-race-<run_id>` artifactへ保存するため、
  外部実行後にM4-7の再利用可能な証跡を取得できる。Linux workflowはrunner上で`build-essential`
  を導入してC compiler不足を解消する。

## Required before M4-8/M4-9 acceptance

1. M4-7で取得したRace raw JSONを外部artifact storeへ保存し、digest、compiler/toolchain、retention期限をfinal auditへ添付する。Linux/Windows workflow artifactはこの保存先の代替経路である。
2. isolated synthetic R2 prefixでraw publication/read verificationを実行する。
3. 1 broker・1 exact symbolのMT5 runを24時間以上継続し、runbook記載のfaultを注入する。
4. 空の別cacheとread-only credentialでraw/replay/scope/part/Parquet/API fetch planを再検証する。
5. production prune CLIをreal R2の隔離scopeで実行し、bounded remote observation、manifest coverage、
   canonical retention proof、frozen plan time、dry-run/executeの実削除経路を再検証する。replay
   outbox/cacheは対応するproofがない限り削除対象に含めない。
6. redacted artifact digest、時刻、tool version、config digest、recovery resultを外部保存し、
   final auditのrequired actionをzeroにする。

skip、短縮、read/write credentialを使った自己検証、fault未注入はM4 completion evidenceではない。
