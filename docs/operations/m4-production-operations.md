# M4 production operation runbook

このrunbookは、scopeごとに分離したGateway、WAL、journal、outbox、publisher、readerを
運用するための安全側の手順です。値の具体例を除き、credential、endpoint、実データ、
absolute pathをrepositoryへ記録しません。

## 共通不変条件

- scope key、campaign、exact symbol、publisher epochを起動前に確認する。
- WAL、journal、outbox、receipt、lock、cacheのrootを別scopeと共有しない。
- remote objectはimmutableで、delete、move、sync、overwriteを通常手順に含めない。
- ACK済みentryを証明なしに捨てない。disk pressureはavailability failureとして扱う。
- pruning、restoreはfresh observationとdigestを記録し、画面表示だけを根拠にしない。
- incident logはsecret scannerを通し、credential valueやtokenを貼らない。

## 起動と日次確認

1. config schema、scope identity、writable root、credential scope、listen addressを確認する。
2. Gatewayを初期化し、statusを保存する。

   ```text
   tick-gateway init --config <gateway-config>
   tick-gateway status --config <gateway-config>
   ```

3. `disk_class`、WAL availability、oldest retained sequence、source lag、publisher epoch、
   last verificationを確認してから`run`へ進む。
4. reader/APIはread-only credentialで起動し、loopback default、non-loopback policy hook、
   `Cache-Control: no-store`、request IDを確認する。

## Disk pressure and pruning

High watermarkではseal・publish・verify・pruneをoperator actionとして明示する。criticalまたは
emergencyではreadinessを落とし、proofなしにACKを進めない。

`tick-gateway prune-local`はstrict `tick-retention-v1` config、durable wall-clock watermark、
read-only R2 backend、bounded fresh observation、manifest coverage、retention proofを使って
candidateを作る。remote outage、clock regression、proof不足はfail-closedでblockedになる。
実削除はdry-runのplan digestを再確認した`--execute`だけで行い、real R2でのoperator validationは
M4-8/M4-9の外部証跡として別途必要である。

1. statusとfilesystem usageを保存し、別scopeのrootでないことを確認する。
2. strictなread-only retention config、Gatewayと同じ完全なscope、durable wall-clock watermarkを確認する。
3. dry-runでcanonical planとdigestを作る。

   ```text
   tick-gateway prune-local --config <gateway-config> --retention-config <retention-config> --dry-run
   ```

4. plan digest、candidate identity、fresh remote observation、grace、checkpoint、blocked reasonを
   2名で確認する。現行CLIのplanにfresh observationまたはproofがない場合は、digestを実行承認に
   使わず停止する。候補が一つでも不明、branch、clock regression、recovery依存なら停止する。
5. dry-run出力の`plan_digest`と`plan_current_wall_time_unix_ms`を2名で確認し、同じdigestと
   frozen plan timeを指定して実行する。local/remote factsが変わればdigest mismatchで停止する。

   ```text
   tick-gateway prune-local --config <gateway-config> --retention-config <retention-config> --execute --plan-digest <sha256> --plan-time-unix-ms <ms>
   ```

6. checkpoint/trash/unlinkの各結果を保存し、再起動後にretained chainを再検証する。digest mismatch、
   symlink、root escape、missing proofは自動repairせずintegrity stopにする。

raw outboxの完了記録は`prune-completions/`へcanonical retention proof、artifact identity、元の
plan digestをdurableに保存する。次回inventoryではこのmetadata directoryをartifactから除外し、
sourceが残る場合は同一bytesとしてunlink retry candidateにし、sourceが既にunlink済みならproof-bound
recovery candidateとして再構成する。別のdry-runを作り直す場合もcompletion記録のhistorical plan digestは監査用に保持し、durable clock進行後のcurrent planを許容する。ただしcompletionが示すartifact identityとcanonical proofが現在のplan
actionへ一致し、remote object keyとcovering manifest keyがcurrent immutable scope/date prefixへ一致することを必須とする。completion directoryの手動編集・削除・移動はintegrity stopとする。

## 保存期限と容量見積もり

次の値はM4の運用baselineであり、実際のscopeではoperatorがbroker、source rate、障害対応時間、
契約上の保存要件から上書きしてrun recordへ固定します。保存期限は削除権限ではありません。
fresh read-only observation、canonical retention proof、durable wall-clock、最小graceが揃わない
artifactは期限を過ぎても削除せず、disk pressureをavailability failureとして扱います。

| artifact | default operational horizon | minimum hold / deletion boundary |
| --- | --- | --- |
| active WAL | process lifetime | ACK済みentryのrecovery boundaryが不要になるまで削除不可 |
| sealed raw WAL | proof成立後7日 | fresh proof成立時刻から`grace_ms`（baseline 24時間）経過後、連続prefixだけ |
| raw outbox | proof成立後7日 | raw proof、covering manifest、同一bytes再検証、completion record、graceが全て成立した後 |
| replay outbox / cache | 無期限（M4ではblocked） | replay専用proofが未実装のため削除不可。raw proofを流用しない |
| publisher receipt | 365日、または依存artifactの保存終了後365日の長い方 | credential valueを含まないredacted recordのみ。手動削除・改変不可 |
| prune checkpoint / completion metadata | 依存するlocal artifactとchainの寿命後365日 | checkpoint chainと監査に必要な間は削除不可 |
| diagnostic log / incident record | 通常30日、incidentは365日またはfinal audit完了後90日の長い方 | secret scan済みのredacted logだけを保存 |
| M4 external evidence | 180日またはfinal audit完了後90日の長い方 | raw artifact digest、toolchain、scope digest、retention期限をsummaryへbind |

各scopeの最低ディスク容量は、次の保守的な見積もりを使って起動前に記録します。
`R_wal`は最大WAL bytes/s、`T_outage`は想定remote outage秒、`T_operator`はoperator対応秒、
`T_recovery`は再起動・再検証秒、`A`はactive WAL headroom、`outage_budget_days`はproofが
成立しないremote outageと対応期間、`raw_outbox_retention_days`と`grace_days`は表のraw outbox
horizonと最低graceです。outage期間とproof成立後の保持期間は重ならない前提で加算します。

```text
minimum_wal_bytes >= 1.25 * R_wal * (T_outage + T_operator + T_recovery) + A
minimum_raw_outbox_bytes >= 1.25 * avg_published_bytes_per_day * (outage_budget_days + raw_outbox_retention_days + grace_days)
minimum_metadata_bytes >= 1.25 * daily_receipt_checkpoint_log_bytes * metadata_retention_days
```

実測値が見積もりを超えた場合は保存期限やproof条件を短縮せず、high watermarkでoperator action、
critical/emergencyでreadiness低下とACK停止へ遷移します。見積もり、実測ピーク、disk watermark、
scope digest、確認者を外部run recordへ保存します。

## Restore and independent verification

restoreは空の別cacheとread-only credentialで開始する。soak hostのcacheやwriter credentialを
再利用してpassを作らない。

1. reader configのstrict version、endpoint、bucket、credential bundle path、size limits、immutable rootを確認する。
2. raw-day manifestとcampaign rootを`tick-verify`で検証する。
3. replay-day、part manifest、Parquet schema/hash/row chain、API fetch planを順に検証する。
4. remote outage、metadata oversized、manifest conflict、client cancelはpartial successにせず、
   request IDとtyped errorだけを記録する。

## Credential change operation

credentialの交換はGatewayを停止した市場休場中に行う。稼働中のwriter切替や、R2上のオンライン切替artifact作成は行わない。

1. Gatewayを停止し、WAL、journal、raw outboxのdurable状態を確認する。
2. R2 provider側でcredentialを交換し、Gateway設定のcredential bundleを更新する。
3. Gatewayを起動し、status、R2接続、既存objectの読み書きを確認する。
4. 確認完了までpruneや本番データ操作を行わない。

## R2 outage and upload failure

R2 S3 APIのtimeoutや5xxでは、retry回数・backoff・最終状態を記録する。immutable objectの同一
bytes retry以外は停止し、different bytes、scope外key、unknown write outcomeはfresh verificationなしに成功扱いしない。
R2復旧後はfresh observation、manifest chain、object hashを検証してからpublicationまたはACKを再開する。

## Forced reboot and recovery

reboot前にrun identity、WAL status、journal checkpoint、disk classを保存する。再起動後の
`tick-gateway status`は完全なread-only probeではなく、`app.LocalGatewayRuntime`を明示的に構築して
`New`→`Start`→`Stop`を行い、WAL/journal recoveryを実施しうるrecovery-capable commandである。
実行前にsnapshotとoperator approvalを取り、出力と
recovery eventを保存しながらWAL segment、checkpoint、journal、outbox、publisher epochを検証する。
矛盾があれば新しいbatchをACKせず、原因と最後のdurable boundaryをescalateする。

## Incident handoff

incident recordには開始/終了時刻、scope key digest、process identity digest、event、expected state、
observed state、operator action、recovery root、artifact digest、next ownerを記録する。データ欠落、
proofなしprune、split brain、credential境界違反、secret露出があればrunをpassにせず、
`delivery_status: incomplete`を維持する。
