# Isolated R2 smoke

このrunbookはproductionの`v1/` namespaceを使わず、synthetic scopeだけで
AWS SDK for Go v2によるR2読み書き境界を確認するためのものです。
credentialの変更やGatewayの再起動は市場の休場中にoperatorが行い、実行中のprocess切替は行いません。
2026-07-16時点ではexternal phase未実施です。この文書だけではM4-8をcompletedにしません。

## Safety boundary

- production bucket、production prefix、bucket-lock対象prefixを使わない。
- 通常のimmutable rootの下にGatewayごとの隔離namespaceを使い、その下に実行ごとのUTC run identityを置く。
  形は`<immutable-root>/gateway=<gateway-id>/run=<utc-run-id>/source=smoke/`とする。
  既存の`run=<utc-run-id>/`を再利用しない。古いdescriptorが同じrootに残ると、readerが別scopeとして検出し、
  smoke verificationが失敗する。
  `..`、backslash、改行、root側の`smoke` path segmentを許可しない。
- credential value、endpoint、account IDをlog、JSON、commitへ書かない。
- remote objectのdelete、move、sync、overwriteは行わない。probeが誤ってwrite成功した場合も
  削除で後始末せず、runをfailとして隔離prefixを保持する。
- R2 uploadはAWS SDK for Go v2のS3 API境界だけを使い、runtimeに外部転送toolを要求しない。

## Raw smoke

実行可能なraw smokeは`r2_smoke` build tagの
`internal/delivery/r2_smoke_test.go`です。リポジトリ直下にignoredな`env.local`を置くと、
testが起動時に読み込みます。すでにprocess environmentへ設定されている値はenv.localより優先します。
`env.local.example`を雛形として使えます。testはsecret valueを表示しません。

必要な値は次の6つです。`SMOKE=1`のような追加flagや固定confirmation文字列は使いません。
smoke実行であることは共通レイアウトの`source=smoke`で表し、Gatewayの所属は
`TICK_GATEWAY_INSTANCE_ID`で表します。`TICK_R2_IMMUTABLE_ROOT`には通常のimmutable rootだけを入れ、
`smoke`、`gateway=...`、`run=...`、`source=...`、`symbol=...`は入れません。

`TICK_R2_BUCKET`、`TICK_R2_IMMUTABLE_ROOT`、`TICK_R2_ENDPOINT`、`TICK_R2_ACCESS_KEY_ID`、
`TICK_R2_SECRET_ACCESS_KEY`、`TICK_GATEWAY_INSTANCE_ID`

`TICK_R2_SECRET_ACCESS_KEY`にはR2 S3のSecret Access Key / Client Secretを入れます。
Cloudflare API token valueそのものを入れてはいけません。R2 S3のSecret Access Keyは、
R2 API token発行時に表示される値、またはCreate Token APIの`value`から導出されるSHA-256です。

```text
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags r2_smoke ./internal/delivery -run TestR2Smoke -count=1 -v
```

testは`TICK_R2_IMMUTABLE_ROOT`を上位rootとして使い、その下に`gateway=<gateway-id>`、
さらにUTC時刻ベースの`run=<utc-run-id>`を自動生成します。
`source=smoke`と`symbol=<symbol>`以降は本番と同じR2 layoutで生成します。
実際のR2 prefixは概ね次の形です。

```text
<immutable-root>/
  gateway=<gateway-id>/
    run=<YYYYMMDDTHHMMSS.nnnnnnnnnZ>/
      source=smoke/
        symbol=EURUSD/
          scope-descriptor-v1.json
          publisher-claims/epoch=1.json
          objects/raw/wal-<sha256>.rtw
          snapshots/raw/day-definition=utc/date=2024-03-09/raw-day-1-<sha256>.json
```

実行時はtest outputをrepository外のevidence storeへ保存し、raw logのdigestだけをsummaryへ転記します。

```text
set -euo pipefail
EVIDENCE_ROOT="/path/to/external-evidence"
mkdir -p -- "$EVIDENCE_ROOT"
set +e
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags r2_smoke ./internal/delivery -run TestR2Smoke -count=1 -json \
  | tee -- "$EVIDENCE_ROOT/r2-smoke-raw.json"
pipeline_status=("${PIPESTATUS[@]}")
set -e
test_status="${pipeline_status[0]}"
tee_status="${pipeline_status[1]}"
if [ "$tee_status" -ne 0 ]; then exit "$tee_status"; fi
if [ "$test_status" -ne 0 ]; then exit "$test_status"; fi
sha256sum -- "$EVIDENCE_ROOT/r2-smoke-raw.json"
```

2026-07-16の実行は必要なR2 envとcredentialがないためskipしました。これは失敗でもpassでもなく、
M4-8 external evidenceの未実施記録です。

2026-07-17の実行はrepository-local `env.local`からR2環境変数を読み込みましたが、R2側の
`remote permission denied`で失敗しました。token、bucket scope、endpoint/account対応、Object Read & Write権限を
修正してから再実行します。この失敗もM4-8 completion evidenceではありません。

同日、bucket設定修正後の再実行は`2026-07-17T15:42:03+09:00`開始、
`2026-07-17T15:42:08+09:00`終了でpassしました。その後、二重run階層を削除し、短いsmoke専用scopeへ
変更した当時のコードでは、誤って`smoke/gateway=<gateway-id>/run=<UTC-based-run-id>`階層を使いました。
この階層へ変更後の実R2 smokeは`2026-07-17T16:11:38+09:00`に再実行しましたが、
R2側の`remote permission denied`で失敗しました。bucket/token scopeの確認後に再実行します。
`#`を含む`EURUSD.pro#`のsmokeも`2026-07-17T16:23:35+09:00`に実行しましたが、同じく
`remote permission denied`でした。直後に通常の`EURUSD`でも同じ失敗を確認したため、この結果は
`#`固有ではなく現在のR2 envのbucket/token scope問題です。

同日、R2 SDKのvirtual-host-style requestへtest doubleを追従させ、smoke buildを通常checkへ追加した後、
実R2 smokeを再実行しました。`env.local`で`TICK_R2_SECRET_ACCESS_KEY`と`TICK_GATEWAY_INSTANCE_ID`が
同一行に連結されていたため、R2署名が`SignatureDoesNotMatch`で失敗しました。
`env.local`の改行を修正した後、`2026-07-17T18:08:37+09:00`までに
`go test -tags r2_smoke ./internal/delivery -run TestR2Smoke -count=1 -v`を再実行し、
`TestR2Smoke`と`TestR2SmokeSymbolWithHash`の両方がpassしました。
その後、root側に`smoke` path segmentを置く実装は誤りとして修正し、smoke識別は
共通レイアウト上の`source=smoke`だけで行う設計へ戻しました。
修正後、`env.local`の`TICK_R2_IMMUTABLE_ROOT`を`v1`へ更新し、本番appのlayout生成も
同じ`gateway=<gateway-id>/run=<UTC-based-run-id>` helperを通すように変更しました。
`2026-07-17T18:21:47+09:00`までに同じ実R2 smokeを通常コマンドで再実行し、
`v1/gateway=<gateway-id>/run=<UTC-based-run-id>/source=smoke/...`階層でpassしました。
credential value、endpoint、bucket名、object keyは
tracked artifactへ記録していません。M4-8完了には、別cache/read-only credential verificationと
外部evidence digestの保存が別途必要です。

## Credential change operation

credentialの交換が必要になった場合は、Gatewayを停止した市場休場中に次の手順を行います。

1. Gatewayを停止し、WAL、journal、raw outboxのdurable状態を確認する。
2. R2 provider側でcredentialを交換し、Gateway設定のcredential bundleを更新する。
3. Gatewayを起動し、status、R2接続、既存objectの読み書きを確認する。
4. 確認完了までpruneや本番データ操作を行わない。

この手順を自動化するオンライン切替artifact、旧writer検証、epoch切替機能は提供しません。

## Required evidence

repository外に次のsecret-free summaryを保存し、tracked verification recordから
artifact digest、保存場所、retention期限だけを参照します。

- run identity、commit、OS、Go version、R2 SDK publication config digest
- synthetic scope key digest、immutable prefix digest
- write result、same-content retry、different-content collision、read-only verificationのredacted status
- raw/read-only verificationのreport digest
- 実行開始・終了時刻、retry回数、timeout/recoveryの結果

credential value、endpoint、bucket名、local absolute pathは保存しません。
