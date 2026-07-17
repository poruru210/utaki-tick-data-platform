# M4 isolated real-R2 smoke

このrunbookはproductionの`v1/` namespaceを使わず、synthetic scopeだけで
AWS SDK for Go v2によるR2読み書き境界を確認するためのものです。
credentialの変更やGatewayの再起動は市場の休場中にoperatorが行い、実行中のprocess切替は行いません。
2026-07-16時点ではexternal phase未実施です。この文書だけではM4-8をcompletedにしません。

## Safety boundary

- production bucket、production prefix、bucket-lock対象prefixを使わない。
- 実行ごとに一意のrun identityを使い、`m2-smoke/m4-<run-id>/`のような隔離namespaceに限定する。
  `..`、backslash、改行、`v1/`直下を許可しない。
- credential value、endpoint、account IDをlog、JSON、commitへ書かない。
- remote objectのdelete、move、sync、overwriteは行わない。probeが誤ってwrite成功した場合も
  削除で後始末せず、runをfailとして隔離prefixを保持する。
- R2 uploadはAWS SDK for Go v2のS3 API境界だけを使い、runtimeに外部転送toolを要求しない。

## Raw smoke

実行可能なraw smokeは`real_r2_smoke` build tagの
`internal/delivery/real_r2_smoke_test.go`です。次の環境変数をsecret storeまたはprocess
environmentから注入します。testはsecret valueを表示しません。

`TICK_M2_REAL_R2_SMOKE`、`TICK_M2_REAL_R2_CONFIRM`、`TICK_M2_REAL_R2_BUCKET`、
`TICK_M2_REAL_R2_PREFIX`、`TICK_M2_REAL_R2_ENDPOINT`、`AWS_ACCESS_KEY_ID`、
`AWS_SECRET_ACCESS_KEY`

```text
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags real_r2_smoke ./internal/delivery -run TestOptionalRealR2Smoke -count=1 -v
```

testはrandom run suffixを付けたsynthetic raw publication、immutable retry、empty-cache read、
day/campaign verificationを行います。`TICK_M2_REAL_R2_PREFIX`は`m2-smoke/`で始まる必要があります。

実行時はtest outputをrepository外のevidence storeへ保存し、raw logのdigestだけをsummaryへ転記します。

```text
set -euo pipefail
EVIDENCE_ROOT="/path/to/external-evidence"
mkdir -p -- "$EVIDENCE_ROOT"
set +e
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags real_r2_smoke ./internal/delivery -run TestOptionalRealR2Smoke -count=1 -json \
  | tee -- "$EVIDENCE_ROOT/m2-real-r2-raw.json"
pipeline_status=("${PIPESTATUS[@]}")
set -e
test_status="${pipeline_status[0]}"
tee_status="${pipeline_status[1]}"
if [ "$tee_status" -ne 0 ]; then exit "$tee_status"; fi
if [ "$test_status" -ne 0 ]; then exit "$test_status"; fi
sha256sum -- "$EVIDENCE_ROOT/m2-real-r2-raw.json"
```

2026-07-16の実行は必要なopt-inとcredentialがないためskipしました。これは失敗でもpassでもなく、
M4-8 external evidenceの未実施記録です。

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
