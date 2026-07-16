# M4 isolated real-R2 smoke and handover

このrunbookはproductionの`v1/` namespaceを使わず、synthetic scopeだけで
real R2の読み書き境界を確認するためのものです。2026-07-16時点では
raw-only smokeとM4 handover harnessのskip記録はありますが、external phaseは未実施です。
この文書だけではM4-8をcompletedにしません。

## Safety boundary

- production bucket、production prefix、bucket-lock対象prefixを使わない。
- 実行ごとにoperatorが生成した一意のrun identityを使い、現行raw smokeでは
  `m2-smoke/m4-<run-id>/`のような隔離namespaceに限定する。M4専用harnessを追加する場合も
  `m4-smoke/<run-id>/`相当の隔離namespaceへ固定する。`..`、backslash、改行、
  `v1/`直下を許可しない。
- old writer、new writer、read-only readerは別credentialとし、credential value、
  endpoint、account IDをlog、JSON、commitへ書かない。
- remote objectのdelete、move、sync、overwriteは行わない。probeが誤ってwrite
  成功した場合も削除で後始末せず、runをfailとして隔離prefixを保持する。
- R2 uploadはAWS SDK for Go v2のS3 API境界だけを使い、runtimeに外部転送toolを要求しない。

## Existing raw smoke

現行の実行可能なraw smokeは`real_r2_smoke` build tagの
`internal/delivery/real_r2_smoke_test.go`です。次の全環境変数を、値を表示しない
secret storeまたはprocess environmentから注入します。

`TICK_M2_REAL_R2_SMOKE`、`TICK_M2_REAL_R2_CONFIRM`、`TICK_M2_REAL_R2_BUCKET`、
`TICK_M2_REAL_R2_PREFIX`、`TICK_M2_REAL_R2_ENDPOINT`、`AWS_ACCESS_KEY_ID`、
`AWS_SECRET_ACCESS_KEY`

実行コマンド:

```text
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags real_r2_smoke ./internal/delivery -run TestOptionalRealR2Smoke -count=1 -v
```

実行時はtest outputをrepository外のevidence storeへ保存し、raw logのdigestだけをsummaryへ
転記します。testがsecretを表示しないことを確認した上で、`-json`の原文を削除・編集せずに
保存してください。

```text
set -euo pipefail
EVIDENCE_ROOT="/path/to/external-evidence"
mkdir -p -- "$EVIDENCE_ROOT"
set +e
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags real_r2_smoke ./internal/delivery -run TestOptionalRealR2Smoke -count=1 -json \
  | tee -- "$EVIDENCE_ROOT/m2-real-r2-raw.json"
pipeline_status=("${PIPESTATUS[@]}")
set -e
test_status=${pipeline_status[0]}
tee_status=${pipeline_status[1]}
if [ "$tee_status" -ne 0 ]; then exit "$tee_status"; fi
if [ "$test_status" -ne 0 ]; then exit "$test_status"; fi
sha256sum -- "$EVIDENCE_ROOT/m2-real-r2-raw.json"
```

このtestはrandom run suffixを付けたsynthetic raw publication、immutable retry、
empty-cache read、day/campaign verificationを行います。`TICK_M2_REAL_R2_PREFIX`
は現行testのstrict boundaryにより`m2-smoke/`で始まる必要があります。

2026-07-16の実行は必要なopt-inとcredentialがないため、次の理由でskipしました。

```text
set TICK_M2_REAL_R2_SMOKE=1 to enable the isolated real-R2 smoke
```

これは失敗でもpassでもなく、M4-8 external evidenceの未実施記録です。

## M4 handover phase

M4のhandover contractは`internal/r2/handover.go`とnetwork-free testで実装・検証
されています。実R2のphase分離harnessは`m4_real_r2_smoke` build tagの
`internal/r2/real_m4_smoke_test.go`です。通常のtestには含まれず、`prepare`と`verify`を
別プロセスで実行します。

共通の非secret環境変数は次のとおりです。credential value自体は、ここで指定する環境変数名の
先にowner-onlyで注入し、testはその値をlogしません。

`TICK_M4_REAL_R2_SMOKE=1`、`TICK_M4_REAL_R2_PHASE`（`prepare`または`verify`）、
`TICK_M4_REAL_R2_CONFIRM=I_UNDERSTAND_M4_NO_OVERWRITE`、`TICK_M4_REAL_R2_BUCKET`、
`TICK_M4_REAL_R2_PREFIX`（`m4-smoke/<run-id>`またはその下位prefix）、
`TICK_M4_REAL_R2_ENDPOINT`、任意の`TICK_M4_REAL_R2_REGION`、
`TICK_M4_REAL_R2_RUN_ID`（英数字、`_`、`-`のみ）を使います。prefixはrun IDで始まり、
`..`、backslash、改行、`v1/`直下を許可しません。

`prepare`は次のcredential環境変数名を追加で要求し、old writerだけでepoch 1の
synthetic prior claimをconditional createします。

`TICK_M4_REAL_R2_OLD_ACCESS_KEY_ENV`、`TICK_M4_REAL_R2_OLD_SECRET_KEY_ENV`

```text
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags m4_real_r2_smoke ./internal/r2 -run TestOptionalM4RealR2HandoverSmoke -count=1 -v
```

`prepare`と`verify`は別のraw artifactへ保存します。`verify`の出力にはold process stop、
credential revoke、old/new/read-only probe、transition、next claimのtyped結果が含まれるため、
credential valueやendpointを追加で出力しないでください。

```text
set -euo pipefail
EVIDENCE_ROOT="/path/to/external-evidence"
mkdir -p -- "$EVIDENCE_ROOT"
phase="${TICK_M4_REAL_R2_PHASE:-}"
case "$phase" in
  prepare|verify) ;;
  *) echo "TICK_M4_REAL_R2_PHASE must be prepare or verify" >&2; exit 2 ;;
esac
artifact="$EVIDENCE_ROOT/m4-real-r2-$phase.json"
set +e
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags m4_real_r2_smoke ./internal/r2 -run TestOptionalM4RealR2HandoverSmoke -count=1 -json \
  | tee -- "$artifact"
pipeline_status=("${PIPESTATUS[@]}")
set -e
test_status=${pipeline_status[0]}
tee_status=${pipeline_status[1]}
if [ "$tee_status" -ne 0 ]; then exit "$tee_status"; fi
if [ "$test_status" -ne 0 ]; then exit "$test_status"; fi
sha256sum -- "$artifact"
```

`verify`は、operatorがold processを停止しold credentialをrevokeした後に、次の追加環境変数と
明示確認を要求します。

`TICK_M4_REAL_R2_OLD_ACCESS_KEY_ENV`、`TICK_M4_REAL_R2_OLD_SECRET_KEY_ENV`、
`TICK_M4_REAL_R2_NEW_ACCESS_KEY_ENV`、`TICK_M4_REAL_R2_NEW_SECRET_KEY_ENV`、
`TICK_M4_REAL_R2_READ_ACCESS_KEY_ENV`、`TICK_M4_REAL_R2_READ_SECRET_KEY_ENV`、
`TICK_M4_REAL_R2_OLD_CREDENTIAL_ID`、`TICK_M4_REAL_R2_PROCESS_STOP_EVIDENCE_DIGEST`、
`TICK_M4_REAL_R2_PROCESS_STOPPED_AT_MS`、`TICK_M4_REAL_R2_CREDENTIAL_REVOKED_AT_MS`、
`TICK_M4_REAL_R2_OPERATOR_ID`、
`TICK_M4_REAL_R2_OPERATOR_CONFIRM=I_UNDERSTAND_OLD_PROCESS_STOPPED_AND_CREDENTIAL_REVOKED`

verifyはnew writerによるprior claim read、old writerのpermission-denied probe、
typed operator evidence（process stop digestとprovider revoke時刻のoperator入力）、
artifact→transition→next claimのfresh bounded handover、same/different-content conditional
collision、canceled read、別read-only credentialによるnext claimとcandidate inventory、
read-only write-denied probeとprobe objectの不存在確認を行います。process停止とprovider側
revokeそのものは外部operator証跡で確認し、harnessの入力だけを独立証明とは扱いません。

2026-07-16時点では、必要なopt-inとcredentialがないためharnessもskipしました。

```text
set TICK_M4_REAL_R2_SMOKE=1 to run the isolated real-R2 handover smoke
```

これは失敗でもpassでもなく、M4-8 external evidenceの未実施記録です。

実施時も、以下のoperator手順を固定条件として守ります。

1. synthetic scopeのprior publisher claimをepoch 1で作成し、old writer credential
   だけで隔離prefixへconditionalに保存する。
2. old Gateway/publisher processの停止を確認し、process identity digestと停止時刻だけを
   `process-stop-evidence-v1`へ記録する。
3. old write credentialをprovider側でrevokeし、credential ID digest、scope digest、
   revoke時刻だけを`credential-revocation-evidence-v1`へ記録する。revokeを推測してはならない。
4. operatorがseal digest、scope key、prior epochを確認し、
   `operator-handover-confirmation-v1`を別に記録する。
5. fresh bounded readでprior claim、artifact、transition、next claim、candidate
   namespaceを観測し、trusted `Layout`から導出したkeyだけでartifact、transition、next
   claimをconditional createする。任意keyをcommand lineから受け取らない。
6. old credentialが一意のwrite probeで`AccessDenied`相当となり、objectが作成されない
   ことを確認する。new writerだけでsame-content retry、different-content collision、
   timeout後のfresh observation、次epochのimmutable publicationを確認する。
7. 空の別cacheと別read-only credentialでraw-day、参照sealed WAL、campaign chain、
   replay manifest、part manifest、Parquet identity、fetch planを再検証する。
8. read-only credentialで一意のwrite probeが拒否され、objectが作成されないことをharnessと
   provider audit log、redacted operator recordで確認する。writeが成功した場合はfailであり、
   probe objectをdeleteして証跡を隠してはならない。

## Required evidence

repository外に次のsecret-free summaryを保存し、tracked verification recordから
artifact digest、保存場所、retention期限だけを参照します。

- run identity、commit、OS、Go version、R2 SDK publication config digest
- synthetic scope key digest、immutable prefix digest、seal digest
- old process stop、old credential revoke、operator confirmationの3つのdigestと時刻
- old write denied、new write accepted、read-only write deniedのredacted status code
- artifact/transition/next claimのobject digestとfresh observation結果
- raw/replay/read-only verificationのreport digest
- 実行開始・終了時刻、retry回数、timeout/recoveryの結果

raw artifactのsha256、保存場所の非secret参照、retention期限を
[`m4-external-evidence-template.md`](../verification/m4-external-evidence-template.md)の
`report_digest`と`artifact_store`へ転記します。skipまたはtest logだけの画面確認ではpassに
しません。

credential value、endpoint、bucket名、local absolute pathは保存しません。
