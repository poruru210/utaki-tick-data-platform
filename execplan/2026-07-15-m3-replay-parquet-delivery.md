# M3リプレイ派生とParquet配信を実装する

このExecPlanは生きた文書である。
実装と検証が進むたびに、`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective`を更新する。
リポジトリにはExecPlan方法論の`PLANS.md`が置かれていないため、この文書自体に再開に必要な実装手順、判断、検証方法を保持する。

M2のマージ済み基準は、Pull Request #3のマージコミット`cc9fc2d`である。
この計画はM3-0の文書作業から始まり、M3の実装、fixture、Parquet、公開、read-only配信、検証までを一つの再開可能な手順にする。
M3-0自体は文書と計画だけを変更し、M3実装を開始または完了したとは扱わない。

## Purpose / Big Picture

M3が完了すると、M2が不変保存したraw Tickから、pollingの重複を除いた順序付きreplay datasetを再生成できる。
利用者は、特定のdataset、campaign、UTC day、replay contract、conversionを選び、Parquet partと検証済みmanifestをread-onlyで取得できる。
同じ入力、同じ変換契約、同じ依存関係、同じwriter設定、同じplatform contractで再実行した場合、論理row、row-chain root、partのrow配置が一致する。
Parquetの全byte一致は、同じconverter build、dependency lock、writer configuration、platform contractの範囲でだけ要求する。

動作は、合成raw fixtureをfake backendへ投入し、replay manifest、part manifest、Parquet、receiptを生成してから、空のlocal cacheへ`tickctl`でfetchし、`tick-verify`でraw binding、part chain、row-chain、Parquet hashを検証することで確認する。
実R2は通常のgateではなく、隔離されたprefixと明示的なcredentialが揃った場合だけ行う非破壊smokeとする。

## Progress

- [x] (2026-07-15 12:00 +09:00) `agent/m3-replay-parquet-delivery`の`HEAD`がPR #3のマージコミット`cc9fc2d`であることを確認した。
- [x] (2026-07-15 12:05 +09:00) M2の`RawDayManifest`、publication journal、ArchiveReader、CLI、`go.mod`、Parquetの空packageを調査した。
- [x] (2026-07-15 12:20 +09:00) M3-1を実装ゲートにし、未確定のcanonical encodingとderivative publication identityを実装者の暗黙判断に残さない構成を決めた。
- [x] (2026-07-15 12:30 +09:00) M3-0のself-containedな実行計画を作成した。
- [x] (2026-07-15 14:10 +09:00、監査で却下) M3-1のProtocol V1契約、verified raw-day input境界、ordered overlap reducer、continuity segment、marker row、canonical row-chainの初回実装を作成したが、toyなcaller-supplied raw inputを受け入れるためgate通過とは扱わなかった。
- [x] (2026-07-15 16:20 +09:00、後続監査で証拠を無効化) M3-1C corrective reworkとして、M2のraw-day manifestとsealed WALを実際に検証するreader境界へ置き換え、selected rangeだけをstreaming reducerへ渡し、MaxRecordsでbounded tailを制限した。
- [x] (2026-07-15 16:25 +09:00) M3-1D監査で、multi-sequence range expansion、producer identity provenance、parent-contract longest-overlap、sink前validationの不足を例外として記録し、M3-1Cの受入証拠を無効化した。
- [x] (2026-07-15 12:42 +09:00) M3-1D corrective reworkとして、inclusive rangeをentryごとの座標へ展開し、leaseをproducer identityから再計算し、最長overlap、証明済みhistory change、sink前canonical validationを実装した。
- [x] (2026-07-15 12:42 +09:00、M3-1E監査で追加修正) M3-1Dのfocused Go `87 passed / 0 failed`、fixture `20 verified`、Python `18 passed / 0 failed`、`mise run check`、`go vet`、`git diff --check`が成功したため、M3-1 gateを再受入した。
- [x] (2026-07-15 12:50 +09:00) M3-1E corrective reworkとして、`SOURCE_HISTORY_CHANGED`をpersisted tail suffixとincoming prefixの境界だけへ制限し、内部windowによるfalse positiveを拒否した。
- [x] (2026-07-15 12:55 +09:00、M3-1E時点の証拠) M3-1Eのfocused Go `74 passed / 0 failed`、`mise run check`、`mise exec -- go vet ./...`、`git diff --check`が成功した。
- [x] (2026-07-15 13:12 +09:00、M3-1F例外監査で再オープン) compact rangeの部分選択を、入力manifestを上書きしたsemantic verifier呼び出しで受理できる状態が判明した。
  M3-1 gateは未完了へ戻し、M2 canonical selectionとの完全一致、campaign-relative keyの完全一致、ReplayResourceLimitsの事前検証がfocused/repository verificationを通過するまでParquet以降を開始しない。
- [x] (2026-07-15 13:25 +09:00、M3-1F実装・検証完了) 入力manifestを不変のまま検証する独立canonical比較、exact `ManifestRelativeKey`、ReplayResourceLimits、compact rangeの正負fixtureを追加した。
  focused Goは`66 passed / 0 failed / 0 skipped`、fixtureは`20 verified`、Pythonは`18 passed / 0 failed`、`mise run check`、`go vet`、`gofmt -l`空出力、`git diff --check`が成功した。
  advisor exception re-auditを依頼するが、M3-1 gateは外部再監査で解除されるまで未完了のままとする。
- [x] (2026-07-15 13:28 +09:00、M3-1F最終再検証) preflight resource checkを含む現行コードでfocused Go `66 passed / 0 failed / 0 skipped`、fixture `20 verified`、Python `18 passed / 0 failed`、`mise run check`、`go vet`、空の`gofmt -l cmd internal`、`git diff --check`を再確認した。
- [x] (2026-07-15 13:35 +09:00) advisor exception re-auditが`pass`となり、M3-1 gateを受け入れてParquet実装を解放した。
  例外再監査へhandoffし、M3-1 gateと下流実装の停止条件は維持する。
- [x] (2026-07-15 14:05 +09:00) compatibility spikeを`go1.24.13 windows/amd64`で実行し、候補tag、module GoVersion、Windows互換APIを確認したうえで`github.com/parquet-go/parquet-go v0.30.1`を`go.mod`と`go.sum`へ固定した。
- [x] (2026-07-15 14:25 +09:00、M3-2実装checkpoint) `ConversionSpec`、fixed nullable Parquet schema、bounded `Generator.WriteRow`/`Close`、close/sync/hash/reopen verifier、strict part/replay manifest builder/verifierを追加した。
  focused testは追加実装を含めてpassしているが、M3-2の最終gateは指定verification全体が終わるまで未完了とした。
- [x] (2026-07-15、M3-2 focused verification checkpoint) focused Goは`parquet 4`、`archive 17`、`protocol 32`、`continuity 30`の合計`83 passed / 0 failed`、fixture `20 verified`、Python `18 passed / 0 failed`、`mise run check`、`go vet`、空の`gofmt -l`、`git diff --check`に成功した。
  M3-2の実装受入証拠として記録し、publicationとdeliveryの停止条件は維持する。
- [x] (2026-07-15、M3-2 strict verification更新) promotion前のstreaming input canonical comparison、promotion後のreopen mutation検出、replay row-chain rootの外部summary bindingを追加し、同じverification一式を再実行して同じ`83 passed / 0 failed`、fixture `20 verified`、Python `18 passed / 0 failed`を確認した。
- [x] (2026-07-15、M3-2 Protocol strictness更新) `PartManifest.Validate`へnonzero bytes、chain anchor、row-range equalityを追加し、replay empty/non-empty root invariantをProtocol verifierへ反映した。
  focused Go、fixture、Python、check、vet、gofmt、diff checkを再実行して全て成功した。
- [x] (2026-07-15、M3-2 key binding更新) part/replay manifest verifierのexpected manifest keyを必須化し、空keyで検証を迂回できないようにした。
  focused Goとrepository gate一式を再実行し、失敗なしを確認した。
- [x] (2026-07-15、後続M3-2Gで解消済みのM3-2レビュー再オープン記録) 固定schema、bounded streaming part生成、part-manifest-v1、replay-day-manifest-v1 M3 form、strict verifierの初回実装証拠は、PartManifestのscope/raw provenanceとfinal row-chain closure不足が判明したため当時は完了扱いに戻さなかった。
  focused Go `83 passed / 0 failed`、fixture `20 verified`、Python `18 passed / 0 failed`、`mise run check`、`go vet`、空の`gofmt -l`、`git diff --check`に成功した。
- [x] (2026-07-15、M3-2レビュー修正着手) PartManifestへexact ReplayScope、raw-day manifest key+domain digest、ConversionTuple、`previous_row_chain_hash`を追加し、strict Go/Python decoder、golden fixture、part-setのscope一致とrow-chain predecessor検証を更新した。
- [x] (2026-07-15、M3-2レビュー修正の現行local gate) Archiveがverified `PartArtifact`からscope-boundなPartManifestInputを構成し、replay rootを最終partの`LastRowChainHash`へ固定した。
  focused Goは`internal/parquet 4 passed / 0 failed`、`internal/archive 19 passed / 0 failed`、`internal/protocol 41 passed / 0 failed`、`internal/continuity 30 passed / 0 failed`の合計`94 passed / 0 failed`である。
  fixtureは`21 verified`、Pythonは`31 passed / 0 failed`、`mise run check`、`mise exec -- go vet ./...`、空の`gofmt -l cmd internal`、`git diff --check`が成功した。
  advisor再監査が完了するまでM3-3は開始せず、publication、receipt、selector、delivery、R2 uploadは未実装のままとする。
- [x] (2026-07-15、advisor exception audit再監査待ち) Protocol文書の未リリースM3 V1修正明記、fixture index再生成、outer replay bindingのPython検証、任意nonzero replay rootのbuild/verify負例、実multi-part Parquet integration testを完了し、現行全gateを再実行した。
  旧83件ではなく現行85件のfocused Go、fixture 21件、Python 24件を受入証拠とする。
  advisor再監査が完了するまでM3-3を開始せず、下流のpublication、receipt、selector、delivery、R2 uploadを解禁しない。
- [x] (2026-07-15、後続M3-2Gで置換済みのadvisor changes_required remediation旧checkpoint) `part_bytes >= 1`、part identity/anchor hashのnonzero規則をProtocol文書とGo/Python regression testへ明記した。
  その時点の`94 passed / 0 failed`、fixture `21 verified`、Python `31 passed / 0 failed`はM3-2G key-contract correctionで再検証対象となり、現行のM3-2完了証拠とは扱わない。
- [x] (2026-07-15、M3-2G key-contract correction) 現行Goalが定めるdate-local chainに合わせ、旧generic derivative key、hour partition例、Go/Pythonのkey導出差分を修正した。
  focused Goは`internal/parquet 4`、`internal/archive 20`、`internal/protocol 42`、`internal/r2 33`の合計`99 passed / 0 failed / 0 skipped`、fixtureは`22 verified`、Pythonは`32 passed / 0 failed`である。
  `mise run check`、`mise exec -- go vet ./...`、空の`mise exec -- gofmt -l cmd internal`、`git diff --check`も成功した。
  local M3-2 gateは完了とするが、advisor exception re-auditを依頼するまでM3-3のpublication、receipt、selector、delivery、R2 uploadは開始しない。
- [x] (2026-07-15、advisor changes_required: parent R2 layout namespace correction) 親ExecPlanのR2 layout図に、既存trusted campaign prefixの先頭`dataset=<sha256(exact dataset-id)>/`を`provider=`より前へ明記した。
  Local outboxは既存M2の`provider/feed/symbol/campaign`単位を維持し、R2だけがdataset identityを含むprefixでnamespaceを閉じることを追記した。
  このdocs-only修正後も`mise run check`（fixture `22 verified`、Python `32 passed`、Go全体、format/lint）と`git diff --check`に成功した。
  これはM3-2Gの文書監査修正であり、M3-3は開始していない。
- [x] (2026-07-15、M3-2G advisor exception audit pass) advisorのchanges_requiredを全て反映した現行M3-2Gについて、focused Go `4+20+42+33=99 passed / 0 failed / 0 skipped`、fixture `22 verified`、Python `32 passed / 0 failed`、repository check、vet、gofmt、diff checkの証拠を再確認し、M3-2Gを受入済みとした。
  これによりM3-3Aのimmutable replay publicationだけを開始できる。read-only replay delivery、CLI、selector、cache、R2 smokeは引き続き後続作業とする。
- [x] (2026-07-15、M3-3A publication checkpoint) raw manifest、全chain object、生成済みParquet artifact、part manifest、replay manifestをtrusted `r2.Layout`から導出し、derivative専用SQLite intent、10段階stage、canonical receipt、crash-safe retryの初回経路を実装した。
  `internal/r2 47`、`internal/archive 20`、`internal/parquet 4`、`internal/protocol 17`のfocused Goは合計`88 passed / 0 failed`で、first publish、idempotent retry、全durable stage後のrestart、empty day、raw/Parquet/part/replay mutation拒否、data-before-manifest、branch、timeout、receipt no-clobber、download failure、conversion collision、rclone command allowlistを確認した。
  part manifest、replay manifest、receiptのremote bytesはrclone `copyto --immutable`、`check --download`、backend GETの順で再検証し、receiptにはpublisher claim、tool lock、journal intent hash、raw object、Parquet、part、replayのbindingを保存する。
- [x] (2026-07-15、M3-3A review correction) 前回の受入証拠を再開し、journal stageだけに依存しないpublisher claim再検証、receipt直前の全remote再検証、streaming derivative graph検証、孤児オブジェクト規則、List重複拒否を実装して再検証した。
  前回の`internal/r2 47`、focused Go合計`88`、fixture `22 verified`、Python `32 passed`は修正前の履歴として保持し、今回のgate完了証拠とは分離した。
- [x] (2026-07-15、M3-3A review correction verification) `ObjectBackend.Open`とS3 streaming reader、claim再assert、receipt直前のraw/Parquet/part/replay全再検証、既存Parquet hash/size、part chain、candidate orphan、List descriptor重複検証を追加した。
  `internal/r2`は未キャッシュで`52 passed / 0 failed`、`mise run check`、`mise exec -- go vet ./...`、空の`mise exec -- gofmt -l cmd internal`、`git diff --check`が成功した。
- [x] (2026-07-15、後続R1からR7で置換済みのM3-3A advisor BLOCK remediation記録) prepare-before-lockのTOCTOUを修正し、campaign/epoch lock取得後にのみremote prepare、graph、list、orphan検査を実行するようにした。
  claimを初回作成用とreceipt直前の既存claim再assert用へ分離し、final raw/Parquet/part/replay検証後のPutIfAbsentとbyte-identical GETを追加した。
  `ReplayPublicationLimits`をjournal intentへcanonicalに含め、S3/fakeの`GetLimited`、`ListLimited`、streaming `Open`、bounded Parquet hashを追加したが、Race Detectorはlocal gcc不足で未実行のためM3-3Aは未完了とする。
- [x] (2026-07-15、後続R1からR7で解消済みのM3-3A advisor BLOCK再オープン記録) prepare-before-lockのTOCTOU、receipt直前のclaim再assert不足、Parquet mismatchの全buffer読み込み、publicationリソース上限未固定が追加blockerとして判明した。
  remote graph/list/orphan検査をcampaign/epoch lock内へ移し、ReplayPublicationLimitsをjournal intentへ固定し、GetLimited/ListLimited/Openによる有界検証と競合テストを通過するまでM3-3Aを完了扱いにしない。
- [x] (2026-07-15、M3-3A-R0設計リセット) 上記の旧stage-based実装とtest countをaccepted evidenceから外し、behavior/failure inventoryとしてquarantineした。
  新しい実装正本は`execplan/2026-07-15-m3-3a-publication-redesign.md`である。
- [x] (2026-07-15、G0 read-only inventory) branch `agent/m3-replay-parquet-delivery`、full HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 24、untracked 26、staged 0を確認した。
  `git diff --check`と`git diff --cached --check`は成功し、remote I/OとGitHub CI確認は行わなかった。
- [x] (2026-07-15、G0A docs alignment) 全dirty差分を保持、境界縮小再利用、useful-case移行後の置換削除へ分類し、R2からR5とM3-5をM3-3A redesign正本へ揃えた。
  G0Aでは四文書だけを更新し、コード、test、fixture、DB、remote、commitを変更しなかった。
- [x] (2026-07-15、G1-R1) `ReplayPublicationBundle`とcomplete final observationのcanonical schema、domain digest、M2 claim relation、10個のresource limitをGo/Python/goldenへ固定した。
  `mise run fixture`は23 fixtures、`mise run test-python`は33 tests、focused Goは`internal/protocol`と`internal/archive`で成功し、両diff checkも成功した。
- [x] (2026-07-15、G2-R2) verified local inputとtrusted `r2.Layout`からProtocol V1 bundleをsealし、最初の未充足barrierだけを返すpure reconcilerを実装した。
  Focused sealer/reconciler test、`internal/r2`全test、gofmt、targeted diff、両diff checkを成功させ、旧stage publisher、journal、receiptを変更しなかった。
- [x] (2026-07-15、G3-R3) replay専用bounded read interface、共有aggregate budget、fresh observer、part graph verifier、replay revision graph verifier、Exact-only final observation変換を実装した。
  Focused `ReplayObservation|ReplayBudget|ReplayPartGraph|ReplayRevisionGraph|Bounded` testと`internal/r2`全testを成功させ、旧M3-3A test countは証拠へ流用しなかった。
- [x] (2026-07-15、G4-R4) ObjectID-only narrow executor、copy直前のlocal snapshot再検証、rclone二操作allow-list、非権威のcanonical diagnostic event storeを実装した。
  Focused `ReplayExecutor|ReplayEvent|Narrow|Allowlist` testと`internal/r2`全testを成功させ、旧test countを証拠へ流用しなかった。
- [x] (2026-07-15、G1E empty-manifest contract correction) G5途中監査で判明したzero-root証明不足をforward-fixし、各replay edgeへtrusted full key、strict canonical manifest bytes、part countを追加した。
  Empty terminalとempty predecessorは埋め込みmanifestが空partsとzero rootsを証明する場合だけ受理し、non-empty zero、mixed root、unproven earlier zero、manifest／key／digest／revision／root／terminal shape mismatchをGoとPythonで拒否する。
  `mise run fixture`は23 fixtures、`mise run test-python`は34 tests、`go test ./internal/protocol ./internal/archive -count=1`は成功した。
  G5Cでpartial G5由来の4 compile errorを解消し、G1RCでtrusted Layout adapter、lock前budget、lock-not-acquired、M2-only claim write、M3 Exact-only、empty-day publisherを再検証した。
- [x] (2026-07-15、G1RC regression reclose) Focused Protocol／R2 regression、`internal/r2`全80 test、fixture 23件、Python 34件、Protocol／archive Go、gofmt、両diff checkを現行設計の証拠として成功させた。
  Shared aggregate budget enforcementはcampaign全体で維持し、receiptのcounterは最後のfresh passだけをbindする。
  Lock取得後かつremote action前にsealed derivative local source全件を再検証し、G5 legacy removalの開始条件を再び満たしたが、削除自体はまだ開始していない。
  旧M3-3Aの52件、86件その他のtest countはG1RCの受入証拠へ流用していない。
- [x] (2026-07-15、G5-R5) Seal／preflight、campaign lock、fresh observe、pure reconcile、approved ObjectID一件のexecute、fresh reobserve、canonical receipt no-clobberだけを接続するthin publisherを完成した。
  Aggregate budgetは全roundで共有し、MaxPublicationRoundsを強制し、final observationは最後のfresh pass counterをbindする。
  Receiptはcomplete canonical bundleとterminal final observation、bundle／observation digest、claim、roots、limits、rclone identityを再検証し、runtime-only stateを含めない。
  `internal/r2/replay_journal.go`と`internal/r2/replay_limits.go`を削除し、`journal.go`からreplay intent／object／transition tableを除去した。
  Useful caseは`TestReplayPublisherFirstPublishAndSameContentRetry`、`TestReplayPublisherReobservesAfterEveryActionAndSharesBudget`、`TestReplayPublisherMaxRoundsStopsWithoutReceipt`、`TestReplayPublisherEventConflictAndTimeoutAreNonAuthority`、`TestReplayReceiptNoClobberSameContentAndConflict`へ移行した。
  Focused R5 tests、`internal/r2`全83 test、fixture 23件、Python 34件、Protocol／archive Go、gofmt、両diff checkを成功させ、旧M3-3A test countを流用しなかった。
- [x] (2026-07-15、R6 completed) 現行R1からR5 testを正本fault matrixへ対応付け、追加fake testでaction後crash、observation後crash、final observation後のreceipt保存crash、stale observation中remote collision、aggregate request／byte exhaustionを検証した。
  Focused fault test、`internal/r2`全88 test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff checkは成功した。
  Repository外のuser scopeへ導入したWinLibs POSIX/UCRT GCC 16.1.0を使い、Go 1.24.13 windows/amd64、CGO_ENABLED=1、CC=gccで指定8 packageのlocal Windows Race Detectorをexit 0で完了した。
  `mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/catalog ./internal/continuity ./internal/parquet -count=1`はingest、wal、archive、r2、delivery、continuity、parquetでpassし、catalogはno test filesだった。
  GCCはrepoまたはmiseのdependencyではなく、GitHub CIとremoteは未確認である。
  Real R2はcredentialと明示確認がないためoptional skipとし、旧M3-3A test countをR6の証拠へ流用していない。
- [x] (2026-07-15、R6 pass) M3-3A-R7のdesign、implementation、fault、resource、CI初回監査を開始した。
- [x] (2026-07-15、R7 changes_required) Expected descriptor bytesのI/O前課金、terminal二回目readのclassification collapse、実消費bytesを返さないrclone Parquet checkをresource bypassとして検出し、G3EへR3を再openした。
- [x] (2026-07-15、G3E bounded actual bytes classification) Per-object limit、category残量、MaxObservationBytes残量の最小値をread capにし、cap+1までの実消費bytesを成功と全失敗経路でaggregate budgetへ課金した。
  Terminal二回目readのUnavailable／Oversized／Ambiguous／Differentを保持し、Parquet verificationを`OpenLimited` bounded streamとSHA-256へ移して`ReplayCheckDownloader`をR3から除去した。
  Publisher-level negative testは各classでaction zero、FinalDigestなし、receipt未保存を確認した。
  Event storeはmalformed eventとconflicting duplicateをstore-localに拒否し、publisher-level append failureはbest-effort diagnosticとしてpublicationをvetoしない方針へ統一した。
  Focused R3／R5／fault、uncached `internal/r2`全test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功した。
  旧M3-3A test countは現行証拠へ流用せず、M3-4はR7再監査の明示的なpassまでblockedである。
- [x] (2026-07-15、G3F final uplift／list descriptor) Final observation upliftを共有budgetへ課金し、Parquet list descriptorをbundle／Open／stream bytesへ照合して現行local gateを再実行した。
- [x] (2026-07-15、R7 third audit pass) Phase `r7_m3_3a_third_audit`がverdict `pass`となり、M3-3A R1からR7をcompletedとした。
- [x] (2026-07-15、G8 completed) M3-4でimmutable replay selector、empty-cache fetch、hash-derived cache、day-only verification、`tickctl` replay list／fetch、`tick-verify replay-day`を実装した。
  既存raw APIとCLI semanticsを保持し、remote write capabilityを追加しなかった。
- [x] (2026-07-15、G9 completed) M3-5でnetwork-free fake end-to-end、既存cross-language golden、repository gate、Go vet、指定8 packageのWindows Race Detectorを検証した。
  Real R2はcredentialと明示確認がないためoptional skipとし、GitHub CI、remote I/O、M3全体final auditは未実施である。
- [x] (2026-07-16、whole-M3 final audit changes_required) G9のM2 publicationがproduction `r2.Publisher`ではなくdirect helperを使っていたため、M3全体の完了証拠をG9Eで再構築することにした。
- [x] (2026-07-16、G9E completed) Production M2 publisher、M3 replay publisher、read-only replay deliveryを同じnetwork-free backendとcanonical identity graphで接続し、現行local gateを再実行した。
- [x] (2026-07-16、whole-M3 final re-audit pass) Advisorのphase `final_m3_whole_reaudit`がverdict `pass`、required actionsなしとなり、M3全体をcompletedとした。
- [x] (2026-07-16、M3 merge) Pull Request #4をmerge commit `cb72752a651c88c3027b409f6f205ac9236f28b8`としてmainへ反映した。
- [x] (2026-07-16、M4 delegation) proof-gated pruning、運用時の認証情報変更、multi-broker/symbol、HTTP、24h soak、live brokerをM3の未完了項目ではなくM4へ明示的に委譲した。M4の正本は`execplan/2026-07-16-m4-production-operations-http-delivery.md`とする。

## Surprises & Discoveries

- 観察: 現在の`internal/parquet`と`internal/continuity`はpackage commentだけで、M3 runtimeは未実装である。
  根拠: `internal/parquet/doc.go`と`internal/continuity/doc.go`だけが存在する。
- 観察: 現在の`go.mod`には`github.com/parquet-go/parquet-go`がなく、Go toolchainは`go1.24.13 windows/amd64`である。
  根拠: `go.mod`の`go 1.24.0`と`toolchain go1.24.13`、`mise exec -- go version`の出力を確認した。
- 観察: 2026-07-15の候補探索では`parquet-go`のtag一覧が`v0.17.0`から`v0.30.1`まで返り、latestのmodule metadataはGo `1.24.9`を要求した。
  根拠: `mise exec -- go list -m -versions github.com/parquet-go/parquet-go`と`mise exec -- go list -m -json github.com/parquet-go/parquet-go@latest`の出力である。
  判断: これは候補の発見結果であり、M3-0で`v0.30.1`をpinしたことを意味しない。
- M3-2 compatibility spike: `go1.24.13 windows/amd64`で`v0.30.1`、`v0.30.0`、`v0.29.0`、`v0.28.0`、`v0.27.0`のmodule metadataを比較した。
  `v0.30.1`のmodule GoVersionは`go1.24.9`であり、`NewGenericWriter`、`NewGenericReader`、`OpenFile`、typed schema、`uint(8/16/32/64)`、明示的writer optionを利用できる。
  選定した`v0.30.1`のmodule checksumは`h1:Oy6ganNrAdFiVwy7wNmWagfPTWA2X9Z3tVHBc7JtuX8=`である。
  `go.mod`の`go` directiveは依存moduleの要求により`1.24.9`へ更新し、toolchainは`go1.24.13`を維持した。
- M3-2実装: `internal/parquet`はrowを一件ずつcanonicalizeしてbounded current partへ入れ、ordered row countとcanonical row bytesだけでpartを閉じる。
  partはtemporary write、sync、close、SHA-256、no-clobber promotion、reopen schema/value/row-chain verificationの順で確定する。
  Parquet byte equalityは同一converter build、dependency lock、writer configuration、platform contractに限定し、logical rowとrow-chain rootは同じ入力で必須一致とした。
- M3-2実装: `internal/archive`のpart manifest builderはpart object summaryからのみmanifestを生成し、strict verifierはcanonical bytes、digest、key、row range、predecessorを再検証する。
  replay-day manifest builder/verifierはraw campaign-relative keyとdomain hash、conversion tuple、ordered part keys、part_set_root、zero-root empty day、revision successorを同時に検証する。
- 観察: M2の`internal/r2.PublicationJournal`は`archive.RawDayManifest`をintentに埋め込み、raw object向けのstageとreceiptを持つ。
  根拠: `internal/r2/journal.go`の`PublicationIntent`、`StageObjectsCopied`、`StageManifestVerified`、`internal/r2/receipt.go`を確認した。
  当時の判断: replay derivativeが同じstateをblind reuseすると、raw publicationのidentityを派生物のidentityと誤認するため、別domainと別stageを要求した。
  M3-3A-R0では別identityを維持し、固定stage authorityを失効させた。
- 観察: M2の`ArchiveReaderV1`はraw snapshot selectorを提供するが、`ListReplaySnapshots`とreplay-specific verifyはまだ提供しない。
  根拠: `internal/delivery/types.go`のinterfaceと`ErrUnsupportedReplay`を確認した。
  判断: replay selectorとcache verificationはM3-5の追加契約として扱う。
- 観察: 既存M0 replay fixtureはempty-parts形式で、M3のraw manifest key binding、revision predecessor、zero-root表現を持たない。
  判断: 既存fixtureは`M0_EMPTY_PARTS_COMPAT`として読み取り専用に厳格受理し、M3の新規manifest writerとpublication bindingからは除外する。
- 観察: 現行repositoryにはParquet依存がなく、M3-1で`go.mod`を変更できる根拠はない。
  判断: M3-1ではParquet writerを実装せず、version discovery、writer設定、part boundaryの最終pinはM3-3へ残す。
- 以前の観察: `.github/workflows/windows-race.yml`のrace対象はM2 packageだけで、M3 packageを含まなかった。
  R6でworkflow対象を指定8 packageへ更新し、local Windows Raceで同じpackage集合を検証した。
  GitHub Windows runnerの結果はremote I/O禁止境界のため未確認であるが、R6のlocal Race gateを妨げない。
- R6ではrepository外のuser scopeへWinLibs POSIX/UCRT GCC 16.1.0を導入し、`go1.24.13 windows/amd64`、CGO_ENABLED=1、CC=gccで指定8 packageのlocal Windows Race Detectorを完了した。
  GCCはrepoまたはmiseのdependencyではなく、workflowのCGO、gcc、mise境界と指定8 packageを変更していない。
  GitHub CIとremoteは未確認であり、local Race passをGitHub CI passとは扱わない。
- 例外監査: 初回M3-1実装の`RawDayInput`は、sealed WALから機械的に導出されるべき`Entries`、manifest identity、chain startをcallerから直接受け取っていた。
  そのため、`{}` manifestと`sealed-raw-object`を成功fixtureにでき、M2のraw-day/sealed-WAL trust boundaryを通過していなかった。
- 例外監査: 初回replay bindingはraw-day domain digestではなく、manifest canonical bytesへのplain SHA-256を使っていた。
  M2の正本は`internal/archive.ManifestDigest`であり、domain prefixの不一致を許してはならない。
- 例外監査: 初回reducerはday全体とsegment rowsを保持し、`Result.Rows`も全rowを蓄積していた。
  これはproductionのstreaming／bounded summary条件を満たさない。
- 例外監査: 初回の`SOURCE_HISTORY_CHANGED`判定はmt5の`capture_sequence`と`time_msc`をstable source identityとして扱っていた。
  stable source IDまたは周辺fingerprintによる一意な位置合わせがない場合は、history changeを断定せず`AMBIGUOUS_OVERLAP`で全occurrenceを保持する必要がある。
- 追加監査: `ReplaySourceInput`はmanifest rangeの開始sequenceだけを選択mapへ登録し、三つ以上のWAL entryにまたがるrangeのmiddle entryをreplayから落としていた。
- 追加監査: reducerは`BatchFrameV1.SessionLeaseID`を`ReplayDataRow.ProducerInstanceID`へ誤用し、producer identityとlease identityのprovenanceを混同していた。
- 追加監査: overlap実装は短いsuffix候補も同時に数え、長い候補が一意でも`AMBIGUOUS_OVERLAP`を出していた。
- 追加監査: sinkへ渡した後にcanonical row validationとrow-chain stateを更新する順序では、invalid rowがdownstreamへ到達し得る。
- M3-1E例外監査: `uniqueHistoryChange`がbounded tailとincomingの任意のwindowを比較しており、実際のtail suffixとincoming prefixの境界が一致しなくても`SOURCE_HISTORY_CHANGED`を出せた。
  内部old subsequenceの一置換はsource historyの証明にならないため、M3-1Dのgate証拠を追加修正の対象とした。
- M3-1D実装中の発見: 既存`VerifyRawDaySnapshot`はM2の一日分selectionを再導出するため、replay manifestのcompactなmulti-entry rangeをそのまま渡すと、正しいpartial rangeでも拒否する。
  対応として、replay側でrangeをentry単位へ展開して選択座標、件数、watermarkを検証し、別コピーでM2のfull-day semantic snapshot検証を通した。
  根拠: `TestOpenVerifiedReplaySourceExpandsMultiEntryRangeExactly`が三つのWAL entryから7座標を得て成功し、M2 verifierのsemantic proofも同時に通過した。
- M3-1D実装中の発見: unique one-substitutionのhistory proofは、mt5のcapture sequenceやtimeをidentityにせず、前後fingerprintが一意な三要素以上のwindowである場合にだけ成立する。
  根拠: `TestReduceReportsProvenSourceHistoryChange`は`[A,B,C]`と`[A,X,C,D]`で`SOURCE_HISTORY_CHANGED`を一件出し、session restartの単独変更例は`AMBIGUOUS_OVERLAP`を維持した。
- M3-1D検証結果: focused Goは`internal/ingest 11`、`internal/wal 16`、`internal/archive 12`、`internal/protocol 32`、`internal/continuity 16`で合計`87 passed / 0 failed`、fixtureは`20 verified`、Pythonは`18 passed / 0 failed`だった。
- M3-1E検証結果: `TestReduceDoesNotUseInteriorHistoryWindowWithoutBoundaryProof`は内部old subsequenceをhistory changeへ昇格せず、`AMBIGUOUS_OVERLAP`とincoming occurrence全件保持を確認した。
  `TestReducePrefersLongestBoundaryHistoryAlignment`と`TestReduceRejectsRepeatedWildcardCompatibleHistoryPositions`も境界候補の選択と反復位置の拒否を確認した。
  focused Goは`internal/continuity 19`、`internal/protocol 32`、`internal/archive 12`、`internal/ingest 11`で合計`74 passed / 0 failed`だった。
- M3-1F例外監査: 現行`verifyReplaySnapshot`は入力manifestの`Objects`、件数、watermark、chain sliceを別のfull-day selectionで上書きしてから`VerifyRawDaySnapshot`へ渡しており、7-of-9のcompact partial selectionを入力証拠として拒否できない。
  これは検証対象を変更してから検証するため、M2 raw-day manifestの不変性を証明しない。
- M3-1F例外監査: `VerifyRawDayManifestKey`はtrusted `r2.Layout`なしに任意のimmutable root付きfull remote keyを受理しており、`OpenVerifiedReplaySource`の入力identityがcampaign-relative keyに限定されていない。
- M3-1F例外監査: archive replay verificationにchain object数、単一object bytes、chain bytesの明示的上限がなく、full-chain loadと再検証の資源量をreplay contractへ結び付けていない。
- M3-1F検証結果: `TestOpenVerifiedReplaySourceRejectsNonEquivalentCompactRange`が7-of-9 partial selectionを拒否し、`TestOpenVerifiedReplaySourceAcceptsEquivalentCompactRange`が9座標の順序一致を確認した。
  zero-record sentinel、source-error、cross-day、arbitrary root、zero/count/object-bytes/chain-bytes/overflow、oversized reverified objectのfocused testも成功した。
  focused Goは`66 passed / 0 failed / 0 skipped`、fixtureは`20 verified`、Pythonは`18 passed / 0 failed`だった。
- M3-2レビュー修正: 初回PartManifestはpart objectとrow summaryだけをcanonical JSONへ持ち、dataset、campaign、day、raw-day manifest binding、ConversionTuple、previous row-chain anchorをpart digestへ含めていなかった。
  そのため、part単体では別day、別campaign、別conversion、別raw revisionの混入を表現できず、part setのidentity境界がreplay-day manifest側へ片寄っていた。
- M3-2レビュー修正: 初回`BuildReplayDayManifest`はnonzeroな任意の`CanonicalStreamRowChainRoot`を受理し、最後のpartの`LastRowChainHash`との一致を要求していなかった。
  今回はpart setの前part row-chain anchor、stream range、scope/raw/conversionの一致を同じ検証鎖へ追加した。
- M3-2レビュー修正: `PartManifestInput`をParquet `PartArtifact`からexact scopeとConversionTupleへ結び付けるarchive helperへ変更した。
  callerがmanifest provenanceを別オブジェクトとして差し替える経路を残さず、旧形式part manifestをM3 writerから出力しない。
- M3-2レビュー修正の検証: `part-manifest-v1.json`をgolden indexへ追加し、Go/Pythonのcanonical JSON、digest、key、outer replay bindingを同じfixtureで検証した。
  Python verifierはpartのdataset、campaign、day、date、replay contract、format、conversion、converter、dependency、writer、platform、raw key+domain digestをouter replay manifestと比較し、変更を拒否する。
- M3-2レビュー修正の検証: generated/reopened Parquetを`PartManifestInputFromArtifact`へ渡し、三つのpartのpredecessor digest、previous row-chain hash、part_set_root、final row-chain rootを持つreplay manifestをbuildして再検証するintegration testを追加した。
- M3-2レビュー修正の検証: `part-manifest-v1`をversion splitせず、branch未mergeのnot-yet-released M3 V1 contract correctionとしてProtocol文書へ明記した。
  canonical JSONのlexicographic field order、part manifest domain、M2 raw-day domain digest、part_set_rootのdigest経由のbindingをGo/Python fixtureと一致させた。
- M3-2レビュー修正の検証結果: focused Goは`4+19+41+30=94 passed / 0 failed`、fixtureは`21 verified`、Pythonは`31 passed / 0 failed`だった。
  `mise run check`、`mise exec -- go vet ./...`、`mise exec -- gofmt -l cmd internal`の空出力、`git diff --check`も成功した。
- Advisor changes_required remediation: `manifests.md`へ`part_bytes >= 1`、`part_sha256`、`first_row_chain_hash`、`last_row_chain_hash`のnonzero規則を追加し、Goはzero bytes、zero identity/anchor、part 0以外のzero predecessorを拒否するregression testを追加した。
  Pythonも同じzero値とsuccessor predecessorの拒否を独立に検証した。
- M3-2G例外修正: 初回M3-2の物理keyは`objects/replay`、`manifests/replay`、`snapshots/replay`へ分散し、parent planにはdate-local chainと矛盾する`hour=HH`例が残っていた。
  現行Goalの「part chains close by date」を優先し、hour partitionを追加しないin-place Protocol V1 correctionとした。
- M3-2G実装判断: derivative keyはProtocol V1の`ExactIdentityPathKey`でexact UTF-8 bytesをSHA-256化し、replay contract、conversion、day definition、dateからcampaign-relative baseを導出する。
  `r2.Layout`はtrusted immutable/campaign rootのprependだけを担当し、full key aliasやgeneric replay prefixを受理しない。
- M3-2G境界確認: required relative keyはcampaign-relativeでcampaign IDをpath componentへ再度入れないため、dataset/provider/feed/symbol/campaignのfull-scope bindingはcanonical manifestとtrusted `r2.Layout`のcampaign prefixで閉じる。
  cross-campaignのrelative key再利用はLayout full-key verifierで拒否し、archiveではpart manifestのcanonical scope bindingで拒否する。
- M3-2G hash-domain修正: date-local part_set_rootが長いphysical keyを扱えるよう、part manifest keyのdomain encodingをU16 `LP`から明示的なU32 byte lengthとpath bytesへ修正した。
  transport stringの255-byte制限は維持し、physical relative keyだけを1024 bytesまで許可する。
- M3-2G advisor pass: advisor exception auditは、dataset先頭のtrusted R2 prefix、M3 V1のpart/replay key binding、outer replay binding、final row-chain closure、zero identity/anchor拒否、生成済みParquetからのmanifest integrationを含む現行証拠を確認してpassした。
  これをM3-3A開始の前提とし、publication以外のdelivery境界を実装完了とは扱わない。
- M3-3A初回実装: raw publisherの`PublicationIntent`とstageを再利用せず、同じSQLite接続内に`replay_publication_intents`、`replay_publication_objects`、`replay_publication_transitions`を追加した。
  M2 raw manifestのfields、stage名、receipt型をderivativeへ流用しないため、raw publicationの完了をParquet publicationの完了と誤認できない。
- M3-3A実装中の修正: part manifestのremote検証にはParquet object keyではなくProtocol V1のpart manifest keyを渡す必要があり、remote Parquet mutationは`archive.ErrIntegrity`へ分類した。
  根拠: fake rcloneの`check --download`が返す単純な比較失敗だけでは、same-key different-contentをcallerがintegrity failureとして識別できなかった。
- M3-3A実装中の発見: receiptがclaimとtool identityだけを持ち、SQLite intentのcanonical hashを持たないと、保存済みreceiptをjournal intentへ再結合できない。
  対応として`journal_intent_hash`をreceipt v1のcanonical fieldへ追加し、非zero検証とno-clobber testを行った。
- M3-3A検証結果: fake backend/rcloneでdurable stageごとのcrash restart、raw-before-Parquet、data-before-manifest、same-key collision、part/replay branch、missing raw predecessor、remote mutation、timeout、empty day、receipt secret exclusionを確認した。
  real R2 smokeは隔離credentialの指定がないため実行せず、production objectを変更していない。
- M3-3A review finding: journal stageが`claimed`以降ならpublisher claimのPutIfAbsentを省略し、削除、変更、別bytesのclaimをstageだけで信頼できた。
  stageは再開位置に過ぎないため、各reconcileで同一canonical claimをPutIfAbsentし、byte-identical GETを通す必要がある。
- M3-3A review finding: `replay_manifest_verified`後からreceipt保存までにParquetまたはpart manifestが変更されても、旧実装はrawとreplay manifestだけを再検証してreceiptを保存できた。
  receipt直前にraw、Parquet、part manifest、replay manifest、exact intent keyを全て再検証するhook testを追加する。
- M3-3A review finding: `inspectDerivativeObjects`は既存Parquet bytesを検証せず、part sequence 1だけの孤児や、manifestが参照する欠落Parquetを許していた。
  ObjectBackendのOpenでhashとsizeを逐次計算し、raw bindingごとのpart chainをsequence 0から連続するprefixとして検証する。
- M3-3A review finding: data-before-manifestの安全な孤児はcurrent candidateとbyte-identicalなParquetとpart manifestだけであり、同じconversion/date prefix内の別孤児を残すとscope collisionを隠せる。
  completed replayのreferenced setとcandidate setを使って、許可対象以外のunreferenced derivative objectをfail closedにする。
- M3-3A review finding: fake backendのListが同一keyの重複や異なるsizeを返しても、既存graph検証が一意性を保証しなかった。
  List結果をkeyとsizeで正規化し、重複またはconflicting descriptorをintegrity failureとして扱う。
- M3-3A review remediation evidence: `TestReplayPublisherReassertsClaimAfterClaimedStage`は、receipt保存済みjournalを再実行してremote claimをmutationした場合に`ErrPublisherConflict`で停止することを確認した。
  `TestReplayPublisherRechecksAllRemoteObjectsBeforeReceipt`は、`replay_manifest_verified`後のParquetまたはpart manifest mutationでreceiptを保存しないことを確認した。
- M3-3A review remediation evidence: `TestReplayPublisherRejectsUnrelatedOrphanData`、`TestReplayPublisherRejectsOrphanPartWithoutPredecessor`、`TestReplayPublisherRejectsDuplicateDerivativeListDescriptor`は、candidate外の孤児、sequence 0のないpart、同一keyのduplicateまたはconflicting sizeを拒否した。
  `ObjectBackend.Open`を用いる既存Parquetのstreaming hash/size検証と、raw bindingごとのpart predecessor、row-chain predecessor、stream range検証も`internal/r2`で通過した。
- M3-3A advisor BLOCK remediation discovery: lock取得前の`prepareReplay`が、並行同一scope publisherによるremote graphのstale inspectionを可能にしていた。
  また、stage-only claim再開ではreceipt直前のclaim削除を再作成として通してしまうため、既存claimの事前GETを含む再assertが必要だった。
- M3-3A advisor BLOCK remediation discovery: Parquet mismatch fallbackの`backend.Get`と`os.ReadFile`は、期待する`PartArtifact`のbytes/hashを基準にしたbounded streamingへ置き換える必要があった。
  remote listのunknown/negative size、per-object/total Parquet bytes、metadata bytes、object countを、bodyを読む前に拒否する境界として固定した。
- M3-3A remediation evidence: `internal/r2`はJSON test eventで`86 passed / 0 failed`、`mise run check`はfixture `22 verified`、Python `32 passed`、全Go test、`go vet ./...`、空の`gofmt -l cmd internal`、`git diff --check`が成功した。
- stageが`claimed`以降の再開でもclaimを再生成せず、開始時の既存claim mutation/deletionを`ErrPublisherConflict`で拒否することを追加テストで確認した。
- M3-3A remediation environment finding: `mise exec -- go test -race ./internal/r2 -count=1`はCGO無効で`go: -race requires cgo`となり、`CGO_ENABLED=1`では`cgo: C compiler "gcc" not found`となった。
  `.github/workflows/windows-race.yml`は既に`gcc --version`、CGO有効化、`./internal/r2`を含むRace実行を持つため、local failureはworkflowへ引き継ぐ環境制約として扱う。
- M3-3A advisor BLOCK findings: `Publish`がremote prepareをlock取得前に実行しており、同一scopeの並行publisherがstale graphを受け入れ得た。
  また、receipt直前のclaim再assert、Parquet mismatchのbounded streaming、metadata/list/Parquetの明示上限とbounded backend APIが不足していた。
- M3-3A corrective decision: local-only shape checkを除くremote list、graph、orphan、raw、claim、artifact acceptanceの全状態をcampaign/epoch exclusive lock保持中に評価する。
- M3-3A-R0 systemic finding: monolithic `ReplayPublisher`がlocal bundle構築、remote観測、候補受入policy、stage transition、receipt authorityを結合していたため、個別のTOCTOU修正や再検証追加だけでは不変条件が収束しなかった。
  根拠: claim、graph、resource accounting、receipt直前観測の各修正が同じruntime flowへ条件分岐を追加し、stageとremote事実の二重authorityを残していた。
- M3-3A-R0 design consequence: 旧実装を受入対象から外し、sealed bundle、bounded fresh observation、pure reconciler、approved action executor、append-only event、final observation receiptへ境界を分割する。
  `MaxMetadataBytes`、`MaxListObjects`、`MaxParquetObjectBytes`、`MaxTotalParquetBytes`をcanonical intentへ含め、retry時にも同じ上限を再検証する。
- M3-3A-R3実装中の発見: 既存`BoundedObjectBackend`はwrite-capableな`ObjectBackend`をembedするため、そのままobserverへ渡すとread-only境界を型で保証できない。
  `GetLimited`、complete flag付き`ListLimited`、`OpenLimited`だけを公開するreplay専用interfaceとadapterへ分離した。
- M3-3A-R3実装中の発見: candidateのAbsentはcomplete derivative listからだけ証明でき、list後のGet失敗、short read、timeout、不完全pagination、negative size、unknown keyはAbsentではない。
  これらをAmbiguous、Unavailable、Oversizedへ分離するnegative testを新しい受入証拠にした。
- M3-3A-R4実装中の発見: canonical metadataはcaller-supplied pathをbundle contractへ含めないため、executorがsealed canonical bytesをdomain検証して一時snapshotへmaterializeする境界が必要だった。
  Parquetも元pathをrcloneへ直接渡さず、reopenとstream hashを通した検証済みsnapshotへ固定した。
- M3-3A-R4実装中の発見: eventのerrorを自由形式文字列にするとcredential、endpoint、local path、retry textを漏らせるため、resultとerrorを固定enumへ限定した。

## Decision Log

- Decision: M3 part chainはdate-localで閉じ、hour partitionを追加しない。
  Rationale: 現行Goalがdate単位のchain closureを明示しており、hour partitionはmarker rowへsyntheticな時間を割り当てる推測とphysical keyの別identityを導入するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: Protocol V1の`ExactIdentityPathKey`をphysical derivative path identityの唯一の導出helperとし、入力のexact UTF-8 bytesをSHA-256化する。
  Rationale: Go、Python、Archive、Parquet、R2でnormalizationやcase foldingの差分を作らず、同じscope/conversion/day keyを再現するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `r2.Layout`は検証済みcampaign-relative derivative keyへtrusted immutable rootとcampaign prefixを一度だけprependし、旧generic key aliasを提供しない。
  Rationale: full remote keyのtrust boundaryをLayoutへ限定し、`objects/replay`、`manifests/replay`、`snapshots/replay`を物理derivative keyとして再利用しないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: part_set_rootのpart manifest key encodingはU32 path length plus path bytesへ固定する。
  Rationale: required date-local keyはtransportのU16 string limitを超えるが、hash domainのpath encodingとしてboundedに表現できるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: ReplayPublisherのremote acceptance境界は、campaign/epoch local exclusive lockを取得してからreceipt stageまで保持する。
  Rationale: prepare、ListLimited、graph、orphan、raw binding、claim、final verificationの観測を同一scopeの直列化された状態へ結び付け、prepare-before-lock TOCTOUを除去するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: ReplayPublicationLimitsの4値をjournal intentのcanonical fieldへ含め、bounded backendをReplayPublisherの必須依存にする。
  Rationale: retryがmetadata、list、per-object Parquet、total Parquetのtrust boundaryを黙って拡張できないようにし、S3/fakeで同じ有界APIを検証するためである。
  実装上限はmetadata 16 MiB、list 100,000 objects、per-object Parquet 1 TiB、total Parquet 16 TiBであり、全てnonzeroかつ`int64` readerの`max+1`がoverflowしない範囲に固定した。
  Date/Author: 2026-07-15 / Codex

- Decision: M2のM3開始基準をPR #3のmerge commit `cc9fc2d`に固定する。
  Rationale: 現在のbranch `HEAD`と`origin/main`が同じmerge commitであり、M2のraw publication、read-only delivery、CI evidenceをこのcommitから再現できるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-1をimplementation gateとし、gate通過前はfixtureと小さなprototype以外のM3実装を進めない。
  Rationale: `part-manifest-v1`、part_set_root、canonical row-chain、marker encoding、raw manifest key/hash binding、replay revision rulesの一つでも未固定なら、後続のParquet、publication、deliveryが別の意味を採用してしまうためである。
  Date/Author: 2026-07-15 / Codex
- Decision: raw snapshotとreplay derivative snapshotを別のrevision axisとpublication identityにする。
  Rationale: late raw evidenceはraw-day revisionを進め、converter変更はconversion tupleを変えるため、同じmanifest chainとjournalへ混ぜると再現条件を失うためである。
  Date/Author: 2026-07-15 / Codex
- Decision: ordered overlap reducerはtimestamp sorting、set deduplication、payload hashだけの推測を使わない。
  Rationale: inclusive cursorによる同一payloadの複数occurrenceを保持し、source history changeと単なるtransport retryを区別するには、ordered fingerprint sequenceとmultiplicityが必要だからである。
  Date/Author: 2026-07-15 / Codex
- Decision: semantic determinismとbyte determinismを分ける。
  Rationale: logical row、row-chain root、part row layoutは同じ入力に対して必須である一方、Parquet physical encodingはwriter、dependency、platformによって変わり得るためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `parquet-go`は候補を列挙してmodule metadata、Go toolchain、API、LinuxとWindowsのtestを確認した後に、実際に選んだtagを一つだけ`go.mod`と`go.sum`へ固定する。
  Rationale: M3-0で版本を推測して書くと、現在のrepositoryとWindows Race workflowで利用できない依存を契約にしてしまうためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-2のParquet依存は、互換性spikeでAPIとmodule GoVersionを確認した`github.com/parquet-go/parquet-go v0.30.1`へ固定する。
  Rationale: `go1.24.13 windows/amd64`でtyped writer、typed reader、footer open、unsigned integer logical typeを確認でき、module checksumを`go.sum`から再検証できるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: ticks-parquet-v1の物理schemaはtop-levelのcommon fieldsとnullable `data`、nullable `marker` groupを固定し、浮動小数点値を使わずunsigned bit columnsへ保存する。
  Rationale: markerとdataを一つのordered row streamへ表現し、NaN、Inf、signed zero、volumeのwire bit patternを変換経路で失わないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `ConversionSpec`はreplay contract、format、conversion、converter build、dependency lock hash、writer configuration hash、target platform、三つの有限limitを必須にする。
  Rationale: part layoutのlogical determinismとParquet byte determinismの適用範囲を同じ入力tupleから判定するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: part manifestとreplay-day manifestのstrict constructionはParquet writer packageから分離し、`internal/archive`でverified part summaryと`protocol.ReplayScope`から行う。
  Rationale: futureのR2 layoutやpublication journalをmanifest digestの証拠へ混ぜず、raw bindingとrevision graphをarchive trust boundaryへ残すためである。
  Date/Author: 2026-07-15 / Codex
- Decision: part boundaryはParquetの圧縮後byte数ではなく、ordered canonical rowの個数とcanonical row bytesの累積値で決める。
  Rationale: 圧縮後サイズはwriterやcodecに依存するが、canonical row bytesはlogical determinismの入力として再現できるためである。
  Date/Author: 2026-07-15 / Codex
- Decision（M3-3A-R0で一部失効）: derivative publicationはraw publicationと別identityとreceiptを持つ。
  旧決定の`replay-publication-intent-v1`、固定stage、journal authorityは失効した。
  Rationale: raw publicationとderivative publicationを分離しつつ、再開判断をlocal stageではなくfresh remote observationから導くためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-2G advisor pass後にM3-3Aを開始し、実装範囲をimmutable replay publicationへ限定する。
  Rationale: Protocol V1、Parquet artifact、part/replay manifestのscope bindingとkey identityが受入済みであり、publicationはその検証済みbytesをR2へ順序付きで移す工程だからである。
  Date/Author: 2026-07-15 / Codex
- Decision（M3-3A-R0で一部失効）: replay publicationはraw publicationと同じcampaign plus publisher epoch lockを使い、identityとreceiptはderivative専用にする。
  SQLite stage tableとintent domainをruntime authorityにする部分は失効した。
  Rationale: raw manifestのpublication stateをParquet derivativeへ移さず、同時にremote事実とlocal stageの二重authorityを廃止するためである。
  Date/Author: 2026-07-15 / Codex
- Decision（M3-3A-R0で失効）: replay verification receipt v1はSQLite journal intent hashをauthorityとして保存しない。
  代わりにsealed bundle digestとcomplete final observation digestを必須bindingとして保存する。
  Rationale: receiptをlocal execution historyではなく、一時点の完全なremote事実へ結び付けるためである。
  Date/Author: 2026-07-15 / Codex
- Decision（M3-3A-R0で置換）: M2 raw publicationだけがpublisher claimを`If-None-Match: *`で作成する。
  M3 replay publicationはclaimを作成せず、各fresh observationで既存claimのExactだけを受理する。
  Rationale: claimの所有者をM2へ一意化し、M3がraw publication epochを派生物側から再定義しないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: existing derivative graphのParquet検証はObjectBackendのOpenから得る逐次readerで行い、全object bytesをメモリへ保持しない。
  Rationale: Parquet objectのkey hashとList sizeを再検証しながら、publication reconcileのmemory使用量をobject sizeに比例させないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: current candidateとcompleted replayが参照しないderivative objectは、candidate objectと完全一致するdata-before-manifestだけを許可し、それ以外をconversion-scope collisionとして拒否する。
  Rationale: prefix内の無関係なParquetまたはpart manifestを残したまま進めると、同じdate/conversion identityへ別系統のbytesを混在させるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-3Aの現行authorityはsealed `ReplayPublicationBundle`と各roundのfresh bounded remote observationだけである。
  Rationale: local stage、event、retry履歴をremote事実の代用にせず、同じ入力から同じ判断を再現するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: final receiptはbundle digestとcomplete final observation digestをbindするpoint-in-time evidenceとする。
  Rationale: receiptが証明する範囲を観測時点のremote一致へ限定し、future immutabilityやdistributed transactionを過大主張しないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-3Aは`execplan/2026-07-15-m3-3a-publication-redesign.md`のR1からR7を順に通過するまで未完了とする。
  Rationale: 新旧authorityを併存させず、Protocol contractからfault matrixまでを段階的に置換するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3のread pathはremote manifestとcontent-addressed local cacheだけを信頼し、Gateway SQLite、write credential、publication journalを要求しない。
  Rationale: 空cacheの新しいconsumerでも同じimmutable selectorから取得と検証を開始できる必要があるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M0のempty-parts replay fixtureは互換読取り形として保持し、M3 formでは`raw_day_manifest_key`、`raw_day_manifest_sha256`、`revision`、`previous_manifest_sha256`、zero-rootを必須にする。
  Rationale: 既存fixtureの意図を壊さず、M3のraw bindingとrevision axisを未定義のまま通さないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `AMBIGUOUS_OVERLAP`は候補が一意でない場合だけでなく、suffix/prefixとして証明できる候補がない場合にも挿入し、incoming occurrenceを全件保持する。
  Rationale: reducerがtimestamp、sort、set dedupe、payload hashだけの推測でraw occurrenceを落とさないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-1Cでは初回M3-1の完了判定を取り消し、M2 verifierを通過したsealed bytesだけをreducerへ渡すcorrective gateを設ける。
  Rationale: caller-supplied Entries、任意のmanifest identity、plain SHA-256 bindingを残したまま後続Parquetへ進むと、raw archiveの不変性を証明できないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: raw-day manifest digestは`internal/archive.ManifestDigest`の`tick-data-platform/raw-day-manifest/v1\0` domainを唯一の正本とし、continuity package内で同じ計算を複製しない。
  Rationale: raw manifest bindingのhash domainをpackageごとに分岐させず、plain SHA-256を負のfixtureで拒否するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: production reducerはverified readerからrowを一件ずつ受け取り、sinkへ送出した後にtail、chain、segment、marker位置だけを保持する。
  Rationale: day全体をoverlap探索用に保持せず、verified ScopeConfigのMaxRecordsでtail容量を上限化するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M2のfull layout manifest keyはarchive境界で検証し、Protocol V1 row identityにはcanonical campaign-relative keyを使う。
  Rationale: `r2.Layout.ManifestKey`のimmutable rootとcampaign prefixを確認しつつ、Protocol V1の255-byte string limitを超えるremote locatorをrow encodingへ持ち込まないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: mt5のsession変更と同一fingerprintの反復はhistory changeと断定せず、`AMBIGUOUS_OVERLAP`で全incoming occurrenceを保持する。
  Rationale: M2 WALへstable producer instance IDが保存されておらず、`capture_sequence`だけではsessionをまたぐ位置を証明できないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: campaign boundaryは検証済みmanifestのscope内では生成せず、cross-campaign scope mismatchをfail closedにする。
  Rationale: caller flagで境界markerを注入するとraw trust boundaryを再び迂回するためであり、固定marker enumはProtocol V1の将来のverified source evidence用に保持する。
  Date/Author: 2026-07-15 / Codex
- Decision: raw object rangeはstart sequenceからend sequenceまで全WAL entryを展開し、first entryのfirst ordinal、middle entryの全ordinal、last entryのlast ordinalをinclusiveに選択する。
  Rationale: M2 manifestのrangeはentry単位のinclusive sequenceとrecord ordinalの組であり、開始entryだけを選ぶとraw事実を欠落させるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: lease導出は`internal/protocol`の単一helperへ集約し、replay inputの明示的producer instanceとmanifest scopeからBatchFrameごとに再計算する。
  Rationale: ingestの既存wire behaviorを変えず、SessionLeaseIDをproducer identityとして再利用せず、wrong producerをsink前に拒否するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: overlapは最長の完全一致suffix/prefixだけを候補とし、その最長sequenceがtail内で一意なら受理する。
  Rationale:短いsuffixも一致すること自体は正常なprefixの性質であり、長い一意候補まで曖昧扱いすると正当な重複を削除できないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: sinkへ渡す前にrow canonicalization、row validation、next row-chain hashを計算し、sink成功後にだけsummary stateをcommitする。
  Rationale: invalid rowのdownstream到達と、sink失敗後のhash／sequence先行更新を防ぐためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3 compact rangeの検証は、M2のfull-day semantic verificationとreplayのexact coordinate verificationを分離する。
  Rationale: M2 verifierの再導出契約を弱めずに、`(start sequence, first ordinal)`から`(end sequence, last ordinal)`までのfirst、middle、last規則を受理するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `SOURCE_HISTORY_CHANGED`は、tailとincomingの三要素以上のwindowで前後fingerprintが一致し、変更位置のalignmentが一意な場合だけ出す。
  Rationale: mt5.mqltick.v1のcapture sequenceとtimeはstable source identityではなく、session restartや反復payloadで推測によるdropを起こさないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-1Dの範囲検証、wrong producer、longest overlap、pre-sink validationを実際のM2 builderとfake readerのfocused testで固定する。
  Rationale: 成功fixtureだけでなく、producer identity mismatchとinvalid rowのsink到達不可を観測可能な結果にするためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `SOURCE_HISTORY_CHANGED`の候補は、保持tailのsuffixとincoming batchのprefixの境界へ限定し、最長候補の長さについてtail内のwildcard-compatible位置を一つだけ許す。
  Rationale: 内部subsequenceの一置換は実際の再開境界を示さず、反復fingerprintが複数位置に対応できる場合はhistory changeではなく`AMBIGUOUS_OVERLAP`としてoccurrenceを保持するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: M3-1Fでは入力manifestを変更せず、verified sealed WALから独立に再導出したM2 canonical selectionと、compact rangeをentry座標へ展開したselectionを全fieldで比較する。
  Rationale: verifierへ渡す値を先に上書きすると、7-of-9のpartial modification、counts、watermarks、chain slice、chain_objects、raw_set_rootの改変を証明できないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: part manifestはReplayScope、raw-day manifest key+domain digest、ConversionTuple、part predecessor、previous row-chain hashをcanonical identityへ含め、part set全体のbindingを全partで一致させる。
  Rationale: replay-day manifestだけにprovenanceを置くと、単独取得したpart manifestが別day、別campaign、別conversion、別raw revisionへ再利用され得るためである。
  Date/Author: 2026-07-15 / Codex
- Decision: part 0の`previous_row_chain_hash`は全zeroとし、successorは直前partの`last_row_chain_hash`から機械的に導出する。
  Rationale: part predecessor digestだけではrow-chainの連続性を検証できず、replay rootを最後のpart以外へbindする余地が残るためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `BuildReplayDayManifest`はempty dayではzero root、non-empty dayでは最後のpartの`LastRowChainHash`と完全一致するrootだけを受理する。
  Rationale: generatorのverified stream summaryとpart manifest chainのrootを別々に受け取っても、両者が同じordered row streamを指すことを再検証できるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `OpenVerifiedReplaySource`はexact campaign-relative `ManifestRelativeKey`だけを受け取り、full remote keyのroot検証を行わない。
  Rationale: remote rootとdataset prefixはtrusted `r2.Layout`の責務であり、archive APIが任意rootを不変性の証拠として扱わないためである。
  Date/Author: 2026-07-15 / Codex
- Decision: `ReplayResourceLimits`の`MaxChainObjects`、`MaxObjectBytes`、`MaxChainBytes`をreplay_contract_idに紐付く必須runtime contractとし、descriptorを開く前に非zero、有限、overflow-safeに検証する。
  Rationale: `VerifySealedSegment`の単一object parseとfull-chain verificationのpeak resourceをboundedにし、上限違反でRowSinkへ一行も渡さないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: M3-3A-R3 observerはwrite backend、SQLite、journal、event、rcloneを受け取らず、replay専用`ListLimited`／`OpenLimited` interfaceと共有`ReplayObservationBudget`だけを使う。
  Rationale: Parquetを含む全remote bodyの実消費bytesをobserver自身がcap+1 bounded streamで課金し、観測I/Oとaction authorityを分離するためである。
  Date/Author: 2026-07-15 / Codex

- Decision: final observationはclaim、raw、全candidate derivative、part chain、replay revision graphがすべてExactで、campaign／epoch lockをdigest生成直前にも再assertした場合だけ生成する。
  Rationale: stale observation、resource failure、graph winner selectionからreceipt証拠を作らないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: M3-3A-R4 executorはbundle ObjectID以外のkey、path、credential、operation名を受け取らず、`copyto --immutable`と`check --download`だけをtool interfaceへ公開する。
  Rationale: 任意remote key実行とrclone operation拡張をcompile-time境界で防ぐためである。
  Date/Author: 2026-07-15 / Codex

- Decision: diagnostic eventはcanonical EventIDを持つappend-only説明資料に限定し、observer、reconciler、executorはevent storeをaction authorityとして参照しない。
  Rationale: eventの欠落、重複、衝突をaction、observation省略、receipt保存の許可へ変換しないためである。
  Date/Author: 2026-07-15 / Codex

## Outcomes & Retrospective

M3-0の成果は、M2 baselineを`cc9fc2d`として記録し、M3を一つのExecPlanだけから再開できる状態にすることである。
初回M3-1ではProtocol V1、Go/Python conformance、verified raw-day boundary、ordered reducer、marker、segment、row-chainを実装したが、例外監査によりgateはブロックされた。
M3-1CでM2 verifierを実入力境界へ接続し、selected rangeだけを出力するstreaming／bounded reducer、domain digest、full layout key検証、厳格な負のintegration testを追加したが、M3-1D監査でrange、identity、overlap、sink orderingの不足が判明し、Cのgate証拠は無効化された。
M3-1Dでmulti-entry range expansion、producer identity provenance、最長overlap、証明済みhistory change、pre-sink validationを修正した。
focused Go `87 passed / 0 failed`、fixture `20 verified`、Python `18 passed / 0 failed`、`mise run check`、`mise exec -- go vet ./...`、`git diff --check`が成功したため、M3-1 gateを再受入した。
その後のM3-1E監査で、history change proofが任意windowを許していたため、上記の完了証拠を追加修正前の履歴として扱う。
M3-1Eでproofをtail suffixとincoming prefixの境界へ限定し、focused Go `74 passed / 0 failed`、`mise run check`、`mise exec -- go vet ./...`、`git diff --check`を再実行して成功した。
positive history proof、repeated-pattern ambiguity、interior-only negative proofを同じreducer test suiteで確認したが、M3-1F例外監査でarchive proof境界の不足が判明したため、M3-1 gateは未完了へ戻した。
M3-1Fでは、入力値の上書き禁止、compact selectionとM2 canonical selectionの完全一致、relative manifest keyの完全一致、ReplayResourceLimitsの事前検証を追加する。
M3-1Fのfocused Go `66 passed / 0 failed / 0 skipped`、fixture `20 verified`、Python `18 passed / 0 failed`、repository check、vet、gofmt、diff checkは成功した。
このevidenceを添えてadvisor exception re-auditを依頼するが、M3-1 gateは外部再監査がpassするまで未完了であり、この時点ではParquet/publication/deliveryは解禁しない。
この時点のParquet、publication、receipt、CLI、read-only deliveryの実装はまだ完了していないという履歴である。

M3-2の最初の実装停止点では、Parquet writer dependencyを推測せず、Windows-compatible compatibility spikeの実測結果から`v0.30.1`を固定した。
`internal/parquet`のgeneratorはcurrent partだけを保持し、part boundaryをcanonical row countとcanonical row bytesから決め、同一tupleでbyte equalityをfocused testにより確認した。
`internal/archive`のmanifest builderとstrict verifierは、part object digest、day-local predecessor、part_set_root、raw binding、conversion tuple、revision successor、empty-day zero rootを検証する。
strict replay verificationには、generatorが返す検証済みrow-chain rootを明示的に渡し、manifest bytesだけから任意のnonzero rootを受理しない境界を追加した。
M3-2の初回指定verificationは成功したが、レビューでPartManifestのscope/raw provenance、previous row-chain anchor、final part root closureが不足していることが判明したため、その83件証拠は再オープン前のcheckpointとして扱う。
今回の修正でstrict Protocol/Go/Python fixture、outer binding、Archive builder/verifier、generated multi-part integrationを更新し、focused Go 94件、fixture 21件、Python 31件、repository check、vet、gofmt、diff checkを再実行して成功した。
このlocal gate証拠をadvisor再監査へ渡すが、M3-3のpublication、receipt、selector、delivery、R2 uploadは開始しない。

M3-2Gでは、required relative keyをProtocol V1のexact identity helperへ集約し、date-local base、Parquet range/hash、part manifest digest、replay-day digest、U32 path encoding、trusted `r2.Layout` full keyをGo/Pythonとgolden fixtureへ揃えた。
parent ExecPlanのhour例をdate-localへ変更し、marker rowへsynthetic hourを割り当てない決定を記録した。
focused Go `99 passed / 0 failed / 0 skipped`、fixture `22 verified`、Python `32 passed / 0 failed`、repository check、vet、gofmt、diff checkが成功したため、M3-2 local gateを完了へ更新する。
advisor exception re-auditは未完了としてhandoffし、その後M3-2G passを受けてM3-3A publicationへ進んだ。

M3-3Aでは、local verified bytesだけをintentへ固定し、trusted `r2.Layout`から導出したfull/rclone keyで、raw verification、Parquet、part manifest、replay manifest、receiptの順にimmutable publicationを行った。
focused Go `47+20+4+17=88 passed / 0 failed`、fixture `22 verified`、Python `32 passed / 0 failed`、repository check、vet、gofmt、diff checkが成功した。
read-only replay delivery、CLI、selector、cache、Windows Race追加対象、real R2 smoke、M3-4以降は未実施である。

M3-3A reviewでは、stageだけを信用するclaim再開、receipt直前の再検証不足、既存Parquetとpart graphの未検証、孤児objectの過剰許可、List descriptor重複の未拒否を検出した。
これらはpublicationのintegrity boundaryに直接関わるため、前回の受入証拠を再開し、修正後にfocused r2、full check、vet、gofmt、diff checkを再実行する。
review correction後の`internal/r2`は未キャッシュで`52 passed / 0 failed`、`mise run check`はfixture `22 verified`、Python `32 passed`、全Go testを成功させ、vet、空のgofmt、diff checkも成功した。
claim再assert、receipt直前の全remote再検証、streaming Parquet hash/size、part chain、candidate orphan、List descriptor重複の各境界を追加した。

今回のadvisor BLOCK remediationでは、remote prepareをlock取得後へ移し、同一scopeの並行Publishがlock解放前にlistを開始しないことをテストした。
claim mutation/deletion、receipt直前の全remote再検証、bounded resource limit、streaming mismatch、孤児とremote descriptorのstrict拒否を追加した。

M3-3A-R0では、修正が収束しない原因を個別不具合ではなく、local stageとremote事実を同じmonolithic flowへ結合した設計にあると判断した。
旧M3-3A実装とそのtest countを受入証拠から外し、behavior/failure inventoryとして保持した。
新しい成果は`execplan/2026-07-15-m3-3a-publication-redesign.md`に、保持、adapter化、削除の境界とR1からR7の実装順序を固定したことである。
M3-3A-R3では、全remote readとrclone checkを共有aggregate budgetへ課金し、完全なderivative inventory、part chain、replay revision chainをwinner selectionなしで検証し、Exact状態だけをProtocol V1 final observationへ変換するread-only境界を実装した。
M3-3A-R4では、sealed ObjectIDだけを実行し、local sourceを検証済みsnapshotへ固定してimmutable copyとdownload checkだけを行うexecutorと、非権威のappend-only event storeを実装した。
現時点のM3-3AはR7初回監査のchanges_requiredをG3Eで修正し、R7再監査とM3-4は未完了である。
Actual-byte budget、typed terminal classification、bounded Parquet streamのfocused／full testとrepository／Race gateは成功した。
R7再監査は再開できるが、M3-3A publication correctionとM3-4はR7の明示的なpassまで完了扱いにしない。

M3-1以降では、各milestoneの完了時にこの節へ実測結果、未解決事項、計画変更理由を追記する。
特に、fixtureのhash、選択した`parquet-go`版本、writer configuration hash、Windows race結果、real R2をskipした条件を記録する。

## Context and Orientation

リポジトリのProtocol V1正本は`protocol/v1/`であり、wire layout、message、source schema、hash domain、manifest、fixture、conformanceを置く。
M3のarchitecture、scope、milestone acceptanceの正本は`.agent/tick-data-platform-execplan-revised.md`であり、このliving ExecPlanは両正本を具体的な作業と検証へ展開する。
Goのraw受信とdurabilityは`internal/ingest`、`internal/wal`、`internal/journal`が担当し、M2のraw archiveとpublicationは`internal/archive`と`internal/r2`が担当する。
read-only raw deliveryは`internal/delivery`、`cmd/tickctl`、`cmd/tick-verify`にある。
M3では`internal/continuity`、`internal/parquet`、`internal/catalog`をruntime boundaryとして育て、既存raw readerを入力として使う。

M2のraw事実は、Gatewayが受理してWALへ記録した`BatchFrameV1`である。
M2はsealed WAL全体を`raw-wal-segment-v1` objectとして保存し、`raw-day-manifest-v1`はそのobjectのkey、hash、bytes、inclusive coordinate range、campaign chain sliceをcanonical JSONへ記録する。
M3はそのmanifestの参照先を再検証してから、Parquetへ投影する。

以下の用語は、この計画での意味を固定する。

- **ordered overlap**：inclusive cursorで新しいbatchの先頭に既読rowが現れるとき、直前のbounded replay tailと新batchのfingerprint列を順序どおり比較する処理である。
  最長の完全一致suffix/prefixを求め、そのsequenceがtail内で一意な場合だけ既読prefixを再追加せず、unmatched suffixを元の順序で追加する。
  最長候補が複数位置に現れる、または一致を証明できない場合は推測で削除、sort、mergeをせず、batch全体を新しいcontinuity segmentへ送る。
- **same-position payload change**：stable source IDがあり、前後のfingerprint列から一意に同じsource positionへ対応付けられ、payloadだけが異なる状態である。
  mt5.mqltick.v1の`capture_sequence`と`time_msc`だけではこの証拠にならず、session restart、繰り返しpayload、tail不足では`AMBIGUOUS_OVERLAP`を出して全occurrenceを保持する。
  一意な証拠は、保持済みtailのsuffixとincoming prefixの境界に三つ以上のoccurrenceを並べ、両端を除く一つだけのfingerprint差を示す場合に限る。
  同じ最長alignmentがtail内の複数位置へwildcard-compatibleに対応する場合や、内部subsequenceだけが一致する場合は`SOURCE_HISTORY_CHANGED` markerを出さない。
- **continuity segment**：順序とoverlapの証明が連続しているreplay rowの区間である。
  `segment_id`は乱数ではなく、raw manifest digest、開始coordinate、開始理由、直前row-chain anchorから決定する。
  overlapが曖昧、確立済み位置のpayloadが変化、source errorまたはgapが発生した場合は新segmentを開始する。
- **continuity marker**：sourceに存在しないsynthetic Tickを作らず、replay列の途中で境界や失敗理由を示す専用rowである。
  markerには`segment_start`、`ambiguous_overlap`、`source_history_changed`、`source_error`、`gap`の固定enumを使い、価格やvolumeの値は持たせない。
- **canonical replay row**：Parquetのfield値から独立に、row-chain hashを計算するための固定順序、固定整数幅、固定bit pattern、固定string encodingで表した一行である。
  data rowはraw object keyとhash、WAL ingest sequence、producer session、batch sequence、record ordinal、source field、fingerprint、observation hash、segment IDを持つ。
  marker rowは同じrow envelopeとstream sequence、segment ID、marker enum、reason、参照coordinate、直前anchorを持つが、source Tick fieldは持たない。
- **row-chain**：canonical replay rowをreducerの順序で一つずつ連結するhash列である。
  前のrow hashを次の計算へ入れるため、同じrowが抜ける、順序が変わる、markerが抜けるとrootが変わる。
- **conversion tuple**：replay結果を識別する`replay_contract_id`、`format_id`、`conversion_id`、`converter_build_id`、`dependency_lock_hash`、`writer_configuration_hash`、`target_platform_contract`の組である。
  raw manifest keyとhashは変換入力のbindingであり、conversion tupleそのものには含めない。
- **deterministic part boundary**：ordered row streamをpartへ分割する、入力だけから再現できる規則である。
  part開始時のrow countとcanonical row byte countを0にし、次rowを追加する前に、追加後のrow countまたはbyte countがM3-1で固定した上限を超えるなら現在partを閉じる。
  空のpartは作らず、単独rowがbyte上限を超える場合はそのrowだけを含むpartを許す。
  圧縮後Parquet bytes、filesystem block、upload responseをboundary判定へ使わない。
- **part manifest**：一つのParquet partのkey、hash、bytes、row範囲、row count、canonical byte count、row-chain開始と終了、前part digestを記録するimmutable manifestである。
  part sequenceはdayごとに0から始まり、最初のpartのpredecessorはnullである。
- **day-local part chain**：同じdataset、campaign、day definition、date、replay contract、conversion tupleの日だけでpart manifest digestを順に結ぶchainである。
  前日partやcampaign-global WAL chainをpredecessorへ使わないため、その日だけ取得してもpart chainを検証できる。
- **replay-day binding**：replay-day manifestにraw manifestのimmutable keyと、`"tick-data-platform/raw-day-manifest/v1\0" || canonical bytes`のSHA-256の両方を保存し、そのkeyのobjectを取得してdomain digestを再計算する規則である。
  keyだけ、hashだけ、prefix listingのlatestだけではbindingを満たさない。
- **replay-day revision**：同じscope、format、conversion tupleのreplay manifestについて、raw-dayのlate evidenceにより入力hashが変わったときに、既存manifestを変更せずsuccessorを追加する番号である。
  converter、dependency、writer、platformが変わったときはconversion tupleまたはconversion_idを変えてrevision 1の別derivativeを作り、旧derivativeを上書きしない。
- **diagnostic event store**：bundle、observation、approved action、resultをappend-onlyで説明する非権威の記録である。
  eventの存在、欠落、重複、衝突をremote事実またはaction authorityへ変換せず、再開時はsealed bundleからfresh observationを作る。
- **publication receipt**：bundle identityとcomplete final observationを結ぶpoint-in-timeのno-clobber検証証拠である。
  receiptが保存されるまでderivative publicationをacceptedと報告せず、receiptから将来のremote不変性を推論しない。
- **read-only replay selector**：mutableな`latest`ではなく、dataset、campaign、date、replay contract、conversion、revision、manifest keyまたはhashでsnapshotを一意に選ぶ入力である。
- **local cache**：remote objectをhash付きpathへ一度保存し、bytesとSHA-256を検証した後だけ利用するread-only側の作業領域である。
  cacheは空でもよく、cacheの存在、Gateway SQLite、publication journal、write credentialをread pathの前提にしない。
- **integrity failure**：bytes、hash、sequence、scope、predecessor、revision、key、canonical encodingのどれかが期待値と一致しない状態である。
  integrity failureではwinnerを推測せず、壊れたobjectを成功扱いにせず、原因と対象identityを返して公開を止める。

### M3-1で固定するencoding

M3-1は、Protocol V1の`LP`、`U8`、`U32`、`U64`、`I32`、`I64`、`H32`、canonical JSONの規則を再利用し、次のdomainとfield順序をfixtureで固定した。
`U8`は符号なし1 byte、`I32`はlittle-endianの符号付き32-bit整数とし、`LP`はProtocol V1と同じU16長のUTF-8 bytesで255 bytesを上限にする。
上限を超えるstringは切り詰めずにrejectする。
ここで示す形式は実装者が省略できない凍結済み契約であり、`protocol/v1/hash-domains.md`、Go codec、Python verifier、`replay-v1-conformance.json`の同値結果を正本とする。

canonical replay rowは、次の順に連結したbytesとする。

    "tick-data-platform/replay-row/v1\0"
    U8(row_kind)
    U64(stream_sequence)
    LP(dataset_id)
    LP(campaign_id)
    LP(day_definition_id)
    LP(date)
    LP(replay_contract_id)
    LP(conversion_id)
    LP(continuity_segment_id)
    H32(raw_day_manifest_sha256)
    LP(raw_object_key)
    H32(raw_object_sha256)

`row_kind=1`のdata rowは、common fieldの後に次を続ける。

    U64(gateway_ingest_sequence)
    LP(producer_instance_id)
    LP(producer_session_id)
    U64(batch_sequence)
    U32(record_ordinal)
    U64(capture_sequence)
    I64(time)
    U64(bid_bits)
    U64(ask_bits)
    U64(last_bits)
    U64(volume)
    I64(time_msc)
    U32(flags)
    U64(volume_real_bits)
    H32(source_payload_fingerprint)
    H32(observation_hash)
    I64(fetch_wall_start_s)
    I64(fetch_wall_end_s)
    U64(fetch_monotonic_start_us)
    U64(fetch_monotonic_end_us)
    I32(copy_ticks_error)
    U32(source_status_flags)

`row_kind=2`のmarker rowは、data rowのsource fieldをzero値で埋めず、common fieldの後に次のmarker payloadだけを続ける。

    "tick-data-platform/replay-marker/v1\0"
    LP(marker_code)
    LP(reason)
    LP(marker_detail)
    U64(reference_gateway_ingest_sequence)
    U32(reference_record_ordinal)
    H32(predecessor_row_chain_hash)
    H32(continuity_segment_start_hash)

marker rowの`raw_object_key`と`raw_object_sha256`は参照raw evidenceがある場合に必須であり、source errorのようにcoordinateだけがある場合はkeyとhashをzero値にし、marker payloadのreferenceへ理由を入れる。
marker rowのParquet表現では、欠損をNULLとして表し、zero値を観測値として解釈できないschemaにする。

row-chainの各hashは、先頭rowのpredecessorを32 byteのzeroとし、次のbytesから計算する。

    SHA256("tick-data-platform/replay-row-chain/v1\0" || U64(stream_sequence) || H32(previous_row_hash) || U32(canonical_row_bytes_length) || canonical_row_bytes)

`canonical_stream_row_chain_root`は最後のrow hashであり、rowがない場合だけzero hashとする。
part manifestにはpart開始時のpredecessor row hashとpart終了時のrow hashを保存し、partを連結し直して全体rootを再計算できるようにする。

`part_set_root`は、day-localで順序付けたpart manifest key、digest、row範囲を次のdomainへ入れて計算する。

    SHA256("tick-data-platform/part-set/v1\0" || U32(part_count) || U32(path_bytes_length) || path_bytes(part_manifest_key) || H32(part_manifest_digest) || U64(first_stream_sequence) || U64(last_stream_sequence) repeated in part_sequence order)

part manifest自身のdigestは`tick-data-platform/part-manifest/v1\0`とcanonical JSON bytesから計算する。
M3-1ではこのbytes、空集合のzero root、part sequenceの連続性、同じpart keyのcollision、predecessorの扱いをgolden fixtureへ固定する。

## G0 worktree inventory and preservation boundary

G0はread-onlyで実施し、branch `agent/m3-replay-parquet-delivery`とfull HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`を確認した。

開始時点のworktreeはtracked modified 24、untracked 26、staged 0であり、`git diff --check`と`git diff --cached --check`は成功した。

G0ではremote I/O、GitHub CI確認、reset、rebase、stash、clean、checkout、一括破棄を行わなかった。

tracked modified 24は次のとおりである。

```text
.agent/tick-data-platform-execplan-revised.md
docs/plan/roadmap.md
go.mod
go.sum
internal/archive/manifest.go
internal/archive/raw_key.go
internal/ingest/gateway.go
internal/protocol/protocol.go
internal/r2/backend.go
internal/r2/doc.go
internal/r2/journal.go
internal/r2/layout.go
internal/r2/layout_test.go
internal/r2/publisher.go
internal/r2/publisher_test.go
protocol/v1/README.md
protocol/v1/fixtures/README.md
protocol/v1/go/codec.go
protocol/v1/hash-domains.md
protocol/v1/manifests.md
testdata/tickdata/golden/index.json
tests/unit/test_protocol_v1.py
tools/tick_fixture_verify.py
tools/tick_protocol.py
```

untracked 26は次のとおりである。

```text
execplan/2026-07-15-m3-3a-publication-redesign.md
execplan/2026-07-15-m3-replay-parquet-delivery.md
internal/archive/replay_manifest.go
internal/archive/replay_manifest_test.go
internal/archive/replay_source.go
internal/archive/replay_source_test.go
internal/continuity/reducer.go
internal/continuity/reducer_test.go
internal/continuity/source.go
internal/parquet/generator.go
internal/parquet/generator_test.go
internal/parquet/row.go
internal/parquet/spec.go
internal/protocol/lease_test.go
internal/protocol/replay.go
internal/protocol/replay_conformance_test.go
internal/protocol/replay_json.go
internal/protocol/replay_test.go
internal/r2/replay_journal.go
internal/r2/replay_limits.go
internal/r2/replay_publisher.go
internal/r2/replay_publisher_test.go
internal/r2/replay_receipt.go
testdata/tickdata/golden/part-manifest-v1.json
testdata/tickdata/golden/replay-key-contract-v1.json
testdata/tickdata/golden/replay-v1-conformance.json
```

- **保持対象**：M3-1とM3-2GのProtocol、fixture、GoとPython conformance、verified replay source、continuity、Parquet、part/replay manifest、exact key binding、trusted `r2.Layout`の差分を保持する。
- **保持更新対象**：親ExecPlan、本living ExecPlan、M3-3A redesign ExecPlan、roadmapの四文書を正本として保持し、各gateの進捗、発見、判断、証拠を同期する。
- **境界縮小再利用対象**：bounded backend、trusted `r2.Layout`、既存のcampaign/epoch lock、pinned tool allow-list、local no-clobber primitive、旧testが表すtimeout、mutation、collision、orphanのuseful fault caseを新APIの境界で再利用する。
- **useful-case移行後の置換削除対象**：monolithic `ReplayPublisher`、`ReplayStage*`、SQLite replay transition、`journal_intent_hash`をauthorityにするreceipt、stage-driven restart testを、新しいtruth tableとfault testが同じ有用事例を覆った直後に置換または削除する。
- **M2保持境界**：publisher claimの作成主体とconditional helperはM2 raw publisherに残し、M3はclaimを作成せずExact observationだけを受理する。

workflow定義はrepo `AGENTS.md`、`.github/workflows/check.yml`、`.github/workflows/windows-race.yml`、`mise.toml`、global `advisor.toml`、`implementer.toml`、`semble-search.toml`、`delivery-orchestrator/SKILL.md`、`delivery-orchestrator/references/contracts.md`を保持し、G0Aでは変更しない。

`semble-search.toml`はrole名`semble_search`として存在するが、G0とG0Aは既知pathとread-only Git確認で完了したため、`semble_search` childを起動せずsemantic searchも使わなかった。

このinventoryはdirty差分の由来と今後の扱いを固定する計画証拠であり、旧M3-3A runtimeまたは過去のtest countを新設計の受入証拠へ昇格させない。

## Plan of Work

### M3-1 Protocol V1とfixtureの実装ゲート

このmilestoneでは、後続実装が読む契約を先に完成させ、Go/Pythonの同一fixtureで検証する。
`protocol/v1/manifests.md`へ`part-manifest-v1`のschemaを追加し、replay-day manifestへ`raw_day_manifest_key`と`raw_day_manifest_sha256`を追加する。
raw keyは対象scope、date、revision、manifest digestからlayoutが導くkeyと完全一致し、取得したcanonical bytesのdigestも同じ値でなければならない。
`part_set_root`、canonical row-chain、marker row、replay revision successorのbytesと拒否条件を`protocol/v1/hash-domains.md`またはM3用のProtocol V1文書へ追加する。

Go codec/verifier、Python計算/verifier、JSON golden fixtureを同じ入力から作る。
fixtureは、通常row、同一payloadの複数occurrence、ambiguous overlap、history change、marker、空day、part境界、raw key/hash mismatch、revision branch、missing predecessorを含む。
GoとPythonが同じcanonical row bytes、part digest、part_set_root、row-chain rootを出すまでM3-1を完了にしない。
このmilestoneのgate通過前は、Parquet runtime、remote publication、deliveryの実装をprototype以外へ進めない。

### M3-2 ordered overlap reducerとsemantic replay

`internal/continuity`には、M2 archiveが`VerifyRawDayManifest`、`VerifyRawDaySnapshot`、WAL verifierを通過したreaderだけを受け取る入力境界と、ordered source occurrenceを展開するstreaming `Reduce`を置く。
callerが`Entries`、chain start、manifest identityを差し替えられる公開structは作らない。
reducerは`chain_objects`をWAL sequence順に検証し、entry内のrecord ordinal順を保ち、raw observationを変更しない。
各batchの`source_payload_fingerprint`列を前回tailと比較し、最長で一意な完全一致prefixだけを既読overlapとみなす。
前後fingerprintが一意に位置を証明するpayload変更だけを`SOURCE_HISTORY_CHANGED`へし、それ以外の位置不確実性は`AMBIGUOUS_OVERLAP`として全occurrenceを保持する。
history changeの証明は、保持tailのsuffixとincoming prefixの境界に限定し、内部subsequenceを候補にしない。
missing predecessor、sequence gap、raw object mutation、scope mismatchは`integrity failure`として返す。
曖昧なoverlapはdropせず、markerを先に置いてnew continuity segmentを開始する。

reducerの出力はsinkへ送ったrow数、marker数、tail、segment、row-chain rootだけを持つbounded summaryにする。
同じraw-day manifestを二度reduceして同じrow bytes、同じmarker位置、同じsegment ID、同じrootになることをunit testとproperty testで確認する。
M3-1C、M3-1D、M3-1Eでは既存M2 builderで、manifest scope/config、domain digest、key、chain_objects、sealed WAL、multi-entry selected range、producer identity、wrong producer、最長overlap、境界限定history changeを実際に検証するintegration testを追加する。
内部old subsequenceだけが一置換で一致するnegative caseでは`SOURCE_HISTORY_CHANGED`を出さず、`AMBIGUOUS_OVERLAP`と全incoming occurrenceを確認する。

### M3-2 version-scoped Parquet生成

依存追加前に、リポジトリrootで次を実行する。

    mise exec -- go version
    mise exec -- go env GOOS GOARCH CGO_ENABLED GOPROXY
    mise exec -- go list -m -versions github.com/parquet-go/parquet-go
    mise exec -- go list -m -json github.com/parquet-go/parquet-go@latest

候補tagごとに、moduleの`GoVersion`がrepositoryのGo toolchain以下であること、必要なAPIが存在すること、Windows互換APIであることを確認する。
このspikeでは`go1.24.13 windows/amd64`、`v0.30.1`のmodule GoVersion `go1.24.9`、typed writer、typed reader、footer open、unsigned logical typeを確認した。
選定tagは`github.com/parquet-go/parquet-go v0.30.1`であり、module checksumは`h1:Oy6ganNrAdFiVwy7wNmWagfPTWA2X9Z3tVHBc7JtuX8=`である。
`go.mod`にはこのtagを直接固定し、`go.sum`のmodule checksumを`ConversionSpec`のdependency lock hashへ反映する。

`internal/parquet`では、implicit defaultを使わず、`ticks-parquet-v1`の明示schema、固定column order、input orderをそのまま書くwriterを作る。
writer設定は次を固定値とする。

    Compression: parquet.Uncompressed
    DataPageVersion: 1
    DataPageStatistics: true
    DeprecatedDataPageStatistics: false
    PageBufferSize: 256 * 1024
    WriteBufferSize: 32 * 1024
    BloomFilters: none
    sorting writer: not used; reducer order is authoritative
    CreatedBy: fixed application, replay format, and converter_build_id
    KeyValueMetadata: fixed keys and canonical values only

`ConversionSpec`はconversion tuple、dependency lock hash、writer configuration hash、target platform contract、`MaxRowsPerPart`、`MaxCanonicalBytesPerPart`、`MaxRowsPerRowGroup`を必須にし、zero、overflow、無限相当の値を拒否する。
partの最大row数と最大canonical row bytesは同じordered streamから決め、圧縮後bytes、filesystem、row group flushをboundaryへ使わない。
partはrow streamをreorderせず、deterministic part boundaryの式だけで切る。
part close後にwriterをCloseし、temporary pathをsyncしてからcontent hashを計算し、同じhashのfinal pathへno-clobberでpromoteする。
Parquetをreopenしてschema、row count、ordered canonical row values、unsigned bit pattern、part row-chain anchor、file hashを検証してから次工程へ渡す。

### M3-2 strict part/replay manifest

`internal/archive.BuildPartManifest`は、Parquet生成後のbounded `PartManifestInput`からのみ`part-manifest-v1`を作る。
part bytes、row count、canonical row bytes、stream range、first and last row-chain hash、object keyとSHA-256、day-local predecessorを検証する。
`VerifyPartManifestObject`はcanonical JSON bytes、content-addressed key、manifest digest、row range、predecessorを再計算し、same-key different-content、branch、gap、overlapを拒否する。

`BuildReplayDayManifest`は`protocol.ReplayScope`のcampaign-relative raw keyとM2 domain hash、固定ConversionTuple、ordered part manifest keys、part_set_root、canonical stream row-chain root、completeness、revisionを一つのM3 formへ結合する。
empty dayはpart manifest keys、part_set_root、row-chain rootを全てzeroまたは空配列にする。
raw revision successorはscopeとconversionを保ち、新しいraw keyとhash、直前digest、revision番号を要求する。
conversion変更はpreviousを受け付けず、別identityのrevision 1として生成する。
`VerifyReplayDayManifestObject`はmanifest bytes、manifest key、scope、conversion、part chain、part_set_root、raw binding、revision predecessorを一括検証する。
物理keyはProtocol V1の一元`ExactIdentityPathKey`からdate-local campaign-relativeに導出し、Parquet key、part manifest key、replay-day keyのbase、scope、conversion、date、range、digestを完全一致させる。
part_set_rootのpart key要素はU32 path byte lengthとpath bytesでhashし、transport stringのU16/255-byte制限とは分離する。
Generatorのreopen verifierはcomputed digest、range、Protocol keyを同時に比較し、`objects/replay`、`manifests/replay`、`snapshots/replay`、hour aliasをno-clobber promotion前に拒否する。
trusted `r2.Layout`だけが検証済みrelative keyへimmutable rootとcampaign prefixを一度だけprependする。
この工程はR2 publication、publication journal、receiptを作らない。

### M3-3 manifest、catalog、derivative publication

M3-3Aの詳細な実装正本は`execplan/2026-07-15-m3-3a-publication-redesign.md`とする。

旧`ReplayPublisher`、`ReplayStage*`、SQLite transition、`journal_intent_hash` receipt authority、stage-driven restart testはbehavior/failure inventoryへquarantineする。

R1ではbundleとfinal observationのProtocol V1 canonical schema、domain digest、Go fixture、Python verifierを固定する。

R2ではlocal verified inputとtrusted `r2.Layout`からimmutable `ReplayPublicationBundle`をsealし、filesystem、network、clock、SQLite、eventから独立したpure reconcilerを実装する。

R2のreconcilerは同じbundleとobservationから決定的に最初の未充足barrierだけを返し、bundleとcomplete final observationのbudgetがlock前に収まることを検査する。

R3ではclaim、raw、derivative namespace、part graph、replay revision graphをaggregate budget内で観測するread-only bounded observerとgraph verifierを実装する。

R3のtimeout、到達不能、不確定readはUnavailable、読取可能な不正、欠落、矛盾はAmbiguous、上限超過はOversizedとし、Absentへ変換しない。

R4ではapproved object IDだけを受けてlocal bytesとhashを再検証し、`copyto --immutable`と`check --download`だけを行うnarrow executorを実装する。

R4ではappend-only diagnostic event storeを実装し、eventをaction authorityまたはremote事実の代用にしない。

R5ではbundle sealing、lock、fresh observation、pure reconcile、approved execution、fresh final observation、receipt保存だけを接続するthin publisherを実装する。

R5は最初のremote observationからcomplete final observationとreceipt no-clobber保存までcampaign/epoch lockを保持し、旧runtimeをuseful-case移行後に削除または置換する。

R6ではcrash、timeout、Unavailable、Different、Ambiguous、Oversized、same-content retry、mutation、branchをfault matrixで検証する。

R7では旧runtime authorityが残っていないことと全gateを監査する。

publication barrierはclaim Exact、raw Exact、Parquet upload/check、part manifest upload/check、全part Exact、replay manifest upload/check、complete final observation、receiptの順に保つ。

M2 raw publisherだけがpublisher claimを作成し、M3は各observationでExactを要求する。

Unavailableはretryableなzero-action、DifferentとAmbiguousはintegrity stop、Oversizedはresource stopとする。

eventは診断証拠であり、reconcileの入力またはremote事実のauthorityにしない。

empty dayではParquetとpart manifestを作らず、zero-root replay manifestだけを最後に公開する。

### M3-4 read-only replay delivery

`internal/delivery`へreplay-specific selectorとdescriptorを追加し、remote listingからrevision、conversion tuple、raw binding、part_set_root、row-chain rootを検証してからsnapshotを返す。
mutableなlatest pointerを作らず、manifest keyまたはhashを解決済みsnapshotへ保存する。
`ArchiveReaderV1`のraw methodを壊さず、`ListReplaySnapshots`、`ResolveReplaySnapshot`、`BuildReplayFetchPlan`、`VerifyReplayDay`に相当するversioned methodを追加する。

`tickctl snapshots replay`はdataset、campaign、date、stream、conversion、任意のrevisionまたはimmutable manifest selectorを受け、descriptorをcanonical JSONでstdoutへ出す。
`tickctl fetch`は選択したreplay manifest、part manifests、Parquet partsをcacheへ取得し、検証済みlocal pathとhashをJSONで報告する。
`tick-verify replay-day`はraw key/hash binding、raw snapshot semantic verification、part manifest chain、part_set_root、Parquet schema、row count、part file hash、canonical row-chain rootを報告する。
day単位の検証はcampaign全体のWAL genesisを検証したとは報告しない。

cacheが空または存在しなくても、read-only readerはremote manifestから処理を開始する。
cache pathはobject hashから決め、temporary fileへdownloadし、sizeとSHA-256を確認してからfinal pathへrenameする。
既存cacheのbytesがhashまたはsizeと一致しない場合は破損として扱い、成功扱いで再利用しない。
cache検証はremoteを変更せず、Gateway SQLite、publication journal、write credentialを開かない。

### M3-5 fake end-to-endと検証gate

fake object backendとfake rcloneで、raw manifestの再検証からreplay reducer、Parquet、part manifest、replay manifest、非権威のappend-only diagnostic event store、final observation receipt、empty-cache fetch、local verifyまでを一つのnetwork-free testにする。
M3-1のGo fixtureをPython verifierで読み、Pythonで計算したrow-chain rootとpart_set_rootをGo生成物と比較する。
M3の通常gateはlocal fake backendで完結し、real R2を必要条件にしない。
`windows-race.yml`のrace対象へM3で追加したpackageを含め、CGO-enabled Windows runnerで検証する。

## Concrete Steps

作業はリポジトリroot `C:\projects\utaki-tick-data-platform`で行う。
M3-0の開始時には、既存変更を保存したまま次を実行し、branchとbaselineを確認する。

    git status --short --branch
    git show -s --format=%H%n%s cc9fc2d

出力はbranchが`agent/m3-replay-parquet-delivery`であり、commit subjectがPR #3のmergeを示すことを確認する。

M3-1では先にProtocol V1の文書、schema、fixture、conformanceを編集し、次を実行する。

    mise trust
    mise install
    mise run bootstrap
    mise exec -- go test ./internal/protocol ./internal/continuity -count=1
    mise run fixture
    mise run test-python

`protocol/v1/go`がtest packageを持たない場合は、Go testの対象から外し、`internal/protocol`とfixture entry pointの成功を記録する。
fixture mismatch、unknown key、noncanonical bytes、raw key/hash mismatch、revision branch、missing predecessorは期待されたintegrity errorとして確認する。

M3-1D corrective reworkでは、実際のM2 builderが作るsealed WALとraw-day manifestだけを入力に使い、次のfocused verificationを実行する。

    mise exec -- go test ./internal/ingest ./internal/wal ./internal/archive ./internal/protocol ./internal/continuity -count=1
    mise run fixture
    mise run test-python
    mise run check
    mise exec -- go vet ./...
    git diff --check

2026-07-15の実測値は、focused Goが`internal/ingest 11`、`internal/wal 16`、`internal/archive 12`、`internal/protocol 32`、`internal/continuity 16`で合計`87 passed / 0 failed`、fixtureが`20 verified`、Pythonが`18 passed / 0 failed`である。
`mise run check`、`mise exec -- go vet ./...`、`git diff --check`も成功した。
三つのWAL entryにまたがるpartial range、wrong producer、longest overlap、repeated-pattern ambiguity、positive history proof、invalid rowのsink未到達をfocused testで確認する。

M3-1Eでは、`internal/continuity/reducer.go`のhistory proofを保持tail suffixとincoming prefixの境界だけへ限定する。
最長境界alignmentについて、三つ以上のoccurrence、両端を除く一つの変更、前後fingerprintの完全一致、tail内のwildcard-compatible位置の一意性を確認する。
内部old subsequenceだけが一置換で一致するnegative testを追加し、`SOURCE_HISTORY_CHANGED`を出さず`AMBIGUOUS_OVERLAP`で全occurrenceを保持する。

M3-1F corrective reworkでは、入力manifestを検証前に上書きしない。
verified sealed WALからM2 full-day canonical selectionを独立に再導出し、compact rangeをper-entry coordinateへ展開したselection、件数、watermark、chain slice、chain_objects、expanded raw_set_rootを全て比較する。
7-of-9 partial selection、cross-day selection、rangeの部分改変は拒否し、全座標と同値なcompact form、zero-record sentinel、source-error entryだけを受理する。
`OpenVerifiedReplaySource`の入力は`ManifestRelativeKey`の完全一致へ限定し、full remote rootの検証をtrusted `r2.Layout`へ残す。
`ReplayResourceLimits`のnon-zero U64 `MaxChainObjects`、`MaxObjectBytes`、`MaxChainBytes`をreplay_contract_idへ紐付け、descriptorと再検証file bytesを開く前にoverflow-safeに検査する。

M3-2レビュー修正では、先に次の契約を再確認する。

`PartManifestV1`のcanonical JSONはexact ReplayScope、raw-day manifest key+domain digest、ConversionTuple、part summary、`previous_row_chain_hash`、manifest predecessorを全て含む。

`PartManifestInputFromArtifact`は検証済みParquet `PartArtifact`、exact scope、ConversionTupleからだけscope-bound inputを作り、part manifestのprovenanceをcallerの別入力から受け取らない。

part setの全partはscope、raw binding、ConversionTupleを一致させ、part 0のprevious row-chain hashをzero、successorの同値を直前partのlast row-chain hash、stream rangeを連続として検証する。

`BuildReplayDayManifest`と`VerifyReplayDayManifestObject`はempty dayのzero root、non-empty dayのfinal part `LastRowChainHash`一致を要求し、任意のnonzero rootを受理しない。

cross-day、cross-campaign、cross-conversion、changed raw binding、wrong previous row-chain anchor、replay root mismatchのnegative testをfixtureとfocused Goへ追加する。

M3-2からM3-5では、変更したpackageへfocused testを先に実行する。

    mise exec -- go test ./internal/continuity ./internal/archive ./internal/parquet ./internal/catalog ./internal/r2 ./internal/delivery ./cmd/tickctl ./cmd/tick-verify -count=1

M3-5では、GoとPythonのfixture、repository gate、静的検証を次の順で実行する。

    mise run fixture
    mise run test-python
    mise run check
    mise exec -- go vet ./...
    git diff --check

`mise run check`はGo test、Python unit/stateful test、fixture、Ruff、gofmt、diff checkを含む。
`go vet`は別コマンドでも成功する必要がある。

Windows Race Detectorは`.github/workflows/windows-race.yml`へM3で追加したpackageを含めてから、workflow dispatchまたはbranchのpush、Pull Request eventで実行する。
workflow内のrace commandは、既存対象に`./internal/continuity`、`./internal/parquet`、M3で追加したreplay、catalog packageを加えた次の形にする。

    mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/catalog ./internal/continuity ./internal/parquet

Windows runnerでは`CGO_ENABLED=1`、`gcc --version`、mise-managed Goのversionを先に確認する。
追加packageのRace Detector結果が失敗した場合、M3のcompletionを宣言せず、failure原因をこのplanのProgressとSurprisesへ記録する。

real R2 smokeは、次のすべてを満たす場合だけ実行する。

    mise exec -- go test -tags real_r2_smoke ./internal/delivery -run 'TestOptionalRealR2Smoke|TestOptionalRealR2ReplaySmoke' -count=1

条件は、productionと分離したbucketとprefix、明示的なendpoint、read/write credential、存在する`RCLONE_CONFIG`、platformに一致するpinned rclone binary、明示的なenable flag、`I_UNDERSTAND_NO_OVERWRITE`相当の確認値である。
条件が一つでも欠ける場合はtestのskip出力をそのまま記録し、smoke未実施を失敗ともM3完了証明とも扱わない。
smokeはsynthetic content-addressed objectだけを使い、`sync`、`move`、`delete`、`purge`、overwrite、production prefixへの書込みを行わない。

## Validation and Acceptance

M3-0の受入条件は、対象3文書を確認でき、parent planとroadmapが`cc9fc2d`をM2 baselineとして記録し、M3実装を開始または完了したという記述を含まないことである。

M3-1は、GoとPythonが同じfixtureから同じcanonical replay row bytes、marker bytes、row-chain root、part manifest digest、part_set_root、replay manifest digestを生成し、raw manifestのkey、domain hash、sealed WAL、selected rangeを検証したときに完了する。

初回M3-1の`18 passed`、fixture `20 verified`、Python `17 passed`という証拠は、caller-supplied Entries、toy manifest、plain SHA-256 bindingを含む実装に対するものであり、監査ブロックにより無効化した。
M3-1Cの`69 pass / 0 fail`は、M3-1D監査で追加不足が判明した時点でgate証拠として無効化した。
M3-1D corrective gateの受入には、以下のコマンドを全て再実行してpass件数と失敗件数を記録することを要求した。

    mise exec -- go test ./internal/ingest ./internal/wal ./internal/archive ./internal/protocol ./internal/continuity -count=1
    mise run fixture
    mise run test-python
    mise run check
    mise exec -- go vet ./...
    git diff --check

M3-1D corrective gateの実測結果は、focused Go `87 passed / 0 failed`、内訳は`internal/ingest 11`、`internal/wal 16`、`internal/archive 12`、`internal/protocol 32`、`internal/continuity 16`である。
`mise run fixture`は`20 verified`、`mise run test-python`は`18 passed / 0 failed`、`mise run check`、`mise exec -- go vet ./...`、`git diff --check`は成功した。
GoとPythonのnegative testは、raw-day domain digestをplain SHA-256へ置き換えたbindingを拒否する。
M3-1Eの検証では、次のコマンドを実行し、focused Go、repository check、vet、diff checkの全てを再確認する。

    mise exec -- go test ./internal/continuity ./internal/protocol ./internal/archive ./internal/ingest -count=1
    mise run check
    mise exec -- go vet ./...
    git diff --check

新規`TestReduceDoesNotUseInteriorHistoryWindowWithoutBoundaryProof`が`SOURCE_HISTORY_CHANGED=0`、`AMBIGUOUS_OVERLAP=1`、incoming data全件保持を示し、既存のpositive history testとrepeated-pattern ambiguity testも成功した場合だけM3-1Eを完了とする。
実測値はfocused Go `74 passed / 0 failed`、`mise run check`成功、`mise exec -- go vet ./...`成功、`git diff --check`成功である。

M3-1Fのexception re-auditでは、次のコマンドを実行する。

    mise exec -- go test ./internal/archive ./internal/continuity ./internal/protocol ./internal/wal -count=1
    mise run fixture
    mise run test-python
    mise run check
    mise exec -- go vet ./...
    mise exec -- gofmt -l cmd internal
    git diff --check

`gofmt -l`は空出力でなければならず、resource limit違反ではOpenまたはreader再検証がRowSinkより前に`ErrIntegrity`で停止しなければならない。
focused testは7-of-9 partial reject、全座標compact accept、cross-day reject、zero-record sentinel、source-error、relative key、arbitrary root、zero/count/object-bytes/chain-bytes/overflow limitを実測する。
M3-1Fの全コマンドが成功しpass/fail件数を記録するまで、M3-1 gateを完了または下流を解禁と扱わない。

`mise exec -- go test -race ./internal/protocol ./internal/continuity -count=1`は既定のCGO無効環境で実行できず、`CGO_ENABLED=1`でもlocal `gcc`が存在しないため実行できなかった。
M3-1を通過するまで、Parquet、publication、deliveryの実装を完成扱いにしない。

M3-1は、inclusive overlapの既読prefixだけを除去し、別occurrenceの同一payloadを保持し、ambiguous overlap、history change、gap、source errorをmarkerと新segmentとして表現し、timestamp sortingや推測dedupeをしないときに完了する。

M3-2は、`v0.30.1`のtag、module checksum、module GoVersion、Windows-compatible APIがExecPlanと`go.mod`、`go.sum`に一致し、固定schema、nullable data/marker、unsigned bit columns、bounded part boundary、close/sync/hash/reopen verificationが成功したときに完了する。
`ConversionSpec`のtuple、dependency lock hash、writer configuration hash、target platform、三つの有限limitが同じ再実行で一致しなければならない。
part manifestのcanonical bytes、digest、key、predecessor、range、part_set_rootと、replay-day manifestのraw key/domain hash、conversion tuple、part keys/root、row-chain root、empty-day、revision successorをstrict negative testで確認する。
logical row、row-chain root、part row layoutは同じ入力で一致しなければならない。
Parquet全byte一致は、同じconverter build、dependency lock、writer configuration、platform contractの場合だけ受入条件とする。

M3-2実装checkpointの実測コマンドは次である。

    mise exec -- go test ./internal/parquet ./internal/archive ./internal/protocol ./internal/continuity -count=1
    mise run fixture
    mise run test-python
    mise run check
    mise exec -- go vet ./...
    mise exec -- gofmt -l cmd internal
    git diff --check

focused Goは`internal/parquet 4 passed / 0 failed`、`internal/archive 17 passed / 0 failed`、`internal/protocol 32 passed / 0 failed`、`internal/continuity 30 passed / 0 failed`で、合計`83 passed / 0 failed`だった。
fixtureは`20 verified`、Pythonは`18 passed / 0 failed`、`mise run check`、`go vet`、空の`gofmt -l`、`git diff --check`は成功した。
この証拠はM3-2のfocused実装checkpointを示すが、Parquet/publication/deliveryの下流gateやadvisor再監査を完了扱いにはしない。

M3-2レビュー修正後の現行受入証拠は、次のコマンドで取得した。

    mise exec -- go test ./internal/parquet ./internal/archive ./internal/protocol ./internal/continuity -count=1
    mise run fixture
    mise run test-python
    mise run check
    mise exec -- go vet ./...
    mise exec -- gofmt -l cmd internal
    git diff --check

focused Goは`internal/parquet 4 passed / 0 failed`、`internal/archive 19 passed / 0 failed`、`internal/protocol 41 passed / 0 failed`、`internal/continuity 30 passed / 0 failed`で、合計`94 passed / 0 failed`だった。
fixtureは`21 verified`、Pythonは`31 passed / 0 failed`だった。
`mise run check`、`mise exec -- go vet ./...`、空の`gofmt -l cmd internal`、`git diff --check`も成功した。
part scope/raw/conversion binding、previous row-chain predecessor、arbitrary nonzero replay rootのbuild/verify負例、generated/reopened three-part Parquet integrationをこの現行gateで確認した。
このlocal evidenceはM3-2G修正前の履歴checkpointであり、現行M3-2完了証拠とは扱わない。
M3-2Gの全gateが再度成功するまでM3-2は未完了で、advisor再監査を依頼し、M3-3を開始しない。

M3-2G key-contract correctionの現行検証は次を実行する。

    mise exec -- go test ./internal/parquet ./internal/archive ./internal/protocol ./internal/r2 -count=1
    mise run fixture
    mise run test-python
    mise run check
    mise exec -- go vet ./...
    mise exec -- gofmt -l cmd internal
    git diff --check

focused Goは`internal/parquet 4 passed / 0 failed / 0 skipped`、`internal/archive 20 passed / 0 failed / 0 skipped`、`internal/protocol 42 passed / 0 failed / 0 skipped`、`internal/r2 33 passed / 0 failed / 0 skipped`の合計`99 passed / 0 failed / 0 skipped`だった。
fixtureは`22 verified`、Pythonは`32 passed / 0 failed`だった。
`mise run check`はrepository Go test、fixture、Python、Ruff、gofmt、diff checkを成功させ、`mise exec -- go vet ./...`、`mise exec -- gofmt -l cmd internal`の空出力、`git diff --check`も成功した。
Go/Pythonのgoldenはcanonical row、marker、row-chain、part digest、U32 path-encoded part_set_root、replay manifest digest、exact Parquet／part／replay keyを一致させた。
現行Goalに従いhour partitionは追加せず、M3-3のpublication、receipt、selector、delivery、R2 uploadは開始しない。

旧M3-3Aの`52 passed`、`86 passed`、repository check成功は、破棄対象runtimeのbehavior/failure inventoryとして保持する。

これらのtest countは新しいM3-3Aのacceptance evidenceとして再利用しない。

M3-3Aは`execplan/2026-07-15-m3-3a-publication-redesign.md`のR1からR7を順に完了した場合だけ受け入れる。

R1ではbundleとfinal observationのgolden fixture、Go conformance test、独立Python verifierを成功させる。

R2ではbundle sealerとpure reconciler、R3ではbounded observerとpart／replay graph verifier、R4ではnarrow executorと非権威のappend-only diagnostic event store、R5ではthin publisherとfinal observation receiptとlegacy removalのfocused testを成功させる。

R6では初回公開、same-content retry、各barrier前後のcrash、timeout、Unavailable、Different、Ambiguous、Oversized、claim conflict、remote mutation、branch、missing predecessorを自動テストする。

R7では`mise run check`、`mise exec -- go vet ./...`、対象packageのRace Detector、GoとPythonのProtocol V1 conformance、`git diff --check`を成功させる。

旧stage rank、stage table、`AdvanceReplayStage`、`journal_intent_hash` receipt authority、stage-driven restart testがruntime authorityとして残る場合は不合格とする。

M3-4のread-only selector、empty-cache verification、CLI、M3-5の追加raceとreal R2 smokeはM3-3A-R7まで開始しない。

M3-4は、空cacheからread-only credentialだけでreplay snapshotを解決、fetch、local verifyでき、cache mutation、manifest branch、missing predecessor、raw binding mismatchを成功扱いにしないときに完了する。
CLIはdescriptor、fetch result、verification reportをmachine-readable JSONで出し、day verifyがcampaign genesis検証を過大主張しない。

M3-5は、focused test、fixture、Python、`mise run check`、`mise exec -- go vet ./...`、`git diff --check`、追加packageを含むWindows Race Detectorが成功したときに完了する。
real R2 smokeは前記の非破壊条件がない場合skipでよく、skip理由を記録する。

## Idempotence and Recovery

raw inputは変更せず、同じraw manifest keyとhash、同じconversion tuple、同じwriter設定でreducerとwriterを再実行できるようにする。
temporary Parquet、manifest、receiptは同じdirectoryへ作り、write、sync、close、hash、no-clobber promoteの順でfinal pathへ出す。
同じkeyに同じbytesがあればretryとして成功し、異なるbytesならintegrity failureで停止する。

reducerが途中で停止した場合、未完成stream stateをaccepted snapshotとして公開しない。
raw manifestを再検証して新しいtemporary outputを作り、完成済みimmutable objectはhash一致を確認して再利用する。
row-chain rootやpart boundaryが変わった場合は同じconversion tupleのretryとみなさず、原因を記録して停止する。

sealed bundleは同じlocal verified input、trusted Layout、conversion tuple、resource limitから同じcanonical bytesとdigestを再生成する。

再起動時はeventまたは旧journal stageを入力にせず、bundleを再検証してfresh bounded remote observationを作る。

M3はpublisher claimを作成せず、既存claimがExactでなければremote actionを返さない。

raw key/hash mismatchはraw publicationを修復せずderivative publicationを停止する。

fault matrixは次の扱いにする。

- raw verificationでmissing object、size mismatch、SHA-256 mismatch、WAL chain gap、raw manifest keyとhashの不一致が出た場合は、Parquet writeとuploadを開始せず、integrity failureを返す。
- Parquet write中にcrash、short write、Close failure、local disk errorが出た場合はtemporary pathを成功扱いせず、再実行時に完成final pathだけをhash検証する。
- Parquet reopen verificationでschema、row count、row-chain、file hashが違った場合はpart manifestを作らず、converterまたはwriter設定を変更して勝手にretryしない。
- Parquet uploadがtimeoutまたはprocess crashで終わった場合は、次回のfresh observationでsame keyのremote bytesをcheckする。
  同じbytesなら次のbarrierを計画し、違うbytesならcollisionとして停止する。
- part manifest uploadまたはcheckに失敗した場合はParquet objectを削除せず、part manifestを再送してからreplay manifestへ進む。
  part sequenceのbranch、duplicate、missing predecessorがあればwinnerを選ばない。
- replay manifest uploadまたはcheckに失敗した場合はmanifestを最後の公開物として再試行し、partとParquetを再生成しない。
  remoteに異なるbytesがあればintegrity failureとする。
- receipt保存前にcrashした場合はpublicationをacceptedと報告せず、新しいcomplete final observationを作る。
  同じfinal observation bytesなら同じreceiptをno-clobberで保存し、既存receiptのbytesが違えばbundle collisionとする。
- retryで同じintent、same key、same bytesを再利用する場合は成功とし、new revisionやnew conversionを作らない。
- remote mutation、object key collision、manifest revision branch、missing predecessor、scope mismatchは全て停止条件である。
  remote stateを勝手に修復、削除、上書きしてはならない。

M3ではlocal pruningを実装しないため、cacheやoutboxの削除を復旧手段にしない。
proof-gated pruningはM4以降の別計画で扱う。

## Artifacts and Notes

tracked artifactはProtocol V1文書、GoとPythonの小さなsynthetic fixture、canonical JSON、row-chain、part manifest、replay manifest、receiptの検証結果だけにする。
実Tick、WAL、SQLite journal、Parquet runtime data、R2 object、credential、rclone config、実行用local TOMLはcommitしない。
既存のruntime ignore規則に従い、M3 fixtureは`testdata/tickdata/golden/`などの小容量fixtureとして管理する。

M3-1のfixtureは、入力raw manifest key、raw manifest digest、ordered rows、marker、part boundary、期待digestを記録する。
M3-2のParquet goldenは、同一platform contractのbyte fixtureと、別platformでも比較できるsemantic row fixtureを分ける。
fixtureのbyte hashをcross-platformのlogical equalityの証明として使わない。

M3-3のreceiptはsecretを含めず、credential valueではなくcredential scope、rclone binary hash、manifestとobjectのdigestだけを保存する。
real R2 smokeのbucket名やprefixは必要ならprivate execution logへ置き、tracked planへ実credentialを記録しない。

## Interfaces and Dependencies

M3のpackage境界はsource producerからParquet、R2、deliveryへ逆流しない。
producerは`internal/continuity`、`internal/parquet`、WAL、R2をimportせず、GatewayがM2 raw archiveを読み取ってderivativeを作る。

`internal/archive.OpenVerifiedReplaySource`は、canonical raw-day manifest bytes、exact campaign-relative `ManifestRelativeKey`、scope/config、chain_objects、sealed WAL objectのsize/hash、WAL sequence、chain roots、raw_set_root、manifest.objectsのselected rangeをM2 verifierで確認する。
multi-entry rangeは、first entryの`first_record_ordinal`からentry末尾、middle entryの全record、last entryの0から`last_record_ordinal`へ展開し、same-entry rangeは指定区間だけを選ぶ。
展開結果は入力manifestを変更せず、verified WALから独立再導出したM2 canonical selectionと全coordinate、summary、watermark、chain slice、chain_objects、expanded raw_set_rootを比較する。
このreplay側のexact coordinate proofと、M2のfull-day semantic snapshot proofを分けるが、どちらも`wal.VerifySealedSegment`のverified bytesから導出する。
`ReplaySourceInput.ResourceLimits`は`replay_contract_id`ごとに明示され、`MaxChainObjects`、`MaxObjectBytes`、`MaxChainBytes`のnon-zero finite値をdescriptorとpathのopen前に検証する。
full-chain loadはこの上限内で順次objectを再検証し、over-limit、overflow、mutated bytes、descriptor mismatchはRowSinkへ到達する前に停止する。
`ReplaySourceInput.ProducerInstanceID`は明示的なverified identityとして必須であり、selected BatchFrameV1ごとに`internal/protocol.DeriveSessionLeaseID`をcampaign、provider、stable feed、broker fingerprint、exact symbol、producer sessionと再計算する。
lease mismatch、scope mismatch、range overlap、out-of-range、raw key/hash mismatchはrowを出す前に`ErrIntegrity`で停止する。

`internal/continuity.Reduce(reader, sink)`はこのverified readerだけを受け取り、ordered `protocol.ReplayRow`をsinkへ逐次送出し、row count、marker count、last segment、bounded fingerprint tail、row-chain rootだけを`Result`へ返す。
overlapはbounded tail内の最長exact suffix/prefixが一意な場合だけ既読prefixを除去する。
tail内の同一fingerprintのmultiplicityが最長候補を曖昧にする場合は`AMBIGUOUS_OVERLAP`を出し、incomingを保持する。
前後fingerprintによる一意なone-substitution alignmentがある場合だけ`SOURCE_HISTORY_CHANGED`を出す。
alignmentはbounded tailのsuffixとincoming prefixの境界に限定し、最長候補長についてtail内のwildcard-compatible位置が一つであることを要求する。
内部subsequenceだけの一致、反復patternによる複数位置、境界の不成立は`AMBIGUOUS_OVERLAP`としてincomingを保持する。
rowはcanonicalize、validate、next chain hash計算をsink呼び出し前に済ませ、sink成功後にだけsummary stateを更新する。

empty dayのreaderはempty streamを表し、synthetic Tickもsynthetic markerも出さず、row-chain rootを全zeroにする。

chain_objectsのうちselected range外のentryは連続性証明だけに使い、replay rowへしない。
WAL chain gap、raw mutation、missing predecessor、scope mismatchはmarkerへ変換せずfail closedする。

`internal/parquet`の実装境界は、`NewGenerator(spec ConversionSpec, scope protocol.ReplayScope, outputRoot string)`、`WriteRow(protocol.ReplayRow)`、`Close() (GenerationResult, error)`である。
`WriteRow`はcanonical row bytesとnext row-chain hashを計算してからbounded current partへ入れ、`Close`はpartごとにtemporary write、sync、hash、no-clobber promotion、reopen verificationを完了する。
`ConversionSpec`はformat、replay contract、conversion、converter build、dependency lock hash、writer configuration hash、target platform、MaxRowsPerPart、MaxCanonicalBytesPerPart、MaxRowsPerRowGroupを持つ。
`PartArtifact`はtemporary pathではなく、sync済みParquet path、SHA-256、bytes、first and last stream sequence、row count、canonical row bytes、previous、first and last row-chain anchorsを返す。
generatorはpart artifactのmetadataだけを複数part分保持し、rowまたはcanonical bytesをday全体について保持しない。

`internal/archive.BuildPartManifest(PartManifestInput, previous)`は`PartArtifact`相当のbounded summaryを`protocol.PartManifest`へ変換する。
`internal/archive.VerifyPartManifestObject`はcanonical bytes、key、digest、bounded range、predecessorをstrictに検証する。
`internal/archive.BuildReplayDayManifest(ReplayDayManifestInput)`と`VerifyReplayDayManifestObject`はraw scope、ConversionTuple、ordered part set、part_set_root、row-chain root、empty day、revision predecessorを検証する。

`internal/protocol`には次の凍結済みデータを定義した。

    type PartManifest struct {
        ManifestVersion string
        PartSequence uint32
        PartKey string
        PartSHA256 [32]byte
        PartBytes uint64
        RowCount uint64
        CanonicalRowBytes uint64
        FirstStreamSequence uint64
        LastStreamSequence uint64
        FirstRowChainHash [32]byte
        LastRowChainHash [32]byte
        PreviousManifestSHA256 *[32]byte
    }

    type ReplayDayManifest struct {
        ManifestVersion string
        ManifestID string
        DatasetID string
        CampaignID string
        DayDefinitionID string
        Date string
        Revision uint64
        RawDayManifestKey string
        RawDayManifestSHA256 [32]byte
        ReplayContractID string
        FormatID string
        ConversionID string
        ConverterBuildID string
        DependencyLockHash [32]byte
        WriterConfigurationHash [32]byte
        TargetPlatformContract string
        CompletenessStatus string
        PartManifestKeys []string
        PartSetRoot [32]byte
        CanonicalStreamRowChainRoot [32]byte
        PreviousManifestSHA256 *[32]byte
    }

`ReplayDayManifest`のcanonical JSON key集合、part manifest key、root、revision、M0互換受理条件は`protocol/v1/manifests.md`と`replay-v1-conformance.json`を正本にする。
Go private typeのfield順序をcanonical formatの根拠にしない。

derivative publicationは`internal/r2`のraw publisherとは別の型として扱う。

    type ReplayPublicationBundleSealer interface {
        Seal(ctx context.Context, input ReplayPublicationInput) (ReplayPublicationBundle, error)
    }

    type ReplayRemoteObserver interface {
        Observe(ctx context.Context, bundle ReplayPublicationBundle, budget ObservationBudget) (ReplayRemoteObservation, error)
    }

    type ReplayPublicationReconciler interface {
        Reconcile(bundle ReplayPublicationBundle, observation ReplayRemoteObservation) (ReplayPublicationDecision, error)
    }

    type ReplayActionExecutor interface {
        Execute(ctx context.Context, action ReplayPublicationAction) error
    }

    type ReplayPublicationEventStore interface {
        Append(ctx context.Context, event ReplayPublicationEvent) error
    }

    type ReplayReceiptStore interface {
        SaveNoClobber(ctx context.Context, receipt ReplayVerificationReceipt) error
    }

`ReplayPublicationBundle`はlocal verified bytes、trusted key、conversion tuple、pinned rclone identity、aggregate resource limitをcanonicalにbindする。

`ReplayRemoteObservation`はclaim、raw、complete derivative inventory、validated replay graphのpoint-in-time状態を表す。

`ReplayVerificationReceipt`はbundle digestとcomplete final observation digestをbindし、journal stage、event、retry文字列、ETagをauthorityとして含めない。

read-only deliveryは次の概念契約を追加する。

    type ReplayArchiveReader interface {
        ListReplaySnapshots(ctx context.Context, scope ReplayDayScope) ([]ReplaySnapshotDescriptor, error)
        ResolveReplaySnapshot(ctx context.Context, selector ReplaySnapshotSelector) (ResolvedReplaySnapshot, error)
        BuildReplayFetchPlan(ctx context.Context, snapshot ResolvedReplaySnapshot) (ReplayFetchPlan, error)
        FetchReplay(ctx context.Context, plan ReplayFetchPlan, destination string) (ReplayFetchResult, error)
        VerifyReplayDay(ctx context.Context, selector ReplaySnapshotSelector) (ReplayVerificationReport, error)
    }

selectorはdataset、campaign、date、replay contract、conversion、revision、manifest keyまたはmanifest hashを持ち、prefix listingの結果から曖昧なwinnerを選ばない。
fetch planはmanifest、part manifest、Parquet objectのkey、hash、bytes、cache pathを持つ。
verification reportはraw binding、part_set_root、row-chain root、object verification、cache verificationの各結果を分けて返す。

依存は`modernc.org/sqlite`、AWS SDK for Go v2のread-only S3/R2 backend、既存のpinned rclone、`github.com/parquet-go/parquet-go v0.30.1`、Python 3.12のfixture verifierに限定する。
Parquet依存のmodule checksumは`go.sum`の`h1:Oy6ganNrAdFiVwy7wNmWagfPTWA2X9Z3tVHBc7JtuX8=`と一致し、M3-2のdependency lock hashへ反映する。
Worker、HTTP API、live broker、Rust DLL、strategy consumerはM3のdependencyに追加しない。

## 改訂記録

改訂記録（2026-07-15）: M2のPR #3が`cc9fc2d`でmerge済みであることをbaselineとして記録し、M3-0のself-contained ExecPlan、M3-1 implementation gate、replay determinism、derivative publication、read-only delivery、fault matrix、validation commandを追加した。
改訂記録（2026-07-15 M3-2開始）: `parquet-go v0.30.1`のWindows互換性spike、module checksum、GoVersion、固定writer設定、nullable Parquet schema、bounded part boundary、streaming generator、strict part/replay manifest builder/verifierを追加した。
改訂記録（2026-07-15 M3-2受入）: 指定focused Go、fixture、Python、repository check、vet、gofmt、diff checkを成功させ、M3-2実装を受け入れた。
publication journal、receipt、read-only delivery、R2 uploadは未実装のままM3-3以降へ残した。

改訂記録（2026-07-15 M3-1）: `part-manifest-v1`、M3 replay manifest、raw key+SHA binding、revision axis、canonical row/marker、segment ID、row-chain、part digest、part_set_root、M0 empty-parts compatibilityをGo/Python fixtureで固定した。

改訂記録（2026-07-15 M3-1C）: `archive.OpenVerifiedReplaySource`、`continuity.VerifiedBatchReader`、sink型`continuity.Reduce`へtrust boundaryを置き換え、M2 builderのmanifest、sealed WAL、chain proof、selected rangeからだけreplay rowを生成するintegration testを追加した。

改訂記録（2026-07-15 M3-1C）: raw-day domain digest、full layout key検証、bounded MaxRecords tail、repeated-pattern/session-restartのambiguous保持、plain SHA-256拒否を追加したが、後続M3-1D監査でgate証拠を無効化した。

改訂記録（2026-07-15 M3-1D）: multi-entry inclusive range expansion、exact selected-coordinate proofとM2 full-day proofの分離、authoritative lease helper、producer identity再計算、longest unique overlap、unique history-change proof、pre-sink row validationを追加した。

改訂記録（2026-07-15 M3-1D）: focused Go `87 passed / 0 failed`、fixture `20 verified`、Python `18 passed / 0 failed`、repository check、vet、diff checkの成功を記録し、M3-1 gateを再受入した。

改訂記録（2026-07-15 M3-1E）: `uniqueHistoryChange`を保持tail suffixとincoming prefixの境界だけへ限定し、最長候補とtail内のwildcard-compatible位置の一意性を検証するnegative testを追加した。

改訂記録（2026-07-15 M3-1E）: focused Go `74 passed / 0 failed`、repository check、vet、diff checkの成功を記録し、M3-1 gateを完了状態へ戻した。

改訂記録（2026-07-15 M3-2レビュー修正）: M3 V1未リリースの同一version契約としてPartManifestへscope/raw binding、ConversionTuple、previous row-chain hashを追加し、canonical field order、domain semantics、outer binding、part-set closure、final replay root closureをGo/Pythonとgolden indexへ反映した。

改訂記録（2026-07-15 M3-2レビュー修正検証）: generated/reopened multi-part Parquetからのscope-bound manifest integration、arbitrary nonzero replay rootのbuild/verify負例、focused Go `94 passed / 0 failed`、fixture `21 verified`、Python `31 passed / 0 failed`、repository check、vet、gofmt、diff checkの成功を記録した。
advisor再監査は未完了であり、M3-3 publication、receipt、selector、delivery、R2 uploadは開始していない。

改訂記録（2026-07-15 Advisor changes_required remediation）: `part_bytes >= 1`、part identity/anchor hashのnonzero規則を`manifests.md`と`hash-domains.md`へ反映し、Go/Pythonのzero bytes、zero identity/anchor、successor zero predecessor regressionを追加した。
現行gateの成功を確認し、M3-2を完了へ更新したが、M3-3は未開始のままとした。

改訂記録（2026-07-15 M3-2G key-contract correction着手）: 現行Goalのdate-local chainを優先し、parent ExecPlanのhour例をdate-localへ修正した。
Protocol V1の`ExactIdentityPathKey`、campaign-relative derivative base、Parquet／part manifest／replay-day manifestのexact key、part_set_rootのU32 path encoding、trusted `r2.Layout` root prepend、Go/Python key fixtureを更新対象とした。
旧94件のevidenceはG修正前の履歴として扱い、全current gate passまではM3-2を未完了、M3-3を未開始とする。

改訂記録（2026-07-15 M3-2G verification）: focused Go `4+20+42+33=99 passed / 0 failed / 0 skipped`、fixture `22 verified`、Python `32 passed / 0 failed`、`mise run check`、`go vet`、空の`gofmt -l cmd internal`、`git diff --check`を確認した。
M3-2 local gateを完了へ更新したが、advisor exception re-auditを未完了として残し、M3-3 publication、receipt、selector、delivery、R2 uploadは開始していない。

改訂記録（2026-07-15 Advisor changes_required: parent R2 layout namespace correction）: 親ExecPlanのR2 layout図で欠落していたtrusted campaign prefixの先頭`dataset=<sha256(exact dataset-id)>/`を`provider=`より前へ追加した。
Local outboxの既存M2 namespaceは変更せず、R2 remote keyだけがdataset、provider、feed、symbol、campaignのexact identity hash prefixを持つことを明記した。
文書監査の修正のみであり、M3-3 publication、receipt、selector、delivery、R2 uploadは開始していない。

改訂記録（2026-07-15 M3-2G advisor exception audit pass）: advisor changes_requiredで指摘されたR2 dataset prefix、M3 V1 key binding、outer replay binding、chain closure、nonzero identity/anchor、multi-part integrationの現行証拠を再確認し、M3-2Gを受入済みへ更新した。
M3-3Aではimmutable replay publicationだけを実装し、read-only replay delivery、CLI、selector、cache、real R2 smokeは開始しない。

改訂記録（2026-07-15 M3-3A immutable replay publication）: M3-2G advisor passを受入前提として記録し、derivative専用SQLite intent/table/stage、trusted `r2.Layout` key導出、raw-before-Parquet verification、生成済みParquetからのpart manifest、ordered immutable transfer、replay manifest、journal intent hash付きreceipt、fault matrixを実装した。
focused Go `47+20+4+17=88 passed / 0 failed`、fixture `22 verified`、Python `32 passed / 0 failed`、`mise run check`、`mise exec -- go vet ./...`、空の`gofmt -l cmd internal`、`git diff --check`を確認した。
この時点ではM3-3A publicationを完了扱いにしたが、その受入判断は後続のM3-3A-R0設計リセットで無効化した。

改訂記録（2026-07-15 M3-1F）: 例外監査で検出した入力manifest上書き、compact partial acceptance、full remote root ambiguity、未bounded archive verificationを記録し、M3-1 gateを未完了へ戻した。
M3-1Fでは、M2 canonical selectionとの独立完全一致、`ManifestRelativeKey`のexact equality、`ReplayResourceLimits`のdescriptor/path事前検証、7-of-9/cross-day/zero-sentinel/source-errorのfocused testを追加した。

改訂記録（2026-07-15 M3-1）: Parquet、publication journal、receipt、read-only delivery、selector、cache、Parquet依存のpinは未実装のままM3-3以降へ残した。

改訂記録（2026-07-15 M3-3A review correction）: advisor reviewで検出したstage依存のclaim再開、receipt直前の再検証不足、既存Parquetとpart graphの未検証、孤児objectの過剰許可、List descriptor重複を修正した。
`ObjectBackend.Open`によるstreaming hash/size、raw binding別part chain、candidateとcompleted replayのreferenced set、claim再assert、receipt直前の全remote再検証を追加し、`internal/r2 52 passed / 0 failed`、`mise run check`、fixture `22 verified`、Python `32 passed`、vet、gofmt、diff checkを確認した。
この時点ではM3-3A publication correctionを受入状態へ戻したが、その受入判断は後続のM3-3A-R0設計リセットで無効化した。

改訂記録（2026-07-15 M3-3A review correction final gate）: claim reassertで既存claimの削除を`ErrPublisherConflict`へ分類する処理を追加し、未キャッシュ`internal/r2 52 passed / 0 failed`、`mise run check`、fixture `22 verified`、Python `32 passed`、`go vet ./...`、空の`gofmt -l cmd internal`、`git diff --check`を最終確認した。

改訂記録（2026-07-15 M3-3A advisor BLOCK remediation）: prepare-before-lock TOCTOUを除去し、campaign/epoch lockをremote prepareからreceipt stageまで保持するよう修正した。
`ReplayPublicationLimits`をcanonical journal intentへ追加し、S3/fake bounded backendの`GetLimited`、`ListLimited`、`Open`、Parquet expected artifactのstreaming hash/size、claim mutation/deletion boundary、同一scope競合テストを追加した。
focused r2 `86 passed / 0 failed`、全Go、fixture `22 verified`、Python `32 passed`、check、vet、gofmt、diff checkは成功したが、local gcc不足のためRace Detectorは未完了である。
M3-3Aは未完了のままadvisor re-auditへhandoffし、read-only delivery、CLI、selector、cache、real R2 smokeは開始しない。

改訂記録（2026-07-15 M3-3A-R0設計リセット）: 旧stage-based publicationと過去のtest countをaccepted evidenceからquarantineし、sealed bundle、bounded fresh observation、pure reconciler、approved action executor、append-only event、complete final observation receiptへ設計を置き換えた。

新しい実装正本を`execplan/2026-07-15-m3-3a-publication-redesign.md`とし、R1からR7の完了までM3-3Aを未完了、M3-4をblockedとした。

改訂記録（2026-07-15 G3-R3）: replay専用bounded read interface、aggregate observation budget、fresh observer、part／revision graph verifier、Exact-only final observation変換、negative test、focused／full `internal/r2` testの成功を同期した。

改訂記録（2026-07-15 G4-R4）: ObjectID-only narrow executor、verified local snapshot、rclone二操作allow-list、canonical diagnostic event、append-only in-memory store、negative test、focused／full `internal/r2` testの成功を同期した。

改訂記録（2026-07-15 G3E-R7-BOUNDED-ACTUAL-BYTES-CLASSIFICATION）: R7初回監査のchanges_requiredに対し、actual-byte aggregate budget、terminal typed result、bounded Parquet stream、publisher-level nonExact testを追加した。

Focused／full／repository／Race gate成功とR7再監査待ちを記録し、旧M3-3A test count非流用とM3-4 blockを維持した。

改訂記録（2026-07-15 G3F-FINAL-UPLIFT-LIST-DESCRIPTOR）: R7二回目監査は、final observationの必要bytesをbuilder内で自己申告して共有budgetへ課金していない点と、Parquet list descriptorのsizeを検証していない点をchanges_requiredとした。

最終passの実課金bytesがcanonical final observationの必要bytesに満たない場合、差分だけを`ReplayObservationBudget.ChargeObservation`へ一度課金し、その後のpass snapshotからfinal observationを作るよう変更した。

過去roundのaggregate counterは保持し、exact-fitは成功し、実課金bytesが必要bytes以上の場合は二重課金せず、uplift上限超過は`ReplayGraph=Oversized`、`Complete=false`、final／digest／action／receiptなしで停止する。

Parquet objectは`RemoteObject.Size`、bundle expected bytes、`OpenLimited` advertised size、実stream bytesの四値一致を要求し、誤ったlist sizeを`Ambiguous`、advertised／actual mismatchをfail-closedの`Unavailable`として保持する。

Focused test、未キャッシュ`internal/r2`全test、fixture 23件、Python 34件、`mise run check`、`go vet ./...`、gofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功した。

この証拠はG3Fで実行した現行testだけから構成し、旧M3-3A test countを流用していない。

G3Fは完了したがR7再監査は未実施であり、M3-4は明示的なR7 passまでblockedのままとする。

改訂記録（2026-07-15 G7A-R7-DOC-UNBLOCK）: Advisorの第三回R7監査receiptはphase `r7_m3_3a_third_audit`、verdict `pass`である。

Scope reviewedはR1のbundle／final observation canonical schema、hash domain、claim relation、limits、golden、Go／Python conformance、R2のsealer、trusted Layout、pure reconciler、pre-lock budget、R3のbounded observer、part／replay graph、campaign／epoch lock、Exact-only classification、R4のnarrow executor、local revalidation、immutable allow-list、event nonauthority、R5のthin publisher、single action、fresh re-observation、receipt、M2-only claim、legacy authority removal、R6のcurrent repository gate、およびG3F remediationとM3-4未着手境界である。

Evidenceは`mise run check`のfixture 23件、Python 34件、全Go、未キャッシュ`internal/r2`、Protocol／Archive、vet、空のgofmt、両diff check、CGOとGCC 16.1.0によるcurrent-diff local Race、branch／HEAD一致、tracked modified 26、untracked 43、staged 0である。

Assumptionsはlocal gateでGitHub CIとoptionalなReal R2を必須にしないこと、G3E／G3Fの現行証拠を使い、旧52件または86件のtest countを流用しないこと、およびG7Aが四正本文書だけを変更するdocs-only taskであることとする。

Known unresolvedはGitHub CI未確認、Real R2 optional skip、M3全体final audit未実施である。

このR7 passによりM3-3A R1からR7はcompletedとなり、M3-4を明示的にunblockするが、M3全体をcompletedまたはfinal audit passとはしない。

次taskはG8のM3-4 read-only deliveryであり、G7Aではcode、test、workflow、fixtureを変更せず、G8実装も開始しない。

改訂記録（2026-07-15 G8-M3-4-READ-ONLY-REPLAY-UX）: `ArchiveReaderV1`へversioned replay list／resolve／plan／fetch／verify boundaryを追加し、既存raw methodを維持した。

Selectorはexact key／domain hash／revisionを検証し、暗黙選択をsingle complete revision graphのterminalへ限定してbranch、duplicate、missing predecessor、ambiguous conversion、raw binding mismatchをfail closedにした。

Strict canonical replay manifest、full／relative key、domain digest、scope、conversion、revision predecessor、raw binding、part_set_root、row-chain rootをsort/list順へ依存せず検証する。

Fetch planはtrusted Layoutからだけmanifest、ordered part manifest、Parquet keyを導出し、caller supplied remote keyまたはcache pathを再検証で拒否する。

Empty cacheはimmutable manifestを起点にbounded downloadし、remote close後にsize／hashを検証したsame-directory temporaryだけをno-clobber promoteする。

Correct cacheは再検証してreuseし、corrupt cache、short／oversize／hash mismatch、temporary open／close failureはpartial promoteなしで失敗する。

`VerifyReplayDay`はraw binding、raw semantic verification、part chain、part_set_root、Parquet schema／row count／file hash、canonical row-chain rootをmachine-readable fieldへ分離し、empty dayをzero root／partなしで受理する。

Verification scopeは`replay_anchored_day`であり、campaign genesis全体を検証したとは表現しない。

Focused `internal/delivery`／`tickctl`／`tick-verify`、関連archive／Protocol／Parquet、fixture 23件、Python 34件、`mise run check`、vet、空のgofmt、両diff check、delivery／CLI local Raceは成功した。

Production delivery codeは`r2.ReadBackend`だけを保持し、`ObjectBackend`、Put、publication journal、SQLite、stage、event storeを使用しないことをliteral searchで確認した。

旧52件または86件のtest countはG8 evidenceへ流用せず、M3-3A R7 pass receiptは維持する。

M3-4はcompletedだがM3全体final auditは未実施であり、M3-5 network-free E2E、Real R2、HTTP API、GitHub CIは後続作業である。

改訂記録（2026-07-15 G9-M3-5-NETWORK-FREE-E2E-FULL-GATES）: `TestM3ReplayDeliveryNetworkFreeEndToEnd`はfake Batchからsealed WAL、canonical raw publication、verified replay source、ordered overlap reducer、Parquet、part／replay manifests、M3 sealer／publisher／receipt、empty-cache fetch／verifyまでを一つのlocal graphで実行する。

M2側はtrusted `r2.Layout`からscope descriptor、publisher claim、raw object、raw-day manifestのexact keyだけをfake backendへ公開し、same-content retryでremote write数が増えないことを検証する。

M3側はsealed bundleとfinal observation receiptのidentity、raw binding、row-chain root、part-set root、claim relation、approved action列を検証する。

Diagnostic event appendを意図的に失敗させてもpublication authorityへ影響せず、same-content M3 retryはremote mutationとactionを重複させない。

Read-only deliveryは空cacheからmanifest、part manifest、Parquetを取得し、hash-derived cache、Parquet再読込、day report、二回目のcache reuse、reader remote write 0を検証する。

Test dependencyはfake backend、fake action tool、temporary binary／fileに限定し、network、AWS credential、real rclone、SQLite、wall clockを使用しない。

Cross-language evidenceは既存`testdata/tickdata/golden/replay-v1-conformance.json`を`tools/tick_fixture_verify.py`と`tests/unit/test_protocol_v1.py`が独立にrow-chain rootとpart-set rootまで再計算する現行fixtureを使用し、新しいfixture countへ置き換えていない。

Focused E2Eと関連10 package、fixture 23件、Python 34件、`mise run check`、`mise exec -- go vet ./...`、空のgofmt、指定8 packageのlocal Windows Race Detectorは成功した。

G9文書同期前のinventoryはbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 32、untracked 48、staged 0である。

旧52件または86件のM3-3A test countはG9の受入証拠へ流用せず、R7第三回監査receiptを維持する。

Real R2はcredentialと明示確認がないためoptional skipであり、GitHub CI、remote I/O、HTTP APIは未確認または未実施である。

M3-5はcompletedだがM3全体final auditは別gateであり、本taskではM3全体をcompletedまたはfinal audit passと宣言しない。

改訂記録（2026-07-16 G9E-PRODUCTION-M2-PUBLISHER-E2E）: Whole-M3 final auditは、G9が`publishM2CanonicalForE2E`でM2 canonical bytesをbackendへ直接投入し、production `r2.NewPublisher`と`Publisher.Publish`を受入経路で実行していない点をchanges_requiredとした。

このfindingにより、G9のdirect helper経路をM2 acceptance evidenceから除外し、production M2 publisherをtemporary `PublicationJournal`、network-free fake rclone、既存fake backendへ接続した。

Temporary SQLite journalはproduction M2 APIの必須境界としてだけ使用し、M2 first publishとsame-content retryの後にcloseする。

M3 sealer、thin publisher、final receipt、read-only selector、fetch、verificationはclose済みM2 journalを参照せず、SQLite、stage、eventをaction authorityへしない。

Fake M2 rclone executorは`version`、`copyto --immutable`、`check --download`だけを受理し、arbitrary operation、異なるflag、trusted campaign prefix外のkeyを拒否する。

First publishはproduction M2 receiptのverification complete、claim hash、scope config hash、raw manifest key／domain digest、raw object hash／bytes、rclone identityを検証し、remote scope descriptor、claim、raw object、raw-day manifestのkeyとbytesをproduction Layoutへ照合した。

同じproduction inputのretryはcanonical receipt identityを維持し、successful conditional claim write、remote mutation、immutable copy mutationを増やさなかった。

M2 receiptのraw manifest key／domain digestとpublisher claimをM3 bundle、M3 verification receipt、resolved replay manifestへ結び、empty-cache `VerifyReplayDay`のraw binding、part root、row-chain rootまで一つのgraphとして検証した。

Focused `TestM3ReplayDeliveryNetworkFreeEndToEnd`と`TestM2RawOffhostDeliveryEndToEndFake`、関連10 packageは成功した。

`mise run fixture`は23件、`mise run test-python`は34件、`mise run check`、`mise exec -- go vet ./...`、空の`mise exec -- gofmt -l cmd internal`、`git diff --check`、`git diff --cached --check`も成功した。

WinLibs POSIX／UCRT GCC 16.1.0、Go 1.24.13 windows/amd64、CGO_ENABLED=1、CC=gccによる指定8 packageのlocal Windows Race Detectorは成功した。

G9E文書同期前のinventoryはbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 32、untracked 48、staged 0である。

G9Eはremote production I/O、Real R2、GitHub CI、HTTP、commit、push、mergeを実行せず、旧52件または86件のM3-3A test countを現行受入証拠へ流用していない。

Whole-M3 final re-auditはpendingであり、advisorの明示的なpassまでM3全体をcompletedまたは`final_audit: pass`としない。

改訂記録（2026-07-16 FINAL-DOCS-M3-COMPLETE）: Advisorのphase `final_m3_whole_reaudit`はverdict `pass`、required actionsなしで完了した。

Scope reviewedはG0のbranch／HEAD／dirty inventoryと保全、Protocol V1 contract、M3-1 replay／continuity、M3-2G Parquet／manifest／key contract、M3-3A R1からR7、M3-4 read-only selector／fetch／verify／CLI、G9E production M2 publisher E2E、および四正本文書である。

M3-3AのR7第三回監査はpublication再設計の完了gateとして維持し、whole-M3 final auditはM3-4とG9Eを含む別gateとして記録する。

Evidenceはproduction `r2.NewPublisher`と`Publisher.Publish`、production M2 receipt／conditional claim／remote exact bytes、M3 bundle／final receipt／resolved replay manifest／empty-cache verificationの単一identity graph、および旧direct injection helper 0である。

現行local gateとしてfocused M3 E2E、M2 E2E、関連10 package、fixture 23件、Python 34件、`mise run check`、`mise exec -- go vet ./...`、空のgofmt、両diff check、指定8 packageのlocal Windows Race Detectorが成功した。

AssumptionsはReal R2をcredentialと明示確認がないためoptional skipとすること、GitHub CIをM3 local acceptanceの必須条件にしないこと、temporary M2 `PublicationJournal`をM3 authorityへ流用しないことである。

Known unresolvedはReal R2 optional skipとGitHub CI未確認であり、HTTP、live broker、commit、push、mergeはM3 scope外である。

Local RaceはGitHub CIの結果ではなく、旧52件または86件のM3-3A test countはwhole-M3 final evidenceへ流用していない。

M3全体はcompletedであり、`delivery_status: completed`、`final_audit: pass`の完了条件を満たす。

改訂記録（2026-07-16 M3-MERGE-M4-HANDOFF）: Pull Request #4のmerge commit `cb72752a651c88c3027b409f6f205ac9236f28b8`をM3のmain統合証拠として記録した。

M3対象外だったproduction operationとHTTP delivery adapterは、`execplan/2026-07-16-m4-production-operations-http-delivery.md`へ委譲した。
