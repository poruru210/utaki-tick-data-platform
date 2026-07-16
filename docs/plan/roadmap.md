# 実装ロードマップ

この文書は、`.agent/tick-data-platform-execplan-revised.md`に定義した実装計画を、段階ごとの到達状態として整理したものです。

ExecPlanの内容を変更する場合は、ExecPlan自体を更新し、この文書との対応を確認します。

## 開発環境

開発ツールはmiseで固定します。

```powershell
mise trust
mise install
mise run bootstrap
mise run check
```

MQL5のコンパイルと実機接続の確認は、MetaTrader 5が動作するWindows環境で実施します。

## M0の契約固定

M0では、実装より先にproducerとGatewayが共有する契約を固定します。

M0ではTCP runtime、live MT5 collection、R2、Parquet、SQLite journal runtime、crash injection、production operationを実行しません。

固定対象はwire framing、message layout、hash domain、canonical JSON、WAL、raw-day manifest、replay-day manifestです。

`part-manifest-v1`の仕様と実装はM3へ延期します。

`producers/fake/`には、決定的かつネットワークを使わないGo製のtest producer/packageを置きます。

Go、MQL5、Pythonが同じ結果を返すcross-language fixtureを作成します。

unknown version、短いframe、CRC不一致、重複、ACK欠落、WAL復旧をfixtureとconformance caseで表現します。

M0の完了条件は、仕様、fixture、Go側検証、MQL5側送信の解釈が一致していることです。

### 2026-07-14時点の進捗

protocol/v1/にwire、message、source schema、WAL、hash domain、canonical JSON、raw/replay manifestの契約を固定しました。

testdata/tickdata/golden/に18 fixtureを追加し、Go decoder、独立Python decoder、fake producerでbytes、hash、正常系、wire異常系、stateful scenarioを検証しました。

MQL5のHello/Batch encoderはMetaEditorで0 errors、0 warningsを確認しました。

MQL5実機でのfixture出力、TCP実通信、live MT5 collection、R2、Parquet、SQLite journal runtimeは実施していません。

## 2026-07-15時点のM1実装

`internal/ingest/`にloopback TCP listener、bounded frame reader、Hello/Resume handshake、session lease、Batch/Ack処理、cursor directive、status metricsを実装しました。

`internal/wal/`に`protocol/v1/wal-layout.md`準拠のactive WALを実装し、BatchFrameV1のappend、file sync、entry CRC、batch hash、entry chain、partial tail recoveryを検証します。

`internal/journal/`にCGo-free SQLite journalを実装し、batch inventoryとcursor stateをWALから再構築できるようにしました。

`producers/mt5/TickCaptureService.mq5`にCopyTicks、built-in TCP、Hello/Resume、in-flight frame保持、ACK判定、再接続、再送を実装しました。

fake producerのTCP integration testで、accepted batch、duplicate、same identity/different bytes、source error、dense boundary、partial frame、WAL sync前後、ACK loss、journal deletion後のrebuildを検証しました。

MetaEditor compileは`Result: 0 errors, 0 warnings`でした。

## M1のローカル収集

M1では、MT5 producerからGo Gatewayへlocalhost TCPでデータを送ります。

GatewayはHello、Resume、Batch、Ack、Errorのmessageを処理します。

producerはACKの欠落、再接続、再送に対応します。

Gatewayは受信済み範囲と重複を判定し、WALへ先行記録します。

M1の完了条件は、fake producerとMT5 producerの両方で、切断後の再接続と重複排除を再現できることです。

M1ではR2、Parquet、HTTP delivery、local pruning、複数producer、24時間soakを実施しません。

## M2のRaw公開

M2では、受信したrawデータを日次単位で保管し、R2へ配置します。

raw-day manifestはsource schema、対象範囲、件数、hash、immutable objectを参照します。

manifestはGoやMQL5の非公開型を参照しません。

M2の完了条件は、ローカル保存とR2配置の結果をmanifestとhashで検証できることです。

### 2026-07-15時点のM2 raw off-host delivery

internal/walはactive WALへTWTR trailerを追加し、seal済みsegmentへ切り替えた後、次のgateway ingest sequenceとentry hash chainを引き継ぐ新しいactive WALを作成します。

起動時にはseal済みsegmentをsequence順に検証し、active WALと合わせてaccepted batch inventoryを復元します。

検証対象はheader、entry length、BatchFrameV1、commit marker、CRC32C、batch SHA-256、entry hash chain、trailer、trailer直前までのfile SHA-256です。

raw objectのkeyには、trailerを含むseal済みfile全体のSHA-256を使います。

internal/archiveは検証済みsegmentを再encodeも圧縮もせず、既存objectを上書きしないatomic operationでローカルoutboxへpromoteします。

同じbytesのretryは同じobjectを返し、同じkeyに異なるbytesが存在する場合はintegrity failureとして停止します。

この段階では明示的なStore.Seal APIだけを提供し、自動rotation policyは実装しません。

Protocol V1 raw-day manifestはverified sealed WAL、day-selected ranges、full chain_objects、revision graph、raw_set_rootをcanonical bytesへ固定します。

M2R-2はAWS SDK for Go v2のR2 publication boundary、campaign publisher claim、local exclusive lock、独立publication journal、immutable transfer、remote recheck、verification receiptを提供します。

M2R-3はread-only ArchiveReader、tickctlのdatasets、campaigns、snapshots raw、fetch、tick-verifyのday、campaign commandsを提供します。

M2R-4はnetwork-free fake end-to-end test、optional isolated real-R2 smoke、repository check workflow、Windows race workflow、verification recordを追加します。

通常のM2検証はfake backendだけで完結し、real R2 smokeは明示的なenable、confirmation、isolated bucketまたはprefix、endpoint、credentialを要求します。

M2の対象外はParquet、replay-dayまたはpart manifest、handover、pruning、Worker、HTTP API、live brokerです。

M2の実装と通常の検証gateは完了しました。
review修正後のRepository checkはpush run `29380482941`とPR run `29380484737`、Windows raceはpush run `29380482973`とPR run `29380484762`で成功しました。
real R2 smokeはproductionと分離したbucketまたはprefixおよび明示的credentialがないためoptionalな未実施境界です。
Pull Request #3のM2実装はmerge commit `cc9fc2d`としてmainへ反映済みです。

## M3-0の計画固定

M3-0はM2のmerge済みbaseline `cc9fc2d`から、replay derivativeとParquet配信を実装するための文書計画を固定します。

詳細な実施計画は`execplan/2026-07-15-m3-replay-parquet-delivery.md`です。

Protocol V1の正本は`protocol/v1/`、architecture、scope、milestone acceptanceの正本は`.agent/tick-data-platform-execplan-revised.md`です。

この段階ではM3実装を開始または完了したとは扱いません。

M3の実装開始前に、M3-1としてProtocol V1、GoとPythonのfixture、`part-manifest-v1`、part_set_root、canonical replay rowとrow-chain、marker row、raw-day manifestのexact keyとhashのbinding、replay revision rulesを固定します。

Parquet dependencyは、現行Go toolchainとの互換性、module API、Linux相当のgate、Windows Race workflowを確認した後に一つのtagを`go.mod`と`go.sum`へpinします。

Parquetの論理的な再現性はrow、row-chain root、part layoutで必須とし、全byte一致はconverter build、dependency lock、writer configuration、target platform contractが同じ場合だけ要求します。

## M3のG0保全inventory

G0はbranch `agent/m3-replay-parquet-delivery`のfull HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`をread-onlyで確認しました。

開始時点はtracked modified 24、untracked 26、staged 0であり、`git diff --check`と`git diff --cached --check`は成功しました。

G0ではremote I/OとGitHub CI確認を行わず、reset、rebase、stash、clean、checkout、一括破棄を行っていません。

M3-1とM3-2GのProtocol、fixture、GoとPython conformance、verified replay source、continuity、Parquet、part／replay manifest、exact key bindingは保持します。

親ExecPlan、M3 living ExecPlan、M3-3A redesign ExecPlan、本roadmapの四文書は保持更新し、各gateの進捗、発見、判断、証拠を同期します。

bounded backend、trusted `r2.Layout`、campaign／epoch lock、pinned tool allow-list、local no-clobber primitive、旧testが表すuseful fault caseは境界を狭めて再利用します。

monolithic `ReplayPublisher`、`ReplayStage*`、SQLite replay transition、`journal_intent_hash`をauthorityにするreceipt、stage-driven restart testは、新しいtestへuseful caseを移した直後に置換または削除します。

publisher claimの作成主体とconditional helperはM2 raw publisherに残し、M3は既存claimのExact observationだけを受理します。

repo `AGENTS.md`、`check.yml`、`windows-race.yml`、`mise.toml`、global `advisor.toml`、`implementer.toml`、`semble-search.toml`、delivery skillとcontractsは保持し、G0Aでは変更しません。

`semble-search.toml`のrole名は`semble_search`ですが、G0とG0Aは既知pathとread-only Git確認で完了したためchildを起動せずsemantic searchも使いませんでした。

旧M3-3Aの52件または86件を含むtest countはbehavior/failure inventoryに限り、新設計の受入証拠へ流用しません。

## M3のReplay配信

M3では、検証済みraw-day manifestを入力に、ordered overlap reducerでreplay streamを生成します。

replay streamはsource payload fingerprintの順序とmultiplicityを使い、inclusive overlapだけを既読prefixとして除去します。

曖昧なoverlap、source history change、gap、source errorはsynthetic Tickへ変換せず、continuity segmentとmarker rowへ記録します。

Parquetは`ticks-parquet-v1`の固定schemaと明示writer設定を使い、canonical row bytesのrow countと累積bytesからdeterministic part boundaryを決めます。

part manifestはday-local predecessor、Parquet object keyとhash、row範囲、row-chain anchorを記録し、ordered part setからpart_set_rootを計算します。

replay-day manifestはraw-day manifestのimmutable keyとSHA-256を両方保持し、replay contract、conversion tuple、part_set_root、canonical stream row-chain rootへbindします。

raw-dayのlate evidenceは同じconversion tupleのreplay revisionを進め、converter、dependency、writer、platformの変更は新しいconversion identityとして公開します。

publicationはclaim Exact、raw Exact、Parquet object upload/check、part manifest upload/check、全part Exact、replay manifest upload/check、complete final observation、derivative receiptの順で行います。

M3 replay publicationはsealed bundleとfresh bounded remote observationをauthorityとし、旧journal stageを再開判断またはremote事実の代用に使いません。

read-onlyの`ArchiveReader`、`tickctl snapshots replay`、`tickctl fetch`、`tick-verify replay-day`は、空cacheからimmutable selectorを解決し、local cacheのsizeとhashを確認してからconsumerへ渡します。

M3の検証はfake backend、GoとPythonのfixture、focused test、`mise run check`、`mise exec -- go vet ./...`、`git diff --check`、追加packageを含むWindows Race workflowで行います。

real R2 smokeは、隔離bucketまたはprefix、明示的credential、非上書き確認が揃った場合だけ実行し、条件がなければskipします。

M3ではproof-gated pruning、handover、multi-broker/symbol、Worker、HTTP `tick-api`、active audit、24時間soak、live broker、mergeを実施しません。

M3の完了条件は、同じraw manifestとconversion tupleから同じlogical rows、row-chain root、part layoutを再生成でき、指定した同一build条件ではParquet bytesも再現できることです。

## M3-3Aの設計リセット

M3-1とM3-2Gの受入済み契約および生成物は保持します。

旧M3-3Aのmonolithic `ReplayPublisher`、固定stage、SQLite transition、journal intent hash receipt authorityは受入対象から外します。

旧実装と過去のtest countは、破棄対象の挙動と失敗経路を示すinventoryとしてだけ保持します。

新しい実装正本は`execplan/2026-07-15-m3-3a-publication-redesign.md`です。

新設計はimmutable `ReplayPublicationBundle`、aggregate budget内のfresh remote observation、pure reconciler、approved action executor、append-only diagnostic event、complete final observationをbindするno-clobber receiptに分割します。

R2はbundle sealerとpure reconcilerを実装し、R3はbounded observerとpart／replay graph verifierを実装します。

R4はapproved object IDだけを扱うnarrow executorと非権威のappend-only diagnostic event storeを実装します。

R5はthin publisher、complete final observation receipt、useful-case移行後の旧runtime削除を統合します。

M2 raw publisherだけがpublisher claimを`If-None-Match: *`で作成します。

M3 replay publisherはclaimを作成せず、各fresh observationで既存claimのExactだけを受理します。

Unavailableはretryableなzero-action、DifferentとAmbiguousはintegrity stop、Oversizedはresource stopです。

M3-3AはR1からR7のProtocol、bundle、observer、reconciler、executor、fault matrix、final auditを順に通過するまで未完了です。

R1は`ReplayPublicationBundle`とcomplete final observationのcanonical schema、二つのdomain digest、M2 claim relation、10個のresource limit、13件のnegative classificationをGo、Python、golden fixtureで固定して完了しました。

R1の受入証拠はfixture 23件、Python 33件、focused Go、gofmt、両diff checkであり、旧52件または86件のtest countを含みません。

R2はverified local inputを再検証するbundle sealerと、bundleとremote observationの値だけから最初の未充足barrierを返すpure reconcilerを実装して完了しました。

R2のsealerはfull keyをtrusted `r2.Layout`だけから導出し、local path、receipt path、runtime transfer detailをcanonical identityへ含めません。

R2のreconcilerはUnavailableをzero-action retry、DifferentとAmbiguousをintegrity stop、Oversizedをresource stop、candidate derivativeのAbsentだけをstable object IDのupload actionへ分類します。

R3は`ObjectBackend`をembedしないreplay専用bounded read interface、共有aggregate budget、fresh observer、part graph verifier、replay revision graph verifierを実装して完了しました。

R3はcomplete derivative inventory内の候補欠落だけをAbsentにし、timeout、short read、stale list、不完全pagination、不正descriptor、graph矛盾をUnavailable、Ambiguous、Oversizedへfail closedします。

R3の受入証拠はnew-designのnegative test、Exact-only final observation test、focused `ReplayObservation|ReplayBudget|ReplayPartGraph|ReplayRevisionGraph|Bounded` test、`internal/r2`全test、gofmt、両diff checkです。

旧M3-3Aのtest countはR3の受入証拠へ流用していません。

R4はbundle ObjectIDだけを受けるnarrow executor、copy直前のverified local snapshot、`copyto --immutable`と`check --download`だけのallow-listを実装して完了しました。

R4 executorはunknown／mismatched actionとbundle digest mismatchをremote call前に拒否し、local mutation、collision、check mismatchをDifferent、timeoutとunknown outcomeをUnavailableへ分類します。

R4のappend-only diagnostic event storeはcanonical EventID、same-byte duplicateのidempotency、conflict検出、Load時再検証を持ちますが、action authority、stage rank、credential、endpoint、local path、自由形式errorを持ちません。

R4の受入証拠はfocused `ReplayExecutor|ReplayEvent|Narrow|Allowlist` test、`internal/r2`全test、gofmt、両diff checkであり、旧test countを含みません。

G1EはR5途中監査で判明したempty-manifest contract不足をforward-fixし、replay edgeへtrusted full key、strict canonical replay-day manifest、part countを追加しました。

Empty terminalとempty predecessorはcanonical manifestが空partsとzero rootsを証明する場合だけ受理し、non-empty zero、mixed root、unproven earlier zero、key／digest／revision／root／terminal shape mismatchを拒否します。

Lock前budget gateは全edgeのfull keyとcanonical JSONのUTF-8 bytes、およびJSON escape後のfinal observation canonical bytesをaggregateし、overflowまたはbudget不足をbundle digest前に拒否します。

G5Cはpartial G5由来の4 compile errorだけをforward-fixし、M2 raw publisherだけがremote claimを作成し、M3 replay publisherはExact claim観測だけを受理する境界を維持しました。

G1RCはempty terminal／predecessor、zero／mixed／unproven、terminal mismatch、trusted Layout adapter、pre-lock exact／aggregate／overflow／exhaustion、lock-not-acquired、claim ownershipを個別再検証しました。

G1RCの現行証拠は`internal/r2`全80 test、fixture 23件、Python 34件、focused Protocol／archive Go、gofmt、両diff checkです。

Aggregate budget enforcementはcampaign全体で共有し、receiptがbindするfinal observation counterは最後のfresh passだけを表します。

Lock取得後かつremote action前の全local derivative再検証を復元し、G5 legacy removalをunblockしました。

G5はseal／preflight、campaign lock、fresh observe、pure reconcile、approved ObjectID一件のexecute、fresh reobserve、canonical receipt no-clobberだけを接続するthin publisherを完成しました。

Aggregate budgetは全roundで共有し、MaxPublicationRoundsを強制し、final observationは最後のfresh pass counterをbindします。

Receiptはcomplete canonical bundle、terminal final observation、bundle／observation digest、claim、roots、limitsを再検証し、journal、stage、event、retry、ETag、time、secret、local pathを含みません。

Diagnostic eventの欠落、重複、衝突、timeoutはpublication authorityにならず、publisherはevent stateをLoadして再開判断を作りません。

`internal/r2/replay_journal.go`、`internal/r2/replay_limits.go`、replay SQLite intent／object／transition table、`ReplayStage*`、rank、`AdvanceReplayStage`をuseful-case移行後に削除しました。

G5の現行証拠はfocused R5 tests、`internal/r2`全83 test、fixture 23件、Python 34件、Protocol／archive Go、gofmt、両diff checkです。

旧M3-3Aのtest countはG1E、G1RC、G5の受入証拠へ流用していません。

R6は現行R1からR5 testを正本fault matrixへ対応付け、action後crash、observation後crash、final observation後のreceipt保存crash、stale observation中remote collision、aggregate request／byte exhaustionをfake testへ追加しました。

Focused fault test、`internal/r2`全88 test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff checkは成功しました。

Windows Race workflowはCGO、gcc、mise確認を維持し、対象を`internal/ingest`、`wal`、`archive`、`r2`、`delivery`、`catalog`、`continuity`、`parquet`へ固定しました。

Repository外のuser scopeへ導入したWinLibs POSIX/UCRT GCC 16.1.0を使い、Go 1.24.13 windows/amd64、CGO_ENABLED=1、CC=gccで指定8 packageのlocal Windows Race Detectorをexit 0で完了しました。

`mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/catalog ./internal/continuity ./internal/parquet -count=1`はingest、wal、archive、r2、delivery、continuity、parquetでpassし、catalogはno test filesでした。

GCCはrepoまたはmiseのdependencyではなく、GitHub CIとremoteは未確認です。

Real R2はcredentialと明示確認がないためoptional skipであり、旧M3-3A test countはR6の受入証拠へ流用していません。

R6は完了し、R7 final auditはunblockedですが、M3-4はR7の明示的なpassまでblockedです。

R7初回監査は、expected descriptor bytesのI/O前課金、terminal二回目readのclassification collapse、Parquet remote checkのactual-byte不明をresource bypassとして指摘し、R3をG3Eで再openしました。

G3EはrequestをI/O前、body bytesを実消費後に課金し、per-object limit、category残量、MaxObservationBytes残量の最小値をread capにするcap+1 bounded streamへ全remote object readを統一しました。

Terminal二回目readはtimeoutをUnavailable、budget／metadata limitをOversized、readable invalidをAmbiguous、valid wrong identityをDifferentとして保持し、非Exactからaction、FinalDigest、receiptを作りません。

R3 observerは`ReplayCheckDownloader`を持たず、Parquet remote bytesを`OpenLimited`とstreaming SHA-256で検証し、`check --download`はR4 executorだけが使います。

Event storeはmalformed eventとconflicting duplicateをstore-local validationで拒否しますが、publisher-level append failureはbest-effort diagnostic failureであり、action、receipt、publicationの許可または拒否に使いません。

Focused R3／R5／fault、uncached `internal/r2`全test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功しました。

旧M3-3A test countはG3Eの受入証拠へ流用せず、M3-4はR7再監査の明示的なpassまでblockedです。

R7二回目監査は、final observationのcounter upliftが共有budgetへ未課金であることと、Parquet list descriptorのsizeがbundle、Open、実streamへ照合されていないことをchanges_requiredとしました。

G3Fはcanonical final observationの必要bytesと最終pass実課金bytesとの差分だけを共有budgetへ一度課金し、課金後snapshotをfinal observationへbindします。

Exact-fitは成功し、実課金bytesが十分な場合は二重課金せず、uplift上限超過は`Oversized`としてfinal digest、action、receiptなしで停止します。

Parquet観測はlist size、bundle expected bytes、Open advertised size、actual stream bytesの一致を要求し、staleまたは誤ったlist sizeを`Ambiguous`として停止します。

Focused test、未キャッシュ`internal/r2`全test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功しました。

旧M3-3A test countはG3Fの受入証拠へ流用していません。

G3Fは完了しましたがR7再監査はpendingであり、M3-4は明示的なR7 passまでblockedです。

AdvisorのR7第三回監査receiptはphase `r7_m3_3a_third_audit`、verdict `pass`です。

監査scopeはR1からR6のcanonical contract、sealer、pure reconciler、bounded observer、graph verification、narrow executor、diagnostic event、thin publisher、receipt、repository gate、およびG3F remediationとM3-4未着手境界です。

監査evidenceはfixture 23件、Python 34件、全Go、未キャッシュ`internal/r2`、Protocol／Archive、vet、空のgofmt、両diff check、CGOとGCC 16.1.0によるcurrent-diff local Raceの成功、およびbranch／HEAD一致、tracked modified 26、untracked 43、staged 0です。

監査assumptionsはlocal gateでGitHub CIとoptionalなReal R2を必須にしないこと、G3E／G3Fの現行証拠を使い、旧52件または86件のtest countを流用しないこと、およびG7Aがdocs-onlyであることです。

Known unresolvedはGitHub CI未確認、Real R2 optional skip、M3全体final audit未実施です。

M3-3A R1からR7はcompletedとなり、M3-4を明示的にunblockします。

R7 passはM3全体final audit passではありません。

次taskはG8のM3-4 read-only deliveryであり、G7Aでは実装を開始しません。

M3-4のread-only delivery、CLI、selector、empty-cache fetchはG7Aでunblockedとなり、次task G8で開始可能です。

G8はM3-4のimmutable replay list／resolve／fetch plan／fetch／day verificationとCLIを実装しました。

Selectorはexact manifest key／domain hash／revisionとsingle complete revision graphのterminalだけを受理し、branch、duplicate、missing predecessor、ambiguous conversion、raw binding mismatchからwinnerを推測しません。

Fetch planはtrusted Layoutとstrict canonical manifestだけからremote key、digest、bytes、hash-derived cache pathを固定し、caller supplied key／pathを拒否します。

Fetchはread-only backend、bounded stream、remote close、size／hash verification、same-directory temporary、no-clobber promotionを使い、correct cacheだけをreuseしてcorrupt cacheを上書きしません。

Replay day reportはraw binding／semantics、part chain／root、Parquet schema／rows／file hash、row-chain rootを分離し、empty dayを受理しながらcampaign genesis全体を検証したとは主張しません。

`tickctl snapshots replay`、raw／replay compatible `tickctl fetch`、`tick-verify replay-day`はcanonical JSONを出力し、invalid flagをnonzero exitにします。

Focused delivery／CLI、関連archive／Protocol／Parquet、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、delivery／CLI local Raceは成功しました。

Production deliveryは`r2.ReadBackend`だけを使い、remote write capability、publication journal、SQLite、stage、eventを使用しません。

旧52件または86件のtest countはG8 evidenceへ流用せず、M3-3A R7 receiptを維持します。

M3-4はcompletedです。

G9はM3-5のnetwork-free fake end-to-endを実装しました。

Fake Batchからsealed WAL、raw-day manifest、M2 claim／raw publication、verified replay source、continuity reducer、Parquet、part／replay manifest、M3 bundle／publisher／receipt、empty-cache readerまでを一つのtestで接続します。

Testはidentity roots、approved action列、diagnostic event失敗の非veto、same-content retry、hash-derived cache、cache reuse、reader remote write 0を検証します。

既存cross-language goldenはPython verifierとPython unit testがrow-chain rootとpart-set rootを独立再計算します。

Focused E2E、関連10 package、fixture 23件、Python 34件、repository check、vet、空のgofmt、指定8 packageのlocal Windows Race Detectorは成功しました。

旧52件または86件のtest countはG9 evidenceへ流用せず、M3-3A R7 receiptを維持します。

Real R2はcredentialと明示確認がないためoptional skipであり、GitHub CIとremote I/Oは未確認です。

M3-5はcompletedですが、HTTP APIとM3全体final auditは未実施であり、M3全体をcompletedまたはfinal audit passとはしません。

M3全体final auditは、G9のM2 publicationがproduction `r2.Publisher`を通らず、test helperによるdirect remote injectionだったためchanges_requiredとなりました。

G9Eはdirect helperを削除し、production `r2.NewPublisher`と`Publisher.Publish`をnetwork-free fake backend、temporary `PublicationJournal`へ接続しました。

SQLite journalはM2 publisherの必須local境界としてだけ使用し、M2 same-content retry後にcloseするため、M3 publicationとread-only deliveryはjournal、SQLite、stageをauthorityまたは依存へ追加しません。

Fake backendは条件付きPutと読戻しVerifyだけを受理し、任意operation、trusted prefix外key、異内容上書きを拒否します。

Production M2 receipt、conditional claim、scope descriptor、raw object、raw-day manifestをremote exact bytesへ照合し、same-content retryがreceipt identityを維持してsuccessful claim writeとremote copy mutationを増やさないことを確認しました。

同じbackend上のraw manifest key／domain digestとclaim relationをM3 bundle、final receipt、resolved replay manifest、empty-cache day verificationまで接続しました。

Focused M3 E2E、M2 E2E、関連10 package、fixture 23件、Python 34件、repository check、vet、空のgofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功しました。

旧52件または86件のM3-3A test countはG9E evidenceへ流用せず、Real R2、GitHub CI、HTTP、commit、push、mergeは未実施です。

G9Eはcompletedです。

Advisorのwhole-M3監査phase `final_m3_whole_reaudit`はverdict `pass`、required actionsなしとなりました。

監査scopeはG0のdirty保全、Protocol V1、M3-1／M3-2G、独立したM3-3A R7 gate、M3-4 read-only delivery、G9E production M2 publisher E2E、四正本文書です。

Evidenceはproduction M2 publisherからM3 final receiptとempty-cache verificationまでの単一identity graph、旧direct injection helper 0、focused E2E、関連10 package、fixture 23件、Python 34件、repository check、vet、空のgofmt、両diff check、指定8 packageのlocal Windows Race passです。

Real R2はoptional skip、GitHub CIは未確認であり、local Race passをGitHub CI passとは扱いません。

HTTP、live broker、commit、push、mergeはM3 scope外であり、旧52件または86件のtest countはwhole-M3 evidenceへ流用していません。

M3全体はcompletedであり、final auditはpassです。

改訂記録（2026-07-16 M3 review findings remediation）: M3-3Aのrevision 3以降をimmediate predecessor successor検証と保守的なfinal-observation pre-lock budgetへ修正し、remote complete graphのrevision 1起点検証を維持しました。

publication round上限を`2*MaxParts+2=20002`へ更新し、Go／Pythonがpart数に対する必要round数をlock前に検査します。

final observationのpre-lock request budgetはraw、derivative、revision graph edgeの各readを含めて検査します。

Replay publisherのlock identityは任意filenameではなく、lock rootとtrusted Layoutのcampaign／publisher epochから導出するcanonical pathへ固定しました。

revision 3 successor、4-part fresh observation、round不足、canonical lock pathのfocused test、Python negative、fixture、repository gateを再確認しました。

`mise run test-python`は35 passed、`mise run fixture`は23 verified、`mise run check`、`mise exec -- go vet ./...`、指定8 packageのlocal Windows Race Detector、両diff checkは成功しました。

再確認後もbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 32、untracked 48、staged 0を維持しました。

Implementerのread-only R7 remediation auditは3件すべてをpassとし、remote I/O、GitHub CI、Real R2を未実施のlocal scope内で残存riskなしと判定しました。

## M4の運用強化

M4の詳細な実施計画は`execplan/2026-07-16-m4-production-operations-http-delivery.md`です。

M4の開始baselineは、M3をmainへ統合したPull Request #4のmerge commit `cb72752a651c88c3027b409f6f205ac9236f28b8`です。

M3はwhole-M3 final re-auditをpassし、mainへ統合済みです。

M4-0では専用ExecPlanだけを固定し、M4実装はまだ開始していません。

M4では、日次運用、pruning、handover、長時間稼働、障害注入を整備します。

必要に応じて、HTTP adapterなどの後続利用者向け接続を追加します。

M4の完了条件は、復旧手順と保存期限を含む運用手順を検証環境で実行できることです。
