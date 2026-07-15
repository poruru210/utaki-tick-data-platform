# Tickデータ収集・保存・配信基盤を構築する

このExecPlanは生きた文書である。実装と運用が進むたびに、`進捗（Progress）`、`想定外の発見（Surprises & Discoveries）`、`意思決定ログ（Decision Log）`、`成果と振り返り（Outcomes & Retrospective）`を最新に保たなければならない。このリポジトリには`PLANS.md`が置かれていないため、本計画は外部のExecPlan方法論に従い、この1ファイルだけで完結する。

本計画におけるリポジトリの作業名は`tick-data-platform`とする。主要な常駐processは`tick-gateway`、MT5 producerは`tick-capture-mt5`、read-only利用者向けcommandは`tickctl`と`tick-verify`、任意のHTTP adapterは`tick-api`とする。リポジトリの関心範囲は、sourceからの収集、local durability、archive、catalog、利用者向け配信interfaceまでである。取引執行、strategy判定、portfolio管理、experiment policyは含めない。

## 目的と全体像（Purpose / Big Picture）

この作業が完了すると、非常に小さなsource shimが、Parquet、Cloudflare、R2、consumer、配信APIを知らなくてもTickを収集できるようになる。現在のprojectではMQL5 Serviceがそのshimである。MQLは`OnTick`を1 Tickにつき1 callbackとみなさず、terminalのbulk履歴APIである`CopyTicks(..., COPY_TICKS_ALL, ...)`を使う。取得した1回分のresponseをversioned binary batchへencodeし、同一Windows host上のGo gatewayへlocalhost TCPで送信する。

Go gatewayはTCP受信、protocol検証、append-only WALへの永続化、idempotency、cursor管理、overlap reconciliation、raw object生成、Parquet変換、manifest生成、Cloudflare R2公開、catalogおよびread-only delivery contractを所有する。GoはWALをdurable flushし、rebuild可能なjournal transactionを完了した後だけMQLへapplication-level ACKを返す。TCP送信成功やsocket write完了はcommitではない。

通常のdata pathは次のとおりである。

    MT5 terminal Tick database
        -> MQL CopyTicks(COPY_TICKS_ALL)
        -> BatchFrameV1をencode
        -> localhost TCP
        -> Go tick-gateway
        -> append-only WAL + durable ACK
        -> raw WAL object + raw-day manifest
        -> replay reducer + Parquet + replay-day manifest
        -> private Cloudflare R2
        -> ArchiveReader / tickctl / tick-verify / tick-api
        -> DuckDB、exporter、分析consumer

MQLは以下だけを担当する。

    CopyTicksの実行
    source固有fieldのlossless encode
    TCP接続とhandshake
    1 batchの送信、ACK待ち、再接続
    gatewayが返したpoll directiveの実行

MQLは以下を担当しない。

    local append-only segment file
    .ready/.processing/.commit sidecar
    two-slot durable cursor state
    SHA-256 file chain
    Parquet
    R2 upload
    manifest
    delivery API

本計画でいう「raw tick」とは、source API境界で公開されたfieldを、整数値、IEEE-754 bit pattern、fieldの有無、source順序、adapter-versioned payloadまで正確に保持したものをいう。broker内部network protocolのpacket captureだとは主張しない。collectorはsource時刻と、acquisition callの開始・終了UTC clockおよびmonotonic clockを分離して記録する。`CopyTicks`はterminal履歴を読めるため、acquisition clockをTick到着時刻やnetwork latencyとは呼ばない。

TCP方式ではraw evidenceのdurability boundaryを次のように定義する。

> raw acquisition evidenceとは、Go gatewayがWALへdurably acceptした`CopyTicks` responseである。

MQLが`CopyTicks`を実行した直後、Goへ送る前にterminalが停止した場合、そのAPI call自体の証跡は残らない。ただしgatewayの最後のcommitted cursorからinclusiveに再取得し、Tick occurrenceを回収する。目的はTick dataの欠落防止と再利用可能な配信であり、MQL processが行った全API callの法科学的記録ではない。後者が必要になった場合だけ、別versionのnative durable spoolを追加する。

v1完了後、operatorまたはconsumerはversioned delivery contractを使い、正確なprovider/feed/symbol/campaign/dayとsnapshot hashを指定できる。`tick-verify`はR2からmanifestと参照raw objectを取得し、WAL entry、batch frame、hash chainまで独立検証する。`tickctl`はdataset discovery、snapshot resolution、fetch plan生成、downloadを行う。`tick-api`は同じ`ArchiveReader`のread-only HTTP adapterであり、大きなParquetやraw objectを必ずしもproxyせず、不変manifest key、hash、direct-download planを返せる。

MT5以外のsourceでも、同じlanguage-neutralな`HelloV1`、`BatchFrameV1`、`AckV1` contractを実装すればよい。MQLはproducerの1つでありsystemそのものではない。Go gatewayとarchive/delivery契約には取引logicを含めない。本計画はorder送信、terminal reconciliation、外部strategy candidate、既存の取引codeを変更しない。

## 進捗（Progress）

- [x] (2026-07-14 00:00Z) PR #32のmerge commitである`origin/main`の`798283fa78641850b5b67b21fb817e3fc7b90ff8`から開始した。
- [x] (2026-07-14 00:10Z) 外部projectの`QuoteRecord`、canonical JSONL importer、repository data policy、MT5 transport境界、strategy master plan、roadmapを確認した。
- [x] (2026-07-14 00:30Z) Cloudflare R2とWorkersの料金、制限、認証、custom domain、整合性、conditional write、lifecycle、bucket lockを公式文書で確認した。
- [x] (2026-07-14 01:00Z) raw observation、replay stream、不変archive、R2公開、独立consumerの境界を定義した。
- [x] (2026-07-14 08:00Z) `CopyTicks`同期状態、hash domain、campaign namespace、converter determinism、crash保証、single publisher、read-only verifierを設計へ反映した。
- [x] (2026-07-14 09:00Z) リポジトリの責務を収集から配信IFまでとし、作業名を`tick-data-platform`、主要processを`tick-gateway`とした。
- [x] (2026-07-14 09:30Z) MQL–Go間のFILE IF、sidecar、producer durable stateを廃止し、localhost TCP、Go WAL、durable application ACKへ置換した。
- [x] (2026-07-14 10:00Z) authoritative pathにUDPを使わず、v1ではMQL built-in TCPを使用し、Rust DLLはbenchmark後のextension pointに限定した。
- [x] (2026-07-14 10:30Z) gateway本体の実装言語としてGoを採用し、bounded allocation、WAL、TCP、HTTP、R2、fuzz testを同じcodebaseで扱う方針を確定した。
- [x] (2026-07-14 11:00Z) raw snapshotとParquet derivative snapshotを別manifest chainへ分離し、day definition、replay contract、conversion IDをR2 namespaceへ含めた。
- [x] (2026-07-14 11:30Z) `ArchiveReader`、`tickctl`、`tick-verify`、任意`tick-api`をrepositoryの配信境界として追加した。
- [x] (2026-07-14) 初期repository scaffoldを作成し、ローカル初回commit `6e4d299`を作成した。Go、Python、MQL5、Protocol V1、fixture、mise、Git設定を含む。
- [x] (2026-07-14) U0としてExecPlanとroadmapの構成参照を現行モノレポへ合わせ、`producers/mt5/`、`producers/fake/`、`protocol/v1/`を正本とした。M0からruntime処理を除外し、`part-manifest-v1`をM3へ延期した。
- [x] (2026-07-14) U1aとしてwire、message、mt5.mqltick.v1、Gateway WAL、hash domain、canonical JSON、raw-day/replay-day manifestの規則を`protocol/v1/`へ固定した。
- [x] (2026-07-14) U1bとして`testdata/tickdata/golden/`に18 fixtureを追加した。Hello、Resume、Batch、Ack、Error、raw/replay manifest、WAL、truncation、CRC mutation、unknown version、unknown message、oversized frame、short response、dense boundary、duplicate retransmission、ACK loss retry、WAL recoveryを含む。
- [x] (2026-07-14) U2としてGo decoder、独立Python decoder、canonical JSON検証、WAL/hash検証、Go/Pythonのfixture testを追加した。
- [x] (2026-07-14) U3として公開Go Protocol V1 adapter、fake producer、MQL5 Hello/Batch encoderを追加した。MetaEditorで0 errors、0 warningsを確認した。MQL5実機でのfixture出力実行はM0の対象外として未実施である。
- [x] (2026-07-14) `mise run check`を実行し、Go、Python、fixture、Ruff、gofmt、`git diff --check`を成功させた。
- [x] (2026-07-14) M0の検証結果、MetaEditor compiler version、fixture coverage、未実施境界をdocs/verification/m0-protocol-v1.mdへ記録した。
- [x] (対象外) MQL5実機でのfixture出力、TCP実通信、live MT5 collection、R2、Parquet、SQLite journal runtimeはM0の対象外であり、未実施である。
- [x] M0でTCP wire layout、WAL layout、hash domain、canonical JSON、raw-day/replay-day manifest contract、Go/Python独立decoder、fake producer、MQL5 encoderを実装した。
- [x] (2026-07-15) M0実装一式をcommit `c690710`へ反映し、`origin/main`へpushした。
- [x] (2026-07-15) M1として`internal/ingest`のloopback TCP、bounded decoder、session lease、cursor、dense directive、idempotent ACK、status metricsを実装した。
- [x] (2026-07-15) M1として`internal/wal`のactive WAL append、file sync、entry chain検証、partial tail recoveryと、`internal/journal`のrebuild可能SQLite batch index/cursor stateを実装した。
- [x] (2026-07-15) M1として`producers/mt5/TickCaptureService.mq5`のCopyTicks、built-in TCP、Hello/Resume、in-flight再送、ACK処理を実装し、MetaEditorで0 errors、0 warningsを確認した。
- [x] (2026-07-15) M1としてfake producerの実TCP integration testでduplicate、identity conflict、source error、dense boundary、partial frame、WAL sync前後、ACK loss、journal deletion後のrebuildを検証した。
- [x] M1で1 broker・1 symbolのMQL Serviceからlocalhost TCP、Go WAL、durable ACK、resume、rebuild可能SQLite journalまでをR2/Parquetなしで実装する。
- [x] (2026-07-15) M1実装をPull Request #1でmainへmergeした。
- [x] (2026-07-15) M2のローカル基盤としてWAL seal、TWTR検証、segment間chain継続、startup recovery、content-addressed outbox promoteを実装した。
- [x] (2026-07-15) M2のローカル基盤へmise run checkを実行し、GitHub ActionsのWindows CGO環境でWAL、archive、ingestのRace Detectorを成功させた。
- [x] (2026-07-15) Pull Request #2のreviewで、valid entry内部のTWTR bytesをtrailerと誤認する問題を修正し、entry-boundary regression testを追加した。
- [x] (2026-07-15) M2R-1からM2R-4として、sealed WALからcanonical raw-day manifestを生成し、pinned rclone、conditional publisher claim、publication journal、remote byte verification、read-only ArchiveReader、`tickctl` raw commands、`tick-verify day/campaign`、fake end-to-end testを実装した。
- [x] (2026-07-15) M2のlocal gateとGitHub Actionsを成功させた。review修正後のRepository checkはpush run `29380482941`とPR run `29380484737`、Windows raceはpush run `29380482973`とPR run `29380484762`で成功した。
- [x] (2026-07-15) Pull Request #3のreview修正commit `53f787a`を含むM2実装をmerge commit `cc9fc2d`としてmainへ反映した。
- [x] (2026-07-15) 実R2 smokeはproductionと分離したbucketまたはprefixおよび明示的credentialがないため、optionalな未実施境界として記録した。
- [x] (2026-07-15) reviewで指摘されたdataset namespace collisionとscope-specific record limitを修正し、回帰テストを追加した。
- [x] (2026-07-15) M2のmerge済みbaselineを`cc9fc2d`に固定し、M3-0のself-contained ExecPlanを`execplan/2026-07-15-m3-replay-parquet-delivery.md`へ追加した。M3実装はまだ開始していない。
- [ ] M3でordered overlap reducer、version-scoped Parquet、day-local part manifest、replay-day snapshotとdelivery contractを実装する。詳細な実装順序とM3-1 gateは`execplan/2026-07-15-m3-replay-parquet-delivery.md`へ委譲する。
- [ ] M4でproof-gated WAL pruning、複数producer、disk pressure、publisher handover、read-only `tick-api`、24時間以上の実機soakとfault injectionを完了する。
- [x] (2026-07-15) M2のGo/MQL contract test、Python fixture test、repository gate、`git diff --check`を成功させた。opt-in real R2 smokeは隔離credential不在のため実施していない。
- [x] (2026-07-15、M3-3A-R0-DOCS) M3-3Aをreopenし、既存のstage-based publication実装と過去のtest countをbehavior/failure inventoryへquarantineした。
- [x] (2026-07-15、M3-3A-R0-DOCS) 新しい設計正本を`execplan/2026-07-15-m3-3a-publication-redesign.md`へ作成し、R1からR7のgate完了までM3-3A実装を凍結した。
- [x] (2026-07-15、G0 read-only inventory) branch `agent/m3-replay-parquet-delivery`、full HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 24、untracked 26、staged 0を確認した。
  `git diff --check`と`git diff --cached --check`は成功し、remote I/OとGitHub CI確認は行わなかった。
- [x] (2026-07-15、G0A docs alignment) M3-1とM3-2Gを保持し、旧M3-3Aをuseful-case移行後の置換削除へ分類し、四正本文書だけを更新した。
  コード、test、fixture、DB、remote、commitは変更せず、旧test countを新設計の受入証拠へ流用しなかった。
- [x] (2026-07-15、G1-R1) replay publication bundle、complete final observation、二つのdomain digest、M2 claim relation、10個のresource limit、13件のnegative classificationをProtocol V1へ固定した。
  新設計の独立証拠としてfixture 23件、Python 33件、focused Go、gofmt、両diff checkを成功させ、旧M3-3A test countを流用しなかった。
- [x] (2026-07-15、G2-R2) local-only bundle sealer、trusted Layout key derivation、stable object ID、pure first-barrier reconciler、deterministic action-plan digestを実装した。
  Focused testと`internal/r2`全testを成功させ、local pathをcanonical identityから隔離し、旧stage publisher、journal、receiptを保持した。
- [x] (2026-07-15、G3-R3) replay専用bounded read interface、共有aggregate budget、fresh observer、part／revision graph verifier、Exact-only final observation変換を実装した。
  Timeout、short read、stale list、incomplete pagination、invalid inventory、missing candidate、check mismatch、branch、lock failureのnegative testとfocused／full `internal/r2` testを成功させた。
- [x] (2026-07-15、G4-R4) ObjectID-only narrow executor、verified local snapshot、rclone二操作allow-list、canonical append-only diagnostic event storeを実装した。
  Unknown／mismatched action、local mutation、timeout、collision、check mismatch、event missing／duplicate／conflict／digest mismatch、secret exclusionのtestとfocused／full `internal/r2` testを成功させた。
- [x] (2026-07-15、G1E contract correction) replay edgeへtrusted full key、strict canonical replay-day manifest、part countを追加し、empty terminal／empty predecessorをmanifest証明付きで受理する契約へR1を修正した。
  Edge stringsとescaped final observation canonical bytesを含むlock前aggregate budget gateをR2へ追加し、fixture 23件、Python 34件、Protocol／archive focused Goを成功させた。
  G5Cでpartial compile failureを解消し、M2 claim helperをraw publisher専用境界へ戻して旧Protocol stubを復活させなかった。
- [x] (2026-07-15、G1RC regression reclose) G1EとG2からG4のfocused regression、`internal/r2`全80 test、fixture 23件、Python 34件、Protocol／archive Goを成功させた。
  Final observationは最後のfresh pass counterをbindし、aggregate budget enforcementはcampaign全体で維持する。
  Lock取得後の全local derivative再検証をremote action前に行い、G5 legacy removalをunblockしたが、旧file／table／stage／journalの削除はまだ開始していない。
  旧M3-3A test countをG1RCの受入証拠へ流用しない。
- [x] (2026-07-15、G5-R5) Thin publisherをseal／preflight、lock、fresh observe、pure reconcile、approved ObjectID execute、fresh reobserve、receipt no-clobberのloopへ限定した。
  Receiptはcomplete canonical bundleとterminal final observationを再検証し、journal、stage、event、retry、ETag、time、secret、local pathをcanonical identityへ含めない。
  Diagnostic eventの欠落、重複、衝突、timeoutをdecision、action、receipt authorityにせず、publisherはeventをLoadしない。
  `replay_journal.go`と`replay_limits.go`、replay SQLite table定義を削除し、canonical limit validationをProtocol V1へ一本化した。
  Useful legacy caseをR5 testへ移行し、focused tests、`internal/r2`全83 test、fixture 23件、Python 34件、Protocol／archive Goを成功させた。
  旧M3-3A test countはG5の受入証拠へ流用していない。
- [x] (2026-07-15、G6-R6 completed) Fault matrixを現行R1からR5 testへ対応付け、action後crash、observation後crash、final observation後のreceipt保存crash、stale observation中のremote collision、aggregate request／byte exhaustionをfake testへ追加した。
  Focused fault test、`internal/r2`全88 test、fixture 23件、Python 34件、`mise run check`、Go vet、gofmt、両diff checkは成功した。
  `.github/workflows/windows-race.yml`の対象は`internal/ingest`、`wal`、`archive`、`r2`、`delivery`、`catalog`、`continuity`、`parquet`の8 packageへ固定した。
  Repository外のuser scopeへ導入したWinLibs POSIX/UCRT GCC 16.1.0を使い、Go 1.24.13 windows/amd64、CGO_ENABLED=1、CC=gccで指定8 packageのlocal Windows Race Detectorをexit 0で完了した。
  `mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/catalog ./internal/continuity ./internal/parquet -count=1`はingest、wal、archive、r2、delivery、continuity、parquetでpassし、catalogはno test filesだった。
  GCCはrepoまたはmiseのdependencyではなく、GitHub CIとremoteは未確認である。
  実R2はcredentialと明示確認がないためoptional skipとし、旧M3-3A test countはR6の受入証拠へ流用していない。
  R6完了によりR7 final auditを開始できるが、M3-4はR7の明示的なpassまでblockedのままである。
- [x] (2026-07-15、R7 changes_required) R7初回監査で、observerがexpected descriptor bytesをI/O前に課金し、terminal二回目readを`nil`へ潰し、Parquet verificationを実消費bytes不明のrclone checkへ委ねていたresource bypassを確認した。
  Event方針も、store-local validationとpublisher-level append failure非vetoの区別がredesign文書内で一致していなかった。
- [x] (2026-07-15、G3E bounded actual bytes classification) RequestをI/O前、body bytesを成功、short read、reader error、cap+1の全経路で実消費後にoverflow-safeな共有aggregate budgetへ課金するobserverへ修正した。
  Terminal二回目readはtimeoutをUnavailable、budget／metadata limitをOversized、readable invalidをAmbiguous、valid wrong identityをDifferentに保持し、非Exactからaction、FinalDigest、receiptを作らない。
  R3 observerから`ReplayCheckDownloader`を除去し、Parquet remote verificationを`OpenLimited`のbounded streamとSHA-256へ移したため、`check --download`はR4 executorだけが使う。
  Event storeはmalformed eventとconflicting duplicateをstore-localに拒否し、publisher-level append failureはbest-effort diagnosticとしてpublicationをvetoしない方針へ統一した。
  Focused R3／R5／fault、uncached `internal/r2`全test、fixture 23件、Python 34件、`mise run check`、Go vet、gofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功した。
  旧M3-3A test countはG3Eの受入証拠へ流用せず、R7再監査までM3-4をblockedにする。
- [x] (2026-07-15、G3F final uplift／list descriptor) Final observationの未課金upliftとParquet list descriptor未照合を修正し、現行local gateを再実行した。
- [x] (2026-07-15、R7 third audit pass) Advisorがphase `r7_m3_3a_third_audit`をverdict `pass`とし、M3-3A R1からR7をcompletedとした。
- [x] (2026-07-15、G7A docs-only unblock) M3-4のblockを解除し、次task G8を開始可能にした。
- [x] (2026-07-15、G8 M3-4 read-only replay UX) Immutable replay selector、hash-derived cache fetch、day-only verifier、`tickctl` replay list／fetch、`tick-verify replay-day`を実装し、現行local gateを成功させた。
- [x] (2026-07-15、G9 M3-5 network-free E2E) Raw truthからpublication、receipt、empty-cache deliveryまでをfakeだけで接続し、現行local gateを成功させた。
- [x] (2026-07-16、whole-M3 final audit changes_required) G9のM2境界がproduction `r2.Publisher`を通らず、test helperでcanonical bytesをremoteへ直接投入していたため、M3全体の完了証拠として不十分と判定した。
- [x] (2026-07-16、G9E production M2 publisher E2E) 同じnetwork-free backend上でproduction M2 publisher、M3 replay publisher、read-only replay deliveryを単一のidentity graphへ接続し、現行local gateを再実行した。
- [x] (2026-07-16、whole-M3 final re-audit pass) Advisorのphase `final_m3_whole_reaudit`がverdict `pass`、required actionsなしとなり、M3全体をcompletedとした。
- [ ] real R2 dataが存在した後のfollow-upへproject固有canonical JSONL exportとstrategy integrationを延期する。配信interface自体は延期しない。

## 想定外の発見（Surprises & Discoveries）

- 観察: 外部projectの既存offline importerは`source_sequence`、UTC `timestamp`、logical `symbol`、`bid`、`ask`だけを受理するが、再利用可能なsource Tickにはより多くのfieldとacquisition provenanceがある。
  根拠: 外部projectのschema定義とimporter実装を参照した。
- 観察: 外部のstrategy planは不変external corpusを要求し、importer内のterminal discoveryを禁止している。独立した収集・配信基盤はこの境界と両立する。
- 観察: MQL5 `OnTick`はmarket change通知であり、受信Tickごとにcallbackされる保証はない。capture sourceは`CopyTicks(COPY_TICKS_ALL)`でなければならない。
- 観察: EAまたはServiceの`CopyTicks`は同期timeout時に利用可能なTickだけを返し、terminalは同期を継続する。short responseだけではcursor advanceを正当化できない。
- 観察: polling sourceはinclusive cursorにより既読rowを返し、同一payloadが正当な別occurrenceである場合もある。set-based deduplicationはraw evidenceを破壊する。
- 観察: file suffix、sidecar、two-slot state、crash fragmentをMQL側へ持たせると、source shimがdurability protocolの一部になり複雑になる。同一hostのGo gatewayへTCP送信し、Goを唯一のdurability boundaryにすると責務が明確になる。
- 観察: TCP write成功はdurabilityを意味しない。WAL flushとjournal commit後のapplication ACKが必要である。
- 観察: authoritative raw pathへUDPを使うとloss、reorder、duplication、repair channelが必要になり、TCPより複雑になる。UDPはhealth telemetryやpreviewに限定すべきである。
- 観察: MQL built-in TCPで必要な機能を実装できるため、Rust DLLを最初から導入する理由は弱い。DLLはABI、native crash、配布、version整合を追加する。
- 観察: gateway停止中の未ACK Tickはterminal databaseからcatch upできるが、terminal/broker履歴は無期限retentionを保証しない。gateway lagを監視し、長期停止を正常状態として扱ってはならない。
- 観察: `modernc.org/sqlite`を使うとCGOなしのWindows Go runtimeでSQLite journalを実装できるが、依存関係はGo 1.24 toolchainを要求した。
  根拠: `go.mod`へ`modernc.org/sqlite v1.39.1`を追加し、`mise exec -- go test ./...`が成功した。
- 観察: MQL5の`SocketRead`は要求した長さより短いbytesを返し得るため、headerとbodyを別々にexact readしなければframe境界を保証できない。
  根拠: MQL5公式のSocketRead仕様と、`TickCaptureService.mq5`の`ReadExact`実装を対応させた。
- 観察: GoのGC pauseはACK latencyを変動させ得るが、batch単位、preallocated buffer、bounded queue、WALとParquet pathの分離によりcorrectness問題にはしない。実測で回収能力を超えた場合だけRustを再評価する。
- 観察: converter更新とlate source evidenceを同じday-manifest chainへ入れると、raw事実のrevisionとderivative実装のrevisionが混ざる。raw snapshotとreplay derivative snapshotは分離する必要がある。
- 観察: day単位取得だけではcampaign-global chainのgenesisから全履歴を証明できない。`tick-verify day`はchain sliceをanchorから検証し、`tick-verify campaign`が完全chainを検証する。
- 観察: Cloudflare R2のoperation無料枠よりretained byteが先に制約になりやすい。request数だけでcapacityを判断してはならない。
- 観察: R2 bucket lockはedge tokenによるoverwrite/deleteを防ぐが、Cloudflare account administratorはlock ruleを削除できる。administrator-resistant WORMとは主張しない。
- 観察: TWTRの`file_sha256`はtrailer直前までを対象とし、raw objectのcontent addressはtrailerを含むsealed file全体を対象とするため、二つのSHA-256を区別する必要がある。
  根拠: `protocol/v1/wal-layout.md`と`testdata/tickdata/golden/wal-entry-v1.json`がtrailer fieldのhash範囲を固定している。
- 観察: active WALをsealed inventoryへ移す処理は、trailer sync前後、inventory link前後、次active header作成中の各停止状態を再開できなければならない。
  根拠: WAL testでpartial trailer、active pathに残ったvalid trailer、inventoryとactiveのduplicate hard link、incomplete next headerを個別に再現した。
- 観察: valid BatchFrameV1を含むWAL entryの内部でも、file末尾から96 byteの位置にTWTRと同じ4 byteが現れ得る。
  根拠: RawMqlTickV1のFlagsを0x52545754にしたregression testで再現した。
- 観察: 既存M3-3AのReplayPublisherは、remote inspection、候補受入れ、stage rank、SQLite journal state、receipt authorityを一つのruntimeへ結合している。
  既存のM3-3A test countはこの挙動とfailure inventoryを残す証拠であり、現設計のacceptance evidenceではない。
- 観察: publisher claimの作成主体はM2 raw publicationであり、M3 replayは既存claimをExactとして再検証するだけである。
  M3がclaimを作成するとrawとderivativeのscope authorityが混ざる。
- 観察: final observationとreceiptはboundedな一時点のremote事実を証明するが、transaction、将来の不変性、admin-resistant WORM、failover、handoverを証明しない。
- 観察: G0のlocal workflow inventoryではrepo `AGENTS.md`、`check.yml`、`windows-race.yml`、`mise.toml`、global `advisor.toml`、`implementer.toml`、`semble-search.toml`、delivery skillとcontractsが存在した。
  `semble-search.toml`のrole名は`semble_search`であるが、G0とG0Aは既知pathとread-only Git確認で完了したためchildを起動せずsemantic searchも使わなかった。
- 観察: R3では既存`BoundedObjectBackend`がwrite-capableな`ObjectBackend`をembedしていたため、observerのread-only capabilityを型で限定できなかった。
  `GetLimited`、complete flag付き`ListLimited`、`OpenLimited`だけを公開するreplay専用interfaceとadapterへ分離した。
- 観察: R3ではAbsentをcomplete derivative inventory内の候補欠落だけに限定し、timeout、short read、stale list、不完全pagination、不正descriptor、graph矛盾をUnavailable、Ambiguous、Oversizedへ保つ必要があった。
- 観察: R4ではcanonical metadataのcaller-supplied pathをcontractへ含めずにrcloneへ渡すため、sealed canonical bytesをdomain検証してexecutor生成の一時snapshotへmaterializeする必要があった。
- 観察: R4のevent payloadを固定enumとdigestだけへ限定すると、credential、endpoint、local path、自由形式retry errorをschema上から除外できた。

## 意思決定ログ（Decision Log）

- 決定: repository名の作業案を`tick-data-platform`とし、収集、永続化、archive、catalog、配信interfaceを同じ責務境界に置く。
  理由: `tick-capture`は配信を表せず、`tick-archive`は静的ZIP保管庫に見える。`platform`は複数producer、gateway、archive、delivery adapterを含められる。
  日付/作成者: 2026-07-14 / Codex
- 決定: systemを薄いsource producer、Go gateway、immutable object bridge、versioned delivery interface、独立consumerへ分離する。
  理由: source環境は取得とencodeだけを担い、durabilityとdistributionをGoへ集中する。
  日付/作成者: 2026-07-14 / Codex
- 決定: v1のMQL–Go IFをlocalhost TCPとし、file handoff、`.sealed`、`.ready`、`.processing`、`.commit`、`ProducerStateV1`を廃止する。
  理由: 同一hostでfile transaction protocolをMQLへ実装するより、GoがWALとACKを所有する方が単純で検証可能である。
  日付/作成者: 2026-07-14 / Codex
- 決定: wire契約を`HelloV1`、`ResumeV1`、`BatchFrameV1`、`AckV1`、`ErrorV1`としてlanguage-neutralに固定する。
  理由: MT5以外のsourceも同じgatewayへ接続でき、MQL固有のfile layoutへarchiveを依存させない。
  日付/作成者: 2026-07-14 / Codex
- 決定: M0ではraw-day manifestとreplay-day manifestを固定し、`part-manifest-v1`はM3へ延期する。
  理由: part manifestはParquetのday-local part chainに属し、M0のwire、WAL、raw、replay契約と同時に固定するとM1以降のruntime境界が混ざる。
  日付/作成者: 2026-07-14 / Codex
- 決定: `producers/fake/`のfake producerは、ネットワークを使わない決定的なGo製test producer/packageとする。
  理由: M0ではTCP runtimeを扱わず、同じfixtureからBatchFrameV1、hash、異常結果を再現できる入力を用意する必要がある。
  日付/作成者: 2026-07-14 / Codex
- 決定: M0の完了条件にTCP runtime、live MT5 collection、R2、Parquet、SQLite journal runtime、crash injection、production operationを含めない。
  理由: M0は契約とcross-language conformanceを固定する段階であり、永続化と運用の実装は後続milestoneの責務である。
  日付/作成者: 2026-07-14 / Codex
- 決定: producerごとに最初は1 batchだけをin-flightとし、MQLはACKを受け取るまで同じbatch bytesを保持する。
  理由: localhostでのACK waitは小さく、terminal Tick databaseが到着分を保持する。最初から複雑なpipeliningを導入しない。
  日付/作成者: 2026-07-14 / Codex
- 決定: Go gatewayのappend-only WALを唯一のlocal durability boundaryとし、ACKはWAL syncとjournal transaction完了後だけ返す。
  理由: network writeとmemory queueをcommitとみなさず、crash後に同じaccepted batch inventoryを再構築できるようにする。
  日付/作成者: 2026-07-14 / Codex
- 決定: cursor、dense-boundary state、ordered boundary multiplicity、campaign ingest sequence、stream sequenceをGo gatewayが所有する。
  理由: MQL側のdurable stateをなくし、reconnect時にgatewayがauthoritativeなresume位置と次poll countを返せるようにする。
  日付/作成者: 2026-07-14 / Codex
- 決定: 同一`producer_instance_id + producer_session_id + batch_sequence`、同一bytesはidempotent duplicateとし、同じidentityで異なるbytesはintegrity collisionとする。
  理由: WAL sync後・ACK前のcrashやconnection lossから安全に再送するためである。
  日付/作成者: 2026-07-14 / Codex
- 決定: authoritative transportにUDPを使わない。v1ではMQL built-in TCPを使用する。
  理由: UDPではretransmission、gap repair、ordering、buffer overflow対策が必要になり、lossless captureのprotocolが複雑になる。
  日付/作成者: 2026-07-14 / Codex
- 決定: Rust DLLはv1へ導入しない。MQL socket throughputまたはserialization CPUが実測上の制約になった場合だけ薄いtransport adapterとして再評価する。
  理由: DLLはWindows ABI、native crash、distribution、terminal settingを増やす。R2、Parquet、WAL、retryをDLLへ入れてはならない。
  日付/作成者: 2026-07-14 / Codex
- 決定: gateway本体をGoで実装する。
  理由: TCP、file/WAL、HTTP、CLI、R2、bounded concurrency、cross-platform binary、fuzz testを小さいdependency setで実装できる。hard real-timeではなくdurable I/Oが支配的である。
  日付/作成者: 2026-07-14 / Codex
- 決定: Pythonは独立fixture decoder、manifest verifier test、DuckDB/analysis consumerに使う。Rustはoptional native adapter、C#は.NET中心の運用事情が生じた場合の代替候補に留める。
  理由: production data planeを1言語へ集中しつつ、別言語によるcontract検証を維持する。
  日付/作成者: 2026-07-14 / Codex
- 決定: raw acquisition responseの全rowとde-overlap済みreplay streamを別datasetとして保持する。
  理由: source APIの観測を保存しながら、consumerにはpolling overlapを除いた実用streamを提供する。
  日付/作成者: 2026-07-14 / Codex
- 決定: `source_payload_fingerprint`、`observation_hash`、`batch_hash`、WAL entry hashをdomain分離する。
  理由: source payloadの同一性、acquisition occurrenceの一意性、transport bytes、durable chainは異なる検証目的を持つ。
  日付/作成者: 2026-07-14 / Codex
- 決定: short `CopyTicks` responseは`copy_ticks_error == 0`なしでcursor advanceに使わない。
  理由: synchronization timeout時のpartial returnはterminal database tailを証明しない。
  日付/作成者: 2026-07-14 / Codex
- 決定: M2ではgateway WAL segmentを再encodeせずcontent-addressed raw objectとしてR2へ保存する。
  理由: 最初のoff-host retentionでcustom raw bundleとcompression determinismを同時に実装せず、accepted wire bytesを直接復元できるようにする。
  日付/作成者: 2026-07-14 / Codex
- 決定: TWTRの`file_sha256`をtrailer直前までのhash、raw object keyのSHA-256をtrailerを含むsealed file全体のhashとして別々に保持する。
  理由: Protocol V1のtrailer contractを変更せず、保存する全bytesへcontent addressを与えるためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: active WALからsealed inventoryへの移行とlocal outboxへのpublishには、既存destinationを置換しないhard linkを使う。
  理由: processがlink後に停止しても両pathのbytesが一致し、別processとの競合時も異なるbytesを上書きしないためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: M2のローカル基盤は明示的な`Store.Seal` APIを提供し、自動rotation policyを後続の運用設定へ残す。
  理由: sealと復旧のcorrectnessを、sizeまたは時刻によるrotation条件から分離して検証するためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: sealed active WALの回収は、headerからentryを順にparseし、entry boundaryで残りが正確に96 byteの場合だけTWTR trailerとして検証する。
  理由: file末尾からの固定offset判定は、valid entry payload内の同じ4 byteをtrailerと誤認するためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: raw-day manifestとreplay-day manifestを分離する。
  理由: late evidenceによるraw revisionとconverter/replay contract変更によるderivative revisionを混ぜない。
  日付/作成者: 2026-07-14 / Codex
- 決定: M2R-1のcanonical JSONはRFC 8785 JCSではなく、Protocol V1の`canonical-json-v1`規則を正本とする。
  理由: Protocol V1はUTF-8 byte順のkey、整数のみ、非ASCIIのlowercase `\u` escape、BOMなし、空白なし、末尾改行なしをGoとPythonで同一に検証し、digestへ再帰的にdigestを埋め込まない必要があるためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: M2R-1のraw WAL object keyは`objects/raw/wal-<64 lowercase sha256>.rtw`へ固定し、day-selected `objects[]`とchain-complete `chain_objects[]`を分離する。
  理由: keyのaliasとpath traversalを拒否しながら、day Aのrangeがobject Aとobject Cに分かれるA/B/A campaignでもmiddle object Bによるchain continuityを自己完結的に検証する必要があるためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: raw-day successorはchain start、scope、publisher epochを保持し、chain end、objects、chain_objects、count、watermarkだけを同じか前方へ単調拡張できる。
  理由: 「chainとcountは変更しない」という旧記述では、late evidenceを含むrevisionの自己完結性とprefix検証を表現できないためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: R2 namespaceへcampaign、day definition、replay contract、format、conversion IDを含める。
  理由: live/backfill、UTC/broker day、Parquet v1/v2、converter buildを同じrevision chainへ混在させない。
  日付/作成者: 2026-07-14 / Codex
- 決定: campaign-global accepted-batch chainを保持し、day manifestにはchain sliceのstart anchorとend rootを記録する。day verifierとcampaign verifierを分ける。
  理由: 1日だけ取得してもその日のsliceを自己完結して検証し、完全global chainを検証したと過大主張しない。
  日付/作成者: 2026-07-14 / Codex
- 決定: Parquet byte determinismをconverter build、dependency lock、writer config、ordered input、grouping decision、platform contractの同一性へscopeし、semantic determinismはcanonical row-chain hashで検証する。
  理由: Parquetは複数の正当なphysical encodingを許す。
  日付/作成者: 2026-07-14 / Codex
- 決定: M3-1をimplementation gateとし、`part-manifest-v1`、part_set_root、canonical row-chain、marker row、raw manifest key/hash binding、replay revision rulesをProtocol V1、Go fixture、Python fixtureで先に固定する。
  理由: rawの事実、replayの論理結果、Parquetのphysical bytes、derivative publicationのidentityを別々に変更できるようにし、未確定のencodingをruntimeへ持ち込まないためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: M3のParquet dependency versionは候補探索、module metadata、Go 1.24互換性、API確認、Windows Race workflowの順に検証してから一つのtagをpinする。
  理由: 現在の`go.mod`にはParquet dependencyがなく、latestを互換性確認なしに採用できないためである。
  日付/作成者: 2026-07-15 / Codex
- 決定（M3-3A-R0で一部失効）: derivative publicationはRawDayManifest-specificなpublication journal stateをblind reuseせず、replay manifestとconversion tupleを含むidentityとreceiptを持つ。
  旧決定のうち、固定stageをremote事実または再開authorityとして使う部分は失効した。
  理由: raw objectのpublication完了をParquet derivativeのpublication完了と誤認せず、再開判断をfresh remote observationから導くためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: repositoryがversioned `ArchiveReader` contractとreference CLIを所有する。M2でraw discovery/fetch、M3でderivative discovery/fetchを実装し、M4でread-only HTTP adapterを追加する。
  理由: 「収集から配信IFまで」という責務をoperator手順だけにせず、consumerが依存できる明示的contractにする。
  日付/作成者: 2026-07-14 / Codex
- 決定: R2 transferには検証済みexact rclone binaryをpinし、`copyto --immutable`と`check --download`だけをallow-listする。publisher claimにはminimal S3 conditional clientを使う。
  理由: append-only publication、remote byte verification、single publisherを別々に証明する。
  日付/作成者: 2026-07-14 / Codex
- 決定: M3-3Aでは旧stage rank、stage transition table、journal intent hash authorityを失効させる。
  新設計は`ReplayPublicationBundle`、bounded fresh remote observation、純粋なreconciler、approved action executor、append-only event、no-clobber receiptを使う。
  理由: local stageをremote事実の代用にすると、mutation、欠落、branch、claim conflictを再開時に見落とせるためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: M2がpublisher claimをIf-None-Match: *で作成し、M3はclaimを作成しない。
  M3は各reconcileで既存claimをExactとして再検証する。
  日付/作成者: 2026-07-15 / Codex
- 決定: M3-3AのR0からR7を順序付きgateとし、R7 final auditまでM3-4をblockedにする。
  理由: 旧stage runtimeを残したまま新旧authorityを併存させないためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: G0で確認した50件のdirty差分を一括操作せず、M3-1とM3-2G、四正本文書、M2 claim ownershipとhelperを保持する。
  bounded backend、trusted `r2.Layout`、lock、tool allow-list、no-clobber primitive、useful fault caseは境界を狭めて再利用し、monolithic publisher、ReplayStage、SQLite replay transition、journal-intent receipt authority、stage testはuseful-case移行後に置換削除する。
  日付/作成者: 2026-07-15 / Codex
- 決定: R3 observerはwrite backend、SQLite、journal、event、rcloneを受け取らず、replay専用`ListLimited`／`OpenLimited` interfaceと共有`ReplayObservationBudget`だけをremote観測能力にする。
  理由: `check --download`は実消費bytesをobserverへ返さないため、Parquetを含む全remote bodyをcap+1 bounded streamで検査しなければaggregate byte budgetを証明できない。
  日付/作成者: 2026-07-15 / Codex
- 決定: Protocol V1 final observationはclaim、raw、全derivative、part chain、replay graphがExactで、campaign／epoch lockをdigest生成直前にも再assertした場合だけ生成する。
  理由: stale observation、不確定read、graph winner selectionからreceipt証拠を作らないためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: R4 executorはbundle ObjectID以外のfull key、rclone key、local path、credential、operation名を受け取らず、immutable copyとdownload checkだけをtool seamへ公開する。
  理由: 任意key、任意path、任意rclone operationの実行をcompile-time境界で防ぐためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: R4 diagnostic eventはcanonical EventIDを持つappend-only説明資料に限定し、observer、reconciler、executorはevent Loadをaction authorityとして参照しない。
  理由: eventの欠落、重複、衝突がpublication policyを変更しないようにするためである。
  日付/作成者: 2026-07-15 / Codex
- 決定: v1は能動的なlate-history discoveryを保証しない。通常poll、reconnect re-fetch、明示的backfill/audit campaignで新 evidenceを実際に観測した場合だけraw-day revisionを追加する。
  理由: 最後のcursorより古いtimestampへ後日追加されたTickは通常pollだけでは発見できない。保証していない機能をday revisionの存在だけで暗示しない。
  日付/作成者: 2026-07-14 / Codex
- 決定: campaignごとにpublisher ID/epoch、local exclusive lock、R2 conditional publisher claimを必須にする。
  理由: clone済みgatewayや旧hostの同時publishによるmanifest branchを防止または検出する。
  日付/作成者: 2026-07-14 / Codex
- 決定: crash保証をMQL process、MT5 terminal、Go process、OS reboot、sudden power/storage write loss、filesystem corruptionに分ける。
  理由: 各failure classで保証可能な範囲が異なる。GoがWAL flushとrotationを所有することでMQL FileMoveのdurability問題は除去する。
  日付/作成者: 2026-07-14 / Codex
- 決定: M1のSQLite実装にはCGo-freeな`modernc.org/sqlite v1.39.1`を使い、SQLiteをraw truthではなくWALから再構築できるindexとして扱う。
  理由: Go GatewayをWindows環境で単一binaryとして動かし、MQL producerへSQLite依存を持ち込まずにjournal transactionをdurable ACKへ接続できる。
  日付/作成者: 2026-07-15 / Codex
- 決定: producer sessionのsession lease IDをproducer identityとsession identityから決定的に生成する。
  理由: TCP connectionだけが切れた場合に、MQLが保持している同一in-flight frameのlease fieldを変更せず再送できる。
  日付/作成者: 2026-07-15 / Codex
- 決定: M1のactive WALは`active.wal`一つに限定し、rotation、R2、Parquet、HTTP、pruningを実装しない。
  理由: M1のdurability boundaryをWAL syncとjournal commitへ限定し、off-host archiveとderivative deliveryの責務を後続milestoneへ残すためである。
  日付/作成者: 2026-07-15 / Codex

## 成果と振り返り（Outcomes & Retrospective）

このrevisionで完了したのはarchitectureの簡素化と責務再配置である。MQLのfile-based durable protocolを削除し、source adapterを`CopyTicks + encode + TCP + ACK/reconnect`へ縮小した。Go gatewayはWAL、cursor、idempotency、archive、catalog、delivery contractを所有する。これにより、MQL側の`.sealed/.ready/.commit`、two-slot state、crash fragment、file suffix state machine、producer-side SHA-256を廃止できる。

raw truthはGoがdurably acceptedした`BatchFrameV1`を含むgateway WAL segmentである。M2はそのbytesをcontent-addressed raw objectとして公開する。M3はraw-day snapshotへbindしたde-overlap済みParquet derivativeを作る。raw snapshotとreplay snapshotを分けるため、late source evidenceとconverter更新は別のrevision axisになる。

配信は後続作業ではなくrepositoryの責務である。M2から`ArchiveReader`、`tickctl`、`tick-verify`を提供し、M4で同じcontractのread-only HTTP adapterを提供する。DuckDB、project固有JSONL、strategy evaluationはconsumer側のversioned derivativeであり、この基盤のraw/archive contractを変更しない。

M0の契約実装とfixture検証は完了した。`protocol/v1/`の正本、18個のgolden fixture、GoとPythonのdecoder、fake producer、MQL5 encoderを整備し、`mise run check`を成功させた。
M1では、fake producerによる実TCP経路、WAL先行記録、SQLite rebuild、durable ACK、duplicate retry、MQL5 Service compileを完了した。
M2では、WAL sealとrotation、segmentをまたぐjournal rebuild、sealed verifier、byte-exactなcontent-addressed outbox promoteを実装した。
M2R-1からM2R-4として、raw-day manifest、immutable publication、publisher claim、publication journal、read-only fetch、day/campaign verifierを実装した。
fake end-to-end、repository check、Go vet、Windows Race Detector、PRのcheck/raceを成功させたため、実装上のM2は完了している。
実R2 smokeは明示的な隔離credentialがないため実施していないが、これはM2の通常gateに含めないoptional smokeである。
PR reviewで指摘されたdataset namespace collisionとscope-specific record limitも修正し、PR #3の最新headで再検証した。
そのM2実装はPull Request #3のmerge commit `cc9fc2d`としてmainへ統合済みである。
M3-0はこのmerge済みbaselineからreplay derivativeの設計を独立したExecPlanへ切り出した段階であり、M3実装を開始または完了したとは扱わない。
M3-3A-R3では、全remote readとrclone checkを共有aggregate budgetへ課金し、complete derivative inventory、part chain、replay revision chainをwinner selectionなしで検証し、Exact状態だけをProtocol V1 final observationへ変換するread-only境界を実装した。
M3-3A-R4では、sealed ObjectIDだけを実行し、local sourceを検証済みsnapshotへ固定してimmutable copyとdownload checkだけを行うexecutorと、非権威のappend-only diagnostic event storeを実装した。
実brokerのlive capture、Cloudflare resource作成、credential provision、deploymentは未着手である。M2からM4までを個別に受け入れ、M2ではraw remote復元、M3ではderivative検証、M4ではproduction fault toleranceとdelivery adapterを証明する。

## 文脈と案内（Context and Orientation）

このrepositoryでは、Protocol V1の正規仕様を`protocol/v1/`、MT5 producerを`producers/mt5/`、決定的なnetwork-free Go test producer/packageを`producers/fake/`、Go gatewayを`cmd/tick-gateway/`、read-only CLIを`cmd/tickctl/`と`cmd/tick-verify/`、任意HTTP adapterを`cmd/tick-api/`へ置く。gateway coreはtrading policyやexperiment policyをimportしない。

以降の用語は次の意味に固定する。

**producer**はsource固有処理を行う最小programである。**producer instance**は1つのinstall済みproducerを表すstable UUIDである。**producer session**はMQL processの1 incarnationを表すUUIDであり、batch sequenceはsession scopeである。**dataset**はprovider、stable feed identity、exact source symbolで定義する。**campaign**はacquisition modeとinitial cursorを固定し、producer/gateway restartをまたいで継続するcollection identityである。**session lease**は1 dataset/campaign/producer instanceに同時に1 active producerだけを許すgateway stateである。

**acquisition batch**は1回の`CopyTicks` callと、そのrequested cursor/count、全ordered response、error、clockである。**accepted batch**はgateway WALへdurably appendされたacquisition batchである。**raw observation**はaccepted batch内の1 rowである。**WAL entry**はaccepted batch bytesとgateway metadataを持つdurable recordである。**WAL segment**は複数entryを含むclose済みappend-only fileである。**raw object**はWAL segmentのbyte-exact copyをcontent-addressed keyへ保存したものである。

**stream tick**はordered overlap reconciliation後の1 occurrenceで、正確に1 raw observationを参照する。**continuity segment**はprior rangeとのoverlapが証明されたstream rangeである。**raw-day manifest**は特定day watermarkまでに認識したraw objectとbatch rangeのimmutable snapshotである。**replay-day manifest**は特定raw-day manifest hash、replay contract、conversion IDへbindしたParquet derivative snapshotである。**publisher claim**はcampaignの1 publisher epochだけにmanifest publicationを許すR2上のconditional-create objectである。**delivery contract**はdataset/snapshotの列挙、resolution、fetch plan、取得、検証を定義するread-only interfaceである。

必須v1 deploymentは次のとおりである。

    Windows source host:
      MT5 + 1 exact symbol専用MQL Service
      Go tick-gateway Windows Service
      local WAL/outbox
      -> private R2

MQLはdefaultで`127.0.0.1:<configured-port>`へ接続し、Goはloopbackだけでlistenする。v1では1 MQL Serviceにつき1 exact source symbolとし、initial synchronizationによるhead-of-line blockingを避ける。同一dataset/campaign/producer instanceへ2つのactive sessionが接続した場合、gatewayは2つ目をrejectする。別hostからのproducer接続はv1対象外であり、将来追加する場合はmTLSまたは認証済みGo-to-Go relayを別contractとして設計する。

## この設計で確認済みのCloudflare条件

以下の値は2026-07-14に公式文書で確認した。service条件は変わり得るため、provision前に再確認しなければならない。

R2 Standardには現在、月10 GBのstorage、Class A 100万operation、Class B 1,000万operation、無料Internet egressが含まれる。Class Aにはput、list、multipart作成、part upload、完了が含まれ、Class Bにはheadとgetが含まれる。無料枠はInfrequent Accessには適用されない。[R2 pricing](https://developers.cloudflare.com/r2/pricing/)を参照する。

R2は最大5 TiBのobjectを受け付ける。single uploadは最大5 GiB、multipartは最大4.995 TiBかつ10,000 partであり、Cloudflareは大きなdatasetまたはresume可能なuploadにmultipartを推奨している。本計画では通常objectを96 MiB未満でsealし、通常はobject単位でretryする。例外的に大きいbackfillではmultipartを使える。[R2 limits](https://developers.cloudflare.com/r2/platform/limits/)と[R2 upload methods](https://developers.cloudflare.com/r2/objects/upload-objects/)を参照する。

Workers Freeは現在、1日100,000 request、HTTP invocationあたり10 ms CPU、128 MB memory、invocationあたり50 external subrequestを許可する。Free Cloudflare accountではincoming request bodyが100 MBに制限される。このためWorkerはcontrol plane専用とする。[Workers limits](https://developers.cloudflare.com/workers/platform/limits/)と[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)を参照する。

R2は直接write/read/delete/listについてstrong consistencyである。本設計はupload後のpublishにこの性質を使うが、checkpoint correctnessをcache-enabled custom domainへ依存させない。[R2 consistency](https://developers.cloudflare.com/r2/reference/consistency/)を参照する。

R2 credentialは特定bucketへscopeでき、temporary credentialはさらにaction、prefix、exact object pathへscopeできる。presigned URLは1回の`GET`、`HEAD`、`PUT`、`DELETE`をsupportし、S3 API domainを使い、custom domainでは使えない。[R2 API tokens](https://developers.cloudflare.com/r2/api/tokens/)、[temporary credentials](https://developers.cloudflare.com/r2/api/s3/temporary-credentials/)、[presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)を参照する。

public R2 custom domainはbucket objectを公開する。defaultは`r2.dev`をdisabledにしたprivate bucketとする。独自domainは任意Workerまたは`tick-api` deploymentの前段に置ける。別のpublic archiveをR2 custom domainへ接続するのは明示的な公開判断後だけである。[R2 public buckets and custom domains](https://developers.cloudflare.com/r2/buckets/public-buckets/)を参照する。

R2 bucket lockはprefixのoverwriteとdeleteを一定期間または無期限に防止できる。v1では完全に不変なproduction archive prefixをlockする。ただしaccount administratorはlock ruleを削除できるため、これはedge credentialに対するretention controlでありadministrator-resistant WORMではない。smoke testは別bucketまたはlock対象外の専用prefixを使う。[R2 bucket locks](https://developers.cloudflare.com/r2/buckets/bucket-locks/)を参照する。

broker/symbol/campaign datasetごとに、WAL rotationとParquet cadenceに応じてraw object、Parquet object、part manifest、day manifestを公開する。object数はrate、WAL rotation、day boundaryに依存するため固定object数をcapacity保証に使わない。runtimeはdatasetごとのretained byte、object数、Class A/B request数を記録するが、無料枠に収めるためだけにraw dataを削除またはdown-sampleしない。

## MQL–Go TCP契約とGateway WAL

全IDはlowercase UUID stringまたはlowercase SHA-256 stringとする。各clock fieldはschema名にunitを含める。source `time`は符号付きUnix秒、source `time_msc`は符号付きUnix millisecond、acquisition wall clockは符号付きUTC Unix秒、MQL monotonic clockは1 producer session内だけで意味を持つunsigned microsecondである。source APIにないsub-second wall precisionを実装が捏造してはならない。

### Configuration

`ProducerConfigV1`は次を含む。

    protocol_version
    producer_instance_id
    producer_build_id
    campaign_id
    provider_id
    stable_feed_id
    broker_server_fingerprint
    exact_source_symbol
    acquisition_mode = live_follow | historical_backfill
    initial_from_msc
    gateway_host = 127.0.0.1
    gateway_port
    connect_timeout_ms
    ack_timeout_ms
    initial_batch_count
    maximum_batch_count
    dense_boundary_hard_cap
    maximum_frame_bytes
    reconnect_backoff policy

account number、password、order setting、R2 credentialは含めない。MQL terminalではloopback endpointをnetwork許可listへ登録する。v1 producerは1 exact symbolだけを扱う。

`GatewayConfigV1`は次を含む。

    listen addresses（defaultは127.0.0.1のみ）
    allowed producer/dataset/campaign identity
    maximum frame bytes/records
    session lease timeout
    WAL root、rotation duration、rotation byte cap
    SQLite journal path
    ACK durability policy
    raw/derivative outbox root
    R2 bucket/prefix
    publisher ID/epoch
    upload cadence
    day definition
    replay contract/conversion configuration
    local retention grace
    disk watermarks
    delivery API bind address
    credential environment variable names

### Wire framing

wire formatはlittle-endianかつself-delimitingとする。M0で正確なoffsetを固定する。全messageは共通envelopeを持つ。

    magic[4]
    protocol_version: u16
    message_type: u16
    frame_length: u32
    header_length: u32
    message-specific header/payload
    crc32c: u32

`frame_length`はenvelopeを含む全bytesを表す。gatewayはconfig済み最大値を超えるframeをpayload allocation前にrejectする。TCPにはmessage boundaryがないため、length prefixに従って`readFull`する。CRC32Cはtransport corruption検出用であり、archive identityにはgatewayが計算するSHA-256を使う。

### HelloV1 / ResumeV1

MQLは接続直後に`HelloV1`を送る。

    producer_instance_id
    producer_session_id
    producer_build_id
    MQL compiler build
    terminal build
    OS contract
    clock API ID
    campaign ID
    provider/feed identity
    exact source symbol
    record schema version
    acquisition mode
    initial cursor fact
    capability flags

Gatewayはidentity、config hash、session leaseを検証して`ResumeV1`を返す。

    accepted protocol/record version
    gateway_instance_id
    session_lease_id
    committed_cursor_msc
    committed_boundary_fingerprint sequence/multiplicity digest
    last_durable_batch_sequence/hash（同一sessionの再接続時）
    next_from_msc
    next_requested_count
    maximum_frame_bytes
    maximum_records
    heartbeat/idle timeout

同一dataset/campaign/producer instanceにactive leaseがある場合、別sessionをfail-closedでrejectする。正当なrestartでは旧TCP connectionのcloseまたはlease timeout後に新sessionを許可する。operatorによる強制lease解除はjournaled administrative actionとする。

### BatchFrameV1

`BatchFrameV1`は1回の`CopyTicks` responseを保持する。

    session_lease_id
    producer_session_id
    batch_sequence
    requested_from_msc
    requested_count
    fetch_wall_start_s
    fetch_wall_end_s
    fetch_monotonic_start_us
    fetch_monotonic_end_us
    returned_count
    copy_ticks_error
    source_status flags
    record_schema_version
    record_count
    ordered RawMqlTickV1 records

`RawMqlTickV1`は全`MqlTick` fieldを正確に保持する。

    time: signed 64-bit seconds
    bid_bits: unsigned 64-bit IEEE-754 binary64
    ask_bits: unsigned 64-bit IEEE-754 binary64
    last_bits: unsigned 64-bit IEEE-754 binary64
    volume: unsigned 64-bit integer
    time_msc: signed 64-bit milliseconds
    flags: unsigned 32-bit integer
    volume_real_bits: unsigned 64-bit IEEE-754 binary64
    capture_sequence: unsigned 64-bit session-scoped integer

returned countが負の場合もrecord 0件のbatchを送る。returned countが非負なら返却array全体を順序どおり送る。`ResetLastError()`は各`CopyTicks`直前に呼び、`GetLastError()`は直後に1回だけ読み取る。MQLはsource qualityを判定せず、timestamp regression、crossed quote、zero price、同値repeatをそのままencodeする。

### AckV1 / ErrorV1

Gatewayはbatch bytesを検証し、WALへappend、durable flush、journal transactionを完了した後だけ`AckV1`を返す。

    producer_session_id
    batch_sequence
    gateway_batch_sha256
    gateway_ingest_sequence
    status
    committed_cursor_msc
    committed_boundary_digest
    next_from_msc
    next_requested_count
    retry_after_ms

statusは少なくとも次を持つ。

    ACCEPTED_ADVANCED
    ACCEPTED_NO_ADVANCE
    DUPLICATE
    DENSE_BOUNDARY_CONTINUE
    DENSE_BOUNDARY_UNRESOLVED
    RETRYABLE_GATEWAY_ERROR
    FATAL_PROTOCOL_ERROR
    SOURCE_STATE_CONFLICT
    SESSION_LEASE_CONFLICT

`DUPLICATE`は同一batch identityと同一SHA-256の場合だけ返し、original ACKと同じcursor/poll directiveを返す。同一identityで異なるbytesなら`SOURCE_STATE_CONFLICT`として該当campaignのingestを停止する。

MQLはACKを受け取るまで1 batchのexact bytesをmemoryに保持する。connectionが切れたがprocessが継続している場合は同じsession/batch sequence/bytesを再送できる。MQL processが停止した場合は新sessionで再接続し、`ResumeV1`のcommitted cursorからinclusiveに再取得する。

### Cursorとdense boundary

Gatewayはaccepted batchをすべてraw evidenceとしてWALへ保存するが、cursor advanceは別判定とする。cursorをadvanceできるのは次をすべて満たす場合だけである。

    copy_ticks_error == 0
    returned_count >= 0
    current committed millisecondのordered multiplicityが解決済み
    returned_count < requested_count、またはresponseがcurrent boundaryより後のtime_mscへ到達

nonzero errorを伴うshort responseはrawとして保存するが、cursorを進めない。次directiveは最後のcommitted cursorからinclusiveに再取得させる。

responseがrequested countを満たし、committed boundaryと同じ`time_msc`で終わる場合、Gatewayは`DENSE_BOUNDARY_CONTINUE`と倍増した`next_requested_count`を返す。MQLは指定されたcountで同じinclusive cursorを再fetchする。error 0のresponseが後続timestampへ到達するか、error 0かつshort responseになるまで続ける。hard capへ達しても解決しない場合は`DENSE_BOUNDARY_UNRESOLVED`とし、cursorを進めずoperator actionを要求する。

Gatewayはcommitted cursorとともに、そのboundaryにあるordered `source_payload_fingerprint` sequenceとmultiplicityを保持する。timestampだけでsame-millisecond occurrenceをskipしない。

### Hash domain

hash inputは曖昧な文字列連結を使わず、domain prefix、固定width field、length-prefixed variable bytesを順に連結する。prefixはM0でcontract freezeする。作業案は次のとおりである。

    source_payload_fingerprint =
      SHA-256("tick-data-platform/source-payload/v1\0" ||
        record_schema_version || time || bid_bits || ask_bits || last_bits ||
        volume || time_msc || flags || volume_real_bits)

    observation_hash =
      SHA-256("tick-data-platform/observation/v1\0" ||
        producer_instance_id || producer_session_id || batch_sequence ||
        record_ordinal || capture_sequence || source_payload_fingerprint)

    gateway_batch_sha256 =
      SHA-256("tick-data-platform/batch/v1\0" || exact BatchFrameV1 bytes)

    wal_entry_hash =
      SHA-256("tick-data-platform/wal-entry/v1\0" ||
        gateway_ingest_sequence || previous_entry_hash ||
        receive metadata || gateway_batch_sha256 || exact BatchFrameV1 bytes)

Overlapと`SOURCE_HISTORY_CHANGED`は`source_payload_fingerprint`だけを使う。acquisition occurrenceの一意性は`observation_hash`、wire batch identityは`gateway_batch_sha256`、durable orderはWAL chainを使う。

### Gateway WALとjournal

WALはraw dataのlocal truthであり、SQLiteはrebuild可能なindex/journalである。active WALは次のようなself-delimiting entryを持つ。

    WAL file header
    repeated WAL entries
      entry length
      gateway ingest sequence
      previous entry hash
      receive wall/monotonic clock
      exact BatchFrameV1 length/bytes
      gateway batch SHA-256
      WAL entry hash
      commit marker/checksum
    optional sealed trailer

GatewayのACK pathは次の順序とする。

    TCP frameを完全受信
    envelope/CRC/schema/identity/leaseを検証
    duplicate identityを検査
    active WALへentryとcommit markerをappend
    WAL file handleをdurable sync
    SQLite transactionでbatch index、cursor、lease、chain rootを更新
    transaction commit
    AckV1を送信

WAL sync後・SQLite commit前にcrashした場合、startup scanがWALからjournalを再構築する。SQLite commit後・ACK前にcrashした場合、producerの再送をduplicateとして処理する。TCP途中切断では不完全network frameをWALへappendしない。WAL tailがpartialなら最後のvalid commit markerまでtruncateまたはquarantineし、ACK済みdataを失った可能性があればintegrity stopにする。

WALは60秒または64 MiBなどconfig済みの早い方でrotateする。rotation時はfileをclose/syncし、対象OS/filesystemに必要なdirectory durability処理をGo側で実施する。exact保証はWindows filesystemとstorage環境でfault testし、API callだけから突然のpower loss耐性を過大主張しない。

Parquet変換、R2 upload、catalog APIはTCP reader goroutineで実行しない。ingest pathとbackground workerの間はbounded queueまたはjournal scanで分離する。disk high-water時はGatewayがACKを止め、MQLにretryさせる。未ACK dataをdropしてcursorを進めてはならない。

### UDPとRust DLL

UDPはauthoritative ingest pathに採用しない。将来、health telemetry、wake-up hint、loss-tolerant previewへ使う場合もraw contractとは分離する。

v1はMQL built-in socketを使い、DLLをimportしない。benchmarkでMQL socketまたはserializationが実際のbottleneckになった場合だけ、Rust DLLを次のような薄いC ABIへ限定して検討する。

    capture_open(config_bytes, config_len)
    capture_submit(batch_bytes, batch_len)
    capture_status(status_out)
    capture_close()

DLLへWAL、R2、Parquet、manifest、delivery APIを入れない。DLL failureをMT5 processのfailure domainへ持ち込むため、採用には別ExecPlanとfault testを必要とする。

## Overlap、Duplicate、Gapの意味

raw acquisitionはdeduplicateしない。2 fetchが同じrowを返したなら、両accepted batchのraw rowを保持する。1 fetchが同じpayloadを2回返したなら、両ordinalを保持する。ACK loss後に同じexact batch bytesを再送した場合だけtransport-level duplicateとして1 WAL entryにする。

replay streamはproducer schemaが宣言するidentity modeを使う。stable native event IDを持つsourceはそのIDを直接mapし、same-ID/different-payload reuseをintegrity collisionとする。`MqlTick`のようにstable IDを持たないsourceはinclusive overlapをfetchする。Go gatewayはpersist済みtailと新accepted batch prefixの最長exact ordered matchを、`source_payload_fingerprint` sequenceとmultiplicityで探す。set、timestamp単独、observation hashはoverlap比較に使わない。unmatched suffixだけを新stream occurrenceとする。

一意なexact overlapがなければ、complete raw batchを保持し、`AMBIGUOUS_OVERLAP`をemitし、新continuity segmentを開始する。推測、drop、sort、mergeを行わない。同じstable IDまたは同じ証明済みoverlap positionでpayloadが変化した場合は`SOURCE_HISTORY_CHANGED`をemitし、新continuity segmentを開始する。source downtimeはmarkerを作るがsynthetic Tickを作らない。

v1のnormal live pollingは最後のcommitted cursorから進むため、cursorより古いtimestampへ後日追加されたTickを能動的に探す保証はない。late history revisionは、reconnect re-fetch、normal polling、明示的historical backfill、または将来の`history-audit-v1` campaignによって実際に新evidenceを観測した場合だけ作る。live campaignへ別modeのaudit結果を暗黙に混ぜない。

## 不変Raw、Parquet、Manifest形式

### Raw WAL object

M2ではclose済み`GatewayWalSegmentV1`のbytesを再encodeも圧縮もせず、sealed file全体のSHA-256を持つ`raw-wal-segment-v1` objectとして保存する。WAL objectはaccepted `BatchFrameV1` bytes、gateway ingest sequence、entry chainを完全に復元できる。active WALはuploadしない。sealed WALだけをlocal verifierが再openし、header、entry length、CRC、batch SHA、entry chain、trailer、trailer直前までを対象とする`file_sha256`、trailerを含むobject SHA-256を検証した後にoutboxへpromoteする。

WAL segmentが複数source dayを含む場合、object自体はcampaign-level content namespaceへ1回だけ保存し、複数raw-day manifestが異なるbatch/record rangeで参照してよい。

### Raw-day manifest

`raw-day-manifest-v1`のscopeは次である。

    dataset_id
    campaign_id
    day_definition_id
    date

revisionはimmutableかつcumulativeである。manifestは少なくとも次を持つ。

    manifest/version identity
    publisher ID/epoch
    producer/gateway config hash
    protocol/record/WAL schema version
    observed_through source/capture watermark
    terminal synchronization state
    settle policy
    completeness_status
    ordered raw object list
    objectごとのaccepted batch/record range
    acquisition row count/error count
    campaign chain slice start sequence
    predecessor anchor hash/root
    campaign chain slice end sequence/root
    raw_set_root
    previous raw-day manifest key/hash
    deterministic logical close time

`tick-verify day`はanchorから当日のchain sliceを検証するが、campaign genesisからの完全chainを検証したとは報告しない。`tick-verify campaign`がgenesisから指定rootまでを検証する。

`completeness_status`は少なくとも次を持つ。

    provisional
    settled_snapshot
    incomplete_source_error
    incomplete_sync
    incomplete_gateway_outage

`settled_snapshot`は「設定watermark時点のsnapshot」であり、将来のlate historyが絶対にないという保証ではない。

### Replay Parquetとmanifest

M3ではde-overlap済みstream occurrenceをParquetへ投影する。1 rowはraw object、WAL entry、batch sequence、record ordinalへ逆参照できなければならない。`ticks-parquet-v1`は少なくとも次を持つ。

    dataset/campaign identity
    producer instance/session
    gateway ingest/stream sequence
    raw object key/hash
    WAL entry/batch/record ordinal
    source time
    acquisition clock
    bid/ask/last valueとbit pattern
    volume/volume_real bit pattern
    flags
    source_payload_fingerprint
    observation_hash
    continuity segment ID
    marker

Parquet partは`dataset + campaign + replay_contract_id + conversion_id + day_definition_id + date`単位でchainを閉じる。各日の最初のpartはpredecessorをnullにし、part manifestはday-local previous hashを持つ。

`replay-day-manifest-v1`は次へbindする。

    exact raw-day-manifest key/hash
    replay_contract_id
    format_id = ticks-parquet-v1
    conversion_id
    converter_build_id
    dependency lock hash
    writer configuration hash
    target platform contract
    ordered part-manifest set
    part_set_root
    canonical stream row-chain root

converter更新はraw-day revisionを進めず、新しい`conversion_id`またはformat versionへ公開する。同一conversion tuple内のupload retryは最初にlocal verifyしたsealed bytesを再利用する。logical rowsのsemantic equalityはcanonical row-chain hashで判定する。

### Local layout

    tick-data-platform/
      wal/provider=<key>/feed=<key>/symbol=<key>/campaign=<uuid>/
        active-<gateway-instance>.wal
        sealed/wal-<start>-<end>-<sha256>.rtw
      journal/gateway.sqlite
      outbox/provider=<key>/feed=<key>/symbol=<key>/campaign=<uuid>/
        objects/raw/wal-<sha256>.rtw
        snapshots/raw/day-definition=<id>/date=YYYY-MM-DD/
          raw-day-<revision>-<sha256>.json
        derivatives/stream=<sha256(exact replay-contract-id)>/format=ticks-parquet-v1/conversion=<sha256(exact conversion-id)>/
          day-definition=<sha256(exact day-definition-id)>/date=YYYY-MM-DD/
            parquet/<start>-<end>-<part-sha256>.parquet
            manifests/part-<8-digit-sequence>-<manifest-domain-digest>.json
            replay-day-<revision>-<manifest-domain-digest>.json
      tmp/
      verification-receipts/

path componentはexact identity bytesのhashから構築し、Windows reserved name、case-insensitive collision、separator、parent traversal、末尾dot/space、path budget違反をrejectする。exact source symbolはmanifestに保持し、Unicode normalizationやcase foldingで変更しない。exact broker server stringはprivate mappingへ置き、archiveにはstable feed IDとserver fingerprintを残す。

### R2 layout

    v1/dataset=<sha256(exact dataset-id)>/provider=<sha256(exact provider-id)>/feed=<sha256(exact stable-feed-id)>/symbol=<sha256(exact exact-source-symbol)>/campaign=<sha256(exact campaign-id)>/
      publisher-claims/epoch=<epoch>.json
      publisher-transitions/from=<prior-claim-sha256>.json
      objects/raw/wal-<sha256>.rtw
      snapshots/raw/day-definition=<day-definition-id>/date=YYYY-MM-DD/
        raw-day-<revision>-<sha256>.json
      derivatives/stream=<sha256(exact replay-contract-id)>/format=ticks-parquet-v1/conversion=<sha256(exact conversion-id)>/
        day-definition=<sha256(exact day-definition-id)>/date=YYYY-MM-DD/
          parquet/<start>-<end>-<part-sha256>.parquet
          manifests/part-<8-digit-sequence>-<manifest-domain-digest>.json
          replay-day-<revision>-<manifest-domain-digest>.json

date-local chainは現行Goalの定義であり、hour partitionは追加しない。

marker rowへsyntheticなhourを割り当てず、trusted `r2.Layout`はProtocol V1のcampaign-relative derivative keyへimmutable rootと、この`dataset=<sha256(exact dataset-id)>/provider=...`で始まるcampaign prefixを一度だけprependする。

Local outboxは既存M2の`provider/feed/symbol/campaign`単位を維持する。R2のtrusted campaign prefixだけはdataset identityを先頭へ含め、同じprovider、feed、symbol、campaignを持つ別datasetとのnamespace衝突を防ぐ。

全manifestは`canonical-json-v1`を使う。

canonical bytesはUTF-8、BOMなし、空白なし、末尾改行なしとする。

これはRFC 8785 JCSの実装を意味せず、Protocol V1固有のcanonical-json-v1であり、GoとPythonが同じbytesを出力する。

object keyはUTF-8 byte列の昇順で並べ、arrayの順序は保持する。

numberはsignedまたはunsigned 64-bit range内の整数だけをdecimal表記で許可し、float、先頭zero、`+`、指数表記、`-0`を拒否する。

stringのASCII以外のcode pointはlowercase hexadecimalの`\u`形式へ変換し、補助平面はUTF-16 surrogate pairで表す。

strict decoderはunknown key、duplicate key、invalid UTF-8、noncanonical bytes、schemaで定義されたrange違反を拒否する。

filename SHA-256とmanifest digestはcanonical bytesへdigest自身を再帰的に埋め込まず、既存domain prefixを前置して計算する。

## R2公開と任意Worker

trust boundaryごとにprivate R2 Standard production bucketを1つprovisionする。`r2.dev`をdisabledにする。そのbucketだけにscopeしたObject Read & Write gateway tokenと、別のObject Read only verifier/delivery tokenを作る。administrative tokenはcollection hostへ置かない。production `v1/`へbucket lockを設定するが、administratorがruleを削除できるthreat modelをdeployment inventoryへ記録する。smoke testは別bucketまたはlock対象外prefixを使う。

M2 raw publisherはcampaign/publisher epochごとのOS exclusive lockを取得し、最初のraw publish前にminimal S3 clientが固定keyのpublisher claimを`If-None-Match: *`で作成する。

既存claimがbyte-identicalならM2 restartとして継続し、異なればraw publicationを停止する。

M3 replay publisherはclaimを作成せず、lock保持中の各fresh observationで既存claimがExactであることを要求する。

publisher handoverはmonotonic epoch、prior claim hash、conditional transition artifact、旧write token revoke、旧gateway停止確認を要求する。

R2 transferには検証済みexact rclone version、platform、binary SHA-256を`tools/tick-data-tools.lock.toml`へpinする。Goはshellを介さずargument vectorでrcloneを実行し、allow-listした`version`、`copyto`、`check` operationだけを使う。

M2 raw publicationは次の順序とする。

    sealed WAL raw objectをcopyto --immutable
    check --downloadでremote bytesを比較
    raw-day manifestを最後にcopyto --immutable
    raw-day manifestをcheck --download

M3 derivative publicationは次の順序とする。

    参照raw object/raw-day manifestを再検証
    Parquet objectをupload/check
    part manifestをupload/check
    replay-day manifestを最後にupload/check

`rclone sync`、`move`、`delete`、`purge`、`--ignore-existing`、`--s3-no-head`は使わない。

同じcontent-addressed keyで異なるremote bytesが存在する場合はintegrity collisionである。

M2 raw publicationは既存journal reconcileを使う。

M3 replay publicationはsealed bundleとfresh bounded remote observationから次のapproved actionを再計算し、upload済みobjectを削除せずに再開する。

Cloudflare Workerはv1 correctnessに不要である。将来、独自domain上のcontrol planeとしてcatalog responseまたは短期path-scoped credentialを提供できるが、大きなraw/Parquet bodyをproxyしない。`tick-api`もdefaultではmanifest/fetch planを返し、data objectはR2 S3 endpointから直接取得できる。

## 配信InterfaceとConsumer統合

repositoryは次のGo interfaceと意味的に等価なversioned read contractを所有する。

    type ArchiveReader interface {
        ListDatasets(ctx context.Context) ([]DatasetDescriptor, error)
        ListCampaigns(ctx context.Context, datasetID string) ([]CampaignDescriptor, error)
        ListRawSnapshots(ctx context.Context, scope RawDayScope) ([]SnapshotDescriptor, error)
        ListReplaySnapshots(ctx context.Context, scope ReplayDayScope) ([]SnapshotDescriptor, error)
        ResolveSnapshot(ctx context.Context, selector SnapshotSelector) (ResolvedSnapshot, error)
        BuildFetchPlan(ctx context.Context, snapshot ResolvedSnapshot) (FetchPlan, error)
        Fetch(ctx context.Context, plan FetchPlan, destination string) error
        Verify(ctx context.Context, snapshot ResolvedSnapshot) (VerificationReport, error)
    }

`ResolvedSnapshot`はmutable latest pointerではなく、選択したimmutable manifest key/hashを持つ。prefix listingからhighest revisionを選ぶ場合も、revision chain、publisher epoch、predecessor、hashを検証し、branchやduplicateがあればwinnerを推測しない。

reference commandsは次である。

    tickctl datasets
    tickctl campaigns --dataset <id>
    tickctl snapshots raw --dataset <id> --campaign <id> --date YYYY-MM-DD
    tickctl snapshots replay --dataset <id> --campaign <id> --date YYYY-MM-DD \
      --stream <replay-contract> --conversion <conversion-id>
    tickctl fetch --manifest <immutable-key-or-sha256> --output <dir>
    tick-verify day --manifest <immutable-key-or-sha256>
    tick-verify campaign --dataset <id> --campaign <id> --through-root <sha256>

M4の`tick-api`は同じcontractをread-only HTTPへmapする。

    GET /v1/datasets
    GET /v1/datasets/{dataset}/campaigns
    GET /v1/snapshots/raw?...query...
    GET /v1/snapshots/replay?...query...
    GET /v1/manifests/{sha256}
    POST /v1/fetch-plans
    GET /v1/health

APIは任意keyを受け取るgeneric R2 proxyにしない。fetch planは検証済みmanifestから導いたrelative object key、SHA-256、size、credential scopeを含む。大きなdataはR2から直接downloadする。APIをInternetへ公開する場合は認証、rate limit、短期credential、`Cache-Control: no-store`を別deployment contractで定義する。

follow-up consumerは独自DuckDB catalogとverified object cacheを持てる。reproducible experimentではexact raw/replay manifest hashとverified local objectを使う。現在のrepository用canonical `QuoteRecord` JSONLはversioned derivativeとして追加し、raw/replay manifest hashへbindする。consumerはR2 object、gateway state、source payloadを変更できない。

## 作業計画（Plan of Work）

### M0 — 契約

M0ではTCP runtime、live MT5 collection、R2、Parquet、SQLite journal runtime、crash injection、production operationを実行しない。networkとMetaTrader terminalも使わず、language-neutralなcontractを固定する。`protocol/v1/`に`HelloV1`、`ResumeV1`、`BatchFrameV1`、`AckV1`、`ErrorV1`、`RawMqlTickV1`、`GatewayWalSegmentV1`、hash domain、`canonical-json-v1`、`raw-day-manifest-v1`、`replay-day-manifest-v1`をnormative byte offset、integer width、maximum size、unknown-version handlingまで記述する。`part-manifest-v1`の仕様と実装はM3へ延期する。

`testdata/tickdata/`にgolden handshake、batch、duplicate、short-response-with-error、dense-boundary、WAL segment、raw-day/replay-day manifestを置く。`producers/fake/`のfake producerは、決定的かつネットワークを使わないGo製のtest producer/packageとする。Go decoderと独立Python decoderが同じbytes、hash、failureを返すことを証明する。mutation/fuzz testがterminal/networkなしで成功し、後続milestoneが推測なしに実装できればM0完了である。

### M1 — Local TCP captureとdurable ACK

M1では1 broker、1 exact symbol、1 MQL Service、1 Go gatewayだけを対象にする。MQLはbuilt-in TCPで`HELLO -> RESUME -> BATCH -> ACK`を実装する。Goはloopback listener、session lease、bounded decoder、WAL、SQLite journal、cursor、dense-boundary directive、idempotent duplicate、status/metricsを実装する。R2、Parquet、delivery HTTP、local pruningはdisabledにする。

fake producerと実MQLで次を注入する。

    TCP frame途中切断
    WAL append前crash
    WAL sync後・journal commit前crash
    journal commit後・ACK前crash
    ACK受信前MQL crash
    MT5 restart
    gateway restart
    duplicate resend
    same identity/different bytes
    nonzero CopyTicks error付きshort response
    dense boundary hard cap
    disk full/slow sync

journalを削除してもWALからsame accepted-batch inventory、cursor、chain rootを再構築でき、ACK済みbatchが失われず、未ACK batchが安全に再送/re-fetchされればM1完了である。

### M2 — Raw off-host retentionとraw delivery

M2ではsealed WAL segmentをcontent-addressed `raw-wal-segment-v1` objectとしてprivate R2へ公開する。exact rclone tool lock、publisher claim、raw-day manifest、remote byte verification、verification receiptを実装する。`tickctl datasets/campaigns/snapshots/fetch`と`tick-verify day/campaign`を追加する。

別machineまたは空cacheからread-only tokenだけでraw-day manifestをresolveし、参照WAL objectをdownloadし、accepted BatchFrameV1とchain sliceを復元できることが終了条件である。前日prefix、gateway SQLite、write credentialを必要としない。

### M3 — Replay derivativeとParquet delivery

M3の詳細な実施計画と検証手順は`execplan/2026-07-15-m3-replay-parquet-delivery.md`へ記録する。
Protocol V1の正本は`protocol/v1/`、architecture、scope、milestone acceptanceの正本はこの親ExecPlanである。

M3-1では、Protocol V1とGo/Python fixtureで`part-manifest-v1`、part_set_root、canonical replay rowとrow-chain、marker row、exact raw-day manifest key/hash binding、replay revision rulesを固定する。
このgateを通過するまで、Parquet、publication、deliveryのprototypeを越える実装を進めない。

M3-2以降では、ordered overlap reducerがrawの順序とmultiplicityを保持し、continuity markerとday-local part chainを生成する。
Parquet writerは互換性確認済みの一つのpinned `parquet-go` tagと明示的writer設定を使い、logical determinismと、同一build、dependency、writer、platformに限るbyte determinismを分けて検証する。
M3-3A replay publicationはraw publicationと別のsealed bundle identityとreceiptを持つ。

M3-3Aは`execplan/2026-07-15-m3-3a-publication-redesign.md`のR0からR7に従い、fresh bounded remote observationと純粋なreconcilerからapproved immutable actionを導く。

R2はbundle sealerとpure reconciler、R3はbounded observerとpart／replay graph verifier、R4はnarrow executorと非権威のappend-only diagnostic event store、R5はthin publisherとfinal observation receiptと旧runtime削除を担当する。

旧stage-based実装はbehavior/failure inventoryとしてquarantineし、R1からR7の受入完了まではM3-3Aを未完了とする。

M3-4のread-only selectorと空cacheからのlocal verificationはR7 final auditまで開始しない。

M3-2Gのkey correctionでは、現行Goalがpart chainをdateで閉じるためhour partitionを採用しない。
Protocol V1の一元exact-identity helperがcampaign-relative physical derivative keyを導出し、trusted `r2.Layout`だけがimmutable rootとcampaign prefixを一度だけprependする。
marker rowへsyntheticなhourを割り当てず、旧generic derivative keyへのaliasも作らない。

M3ではproof-gated pruning、publisher handover、multi-broker/symbol運用、Worker、HTTP `tick-api`、active audit、24時間soak、live broker、mergeを実施しない。

同じraw snapshotから別conversion IDを作ってもraw revisionが変わらず、同じdayだけを取得してpart chainを自己完結検証でき、live/backfillの別campaignが混ざらなければM3完了である。

### M4 — Production operationとHTTP delivery adapter

M4ではproof-gated WAL/outbox pruning、disk pressure、複数broker/symbolを別MQL Serviceで運用する構成、publisher handover、verification receipt、read-only `tick-api`を実装する。1 broker・1 symbolで24時間以上の実機soakを行い、process restart、MT5 restart、forced reboot、network/R2停止、rclone timeout/retry、disk high-water、gateway長時間停止、publisher failoverを注入する。

fake producerで想定最大rateの10倍を入力し、memory/queueがbounded、WAL recoveryが完全、GC/ACK lagがterminal履歴の回収余力を超えないことを確認する。実測でGoが制約になった場合だけRust adapterまたはgateway再評価をDecision Logへ追加する。

`tick-api`がimmutable snapshot discoveryとfetch planを返し、raw/Parquet dataを変更せず、read-only credentialで動作できればM4完了である。能動的late-history auditは別campaignまたは別follow-upであり、v1 acceptanceに暗黙に含めない。

各milestone完了時に`docs/architecture/repository-layout.md`、`docs/plan/roadmap.md`、このExecPlanのProgress/Decision Log/Outcomesを更新する。M3以降に外部strategy planから新しいacquisition sourceを参照する場合も、現行のsource-neutral contractを置き換えず、これだけでMilestone 3Aをacceptedにしない。

## 具体的な手順（Concrete Steps）

repository layoutは次を基準とする。

    producers/mt5/TickCaptureService.mq5
    cmd/tick-gateway/
    cmd/tickctl/
    cmd/tick-verify/
    cmd/tick-api/
    internal/protocol/
    internal/ingest/
    internal/wal/
    internal/journal/
    internal/continuity/
    internal/archive/
    internal/parquet/
    internal/catalog/
    internal/delivery/
    internal/r2/
    tools/tick_fixture_verify.py
    testdata/tickdata/
    protocol/v1/

MQL Serviceのcontrol flowは次へ限定する。

    OnStart
      -> Connect
      -> SendHello / ReadResume
      -> loop:
           directiveに従いCopyTicks
           EncodeBatch
           SendAll
           ReadAck
           reconnect時はsame in-flight resendまたはnew-session resume

MQLはfile spool、SQLite、R2、Parquet、DLLを使わない。Goはcapture/upload中にPythonやDuckDBを呼び出さない。

operator向けcommandは次のとおりとする。

    go run ./cmd/tick-gateway init --config local/tick-gateway.toml
    go run ./cmd/tick-gateway run --config local/tick-gateway.toml
    go run ./cmd/tick-gateway status --config local/tick-gateway.toml
    go run ./cmd/tick-gateway reconcile --config local/tick-gateway.toml --dry-run
    go run ./cmd/tick-gateway verify-local --config local/tick-gateway.toml
    go run ./cmd/tick-gateway claim-publisher --config local/tick-gateway.toml
    go run ./cmd/tick-gateway seal-wal --config local/tick-gateway.toml
    go run ./cmd/tick-gateway build-raw-snapshot --config local/tick-gateway.toml --date YYYY-MM-DD
    go run ./cmd/tick-gateway build-replay --config local/tick-gateway.toml --date YYYY-MM-DD
    go run ./cmd/tick-gateway upload --config local/tick-gateway.toml
    go run ./cmd/tick-gateway prune-local --config local/tick-gateway.toml --dry-run

    go run ./cmd/tickctl datasets --config local/tick-reader.toml
    go run ./cmd/tickctl snapshots raw --config local/tick-reader.toml --dataset <id> --campaign <id> --date YYYY-MM-DD
    go run ./cmd/tickctl fetch --config local/tick-reader.toml --manifest <key-or-sha256> --output <dir>
    go run ./cmd/tick-verify day --config local/tick-reader.toml --manifest <key-or-sha256>
    go run ./cmd/tick-verify campaign --config local/tick-reader.toml --dataset <id> --campaign <id> --through-root <sha256>
    go run ./cmd/tick-api serve --config local/tick-api.toml

focused verificationとfull verificationを実行する。

    gofmt -w cmd internal
    gofmt -l cmd internal
    go vet ./...
    go test ./...
    go test -race ./...
    go test -fuzz=FuzzBatchDecoder ./internal/protocol
    uv run pytest tests/unit/test_tick_data_contract.py tests/stateful/test_tick_data_invariants.py
    uv run pytest
    uv run ruff check .
    uv run ruff format --check .
    uv run ty check src tests
    uv run python -m tools.export_statecharts
    git diff --exit-code -- docs/generated/statecharts
    git diff --check

productionとは別のsmoke bucket/prefix credentialを明示的に与えた場合だけ、append-only R2 smoke testを実行する。

    go run ./cmd/tick-gateway smoke-r2 --config local/tick-gateway-smoke.toml

smoke testはsynthetic dataだけをuploadし、publisher claim、raw object、raw-day manifest、M3以降のParquet/part/replay-day objectを検証する。remote objectをdelete、move、sync、overwriteしない。

## 検証と受入条件（Validation and Acceptance）

deterministic fake producerは次を生成しなければならない。

    normal Tick
    same timestampの別Tick
    byte-identicalな正当repeat
    inclusive overlap
    timestamp regression
    empty response
    source error
    short response + nonzero synchronization error
    full same-millisecond batch
    dense-boundary continuation/hard cap
    changed historical row
    ACK loss後のsame-byte resend
    process restart後のinclusive re-fetch
    TCP partial frame
    oversized/malformed frame

cross-language fixtureにより、MQLがencodeした`BatchFrameV1`をGoとPythonが同一にdecodeし、全IEEE-754 bit pattern、unsigned volume、Unicode symbol、boundary integerをround-tripすることを証明する。capture sequenceだけが異なる同一payloadは、同じsource fingerprintと異なるobservation hashを持つ。CRC mutation、length mutation、unknown version、truncated frameはfail-closedである。

crash injectionは次の直前と直後でprocessを停止させる。

    TCP frame complete receive
    WAL entry header write
    batch bytes write
    WAL commit marker write
    WAL sync
    SQLite transaction begin/commit
    ACK write
    WAL rotation/rename
    raw outbox write/rename
    rclone data upload
    remote check
    raw-day manifest upload
    Parquet write/verify
    part manifest upload
    replay-day manifest upload

必須scenarioは次である。

1. WAL sync後・ACK前にGoが停止し、MQLがsame batchを再送しても1 accepted batchだけになる。
2. MQLがACK受信前に停止し、新sessionがgateway cursorからinclusiveに再fetchしてもstream occurrenceに欠落がない。
3. short response + nonzero error後のcallで古いTickが追加されてもcursorが進んでおらず回収できる。
4. gateway disk high-waterでACKを停止しても未保存dataをdropせず、healthがnonzeroになる。
5. SQLiteを削除してもWALからaccepted inventory、cursor、chain rootを再構築できる。
6. raw-day manifestだけからWAL objectとBatchFrameV1を別cacheへ復元できる。
7. replay-day manifestがexact raw-day hashへbindし、converter変更がraw revisionを変更しない。
8. duplicate day revision、branch、publisher epoch conflict、missing predecessorを推測で解決せず停止する。

Go benchmarkは想定最大rateの10倍を一定時間入力し、次を確認する。

    memoryがconfig上限内でbounded
    goroutine/channelがbounded
    WAL sync latencyとACK latencyを計測可能
    GC pauseでcorrectnessが変わらない
    Parquet/R2停止がTCP ingestを直接blockしない
    diskが許す範囲でbacklogを回収できる

Goが基準を満たさない場合、profile結果をDecision Logへ記録してからRustを再評価する。抽象的な低遅延期待だけで言語を変更しない。

crash保証は別々にtest/reportする。

- A: MQL process crash — gateway cursorから自動resumeする。
- B: MT5 terminal crash —新sessionで自動resumeする。
- C: Go process crash — WAL/journalから自動復旧する。
- D: OS crash/forced reboot — productionと同じfilesystem/storageで実機testする。
- E: sudden power/storage write loss — partial/missing WALを検出し、ACK済みdata lossの可能性があればintegrity stopにする。
- F: filesystem corruption — 自動repairせずintegrity stopにする。

fake rclone、fake conditional-claim backend、実R2 smoke testにより、初回upload、same-content retry、same-key different-content拒否、`check --download`、timeout/retry、data-before-manifest、raw-before-replay、publisher conflict、epoch handover、verification receipt、proof/grace前のpruning阻止、全条件後のpruning許可を証明する。

`tick-verify day`は選択したraw/replay manifestのhash、revision chain、publisher claim、raw object、chain slice、Parquet part chainを検証する。`tick-verify campaign`はcampaign genesisから指定rootまでのcomplete accepted-batch chainを検証する。day verifierはglobal chain全体を検証したと表示してはならない。

v1 acceptanceはM0からM4を順に通過し、1 broker・1 symbolの24時間以上の実機soak中にMQL/MT5/Go restart、forced reboot、network/R2停止、retry、disk pressureを注入した後、別cacheとread-only tokenだけでraw snapshot、全参照WAL object、M3 Parquet、delivery fetch planを検証できた時点で完了する。canonical JSONL import、candidate evaluation、active late-history auditはv1 acceptanceに含めない。

## 冪等性と復旧（Idempotence and Recovery）

MQLはdurable cursorを所有しない。process中は1つのin-flight batch bytesを保持し、ACKを受け取れば破棄する。connectionだけが切れた場合はsame identity/bytesを再送する。processまたはterminalが停止した場合は新sessionで`HelloV1`を送り、Gatewayの`ResumeV1`から再開する。last accepted batchがcursorをadvanceしなかった場合は同じinclusive rangeを再取得するため、raw duplicate observationが追加され得るが、replay overlap ruleで処理する。

Gateway WALはraw truthである。SQLite journalのlogical stateは少なくとも次を持つ。

    RECEIVED
    WAL_APPENDED
    WAL_SYNCED
    INDEXED
    ACKED
    WAL_SEALED
    RAW_REMOTE_VERIFIED
    RAW_SNAPSHOT_PUBLISHED
    REPLAY_SEALED
    REPLAY_REMOTE_VERIFIED
    REPLAY_SNAPSHOT_PUBLISHED
    LOCAL_PRUNABLE
    LOCAL_PRUNED

実装上はWAL scanから導出できるstateを冗長に永続化しなくてよいが、operator statusはこの意味的stateで報告する。

M2 raw `reconcile`はSQLiteを真実源と仮定せず、WAL、outbox、R2 proof、verification receiptとの差分を再構築または説明する。

M3 replay `reconcile`はjournal stageをauthorityにせず、sealed bundleとfresh bounded remote observationだけから決定的なaction、retry、integrity stop、resource stopを返す。

M3 receiptはcomplete final observationをbindするpoint-in-time evidenceであり、将来のremote不変性を証明しない。

R2 publicationはcontent-addressed objectとimmutable manifest revisionによりidempotentである。publisher claim以外のobject keyはfull SHA-256を含む。same-content retryは成功し、different-content collisionは停止する。mutable `latest` objectをv1に置かない。delivery layerがprefix listingからhighest revisionを選ぶ場合はchainを検証する。

local pruningはM4までdisabledとする。raw WAL segmentをpruneできるのは次をすべて満たす場合だけである。

    WAL segmentがsealedかつlocal verify済み
    byte-identical raw objectがremote check済み
    該当全accepted batchが一意なhighest valid raw-day manifestから発見可能
    independent tick-verifyがverification receiptを生成済み
    configured graceを経過
    active cursor/recoveryに不要

raw WAL pruningをParquet生成へ依存させない。Parquetはremote rawから再生成可能である。local Parquet/outboxをpruneする場合はreplay-day manifestとverification receiptを別に要求する。proof不足、branch、clock regression、delete failureがあれば保持する。容量枯渇時もproofを迂回せず、GatewayはACKを止めてintegrity/availability failureを報告する。

terminal historyは無期限bufferではない。runtimeは次を監視する。

    last durable source time
    current source time
    uncommitted lag
    gateway downtime
    WAL free space
    terminal synchronization state
    oldest retrievable tick（取得可能な場合）

lagがoperator設定の安全上限を超えた場合はcritical healthを出す。履歴回収可能性を確認できないまま「停止してもいつでもcatch upできる」と主張しない。

## 成果物と注記（Artifacts and Notes）

implementationは実Tick WAL、SQLite journal、raw object、Parquet archive、Cloudflare credential、rclone config、domain secretをtracked fileとして作成しない。次をignore ruleへ追加する。

    *.wal
    *.rtw
    *.parquet
    gateway.sqlite*
    outbox/
    verification-receipts/
    local/*.toml
    rclone.conf

trackするのはgeneratorとhashがtestに含まれる小さなsynthetic golden batch/WAL/Parquet/manifestだけとする。

Cloudflare account ID、production/smoke bucket名、control hostname、token identifier、publisher ID/epoch、rclone exact version/platform/binary SHA-256、lock/lifecycle configuration、administrator-removable bucket lock threat modelはsecret-free deployment inventoryへ記録する。access key、account number、trading identity、exact broker server stringをmanifestへ記録しない。archiveはstable feed IDとserver fingerprintを持ち、exact mappingはoperator管理のprivate storeへ置く。

provenanceにはproducer build、MQL compiler build、terminal build、Go gateway build、Go toolchain、OS contract、clock API、protocol version、dependency lockを含める。runtime metricsにはdataset/campaignごとのconnection state、session lease、CopyTicks error、accepted batch/row、committed cursor、uncommitted lag、WAL bytes、WAL sync latency、ACK latency、duplicate、queue depth、GC pause、raw/replay publish state、publisher claim、R2 request、delivery request、integrity stopを含める。

source/transport契約はMQL5公式の[event queue](https://www.mql5.com/en/docs/event_handlers)、[CopyTicks](https://www.mql5.com/en/docs/series/copyticks)、[MqlTick](https://www.mql5.com/en/docs/constants/structures/mqltick)、[network functions](https://www.mql5.com/en/docs/network)、[GetMicrosecondCount](https://www.mql5.com/en/docs/common/getmicrosecondcount)に依存する。R2 transferはCloudflareの[rclone configuration](https://developers.cloudflare.com/r2/examples/rclone/)、rcloneの[copyto](https://rclone.org/commands/rclone_copyto/)、[check](https://rclone.org/commands/rclone_check/)、[copy](https://rclone.org/commands/rclone_copy/)に依存する。implementation前とdeployment前に再検証する。

## Interfaceと依存関係（Interfaces and Dependencies）

v1の再利用可能なboundaryは次である。

    ProducerProtocol
    SessionLeaseStore
    BatchDecoder
    DurableWal
    GatewayJournal
    CursorManager
    ContinuityReducer
    RawObjectSealer
    RawSnapshotWriter
    ParquetWriter
    ReplaySnapshotWriter
    ReplayPublicationBundleSealer
    ReplayRemoteObserver
    ReplayPublicationReconciler
    ReplayActionExecutor
    ReplayPublicationEventStore
    ReplayReceiptStore
    PublisherClaimStore
    RcloneTransport
    ArchiveReader
    ArchiveVerifier
    DeliveryHTTPAdapter

Go packageのdependency directionは一方向とする。

    protocol <- MQL producer and other producers
    protocol <- ingest <- WAL/journal/cursor
    WAL/journal <- raw archive/continuity/Parquet/manifest
    publisher claim <- minimal conditional S3 client
    immutable outbox <- pinned rclone <- private R2
    verified replay bundle -> bounded remote observation -> pure reconcile -> approved immutable action -> fresh final observation -> receipt
    immutable R2 <- ArchiveReader/Verifier <- tickctl/tick-api/consumer

Goを選択する理由は、gatewayの主要負荷がTCP、durable file I/O、hash、bounded worker、HTTP、object storageであり、hard real-time executionではないためである。Go実装は次を必須規則とする。

    Tick単位でgoroutineを作らない
    producer connectionごとにbounded reader stateを持つ
    maximum frame sizeをallocation前に検査する
    receive bufferを再利用する
    raw batch bytesのままWALへappendする
    unbounded channelを使わない
    Parquet/R2をACK pathから分離する
    file Sync時間、allocation、GC、queue depthを計測する
    protocol decoderをcoverage-guided fuzzingする

Go moduleはSQLite driverにCGo-free `modernc.org/sqlite`、M3 Parquetに`github.com/parquet-go/parquet-go`、publisher claimにAWS SDK for Go v2を候補とし、実装時にexact versionを`go.mod`/`go.sum`へpinする。R2 data transferはpinしたrcloneを使う。Windows Service integrationにはGoのWindows service packageを使用できる。binaryはWindows amd64を必須とし、Linux amd64/arm64のreader/verifier/API buildも生成する。

Python 3.12は独立fixture decoder、property-based test、DuckDB consumerへ使う。RustはMQL built-in TCP benchmark失敗時のthin DLLまたはhard latency/shared-memory要件が明示された場合だけ追加する。C#は組織の.NET運用がGoより支配的になった場合の代替であり、現設計上の技術的必須ではない。

`domain`、`smartcloser`、`runtime`、`simulation`、`experiments`のいずれもcapture implementationをimportしてはならない。MQL producerはtrading includeやorder callを含まず、Go gatewayは既存のorder-capable MT5 adapterをimportもinvokeもしない。

改訂記録（2026-07-14）: 自己運用のraw Tick収集、Cloudflare R2 bridge、source-neutral archiveを目的として初版を作成した。

改訂記録（2026-07-14）: `CopyTicks`同期error、hash domain、campaign namespace、Parquet determinism、publisher claim、read-only verifier、M0〜M4を追加した。

改訂記録（2026-07-14）: リポジトリの関心範囲を収集から配信IFまでへ拡張し、作業名を`tick-data-platform`とした。`ArchiveReader`、`tickctl`、`tick-verify`、`tick-api`を追加した。

改訂記録（2026-07-14）: MQL–Go間のFILE IFを廃止し、localhost TCPとGo WALによるdurable application ACKへ置換した。MQLの`.sealed/.ready/.commit`、two-slot state、crash fragment、file suffix state machineを削除した。

改訂記録（2026-07-14）: authoritative pathではUDPを採用せず、v1はMQL built-in TCPを使うことを決定した。Rust DLLは実測後のthin extension pointに限定した。

改訂記録（2026-07-14）: gateway本体の実装言語をGoに確定した。bounded allocation、WAL、TCP、HTTP、R2、fuzz testをGoへ集中し、Pythonを独立検証、Rustをoptional native adapterへ限定した。

改訂記録（2026-07-14）: raw-day manifestとreplay-day manifestを分離し、day definition、replay contract、conversion IDをnamespaceへ追加した。day verifierはchain slice、campaign verifierはcomplete chainを検証する契約へ修正した。

改訂記録（2026-07-15）: M0の契約実装、fixture検証、`mise run check`の成功、およびcommit `c690710`の`origin/main`へのpushをProgressと成果と振り返りへ反映した。

改訂記録（2026-07-15）: M1のGo Gateway、WAL、SQLite journal、fake producer TCP integration、MQL5 Service compile、roadmap、検証記録を反映した。

改訂記録（2026-07-15）: M1のPull Request #1 mergeと、M2のローカルWAL seal、segment recovery、sealed verifier、content-addressed outbox promoteを反映した。この時点ではM2のR2 publicationとraw deliveryは未完了だった。

改訂記録（2026-07-15）: M2のローカル基盤に対するrepository gateとWindows Race Detectorの成功を反映した。

改訂記録（2026-07-15）: Pull Request #2のreviewを受け、sealed trailer検出をentry-boundary parseへ修正した。

改訂記録（2026-07-15）: M2R-1からM2R-4、fake end-to-end、repository check、Windows Race Detector、PR #3のcheck/race成功、merge-ready状態を反映した。実R2 smokeは隔離credential不在のためoptionalな未実施境界として記録した。

改訂記録（2026-07-15）: PR #3のreview指摘を妥当と評価し、R2 prefixへのdataset identity追加、scope-specific ProtocolLimits verification、fake producer reconnect retryを実装して回帰検証した。

改訂記録（2026-07-15）: PR #3のmerge commit `cc9fc2d`をM2 baselineとして記録し、M3-0 ExecPlan、M3-1 implementation gate、pinned Parquet dependency selection、derivative-specific publication、read-only replay deliveryの方針を追加した。

改訂記録（2026-07-15）: thread-aware review readで2件の未解決P2 threadを確認した。指摘内容は修正済みであり、返信とresolveは明示依頼がないため実施していない。

改訂記録（2026-07-15 M3-3A-R0-DOCS）: 旧stage-based replay publicationを受入対象から外し、sealed bundle、fresh bounded observation、pure reconciler、approved action executor、append-only event、final observation receiptへ設計を置き換えた。

M3-3Aの再実装手順は`execplan/2026-07-15-m3-3a-publication-redesign.md`のR1からR7で規定し、R7完了までM3-4をblockedとした。

改訂記録（2026-07-15 G0A-DOC-ALIGN）: branchとfull HEAD、tracked modified 24、untracked 26、staged 0、両diff check成功を記録し、全dirty差分を保持、境界縮小再利用、useful-case移行後の置換削除へ分類した。

G0Aは四正本文書だけを更新し、旧52件または86件を含むM3-3A test countを新設計の受入証拠へ流用しなかった。

改訂記録（2026-07-15 G3-R3）: replay専用bounded read interface、aggregate observation budget、fresh observer、part／revision graph verifier、Exact-only final observation変換、negative test、focused／full `internal/r2` testの成功を同期した。

改訂記録（2026-07-15 G4-R4）: ObjectID-only narrow executor、verified local snapshot、rclone二操作allow-list、canonical diagnostic event、append-only in-memory store、negative test、focused／full `internal/r2` testの成功を同期した。

改訂記録（2026-07-15 G3E-R7-BOUNDED-ACTUAL-BYTES-CLASSIFICATION）: R7 changes_requiredを受け、expected-size事前課金をactual-byte事後課金へ置換し、terminal二回目readのtyped classificationとbounded Parquet streamを実装した。

Event方針をstore-local validationとpublisher-level append failure非vetoへ統一し、現行repository／Race証拠とR7再監査待ち、M3-4 blockedを同期した。

改訂記録（2026-07-15 G3F-FINAL-UPLIFT-LIST-DESCRIPTOR）: R7二回目監査のchanges_requiredを受け、final observation builderによる未課金counter補正を削除した。

Final observationに必要なcanonical bytesと最終passの実課金bytesとの差分だけを共有`ReplayObservationBudget`へ一度課金し、上限超過を`Oversized`としてfinal digest、action、receiptなしで停止するよう修正した。

Parquet観測ではlist descriptor、bundle expected bytes、Open advertised size、stream actual bytesの一致を要求し、staleまたは誤ったlist sizeを`Ambiguous`として停止するよう修正した。

Focused test、未キャッシュ`internal/r2`全test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功した。

旧M3-3A test countはG3Fの受入証拠へ流用せず、R7再監査の明示的なpassまでM3-4をblockedに保つ。

改訂記録（2026-07-15 G7A-R7-DOC-UNBLOCK）: Advisorの`r7_m3_3a_third_audit`はverdict `pass`を返した。

監査scopeはR1のcanonical contract、R2のbundle sealerとpure reconciler、R3のbounded observerとgraph verification、R4のnarrow executorとdiagnostic event、R5のthin publisherとreceipt、R6のrepository gate、およびG3F remediationとM3-4未着手境界である。

監査証拠はfixture 23件、Python 34件、全Go、未キャッシュ`internal/r2`、Protocol／Archive、vet、空のgofmt、両diff check、CGOとGCC 16.1.0によるcurrent-diff local Raceの成功、およびbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 26、untracked 43、staged 0である。

監査はlocal gateでGitHub CIとoptionalなReal R2を必須にしないこと、G3EとG3Fで受入証拠を現行差分へ更新して旧52件または86件のtest countを流用しないこと、およびG7Aがdocs-onlyであることをassumptionとした。

Known unresolvedはGitHub CI未確認、Real R2 optional skip、M3全体final audit未実施であり、R7 passとM3全体final passを同一視しない。

M3-3A R1からR7はcompletedとし、M3-4を明示的にunblockする。

次taskはG8のM3-4 read-only deliveryであり、G7Aでは実装を開始しない。

改訂記録（2026-07-15 G8-M3-4-READ-ONLY-REPLAY-UX）: 既存`ArchiveReaderV1`のraw APIを変更せず、`ListReplaySnapshots`、`ResolveReplaySnapshot`、`BuildReplayFetchPlan`、`FetchReplay`、`VerifyReplayDay`を追加した。

Replay selectorはdataset、campaign、date、任意day definition、stream、conversion、任意revision、immutable manifest keyまたはdomain hashを受け、single complete revision graphのterminalだけを暗黙選択する。

Branch、duplicate、missing predecessor、ambiguous conversion、raw binding mismatchを拒否し、list順からwinnerを推測せず、resolved descriptorへimmutable manifest keyとhashを固定する。

Fetch planはstrict canonical replay manifestとordered part manifestからtrusted `r2.Layout`でremote keyを導出し、manifest、part manifest、Parquetのdigest、bytes、hash-derived cache pathを固定する。

Fetchは`r2.ReadBackend`だけを使い、bounded download、remote close、size／hash検証、same-directory temporary、no-clobber promotionの順に処理し、正しいcacheだけを再利用してcorrupt cacheを上書きしない。

Replay day verificationはraw bindingと既存raw day semantics、part chain、part_set_root、Parquet schema／rows／file hash、canonical row-chain rootを分離したreportで返し、`replay_anchored_day`をcampaign genesis verificationと表現しない。

Focused delivery／CLI、archive／Protocol／Parquet、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、およびdelivery／CLIのlocal Raceは成功した。

G8の受入証拠へ旧52件または86件のtest countを流用せず、M3-3A R7 receiptを変更または無効化していない。

改訂記録（2026-07-15 G9-M3-5-NETWORK-FREE-E2E-FULL-GATES）: `TestM3ReplayDeliveryNetworkFreeEndToEnd`でfake Batchからsealed WAL、raw-day manifest、M2 claim／raw publication、verified replay source、continuity reducer、Parquet、part／replay manifest、M3 bundle／publisher／receipt、empty-cache readerまでを一つのnetwork-free graphへ接続した。

Testはraw binding、row-chain root、part-set root、receipt digest、M2 claim、approved object ID順の`copyto --immutable`／`check --download`、diagnostic event失敗の非veto、M2／M3 same-content retry、hash-derived cache、cache reuse、reader remote write 0を検証した。

Fake backend、fake action tool、temporary fileだけを使用し、network、AWS credential、real rclone、SQLite、wall clockへ依存していない。

Cross-language root証拠は既存`replay-v1-conformance.json`をPython verifierとPython unit testが独立再計算する現行fixtureへ固定し、旧52件または86件のtest countをG9の証拠へ流用していない。

Focused E2E、関連10 package、fixture 23件、Python 34件、`mise run check`、`go vet ./...`、空のgofmt、指定8 packageのlocal Windows Race Detectorは成功した。

Branchは`agent/m3-replay-parquet-delivery`、HEADは`cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`のままで、G9文書同期前inventoryはtracked modified 32、untracked 48、staged 0だった。

Real R2はcredentialと明示確認がないためoptional skipとし、GitHub CIとremote I/Oは未確認である。

M3-5はcompletedだが、M3全体final auditは未実施であり、M3全体をcompletedまたはfinal audit passとはしない。

改訂記録（2026-07-16 G9E-PRODUCTION-M2-PUBLISHER-E2E）: M3全体final auditは、G9のM2受入経路がproduction `r2.NewPublisher`と`Publisher.Publish`を実行せず、`publishM2CanonicalForE2E`でscope descriptor、claim、raw object、raw-day manifestを直接投入していたためchanges_requiredとなった。

G9Eは直接投入helperを削除し、temporary `PublicationJournal`をproduction M2 APIの必須local境界として使用して、同じfake backendへscope descriptor、conditional claim、raw object、raw-day manifestを公開した。

このSQLite journalはM2 publisherの再開状態だけに使用し、M2 publishとsame-content retry後にcloseしてから、M3 sealer、thin publisher、receipt、read-only selector、fetch、verificationを実行した。

したがって、M3 publicationとread-only deliveryはjournal、SQLite、M2 stageをaction authorityまたは依存として使用しない。

Network-free fake rcloneは`version`、`copyto --immutable`、`check --download`だけを受理し、trusted rclone campaign prefixをimmutable campaign prefixへ写像して同じbackendへexact bytesを反映し、任意operationとflag差し替えを拒否する。

Production M2 receiptのclaim hash、scope hash、manifest keyとdomain digest、raw object hashとbytes、tool identityをremote canonical bytesへ照合し、初回conditional claim write一件とscope／raw／manifestの三immutable mutationを確認した。

同じproduction inputのretryはreceipt canonical identityを維持し、conditional claimの成功write、remote mutation、immutable copy mutationを追加しなかった。

M2 receiptとremote raw manifestのkey／domain digest／claim relationをM3 bundle、M3 final receipt、resolved replay manifest、day verification reportまで照合したため、raw truthからread-only deliveryまでの受入経路は一つのidentity graphである。

Focused M3 E2E、既存M2 E2E、関連10 package、fixture 23件、Python 34件、`mise run check`、`mise exec -- go vet ./...`、空の`mise exec -- gofmt -l cmd internal`、両diff checkは成功した。

WinLibs POSIX／UCRT GCC 16.1.0、Go 1.24.13 windows/amd64、CGO_ENABLED=1、CC=gccで指定8 packageのlocal Windows Race Detectorも成功した。

G9E文書同期前のinventoryはbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 32、untracked 48、staged 0である。

Real R2、GitHub CI、HTTP、commit、push、mergeは未実施であり、旧52件または86件のM3-3A test countはG9Eの受入証拠へ流用していない。

G9Eは完了したが、M3全体の`final_audit: pass`はadvisor re-auditまでpendingであり、ここではM3全体をcompletedと宣言しない。

改訂記録（2026-07-16 FINAL-DOCS-M3-COMPLETE）: Advisorのwhole-M3再監査receiptはphase `final_m3_whole_reaudit`、verdict `pass`、required actionsなしである。

監査scopeはG0のbranch／HEAD／dirty保全、Protocol V1、M3-1／M3-2G、M3-3A R1からR7、M3-4 read-only delivery、G9E production M2 publisher E2E、および四正本文書の同期である。

M3-3A R7監査はpublication再設計だけの独立gateであり、今回のwhole-M3 final auditとは同一視しない。

監査evidenceはproduction `r2.NewPublisher`／`Publisher.Publish`からM3 bundle／receipt／read-only verificationまでの単一identity graph、旧direct M2 injection helper 0、focused M3／M2 E2E、関連10 package、fixture 23件、Python 34件、repository check、vet、空のgofmt、両diff check、および指定8 packageのlocal Windows Race Detector passである。

監査assumptionsはReal R2をcredentialと明示確認がないためoptional skipとすること、GitHub CIをlocal gateの必須条件にしないこと、およびtemporary M2 `PublicationJournal`をM3 publication／delivery authorityへ流用しないことである。

Known unresolvedはReal R2 optional skipとGitHub CI未確認であり、HTTP、live broker、commit、push、mergeはM3のscope外である。

Local Windows Race passはGitHub CI passを意味せず、旧52件または86件のM3-3A test countはwhole-M3 final evidenceへ流用していない。

M3全体はcompletedであり、`final_audit: pass`とする。

改訂記録（2026-07-16 M3 review findings remediation）: revision 3以降のsealerをimmediate predecessor successor検証とrevision数に対する保守的なfinal-observation pre-lock budgetへ修正し、remote complete graphをrevision 1からの受入根拠として維持した。

publication round上限を`2*MaxParts+2=20002`へ拡張し、Go／Pythonのbundle validationが実part数に対する不足roundをlock前に拒否するようにした。

final observationのpre-lock request budgetはraw、derivative、revision graph edgeの各readを含めて検査する。

Replay publisherは任意lock filenameを受け付けず、lock rootとtrusted Layoutのcampaign／publisher epochからcanonical lock pathを導出する。

revision 3 successor、4-part fresh observation、round不足のlock前拒否、canonical lock pathのfocused Go test、Python negative、fixture、repository gateを再確認した。

`mise run test-python`は35 passed、`mise run fixture`は23 verified、`mise run check`、`mise exec -- go vet ./...`、指定8 packageのlocal Windows Race Detector、`git diff --check`、`git diff --cached --check`は成功した。

再確認後もbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 32、untracked 48、staged 0を維持した。

Implementerのread-only R7 remediation auditは3件すべてをpassとし、remote I/O、GitHub CI、Real R2を未実施のlocal scope内で残存riskなしと判定した。
