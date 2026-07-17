# ExecPlan: Gateway常駐R2 publicationとCredential Provider／Uber Fx統合

このExecPlanは生きた文書である。
実装と検証が進むたびに、`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective`を更新する。
リポジトリにはExecPlan方法論の`PLANS.md`がないため、この文書自体に再開に必要な前提、実装順、停止条件、検証方法、review gateを保持する。

この計画は、`.agent/tick-gateway-provider-fx-refactor-spec.md`を基礎にする。
ただし同仕様の「R2 uploader／verifierの新規実装は対象外」という範囲は、2026-07-16のユーザー判断により上書きする。
理由は、現在のproduction `tick-gateway run`にWALからR2までの常駐経路がなく、ProviderとFxだけを導入してもGatewayの主要機能が欠落したままになるためである。

`.agent/tick-gateway-r2-windows-spec.md`は保留を継続する。
DPAPI、Windows Service installer、専用ACL provisioning、service accountの配備手順は本計画へ含めない。
ただしFileProviderのWindows ACL検証とWindows CIは、Provider/Fx仕様の受入条件として実施する。

`execplan/2026-07-16-r2-sdk-publication-boundary.md`は失効済みの履歴文書とする。
同文書で確定したAWS SDK for Go v2、条件付きPUT、全量Get検証、`remote_verified`状態の決定は本計画へ引き継ぐ。

## Purpose / Big Picture

最終状態では、`tick-gateway run`が受信データをlocal WALへdurably記録した後、同じ常駐プロセス内の非同期workerがデータをR2へ送る。
R2が一時的に利用できなくても、受信済みデータはlocal WALとraw outboxへ残り、再起動後に自動再開する。
R2から全量を読み戻してSHA-256とsizeが一致した事実をdurably記録するまで、local dataを削除可能と扱わない。

目標経路は次のとおりである。

```text
MQL / Rust DLL
  -> ingest listener
  -> active local WAL
  -> automatic WAL seal
  -> verified sealed WAL
  -> content-addressed raw outbox promotion
  -> provisional raw-day manifest
  -> AWS SDK for Go v2 PutObject
  -> R2 GetObject full-byte verification
  -> durable remote_verified state
  -> proof-gated prune-local
```

Uber Fxはcomposition rootとして、設定、Credential Provider、local storage、publication worker、ingest listenerの組立てと開始停止順序だけを管理する。
`internal/wal`、`internal/archive`、`internal/r2`、`internal/ingest`のドメインロジックへ`fx.In`、`fx.Out`、`fx.Lifecycle`を持ち込まない。

受信ACKの意味は変更しない。
ACKはlocal WALとjournalの既存durability条件に基づき、R2 upload完了を同期的に待たない。
R2障害時はlocal backlogを保持し、既存disk pressure policyがcriticalに達した場合だけ新規ACKを停止する。

## Progress

- [x] (2026-07-16) Provider/Fx仕様を全文確認した。
- [x] (2026-07-16) R2 Windows仕様を本計画では保留すると確認した。
- [x] (2026-07-16) dirty working treeを確認し、R2 SDK移行差分が未commitであることを記録した。
- [x] (2026-07-16) `mise run check`を実行し、Go test、Python test 35件、Protocol fixture 24件、Ruff、gofmt、`git diff --check`が成功した。
- [x] (2026-07-16) production非testコードに`wal.Store.Seal()`、`archive.PromoteSealedSegment()`、`archive.BuildRawDayManifest()`、`r2.NewPublisher()`、`Publisher.Publish()`の呼出しがないことを確認した。
- [x] (2026-07-16) RClone削除前の`tick-gateway run`にもpublication呼出しがなく、RClone削除が常駐workerを消したのではなく、常駐worker自体が未実装だったことを確認した。
- [x] (2026-07-16) 旧R2 SDK ExecPlanを失効扱いとし、本計画をR2 publication completionの正本にした。
- [x] (2026-07-16) 初回アーキテクトレビューで、`ingest.Open`による依存隠蔽、Credential所有権の重複、local catalogとR2 journalの混在、status責務、config loader、Provider security/test条件の不足を確認した。
- [x] (2026-07-16) 初回レビューの妥当な指摘を反映し、production graphの明示的constructor、module所有権、local/remote state分離、application status集約、strict typed configを計画へ追加した。
- [x] (2026-07-16) 修正後に`mise run check`、`git diff --check`、未追跡ExecPlanの末尾空白検査が成功した。
- [x] (2026-07-16) G0シニア／アーキテクトレビューを実施し、既存`Open`の副作用、R2 Journalの既存履歴schema、Publisherの時刻取得、flat configからtyped configへの変換境界を実装前に確認した。
- [x] (2026-07-16) G0レビュー後、legacy `Open` wrapper、R2 Journal互換境界、Catalogのlocal authority、clock注入、config変換境界を確定した。
- [x] (2026-07-16) G1 Credential Provider、strict typed config、credential-bound R2 backendを実装し、Linux unit testとWindows cross-compileを通した。
- [x] (2026-07-16) G1実装後、別モデルのワーカー（GPT-5.5、reasoning effort xhigh）でG1限定の独立レビューを再実施した。reader／retentionのFileProvider移行、Linux 0400／0600と親directory検証、単一config変換、typed credential errorの`errors.Is`／`errors.As`を確認し、G1判定はPASSとなった。
- [x] (2026-07-16) G2 Fx composition rootとbounded Lifecycleを実装し、production `run`をFx appへ切り替えた。内部レビューの指摘を修正して通常テストを通した。raceはこの環境にC compilerがないため未実行で、CI検証へ持ち越す。
- [x] (2026-07-16) G2実装後の内部シニア／アーキテクトレビューを実施した。`NewProductionApp`が構築エラーを隠さないこと、内部GatewayエラーでFxの停止通知をCLIが受け取ること、Start contextの寿命を常駐accept loopへ持ち込まないことを確認し、該当箇所を修正した。独立ワーカーによるフェーズゲートレビューは後日別途実施する。
- [x] (2026-07-16) G2レビュー後、`go test ./internal/ingest ./internal/app ./cmd/tick-gateway`を再実行して成功した。`go test -race`はC compiler不足で実行できなかった。
- [x] (2026-07-16) G3 automatic seal、raw promotion、day manifest planningを実装し、Fx常駐Lifecycleへ接続した。sealed WAL、content-addressed raw outbox、canonical provisional manifest、local Catalogの再構築をnetwork-free testで確認した。
- [x] (2026-07-16) G3実装後の内部シニア／アーキテクトレビューを実施した。manifest専用rootの明示化、manifest catalogのfilesystem再構築、revision chainの欠落／先祖不一致検出、local worker errorのFx停止通知、Windows directory sync差異を妥当な指摘として修正した。独立ワーカーによるフェーズゲートレビューは後日別途実施する。
- [x] (2026-07-16) G3レビュー後、`go test ./internal/config ./internal/publication ./internal/app ./cmd/tick-gateway`を再実行して成功した。
- [x] (2026-07-16) G1〜G4を対象に、別モデルのワーカー（GPT-5.5、reasoning effort xhigh）による独立レビューを実施した。判定はG1=FAIL、G2=CONDITIONAL PASS、G3=FAIL、G4=FAILであり、従来の内部レビューだけで完了扱いにしていた記録を撤回した。
- [x] G1独立レビューの指摘を修正し、reader／retentionの環境変数credential経路、Providerテスト網羅、OS別ACL検証を完了した。Windows ACL実行証拠はWindows CIで取得する。
- [x] (2026-07-16) G2独立レビューを別モデルのワーカー（GPT-5.5、reasoning effort xhigh）で再実施した。Start／Stop、rollback、timeout、listener終了、worker error、goroutine leakの直接保証と、Linux／Windows race workflowへの`internal/app`追加を確認し、重大な不足なしのCONDITIONAL PASSとなった。
- [x] (2026-07-17) G2 gateの最終証跡を取得した。PR #5のcommit `28fda13`でLinux／Windows race各2件とRepository check各2件がすべてpassした。ローカルはC compiler不在のため`go test -race`を実行できないが、CIでproduction graph、Lifecycle、ingest integration、race対象testを検証済みである。
- [x] (2026-07-17) G3独立レビューの指摘を修正した。graceful stopのforce seal、Fxからのpublication clock／ticker注入、TCP fake ingestからGateway、WAL、raw promotion、canonical provisional manifestまでのnetwork-free integration testを追加した。
- [x] (2026-07-17) G3修正後に別モデルのワーカー（GPT-5.5、reasoning effort xhigh）でG3限定の再レビューを実施し、G3判定PASSとなった。
- [x] (2026-07-17) G4 SDK uploader、全量verification、durable retryを常駐workerへ接続した。AWS SDK for Go v2を実HTTP transportのfakeへ接続し、Fx相当のfake producer→Gateway→WAL→publication→remote verification経路を検証した。
- [x] (2026-07-17) G4独立レビューの指摘を修正した。remote intentの再起動復元、local Catalog／remote PublicationJournalの責務分離、PUT成功後の応答消失、412同一／衝突、backoff再起動、duplicate wake-up、秘密値非漏洩を追加検証し、GPT-5.5 xhighワーカーのG4限定再レビューでPASSを得た。G4修正後の`mise run check`も成功した。
- [x] (2026-07-17) G5の基盤を実装した。Catalogからpending segment／manifest bytesを再計算し、共有DiskStateMachineへ反映するbackpressure、versioned `operations.StatusService`、`remote_verified`必須のplanner／executor、`prune-local`の`local_pruned`記録を接続した。focused testと`mise run check`は成功した。
- [x] (2026-07-17) G5限定のGPT-5.5 xhigh独立レビューを実施した。判定はFAILで、worker priorityの未実効化、`prune-local` truth table／CLI統合テスト不足、unlink後から`local_pruned`記録前の再開証跡不足、backlogからGateway ACK停止までの統合テスト不足が指摘された。G5完了扱いにはしない。
- [x] (2026-07-17) G5レビュー指摘のうちworker priorityを実効化した。DiskStateMachineのpriority wake-upをLocalPipeline／Uploaderへbroadcastし、backlog閾値がACK停止へ伝播するGateway integration testと、due retryがpriority wake-upで即時drainされるUploader testを追加した。
- [x] (2026-07-17) Catalogのpending manifestをLocalPipelineのPendingSinkからDiskStateMachineへ渡し、Gatewayの実接続でACK停止に到達するbackpressure integration testを追加した。
- [x] (2026-07-17) `prune-local` truth tableを実際のPublicationJournalのintent／receipt／ETag／`remote_verified`状態で検証するテストへ置き換えた。raw outboxについては実sealed WALをexecutorでunlinkし、completion inventory、Journal再起動、remote identity再確認、`local_pruned`記録まで通すcrash-retry testを追加した。
- [x] (2026-07-17) Catalogのpending segment／manifest／retry統計がSQLite再起動後も一致することを追加検証した。focused G5 testsは現時点で成功している。
- [x] (2026-07-17) GPT-5.5 xhigh G5レビューの追加指摘を修正した。`status` commandをversioned `operations.StatusService`へ接続し、R2 network／credentialなしでlocal catalogとremote verification ledgerを表示できるようにした。`local_pruned`はCatalogだけがruntime authorityを持ち、R2 Journalは`remote_verified`までに限定した。pruned segmentをpublication reconciliationが再度raw fileとして読まないrestart testも追加した。
- [x] (2026-07-17) 最終G5レビューの指摘をさらに修正した。`runPruneLocal`のretention config、inventory、remote binding、dry-run JSONまでを実行するcommand-level testを追加し、sealed WALが残った再起動でも`local_pruned`済みraw outboxを再promotionしないようにした。
- [x] (2026-07-17) G5最終ゲートをGPT-5.5 xhigh workerで再レビューし、worker priority、R2 outage recovery、status CLI、Catalog `local_pruned`、`runPruneLocal` command path、sealed-WAL restart境界を含めてPASS（actionable findingsなし）となった。
- [x] (2026-07-17) G5完了後の実装をcommit `f12c28b`、ExecPlan更新を`eaf2a26`としてPR #5へpushした。`eaf2a26`のLinux race 2件、Windows race 2件、Repository check 2件はすべてpassした。ローカルの通常check、vet、buildも成功している。
- [x] (2026-07-17) 未リリースのGateway設定から旧`outbox_root`、`r2_bucket_env`、`r2_prefix`の互換aliasとfallbackを削除し、`raw_outbox_root`と`[credentials]`／`[r2]`／`[publication]`だけを受け付ける新形式へ整理した。旧形式を拒否するconfig testと新形式のrun／smoke exampleを更新した。
- [ ] G6 Linux／Windows CIとreal R2 smokeでend-to-end経路を検証する。

## Baseline

### Current entrypoints

`cmd/tick-gateway/main.go`は`init`、`run`、`status`、`reconcile`、`verify-local`、`prune-local`を手動で分岐する。
`run`は`ingest.Open(config)`の後に`gateway.ListenAndServe(ctx)`を同期実行する。
`init`、`status`系commandは`ingest.Open`と`Close`を直接呼ぶ。
`prune-local`は別のretention configを読み、read-only R2 backendを手動で構築する。

### Current local data path

`internal/ingest.Gateway`はWAL、ingest journal、listener、connection handler、disk stateを所有する。
受信batchはlocal WALとjournalへ記録される。
`wal.Store.Seal`はthread-safeで、空segmentを`ErrEmptySegment`として拒否し、durable trailerを書いて次のactive segmentを作れる。
しかしproduction codeは`Seal`を呼ばない。

`archive.PromoteSealedSegment`はverified sealed WALをcontent-addressed raw outboxへno-clobberで確定できる。
`archive.BuildRawDayManifest`はverified raw objectからUTC dayのselected rangeを再導出し、revision chainを構築できる。
しかしproduction codeはpromotionもmanifest buildも呼ばない。

### Current remote data path

`internal/r2.S3Backend`はAWS SDK for Go v2を使う。
working tree上の実装は`PutObject`へ`IfNoneMatch: "*"`と`Content-MD5`を設定し、412または結果不明時に既存objectを全量検証する。
`Publisher`はraw object、scope descriptor、manifestをremote PublicationJournal付きで再開可能に送信できる。
しかしproduction commandまたはbackground workerから`Publisher.Publish`を呼ぶ経路がない。

### Current credential access

`internal/r2.NewS3Backend`はAWS default credential chainを使用できる。
`NewS3BackendWithEnv`と`NewS3ReadBackend`は指定された環境変数名からAccess Key IDとSecret Access Keyを直接読む。
`internal/delivery`と`cmd/tick-gateway prune-local`はこの環境変数方式をproduction pathで使用する。
Credential Provider抽象と資格情報file bundleは存在しない。

### Current lifecycle

`ingest.Open`はconfig validation、WAL recovery、WAL open、wall-clock publish、journal open、disk state作成、journal reconciliationを同期実行する。
`ListenAndServe`はlistenerを開いてaccept loopをblockする。
`Close`はlistenerとconnectionを閉じ、handler完了を無期限に待ってからWALとjournalを閉じる。
起動途中のrollback、bounded Stop、worker開始停止順序をapplication単位で検証する仕組みはない。

### Current tests and CI

ドメインunit testとnetwork-free fake backend testは存在する。
手動で`Open`、`Serve`、`Close`するintegration testは存在するが、Fx graph testとLifecycle rollback testはない。
GitHub ActionsにはUbuntuのrepository check、Linux race、Windows race workflowがある。
Provider/Fx仕様が要求するUbuntu／Windows双方の`go test ./...`、`go vet ./...`、`go build ./...`を同じmatrixで確認するworkflowはない。

### Known failures

2026-07-16の`mise run check`に既知失敗はない。
real R2、Windows ACL、Windows build、Linux raceは本計画作成時には再実行していない。
未実行を成功として扱わない。

## Surprises & Discoveries

- 観察: RClone削除前のPublisherはRCloneでobjectを転送したが、production `tick-gateway run`はそのPublisherを生成も実行もしていなかった。
  判断: SDK置換は転送境界を改善したが、常駐publication機能の欠落を新たに作ったわけではない。
  判断: 過去の「publication実装済み」という表現はlibraryとtest seamの完成を指しており、production runtimeの完成を証明していなかった。
- 観察: production codeにはautomatic seal、raw promotion、manifest buildの呼出しもない。
  判断: uploaderだけを追加すると入力となるsealed artifactが生成されないため、WALからR2までを一つのend-to-end scopeとして扱う必要がある。
- 観察: `PublicationJournal`はremote intent再開とobject verification stateを保持するが、未処理sealed segmentの列挙、day revision planning、retry時刻の永続化を提供しない。
  判断: remote publication ledgerを`internal/r2`へ維持し、local segment、day、retry coordinationは`internal/publication.Catalog`へ分離する。
  判断: filesystem inventoryをlocal truthとし、local catalogはfilesystemから再構築可能にする。
- 観察: raw-day manifestは一つのsealed segmentが複数UTC dayを含む場合と、late dataで過去dayへ新しいrevisionが必要な場合を既に表現できる。
  判断: workerは「segment一つにつきmanifest一つ」と仮定せず、各segmentからaffected UTC dateを導出する。
- 観察: `settle_policy`は`manual-v1`であり、terminal synchronizationの完了をGateway単独では証明できない。
  判断: 常駐workerは`provisional` manifestだけを自動発行し、`settled_snapshot`を時刻経過だけで作らない。
- 観察: Gateway config loaderはflatな独自`key=value` parserで、Provider仕様の`[credentials]` tableを読めない。
  判断: 既存依存の`pelletier/go-toml/v2`を使うstrict typed root configを`internal/config`へ置き、独自parserへTOML機能を継ぎ足さない。
  判断: 既存top-level keyはtyped wire structで読み、旧credential env fieldだけを明示的なmigration errorにする。
- 観察: exportedな`credentials.Credentials`はGo標準の`%+v`で値が表示され得る一方、仕様は`String`と`GoString`を禁止し、公開型のformatからsecretが漏れないことを要求する。
  判断: `String`／`GoString`は実装せず、必要ならredacted `fmt.Formatter`を実装してformat verbにかかわらず値を出さない。
- 観察: 現在の`wal.OpenWithAnchor`、`journal.Open`、`r2.OpenPublicationJournal`、`ingest.Open`はfile、SQLite、recoveryをconstructor内で実行する。
  判断: 新しいproduction graphは副作用のない`New...`と`Start`／`Stop`を正本とし、既存`Open`は同じ境界を手動合成するcompatibility wrapperへ限定する。
- 観察: 既存`internal/r2.PublicationJournal`の履歴schemaには`local_path`と`sealed_local`等の状態があり、既存Publisher testと再開契約がこれを使用する。
  判断: 既存schemaをこの計画で破壊的に削除しない。
  新しいruntimeのretry、pending列挙、manifest planning、`local_pruned`判定は`internal/publication.Catalog`だけを正本とし、R2 Journalの既存local locatorはimmutableなintent／verification bindingの互換履歴として扱う。
- 観察: 既存`r2.Publisher`はremote verification時刻を`time.Now()`から取得する。
  判断: production workerではclockを注入し、既存`NewPublisher`はsystem clockを使う互換wrapperとして残す。
- 観察: AWS SDKの`config.LoadDefaultConfig`は、静的Providerを後から設定しても周辺環境を探索する。
  判断: credential-bound production backendは`aws.Config`を直接構築し、AWS credential環境変数と共有profileへ依存しない。
- 観察: Fxの`Shutdowner`が内部Gatewayエラーを通知しても、CLIがOS signalだけを待つと常駐プロセスが停止処理へ移行しない。また、FxのOnStart contextをそのままaccept loopの寿命に使うと、startup用contextのキャンセルがruntime停止を意図せず引き起こし得る。
  判断: production CLIはFxの`Done`通知とOS signalの両方を停止条件とし、GatewayはStart後に自身のruntime contextを所有してStopでcancelする。開始時のcontextはrecovery／bindの期限だけに使う。
- 観察: manifestを`receipt_root`の下へ暗黙配置すると、receiptとcanonical manifestの所有境界が曖昧になり、設定を失ったときにmanifest revisionを再構成できない。
  判断: `manifest_root`を独立した必須設定にし、complete canonical manifestからCatalogを再構築する。再構築時には各dateのrevisionが1から連続し、各revisionが直前digestを参照することも検証する。
- 観察: local publication workerのエラーはGatewayのaccept loopとは別のruntime failureである。Gatewayだけを監視すると、worker停止後も受信を続けてlocal backlogを増やし得る。
  判断: composition rootのfatal monitorはGatewayとlocal publication pipelineの両方を監視し、integrity failureをFx停止へ伝播する。
- 観察: G4実装後も、Catalogのpending量をGatewayの既存disk policyへ渡す境界が存在しなかった。
  判断: Catalogがpending segment／manifest bytesを毎回再計算し、DiskStateMachineだけがACK可否とworker priorityを判定する。in-memory通知はwake-upに限定し、再起動後もSQLiteから同じ判定を再現する。
- 観察: `remote_verified`をretention plannerへ渡すだけでは、実際の削除完了とremote journalの状態遷移が別々に進み得る。
  判断: planner／executorは`remote_verified`を必須条件として再検証し、raw outboxのunlink完了後だけCatalogの同一segmentを`local_pruned`へ進める。R2 Journalはremote verificationの正本に限定し、local lifecycleを重複保持しない。
- 観察: G5初回実装ではDiskStateMachineが`WorkerPriority`を表示するだけで、LocalPipeline／Uploaderの実行周期やwake-upは変わらなかった。
  判断: priorityは状態表示ではなく、publication workerがread-only decisionを読んでdrain cycleを短縮／即時起動する実効的な制御として実装する。proof条件は変えない。
- 観察: `prune-local`の各部品テストだけでは、remote observation、receipt、ETag、journal存在の誤った組合せを削除成功と扱わないことをCLI経路で証明できない。
  判断: prune integration testでtruth tableを固定し、executor完了後にjournal記録前で落ちる境界を注入して、次回実行がcompletion inventoryから`local_pruned`へ収束することを検証する。
- 観察: raw outboxを削除してもsealed WALがretention対象外として残る場合があり、再起動時のsealed inventory再走査が同じraw objectを再生成し得る。
  判断: Catalogで`local_pruned`と確認したsegmentはpromotion対象から除外し、削除済みlocal lifecycleをreconciliationが巻き戻さない。

## Decision Log

- Decision: Provider/Fxとproduction R2 publicationを一つのExecPlanで管理し、実装gateは分離する。
  Rationale: composition rootだけ先に完成扱いにすると、再びR2へ届かない状態を完了と誤認できるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `execplan/2026-07-16-r2-sdk-publication-boundary.md`は削除せず失効表示を付け、監査履歴として保持する。
  Rationale: 当時のSDK判断と検証結果は有効だが、production completion authorityとしては不十分だからである。
  Date/Author: 2026-07-16 / Codex
- Decision: production `tick-gateway run`ではR2 publication configとFileProviderを必須にする。
  Rationale: upload不能なGatewayを正常稼働と表示するとlocal dataが無制限に増え、運用者が欠落を認識できないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `init`とlocal `status`はremote availabilityを要求しない。
  Rationale: R2障害中でもlocal recoveryと状態確認を可能にし、remote outageをlocal dataへのアクセス不能へ拡大しないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: production credentialは単一JSON file bundleだけから読み、AWS default chainと平文secret環境変数をproduction wiringから外す。
  Rationale: credential sourceを一つにし、Windows、systemd、containerのsecret deliveryをfile contractへ統一するためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `TICK_R2_CREDENTIALS_FILE`はsecret値ではなくpath overrideとして許可する。
  Rationale: systemd credentials等の動的mount pathへ対応しながら、secret本体をenvironmentへ載せないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: 常駐workerのdurable input authorityはin-memory channelではなくsealed WAL、raw outbox、canonical manifest、local Publication Catalog、remote PublicationJournalとする。
  Rationale: process crash後に通知が失われても、全処理をinventoryから再構成できるようにするためである。
  Date/Author: 2026-07-16 / Codex
- Decision: automatic manifestは`provisional`だけとし、terminal sync statusは証拠がない限り`unknown`とする。
  Rationale: wall clockやgrace periodだけでsource completenessを捏造しないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: R2 transient failureはlocal ingestを直ちに停止せず、immutable collision、local integrity failure、credential/config failureはfail closedにする。
  Rationale: availability failureではlocal durabilityを維持できるが、identity collisionや改変をretryで正常化してはならないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `remote_verified`はpruneの必要条件にするが、それだけで削除を許可しない。
  Rationale: local deletionには既存retention proof、WAL continuity、grace、path再検証も必要だからである。
  Date/Author: 2026-07-16 / Codex
- Decision: Fx importは`cmd/tick-gateway`と`internal/app`およびそのtestへ限定する。
  Rationale: domain constructorとunit testを通常のGo APIのまま維持するためである。
  Date/Author: 2026-07-16 / Codex
- Decision: production Fx graphは`ingest.Open`を呼ばず、WAL、ingest journal、publication catalog、R2 backend、Gateway、publication coordinatorを明示的なconstructorで組み立てる。
  Rationale: `ingest.Open`の内部へ依存とresource acquisitionを隠すと、Fx graph validation、開始順序、部分起動rollbackを証明できないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: 既存`ingest.Open`、`wal.Open`、journal open APIはcompatibility wrapperとして残してよいが、production `run`のcomposition rootでは使用しない。
  Rationale: 既存one-shot commandとunit testを一括破壊せず、production architectureだけを正しいdependency graphへ移行するためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `FileCredentialModule`だけが`credentials.Provider`を生成し、credential-bound R2 backend componentだけがOnStartで一度だけ`Provider.Load`を呼ぶ。
  Rationale: Providerの生成、読取り、再読取り責務を一意にし、PublicationModuleによる重複registrationと暗黙reloadを防ぐためである。
  Date/Author: 2026-07-16 / Codex
- Decision: local publication coordination stateとremote verification stateを別package、別schema ownerへ分ける。
  Rationale: sealed WAL、manifest revision、retry scheduleはlocal application pipelineの責務であり、R2 API境界の責務ではないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: ingest statusへpublication fieldを追加せず、`internal/operations.StatusService`がingest、publication、disk healthをread-only interfaceで集約する。
  Rationale: ingest domainをR2 publicationへ依存させず、operator viewだけをapplication boundaryで合成するためである。
  Date/Author: 2026-07-16 / Codex
- Decision: application configは`internal/config`のstrict typed TOML loaderを正本とし、既存のGo TOML依存を再利用する。
  Rationale: 独自flat parserへsection、escape、strict field規則を再実装すると、設定契約とsecurity validationが分散するためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `ingest.Config`はdomain compatibility valueとして維持し、`internal/config.Config`からの明示的な変換を一箇所へ置く。
  Rationale: production composition rootがflat parserを直接参照せず、既存unit testとone-shot commandの移行を同じ意味で検証できるようにするためである。
  Date/Author: 2026-07-16 / Codex
- Decision: 既存R2 Journalのlocal locator列と過去のobject state列はadditive migrationで残すが、常駐runtimeのlocal queue、retry、prune authorityには使用しない。
  Rationale: 既存Publisherの再開履歴とtest fixtureを壊さず、local orchestrationの責務だけを`internal/publication.Catalog`へ移せるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: remote verification timestampのsource of truthはpublication workerから注入するclockとする。
  Rationale: restart、fake clock、境界時刻のtestで再現性を確保し、domain pathに隠れたwall-clock readを残さないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: credential-bound backendはAWS default config loaderを使わず、static credentialsと明示endpoint／regionから`aws.Config`を構築する。
  Rationale: production processがAWS credential環境変数、shared profile、metadata serviceを探索する余地をなくすためである。
  Date/Author: 2026-07-16 / Codex
- Decision: Fx構築時のconfiguration errorは`NewProductionApp`から直ちに返し、CLIのStart時まで隠さない。
  Rationale: graph construction failureとruntime Start failureを分け、設定不備を「起動後の障害」として扱わないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: Gatewayのproduction Startはlistenerをbindした後、独自のruntime contextでaccept loopを起動する。Stopだけがそのcontextをcancelする。
  Rationale: FxのOnStart contextはhookの期限管理用であり、常駐処理の寿命を外部callerの一時contextへ依存させないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: canonical manifest spool pathを`receipt_root`から分離し、`publication.manifest_root`を必須にする。
  Rationale: manifestとverification receiptは異なる耐久化・再構築責務を持ち、同じdirectoryを暗黙共有させないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: local Catalogはmanifestファイルを正本にせず、起動時にcanonical file inventoryから再構築可能なindexとする。既存ファイルを再登録するとき、remote publication stateを上書きしない。
  Rationale: SQLite消失・破損後も同じrevision identityを維持し、既にremoteへ進んだ状態をlocal scanで巻き戻さないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: publication backlogの共有判定は`internal/ingest.DiskStateMachine`へ集約し、Catalog、Gateway、StatusServiceが別々に閾値判定しない。
  Rationale: pending bytesの計算とACK停止条件が分裂すると、statusが正常でもlocal spoolだけが膨張するためである。高水位ではACKを直ちに緩和せず、critical／limit超過ではfail closedとする。
  Date/Author: 2026-07-17 / Codex
- Decision: `PendingStats`は未公開segment bytesと未公開manifest bytesを保守的に合算する。
  Rationale: 同じraw objectが複数revisionへ現れる場合の重複を過小評価するより、local disk保護のために安全側へ倒す方が適切だからである。
  Date/Author: 2026-07-17 / Codex
- Decision: disk high／backlog pressure時のworker priorityは、共有DiskStateMachineのread-only decisionをpublication workerが読むことで実効化する。
  Rationale: status用のフラグだけではlocal backlogの収束速度が変わらず、disk保護の運用条件を満たさないためである。
  Date/Author: 2026-07-17 / Codex
- Decision: `local_pruned`は`internal/publication.Catalog`だけが所有し、R2 PublicationJournalは`remote_verified`までを所有する。
  Rationale: local削除の完了はretention executorとlocal pathの事実であり、remote intent／ETag／verificationを所有するR2境界へ書き戻すとlocal lifecycleが二重化するためである。既存Journalのlegacy `local_pruned`行は読取り互換のため残しても、runtime authorityにはしない。
  Date/Author: 2026-07-17 / Codex
- Decision: `local_pruned` segmentはsealed WALが残っていても再promotionしない。
  Rationale: raw outboxを再生成すると、Catalogが削除済みと記録したlocal lifecycleと実filesystemが食い違い、削除後の容量回収と再起動再現性を壊すためである。
  Date/Author: 2026-07-17 / Codex

## Scope

### In scope

- `credentials.Provider`、strict JSON bundle、`FileProvider`を追加する。
- Linux `native-acl`、Windows `native-acl`、`managed-mount`のsecurity validationを追加する。
- AWS SDK backendのproduction constructorをProviderから取得したcredentialへ統一する。
- Gateway、reader、retentionのactive configから平文credential環境変数依存を除く。
- `internal/config`へstrict typed application configと既存flat top-level keyの互換decodeを追加する。
- `go.uber.org/fx`と`go.uber.org/fx/fxtest`を追加する。
- WAL、ingest journal、publication catalog、R2 backend、Gatewayへ副作用のないconstructorと明示的なStart／Stop契約を追加する。
- `internal/app`にConfig、Credential、Storage、Remote、Publication、Core、Runtime moduleを追加する。
- `tick-gateway run`をFx applicationから起動する。
- size／time policyによるautomatic WAL sealを追加する。
- sealed WALのverificationとraw outbox promotionを常駐処理へ接続する。
- affected UTC dateごとのprovisional raw-day manifestをdurably生成する。
- existing `r2.Publisher`をsingle-writer background workerから呼ぶ。
- R2 upload、全量read-back verification、`remote_verified`記録を常駐経路へ接続する。
- restart reconciliation、bounded retry、health、metrics、backpressureを追加する。
- `internal/operations.StatusService`でingest、publication、disk状態を集約する。
- `prune-local`へ`remote_verified`必要条件を追加し、unlink完了後の`local_pruned`はlocal Catalogへ記録する。R2 Journalへlocal lifecycleを戻さない。
- Linux／Windows CIとnetwork-free end-to-end testを追加する。
- protectedなreal R2 smokeでproduction app graphのend-to-end経路を検証する。

### Out of scope

- RCloneまたは別の外部upload CLIを復活させること。
- AWS SDK for Go v2以外のR2 write implementationを追加すること。
- R2 PutObject、412、timeout、GetObject verificationの既存意味論を変更すること。
- Protocol V1 wire message、producer/Gateway通信、ACK authorityを変更すること。
- raw WAL、sealed WAL、raw object、raw-day manifestのformatを変更すること。
- automatic `settled_snapshot`判定を追加すること。
- replay Parquet generationを常駐化すること。
- local dataを自動削除すること。
- DPAPI Provider、credential hot reload、automatic key rotationを追加すること。
- Windows Service installer、systemd unit、Kubernetes manifestを全面再設計すること。
- 複数scopeを一つのGateway processへ同居させること。
- multipart uploadを導入すること。

## Specification conflicts and resolutions

| 衝突 | 現在の挙動 | 最小変更 | 互換性リスク | 採用案 | 不採用案 |
| --- | --- | --- | --- | --- | --- |
| Provider/Fx仕様はuploaderを非対象とする | production upload pathが存在しない | 既存domain部品をworkerで接続する | PR規模が増える | 同一ExecPlan内でG1/G2とG3/G4を別gateにする | Provider/Fxだけを完了扱いにする |
| production `run`はR2 configを要求しない | local WALだけで起動できる | `run`だけpublication configを必須にする | 旧configはstrict decodeで拒否される | fail-fast decode errorと新形式example更新 | migration alias、uploadなしで正常起動するsilent fallback |
| credentialを環境変数から読む | reader／retention／backendがsecret envを参照する | FileProviderへ置換する | 既存deploymentのsecret注入変更が必要 | file bundleへの明示migration | production EnvProviderを残す |
| config loaderはflat形式だけ | `[credentials]`等を読めない | existing TOML dependencyでtyped root configを追加する | strict decodeで旧曖昧入力が拒否される | `internal/config`を正本にし、新形式のtyped sectionだけを読む | 旧top-level aliasを残す、独自parserへsection処理を継ぎ足す |
| `ingest.Open`がresourceを開く | WAL、journal、disk、recoveryが一つの関数に隠れる | 各resourceへconstructorとStart／Stopを追加する | production startup pathが変わる | production graphは明示constructorを使い、`Open`は互換wrapperだけにする | Fx OnStartから既存`ingest.Open`を呼ぶだけのadapter |
| `Close`がhandlerを無期限に待つ | Stop deadlineを守れない | bounded `Shutdown(ctx)`を追加し`Close`を互換wrapperにする | timeout時のerrorが増える | deadlineを伝播して未完了を報告する | Fx hook内で無期限waitする |
| publication journalにlocal pending scanがない | retryは同じ入力を手動再実行した場合だけ | `internal/publication.Catalog`を追加する | additive local schemaと新しいcatalog pathが必要 | filesystem reconciliationを正本にし、local CatalogとR2 journalを分離する | local queue tableを`internal/r2.PublicationJournal`へ追加する |
| pruneがR2を直接観測する | `remote_verified` local stateが必要条件ではない | retention proofにjournal stateを追加bindする | 旧journalだけではprune不能 | fail closedで再verification後にstateを補う | R2存在だけで削除する |
| statusはingest型だけで表現される | publication backlogを追加するとingestがR2を知る | application status aggregatorを追加する | status JSONのtop-level構造をversion管理する必要がある | `internal/operations.StatusService`でread-only snapshotを合成する | `ingest.Gateway.StatusSnapshot`へpublication依存を追加する |

## Design

### Credential Provider

`internal/credentials`へ次の最小interfaceを追加する。

```go
type Credentials struct {
    AccessKeyID     string
    SecretAccessKey string
}

type Provider interface {
    Load(context.Context) (Credentials, error)
}
```

production実装は`FileProvider`だけとする。
bundleは`format_version`、`access_key_id`、`secret_access_key`を持つUTF-8 BOMなしJSON objectとする。
最大sizeは64 KiBとする。
unknown field、duplicate key、trailing JSON、非object、空値、version不一致を分類可能なerrorで拒否する。
error、log、Fx event、format outputへAccess Key IDとSecret Access Keyを出さない。
`Credentials`へ`String`または`GoString`を実装しない。
公開型のformat漏洩を防ぐ必要がある場合は、値を一切出さないredacted `fmt.Formatter`だけを実装し、canary testで全verbを固定する。

`ProtectionMode`未指定時は`native-acl`とする。
未知のmodeを`managed-mount`へfallbackせず、constructorで拒否する。
v1ではcredential fileを起動時に一度だけ読み、file watch、hot reload、automatic rotationを行わない。

`Load`は、cancel済みcontext、空path、file identity、security policy、size、strict JSON、version、必須値の順に検査する。
最初に`ctx.Err()`を確認し、I/O開始後も返却前にcancelを確認する。
可能なOSではopen済みfile handleに対してstat、security check、bounded readを行い、pathの再openによるTOCTOU窓を増やさない。

credential errorは最低限、path required、unsafe file、too large、malformed、unsupported version、incompleteへ分類し、`errors.Is`または`errors.As`で判定可能にする。
parser由来errorと入力断片をそのまま返さない。

`native-acl`ではsymlinkを拒否し、open済みhandleのstat結果に対してregular fileとsecurity policyを確認する。
Linuxではgroup／other bitが0で、ownerが実行userまたはrootであることを要求する。
Linuxでは親directoryが不用意にworld-writableでないことも検査し、sticky bit付き標準一時directoryの例外はtest profileだけで明示する。
Windowsではprocess tokenのuser SIDをGateway service identityとして扱い、SYSTEM、Administrators、service identity以外へread可能なACEがないことを検査する。
Windows Service固有SIDを明示できるvalidator seamは残すが、service installerは本計画へ含めない。

`managed-mount`はKubernetes等のsymlink layoutを許可し得るため、final opened handleがregular fileであること、size、readability、strict JSONだけをProvider側で検査する。
managed mountのACLやmodeをnative fileと同じ基準で拒否しない。

### Configuration

`internal/config`へapplication全体のstrict typed root configを追加する。
decoderは既存依存の`github.com/pelletier/go-toml/v2`を使い、unknown fieldを拒否する。
ConfigModule以外がconfig fileを直接読まない。
root configは既存top-level Gateway keyをtyped fieldとして維持し、package固有configへ変換する純粋なmethodを持つ。
独自flat parserへsection、escape、comment規則を追加しない。

次のsectionを追加する。

```toml
[credentials]
provider = "file"
path = "/run/credentials/tick-gateway.service/r2-writer"
protection = "managed-mount"

[r2]
endpoint = "https://<account-id>.r2.cloudflarestorage.com"
bucket = "tick-raw"
region = "auto"
immutable_root = "v1"

[publication]
catalog_path = "./publication/catalog.sqlite"
remote_journal_path = "./publication/remote-publication.sqlite"
manifest_root = "./publication/manifests"
receipt_root = "./publication/receipts"
seal_max_bytes = 67108864
seal_interval_ms = 60000
scan_interval_ms = 1000
retry_min_ms = 1000
retry_max_ms = 300000
max_pending_segments = 100000
max_pending_bytes = 1099511627776
```

上記数値はconfig shapeを示す例であり、暗黙defaultではない。
G0でseal latency、single-object上限、想定ingest rate、disk critical reserveからproduction profileを導出し、各値を明示必須にする。
`max_pending_bytes`到達時は正常状態として継続せず、disk pressure controllerがreadinessとACK可否をfail closedで更新する。

`run`はこの三sectionを必須とする。
`init`、local `status`、`verify-local`はremote接続を必須にしない。
`prune-local`とread-only delivery configにもFileProvider pathとprotectionを追加する。
secret値を受ける環境変数は追加しない。

旧`access_key_env`と`secret_key_env`はactive exampleから削除する。
旧fieldを指定した場合はsilent fallbackせず、FileProviderへのmigrationを示すsecret-free errorを返す。
これは外部config互換性に対する意図的な例外であり、security要件とproduction機能完成を優先する。

### Fx modules

`internal/app`に最低限次を定義する。

```text
ConfigModule
FileCredentialModule
StorageModule
RemoteModule
PublicationModule
CoreModule
RuntimeModule
```

`ConfigModule`はconfig pathからstrict root configを一度だけ読み、package固有config valueを提供する。
`FileCredentialModule`は`credentials.Provider`だけを生成し、`Load`を呼ばない。
`StorageModule`はWAL、ingest journal、local publication Catalog、remote PublicationJournalの通常constructorだけを登録する。
`RemoteModule`はProviderとR2 configからcredential-bound S3 backend componentを構築し、OnStartで一度だけcredentialをLoadできる状態にする。
`PublicationModule`はlocal planner、manifest spool、`r2.Publisher`、publication coordinatorを構築し、FileProviderを生成しない。
`CoreModule`はingest Gateway、disk pressure controller、application status serviceを構築する。
`RuntimeModule`は既に構築済みcomponentをFx Lifecycleへ接続し、domain objectを生成しない。

`ProductionOptions(configPath)`は上記moduleを一度だけ組み立てる。
`TestOptions`はfake Provider、fake remote backend、fake clock／ticker、temporary rootsを明示的に供給する。
production Providerとfake Providerを同じgraphへ登録して`fx.Replace`で上書きしない。
production codeで`fx.Populate`を使わない。
`fx.Invoke`はapplication rootの到達可能化とLifecycle hook登録だけに使う。
起動順序が重要なcomponentをunordered value groupへ入れない。

production dependency graphは次を正本とする。

```text
typed Config
  ├─ WAL Store ───────────────┐
  ├─ Ingest Journal ──────────┤
  ├─ Publication Catalog ─────┤
  ├─ Remote PublicationJournal├─> Publication Coordinator
  ├─ File Credential Provider ─> Credential-bound S3 Backend ─> R2 Publisher ─┘
  ├─ Disk Pressure Controller ──────────────────────────────────┐
  └─ Ingest Gateway <─ WAL Store / Ingest Journal ─────────────┤
                                                                └─> Status Service
```

`ingest.Open`、`wal.Open`、既存journal open helperはこのproduction graphに含めない。
互換wrapperは同じ新constructorとStart／Stopを手動で合成し、別の初期化規則を持たない。

### Lifecycle order

Configのdecodeとvalidationは副作用を限定したFx constructorで完了させ、Lifecycle hookとして扱わない。
FileProvider constructorはpathとmodeの静的検証だけを行い、fileを読まない。

Start順序は次とする。

```text
1. WAL Store recovery and open
2. Ingest Journal recovery and open
3. local Publication Catalog recovery and open
4. remote PublicationJournal recovery and open
5. credential-bound S3 BackendがFileProviderを一度Loadしてreadyになる
6. publication filesystem reconciliation
7. R2 publication worker start
8. WAL seal coordinator start
9. ingest listener bind and accept start
```

Stop順序は次とする。

```text
1. ingest listenerを閉じて新規受付を止める
2. active connectionをcancelし、有界時間でhandlerを待つ
3. non-empty active WALを一度sealする
4. sealed artifactをworkerへ通知する
5. 実行中publicationをStop contextの期限内で完了またはlocal Catalogのdurable retry状態へ戻す
6. publication workerを止める
7. credential-bound S3 Backendを停止する
8. remote PublicationJournal、local Publication Catalog、ingest journal、WALを逆順で閉じる
```

Fx hookは依存resourceを先に登録し、利用側を後から登録する。
worker goroutine内のfatal integrity errorはhealthへ記録し、app-owned monitorが`fx.Shutdowner`を使ってprocess shutdownを要求する。
domain componentはFxをimportしない。
WAL、journal、Catalog、backend、Gateway、coordinatorは通常のGo constructorとStart／Stopを持ち、resource acquisitionとgoroutine開始をconstructorで行わない。

### Automatic seal and local publication planning

seal coordinatorはactive WAL sizeが`seal_max_bytes`以上になった場合、またはnon-empty active WALが`seal_interval`以上継続した場合に`Store.Seal()`を呼ぶ。
sizeとtimeの両方を設定可能にし、zeroまたはoverflowを拒否する。
concurrent ingestとの排他は`wal.Store`の既存mutexをauthorityとする。
空segmentは正常なno-opとして扱い、integrity errorへ変換しない。

workerはstartupと通知時にsealed inventoryをsequence順で再走査する。
各segmentを再検証し、content-addressed raw outboxへidempotentにpromoteする。
symlink、sequence gap、chain mismatch、same key different bytesをintegrity failureとして停止する。

各verified segmentのWAL entryをdecodeし、含まれるUTC date集合を導出する。
zero-record batchは既存manifest規則どおり`RequestedFromMSC`のUTC dateへ割り当てる。
affected dateごとに、既知raw objectからそのdateを覆う最小の連続segment sliceを選ぶ。
pure plannerがlatest local manifestと新しいcoverageを比較し、選択範囲またはstatusが変化した場合だけ次revisionを生成する。
同じdateでは一つ前のrevisionがlocal verificationとremote publicationを完了してからsuccessorを発行し、未発行predecessorを飛び越えない。

automatic revisionは`CompletenessStatus: "provisional"`、`TerminalSyncStatus: "unknown"`、`LogicalCloseTimeS: 0`とする。
manual settleの証拠がない限り`settled_snapshot`を生成しない。
canonical manifestはlocal spoolへno-clobberで保存し、directory durabilityを確認してからpublication jobへ渡す。

### Publication worker and durable state

`internal/publication`を新設し、local orchestrationを`internal/r2`から分離する。
consumer側に小さいPublisher interfaceを定義し、productionでは`*r2.Publisher`、testではfakeを注入する。

workerのin-memory channelはwake-up通知だけに使う。
処理対象は毎回sealed inventory、raw outbox、manifest spool、local Publication Catalog、remote PublicationJournalから再構築する。
single scopeにつき一つのworkerと既存publication lockを使い、同じpublisher epochの並行writerを許可しない。

`internal/publication.Catalog`はsegment identity、promotion state、affected date、latest local manifest、attempt count、next retry time、last error classを所有する。
Catalogはlocal orchestrationだけを表し、remote ETag、remote verification authority、AWS request stateを所有しない。
`internal/r2.PublicationJournal`はremote intent、remote object transition、ETag、verification timeだけを所有する。
既存Journalの`local_path`と過去の`sealed_local`等の列は、旧Publisherの再開履歴とverification bindingを保つために残してよい。
ただしそれらはlocal pending列挙、retry schedule、manifest planning、prune判定の根拠にしない。
Catalogを失った場合はfilesystem inventoryから再構築し、remote stateが不明なobjectは再verificationへ戻す。

`internal/publication.Catalog`の実行時正本は、sealed segmentのcontent hashとpath、promotion済みraw object、affected UTC date、manifest revision、retry class、attempt count、next retry time、`local_pruned`のlocal lifecycleである。
Catalogの再構築はfilesystemを走査してcontent hashとsealed WAL検証結果を再取得し、manifestとretryの欠損だけを再計画する。
remote Journalからlocal pendingを復元しない。

object stateは次の順序を保つ。

```text
sealed_local
  -> uploading
  -> remote_committed
  -> remote_verified
  -> Catalog.local_pruned
```

PUT成功、412、timeout／結果不明の分岐は既存SDK実装を維持する。
`remote_verified`はR2 `GetObject`をstreamし、publication intentがbindするlocal objectのsizeとSHA-256へ一致した場合だけ記録する。
manifestとscope descriptorもimmutable objectとして同じpublication completionに含める。

retryはlocal Catalogへerror class、attempt count、next retry timeをdurably記録する。
backoffは1秒から開始し、2倍で増加し、5分で上限とする。
testではfake clockと明示tickを使い、sleepへ依存しない。
credential value、request body、endpoint query、local file bytesをerror記録へ含めない。

### Failure and backpressure

R2 timeout、5xx、temporary DNS failureはretryableとする。
R2 412で既存bytesが一致する場合は冪等成功とする。
R2 412でbytesが異なる場合はimmutable collisionとしてprocess readinessをfalseにし、新規acceptを停止する。
credential拒否はretry stormにせず、healthへcredential error classを記録してoperator actionを要求する。

R2 outage中もdiskがnormal／highでlocal WALへ書ける限り、既存ACK contractを維持する。
`internal/operations.StatusService`はread-only status interfaceから、pending segment count、pending bytes、oldest pending age、last successful verification、retry count、last error classを集約する。
`ingest.Gateway`とその`StatusSnapshot`へpublication dependencyを追加しない。
disk pressure controllerはfilesystem usageとpublication backlogを入力にし、ingest readinessとpublication priorityへ別々のread-only decisionを提供する。
critical／emergency disk policyではproof条件を緩和せず、新規ACKを止める。

### Prune integration

raw outboxの`remote_verified` recordはlocal artifactのkey、path、size、SHA-256へ一致しなければならない。
sealed WALはpromotion元なのでpath一致を要求せず、同じcontent hash、size、raw object keyとcovering manifestをretention proofでbindする。
`prune-local`は既存のfresh remote observationとretention proofに加え、この対応関係を満たす`remote_verified` recordを要求する。
raw outboxの削除成功後だけ、Catalogの同じraw key、local path、size、SHA-256を持つsegmentを`local_pruned`へdurably記録する。
sealed WALの削除完了は既存prune checkpointで記録し、raw outboxが残っているのにCatalogのsegmentを`local_pruned`へ進めない。
`remote_verified` recordがない旧publicationは、read-back verificationを再実行して記録を補うまでpruneしない。
本計画では自動prune workerを追加しない。

## Implementation Plan

### G0: Plan freeze and review gate

G0では本計画、Provider/Fx仕様、失効したR2 SDK計画の関係をreviewする。
シニアreviewではPR規模、既存API互換、test分割、migration手順、運用診断性を確認する。
アーキテクトreviewではACK authority、durable queue、day revision、single writer、failure classification、prune authorityを確認する。

次が確定するまでG1へ進まない。

- production `run`ではR2 configが必須である。
- automatic manifestはprovisionalだけである。
- filesystemとlocal Catalogがlocal pending workのtruthであり、remote Journalはremote intent／verification ledgerである。
- local Catalogとremote PublicationJournalのschema ownerが分かれている。
- 既存R2 Journalの履歴列は互換性のため残してよいが、local retry／manifest／prune authorityへ流用しない。
- R2 failureはlocal spoolを保持し、collisionはfail closedである。
- Fxはcomposition rootだけに存在する。
- production graphが`ingest.Open`を使用せず、全resourceを明示constructorで組み立てる。
- FileProviderを生成するmoduleと`Load`するcomponentが一つずつである。
- runtimeのremote verification時刻は注入clockから取得する。

### G1: Credential Provider and config migration

`internal/credentials/provider.go`、`bundle.go`、`file.go`を追加する。
OS固有security validatorはbuild tag付きfileへ分ける。
strict decoderはduplicate keyをfield trackingで検出し、`io.LimitReader`で64 KiB超過を読み切らず拒否する。
FileProviderはcancel済みcontextをI/O前に拒否し、未指定protectionを`native-acl`へ正規化し、未知modeを拒否する。
Linux native validatorはfile mode、owner、親directoryを検査する。
Windows native validatorはactual ACLとservice identityを検査し、差替え可能なvalidator seamを持つ。
FileProviderは起動後にfileを再読込せず、hot reloadを行わない。

Provider unit testは正常bundle、fileなし、directory、空file、非object、malformed JSON、unknown field、duplicate field、trailing JSON、version不一致、必須値欠落、64 KiBちょうど、64 KiB超過、cancel済みcontext、unsafe permission、managed mount、secret canaryを含める。
credential errorは`errors.Is`／`errors.As`で分類できることを確認する。

`internal/config`へ`Config`、`CredentialsConfig`、`R2Config`、`PublicationConfig`とstrict TOML loaderを追加する。
設定契約は新しいtyped sectionと`raw_outbox_root`だけとし、旧`outbox_root`、`r2_bucket_env`、`r2_prefix`はdecode対象にしない。
`internal/config.Config`から`ingest.Config`と既存one-shot command用configへ変換する関数を一つだけ設け、production `run`は変換済みvalueをFxへ供給する。
既存`ingest.LoadConfig`はこのloaderのcompatibility wrapperへ変更し、flat parserを残したままproduction graphから呼べる状態にはしない。
graph validation testにはfile I/Oを行わないconfig supply optionを用意する。
reader／retention configもFileProvider pathへ移行する。
local exampleとactive runbookを更新するが、実credential fixtureをcommitしない。

`internal/r2`のcredential-bound S3 backend componentは`credentials.Provider`を一つだけ受け、Start時に一度だけLoadする。
`r2.Publisher`にはclockを注入できるconstructorを追加し、production workerはそのconstructorを使う。
既存`NewPublisher`はsystem clockを使うcompatibility wrapperとし、既存library testの呼出しを壊さない。
PublicationModuleとdomain PublisherはFileProviderまたはcredential file pathを受け取らない。
AWS SDKのdefault credential chainとsecret env lookupをproduction call pathから除く。
unit testではfake Providerを直接渡し、user environmentを読まない。

G1 gateはProvider unit test、config test、secret scan、Linux permission test、Windows buildがpassすることである。

### G2: Fx composition root and bounded lifecycle

`go.uber.org/fx`をdirect dependencyとして追加する。
WAL、ingest journal、local Publication Catalog、remote PublicationJournal、credential-bound S3 backend、Gateway、publication coordinatorへ副作用のないconstructorとStart／Stopを追加する。
`internal/app`へmodule、Lifecycle registration、fatal error monitorを追加する。
`tick-gateway run`はconfig pathをapplicationへ渡し、Fx appを起動する。OS signalだけでなく、内部componentが`fx.Shutdowner`へ送る停止通知も待ち、どちらの場合もbounded Stopへ移行する。
one-shot commandを無理にlong-running Fx appへ変換しない。

`ingest.Gateway`へbounded `Shutdown(ctx)`を追加し、既存`Close()`は後方互換wrapperとして維持する。
production graphは`ingest.Open`を呼ばず、Gateway constructorへWAL、ingest journal、disk pressure decisionを明示的に渡す。
Gateway OnStartはlistenerを同期bindしてからaccept goroutineを開始する。
bind失敗または後続Start失敗では、開始済みresourceを逆順で閉じる。
既存`ingest.Open`は新constructorとStartを手動合成するcompatibility wrapperへ書き換える。

`fx.ValidateApp`でproduction graphと必須Provider欠落のnegative graphを検証する。
`fxtest`で正常Start／Stop、二重Start／Stop、途中Start失敗、rollback、OnStop error、Stop timeout、worker error、listener終了、Stop後の受付拒否、goroutine leakを検証する。G2で実施できた正常起動、credential一度読み込み、listener bind失敗rollback、OnStop error、構築エラー返却はテスト済みである。
graph validationではconstructorが実行されないため、fake依存によるapplication起動testを別に必須とする。
pure domain testはFxへ変換しない。

G2 gateはproduction graph、Lifecycle test、既存ingest integration test、race対象testがpassすることである。

### G3: Automatic seal, promotion, and manifest spool

`internal/publication`へsealer、inventory reconciler、day planner、manifest spoolを追加する。manifest spoolのrootは`publication.manifest_root`で明示し、receipt directoryから推測しない。
`internal/publication.Catalog`をlocal coordination schemaの唯一のownerとして追加する。
最初にpure planner testで、single day、cross-midnight、late previous-day data、zero-record batch、sequence gap、chain mismatch、duplicate notificationを固定する。

seal coordinatorへfake clock／tickerを注入し、size threshold、time threshold、graceful stop seal、empty no-op、concurrent appendを検証する。
promotionはexisting `archive.PromoteSealedSegment`だけを使い、別copy実装を作らない。
manifest buildはexisting `archive.BuildRawDayManifest`だけを使い、selected range規則を複製しない。

manifest spoolはtemporary write、sync、close、no-clobber publish、directory syncの順で確定する。
startup reconciliationはpartial temporary fileをauthorityにせず、complete canonical fileだけを採用する。Catalogが空でもcanonical file inventoryから同じrevision identityを復元し、dateごとのrevision gapとpredecessor digest不一致をintegrity failureとして停止する。
latest revision branch、same revision different digest、missing predecessorをintegrity failureとして停止する。

G3 gateはnetworkを使わず、fake ingestからsealed WAL、raw object、canonical provisional manifestまでが自動生成されるintegration testがpassすることである。

### G4: R2 uploader, verifier, and restart recovery

FileCredentialModuleはProviderを生成し、RemoteModuleはcredential-bound S3 backendを生成する。
PublicationModuleはreadyなbackend capability、Layout、remote PublicationJournal、Publisher、local Catalog、publication coordinatorを組み立てる。
workerはG3のmanifest spoolを読み、existing `Publisher.Publish`を呼ぶ。
起動時にlocal Catalogが未登録manifestとdue retryを列挙し、remote PublicationJournalが未完了remote intentを列挙する。
どちらか一方のstoreへlocal stateとremote stateを混在させない。

network-free fake backend testで次を検証する。

- normal PUT、full Get verification、`remote_verified`。
- PUT成功後response loss、existing object match、冪等成功。
- 412 same bytes、冪等成功。
- 412 different bytes、collision停止。
- transient failure、durable backoff、再起動後成功。
- crash after seal、promotion、intent、PUT、commit、verificationの各境界からの再開。
- duplicate wake-up、同一objectへのremote mutationが一回以下であること。
- secret canaryがerror、log、Fx event、journalへ出ないこと。

G4 gateはfake producerからFx production-equivalent appへ送ったbatchが、fake R2のverified immutable objectと`remote_verified` stateになるend-to-end testがpassすることである。

### G5: Health, disk pressure, and prune gate

`internal/operations.StatusService`へingest、publication、diskのread-only status sourceを注入し、versioned application statusを生成する。
`ingest.Gateway.StatusSnapshot`はingest固有状態のまま維持する。
statusはsecret、bucket credential、raw error bodyを出さない。
transient outage、credential failure、collision、local integrity failureを別のerror classで表示する。

disk pressure controllerへfilesystem usageとpending publication bytesを入力し、ingestとpublicationが同じ判定結果を読む。
disk highではoperator warningとworker優先実行を行うが、proofを緩和しない。
disk criticalではlistener readinessとnew ACKを停止する。

`prune-local`へ`remote_verified` lookupを追加する。
remote observationだけ、journalだけ、receiptだけ、ETagだけでは削除できないnegative testを追加する。
successful prune後の`local_pruned`記録とcrash retryを検証する。

G5 gateはR2 outageからのrecovery、disk pressure、prune truth table、restart testがpassすることである。

### G6: Cross-platform CI and real R2 proof

UbuntuとWindowsのmatrixで次を実行するworkflowを追加または既存workflowへ統合する。

```text
go test ./...
go vet ./...
go build ./...
```

Linuxでは`go test -race ./...`を実行する。
WindowsではFileProvider ACL fixtureをactual filesystem ACLで作成し、許可／過剰許可を検証する。
Linuxでは0400、0600、group／other拒否、owner拒否、world-writable parent拒否、managed mountを検証する。

real R2 smokeはprotected `workflow_dispatch`またはoperator環境で実行する。
smokeはproduction Fx graph、FileProvider、synthetic producer、automatic seal、promotion、manifest、SDK upload、Get verification、restartを通す。
R2へ直接Publisherを呼ぶだけのsmokeをend-to-end acceptanceの代替にしない。

real R2 smokeではisolated bucketまたはprefixを使う。
raw prefixへBucket Lockを適用する運用判断と保持期間を記録する。
credential、endpoint、bucket、account ID、local absolute pathをartifactへ保存しない。

G6 gateはLinux CI、Windows CI、race、real R2 smokeの実行結果がExecPlanへ記録されることである。
real R2を実行できない場合は本計画をcompletedにしない。

### G7: Final audit

active code、config example、README、runbook、testからRClone runtime dependencyが消えていることを確認する。
historical ExecPlan内の記述は履歴として残してよいが、active instructionとして参照しない。
Fx importが許可範囲外へ漏れていないことを確認する。
Provider以外がcredential fileまたはsecret envを読んでいないことを確認する。
production graphまたはmoduleに`ingest.Open`、duplicate Provider registration、local stateを持つR2 journal tableがないことを確認する。

`tick-gateway run`のproduction graphから、listener、sealer、promoter、manifest planner、publisher、verifier、state journalまで到達可能であることをgraph testとend-to-end testの両方で確認する。
全受入条件と未解決事項を`Outcomes & Retrospective`へ記録する。

## Validation

Baseline commandは次である。

```bash
mise run check
```

focused testは実装段階に応じて次を使用する。

```bash
mise exec -- go test ./internal/credentials -count=1
mise exec -- go test ./internal/config ./internal/app ./internal/publication ./internal/operations -count=1
mise exec -- go test ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/retention -count=1
mise exec -- go test -race ./... -count=1
mise exec -- go vet ./...
mise exec -- go build ./...
```

Windows runnerでは次を実行する。

```text
go test ./...
go vet ./...
go build ./...
```

TestはDNS、Cloudflare、AWS、user home、ambient AWS profile、実credential environmentへ依存しない。
real R2 testだけを明示tagまたはprotected workflowで分離する。

各gateで`git diff --check`とsecret scanを実行する。
`mise run check`が失敗した場合は、baseline既知失敗と新規失敗を区別してProgressへ記録する。

## Acceptance Criteria

次をすべて満たした場合だけ本計画をcompletedにする。

### Production data path

- `tick-gateway run`がFx production graphから起動する。
- syntheticまたはreal producerのaccepted batchがlocal WALへ既存contractどおり記録される。
- sizeまたはtime policyでnon-empty WALが自動sealされる。
- sealed WALが自動でverified raw outboxへpromoteされる。
- affected UTC dayのcanonical provisional manifestが自動生成される。
- existing AWS SDK Publisherがmanifestとraw objectをR2へ送る。
- R2から全量を読み戻し、sizeとSHA-256一致後に`remote_verified`が永続化される。
- process restart後に未完了処理がoperator commandなしで再開する。
- R2 outage中にlocal dataが失われず、復旧後にbacklogが収束する。
- immutable collisionでは新規acceptが停止し、上書きまたは無限retryを行わない。

### Credentials and Fx

- production credential実装はFileProviderだけである。
- 未指定のprotection modeは`native-acl`となり、未知のmodeは起動前に拒否される。
- FileProviderはcancel済みcontextをI/O前に拒否し、起動後にcredential fileを再読込しない。
- Linuxではfile owner、group／other permission、親directoryを検証し、Windowsではactual ACLとservice identityを検証する。
- Access Key IDとSecret Access Keyを平文環境変数から読まない。
- strict JSON、64 KiB、version、必須field、permission policyが検証される。
- secret canaryがerror、log、Fx event、journal、format outputへ出ない。
- ConfigModuleだけがconfig fileを読み、既存TOML libraryによるstrict typed root configからpackage固有configを供給する。
- FileCredentialModuleだけがProviderを生成し、credential-bound S3 BackendだけがOnStartで一度だけ`Load`する。
- PublicationModule、domain Publisher、Coordinatorはcredential file pathを受け取らない。
- production graphは`ingest.Open`、`wal.Open`、journal open helperを呼ばず、resourceごとのconstructorとStart／Stopで構築される。
- production graph validationと必須依存欠落negative testがある。
- fake依存を使ったapplication startup testがあり、`fx.ValidateApp`だけを起動検証の根拠にしない。
- Start／Stop、rollback、timeout、worker終了順序が`fxtest`で検証される。
- 二重Start／Stop、OnStop error、Stop後の受付拒否、goroutine leakが検証される。
- Fx importがcomposition rootとgraph testへ限定される。

### Safety and compatibility

- Protocol V1 wire、ACK authority、WAL format、raw object format、manifest format、R2 immutable write semanticsを変更しない。
- automatic `settled_snapshot`を生成しない。
- `remote_verified`だけでpruneを許可せず、既存retention proofも要求する。
- local segment、manifest、retry、`local_pruned`は`internal/publication.Catalog`が所有する。
- remote intent、ETag、verification、`remote_verified`は`internal/r2.PublicationJournal`が所有する。
- local Catalogとremote PublicationJournalのどちらも、他方の状態を正本として重複保持しない。
- `ingest.Gateway.StatusSnapshot`はingest固有状態だけを返し、application statusは`internal/operations.StatusService`がread-only sourceから集約する。
- RCloneをruntime dependencyとして使わない。
- `init`とlocal `status`がR2 outage中も利用できる。
- 新credential configをrunbook／exampleへ明記し、旧credential configはstrict decodeで拒否する。

### Verification evidence

- `mise run check`が成功する。
- Linuxの`go test ./...`、`go vet ./...`、`go build ./...`が成功する。
- Windowsの`go test ./...`、`go vet ./...`、`go build ./...`が成功する。
- Linux raceが成功する。
- Windows／LinuxのOS固有FileProvider testが成功する。
- network-free end-to-end testが成功する。
- protected real R2 end-to-end smokeが成功する。

## Risks and rollback

最大の実装riskは、Provider/Fx refactorと欠落していたproduction publication pathを同じPR系列で扱うため、差分が大きくなることである。
このriskはG1からG6を独立review gateにし、各gateで既存testをgreenへ戻してから次へ進むことで抑える。

最大のdata riskは、day revision plannerがlate dataまたはcross-midnight segmentを誤って除外することである。
pure planner test、existing `BuildRawDayManifest`による再導出、remote read-back、manifest verificationをすべて要求する。

最大のoperational riskは、旧configで新binaryが起動しないことである。
silent local-only fallbackは採用せず、移行error、example、runbook、doctor相当のconfig validationを提供する。

rollback時はGatewayを停止し、WAL、sealed inventory、raw outbox、manifest spool、local Publication Catalog、remote PublicationJournal、receiptを削除しない。
ここでpublication stateには、local `internal/publication.Catalog`とremote `internal/r2.PublicationJournal`の両方を含む。
各schema変更はowner packageごとにadditiveにし、local stateとremote stateを一つのtableへ統合しない。
旧binaryが未知tableまたは追加columnを安全に無視できない場合は、schema migration前のbinary rollbackを禁止し、forward fixをrunbookへ明記する。
ただし旧binaryにはautomatic R2 publicationがないため、rollback後をproduction正常状態とは扱わない。
修正版を再deployするまでlocal disk監視とingest停止判断をoperatorが行う。

remote objectはimmutableであり、rollbackのために削除または上書きしない。
same-content retryだけを許可し、different-content collisionは人手調査まで停止する。

## Interfaces and dependencies

新しい主要interfaceは、利用側packageが必要とする操作だけを定義する。

```go
package credentials

type Credentials struct {
    AccessKeyID     string
    SecretAccessKey string
}

type Provider interface {
    Load(context.Context) (Credentials, error)
}
```

```go
package publication

type RemotePublisher interface {
    Publish(context.Context, r2.PublicationInput) (r2.VerificationReceipt, error)
}

type Clock interface {
    Now() time.Time
}

type Ticker interface {
    C() <-chan time.Time
    Stop()
}
```

`publication.Coordinator`は広いlifecycle interfaceを公開せず、具体型として扱う。
そのconstructorはsealed inventory、raw outbox、manifest spool、local Catalog、RemotePublisher、remote PublicationJournal、clock、tickerを明示的に受ける。
Fx adapterだけが具体型の`Start`、`Stop`、`Notify`をLifecycleへ接続する。
global state、`init()` side effect、ambient environment lookupを持たない。

statusとretentionでは、利用側packageに次のようなread-only interfaceを置く。

```go
package operations

type IngestStatusReader interface {
    IngestStatus(context.Context) (IngestStatus, error)
}

type PublicationStatusReader interface {
    PublicationStatus(context.Context) (PublicationStatus, error)
}

type DiskStatusReader interface {
    DiskStatus(context.Context) (DiskStatus, error)
}
```

```go
package retention

type RemoteVerificationIndex interface {
    LookupVerifiedObject(context.Context, ObjectIdentity) (VerifiedObject, bool, error)
}
```

`operations.StatusService`は上記status readerだけを受け、ingest、publication、diskの具体型を知らない。
retentionは`RemoteVerificationIndex`と既存retention proofだけを受け、R2 clientまたはpublication workerを直接操作しない。

production composition rootでは、少なくともWAL store、ingest journal、local Publication Catalog、remote PublicationJournal、credential-bound S3 Backend、ingest Gateway、publication Coordinator、operations StatusServiceを個別constructorで生成する。
予定する構築境界は次の形とし、既存の`Open`関数へ戻る抜け道を作らない。

```text
wal.NewStore(config)
journal.NewStore(config)
publication.NewCatalog(config)
r2.NewPublicationJournal(config)
r2.NewCredentialBackend(config, credentials.Provider)
ingest.NewGateway(config, walStore, ingestJournal, diskDecision)
publication.NewCoordinator(config, catalog, remoteJournal, remotePublisher, clock, ticker)
operations.NewStatusService(ingestStatus, publicationStatus, diskStatus)
```

各constructorは依存保持と純粋なvalidationだけを行い、file open、recovery、network access、listener bind、goroutine開始は対応する`Start`へ移す。
対応する`Stop`はcontext deadlineを受け、部分起動rollbackと二重呼出しで安全に終了する。

新しいdirect dependencyは`go.uber.org/fx`だけである。
R2 SDKは既存のAWS SDK for Go v2を維持する。
JSON、filesystem、ACL実装には標準libraryと既存`golang.org/x/sys`を優先し、新しいsecret manager SDKを追加しない。

## Outcomes & Retrospective

G0〜G4は実装とフェーズ限定のGPT-5.5 xhigh独立レビューを完了した。
G2ではPR #5のLinux／Windows race各2件とRepository check各2件がpassしている。
G5は完了した。Catalog pending量からGateway ACK停止までのbackpressure、実効的なworker priority、R2 outageのdurable retry、versioned status CLI、実Journal truth table、executor unlink後の再起動収束、Catalog主導の`local_pruned`、sealed WALを残したreconciliation境界、`runPruneLocal`本体dry-runを検証した。最新のG5限定GPT-5.5 xhigh reviewはPASS（actionable findingsなし）である。
G6のLinux／Windows CIはPR #5の`eaf2a26`でpassした。一方、protected real R2 smokeは未実施であり、実R2への認証・接続証跡がないため、これが完了するまで全体計画はcompletedにしない。
最新のローカル検証は`mise run check`、`go vet ./...`、`go build ./...`が成功している。ローカル`go test -race`はC compiler不足のため未実行で、CI証跡へ持ち越す。
各gate完了時に、実際に変更したfile、test分類、Linux／Windows結果、real R2結果、未解決事項を追記する。
