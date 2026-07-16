# M4 Production OperationとHTTP Delivery Adapterを実装する

このExecPlanは生きた文書である。
実装と検証が進むたびに、`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective`を更新する。
リポジトリにはExecPlan方法論の`PLANS.md`がないため、この文書自体に再開に必要な実装順、停止条件、検証方法、外部環境条件を保持する。

M4の開始基準は、Pull Request #4のmerge commit `cb72752a651c88c3027b409f6f205ac9236f28b8`である。
このcommitではM3のwhole-M3 final re-auditがpassし、ordered replay、Parquet、immutable publication、read-only deliveryがmainへ統合済みである。
M4-0は本計画だけを固定し、pruning、handover、HTTP service、remote write、実機soakを開始または完了したとは扱わない。

## Purpose / Big Picture

M4が完了すると、運用者はremoteから再構築できることを独立検証したlocal WALとoutboxだけを、設定したgrace経過後に安全に削除できる。
ディスク逼迫時も証拠条件を迂回せず、削除可能なartifactがなければGatewayはdurable ACKを停止し、availability failureを明示する。
運用者は旧publisherを停止してwrite credentialを失効させ、prior claimへbindしたconditional handover artifactを経由して、より大きいpublisher epochへ一方向に切り替えられる。
複数brokerまたはsymbolはscopeごとに分離したMQL Service、Gateway process、WAL、journal、outbox、lock、credential scopeで運用し、一つのprocess内へ暗黙に多重化しない。
利用者は`ArchiveReaderV1`と同じimmutable selectorおよびfetch-plan契約を、read-onlyな`tick-api`から取得できる。
大容量raw WALまたはParquet bodyはAPIがproxyせず、検証済みmanifestから導出したobject identityを使ってR2から直接取得する。

M4の動作は、network-free fault test、想定最大rateの10倍を使うbounded load test、隔離したreal R2 smoke、1 broker・1 symbolのMetaTrader 5環境を使う24時間以上のsoakを段階的に実行して確認する。
24時間soakではprocess restart、MT5 restart、forced reboot、networkまたはR2停止、rclone timeoutとretry、disk high-water、Gateway長時間停止、publisher handoverを注入する。
最後に別cacheとread-only credentialだけを使い、raw snapshot、参照WAL object、M3 Parquet、part chain、replay chain、HTTP fetch planを再検証する。

## Progress

- [x] (2026-07-16、M4-0 plan freeze) M3のmain統合commit、現行package、CLI、設定例、WAL復旧境界、publication receipt、`ArchiveReaderV1`、`tick-api` scaffoldをread-onlyで確認した。
- [x] (2026-07-16、M4-0 plan freeze) M4をM4-1からM4-9の順序付きgateへ分け、proof-gated pruning、disk pressure、handover、multi-scope運用、HTTP adapter、fault/load、外部環境、final auditの停止条件を固定した。
- [x] (2026-07-16、M4-1完了) retention proof、local prune checkpoint、publisher handover artifact、HTTP responseのversioned contractとresource limitを固定した。Go/Python strict decoder、golden fixture、focused test、fixture verifier、ruff、diff checkを通過し、GPT-5.5 xhigh self-reviewをcleanで完了した。
- [x] (2026-07-16、M4-2完了) fresh read-only verificationからだけ削除候補を返すpure pruning plannerを実装し、claim照合、aggregate proof budget、WAL byte cap、manifest range座標、zero-record sentinelの正負テストをGPT-5.5 xhigh self-review cleanで確認した。
- [x] (2026-07-16、M4-3完了) crash-safe pruning executor、canonical proof付きcheckpoint/trash recovery、disk watermark、health、dry-run/execute CLIを実装した。strict gateway/retention scope binding、frozen plan time、scope-bound raw outboxのWAL proofを追加し、replay outbox/cacheはproof未実装のためblockedにした。non-WAL executor bypass、cross-date、bridge-only、WAL branch、filesystem child directory、completion retry境界を修正し、GPT-5.5 xhigh最終レビューをclean、repository gateをpassした。
- [x] (2026-07-16、M4-4完了) artifact/prior claim/transition/next claimのtrusted keyをfresh bounded observationで検証し、typed stop/revoke evidence、operator confirmation record、pure reconciler、artifact→transition→nextのconditional executor、same-content retryを実装した。GPT-5.5 xhigh再レビュー2件でcleanを確認した。
- [x] (2026-07-16、M4-5完了) canonical scope inventory、loopback listener/writable-root collision検査、stable supervisor plan、scope aggregate healthを実装した。flock、実Gateway net.Pipe ACK/WAL、disk/R2/publisher status failure isolationを統合テストし、GPT-5.5 xhigh再レビュー2件でcleanを確認した。
- [x] (2026-07-16、M4-6完了) `ArchiveReaderV1`だけをauthorityとするread-only `tick-api`を実装した。strict versioned API/reader config、loopback default、実体のあるnon-loopback policy hooks、bounded ListLimited/GetLimited reader capability、typed query/date、digest-only manifest/fetch-plan selector、request/response/item/concurrency/timeout/body bounds、secret/path-free error、context cancellation、graceful CLI shutdownをテストした。GET discovery、raw/replay、manifest、fetch-plan、health、404/409/502/504、policy、unknown query/body、pre-build plan limit、`Fetch`/`FetchReplay`非呼出しを確認し、GPT-5.5 xhigh再レビュー2件でcleanを確認した。
- [x] (2026-07-16、M4-7完了) network-free fault matrix、10倍rate、bounded resource、crash recovery、repository gate、Linux-equivalent Raceを完了した。loopback許可付きで8 packageをGCC 15.2.0、CGO_ENABLED=1で実行し、exit code 0、failなし、`DATA RACE`なしを確認した。Linux/Windows両workflowはrace JSON、compiler/toolchain metadataをartifact保存する。raw Race JSONの外部retentionはM4-9で再監査する。
- [ ] M4-8で隔離real R2 smokeと、1 broker・1 symbolの24時間以上の実機soakを完了する。raw smokeと`m4_real_r2_smoke`のphase分離harness、環境変数、operator手順は固定したが、real R2 handover、別read-only credential、MT5実機証跡は未実施。
- [ ] M4-9で運用手順、復旧手順、保存期限、容量見積もり、外部証拠を再監査し、M4 final auditをpassする。strict retention CLI、保存期限・容量baseline付きrunbook、未完了audit checklist、secret-free external evidence templateは追加したが、外部実行とrequired action zeroは未達である。

## Surprises & Discoveries

- 観察（M4-0 baseline時点）: `cmd/tick-api/main.go`はread-only adapterを開始しないscaffoldであった。
  根拠: M4-0 plan freeze時点のmainは固定messageを標準出力へ書くだけで、HTTP listenerまたは`ArchiveReaderV1`を構築していなかった。M4-6でstrict config、reader、HTTP serverを実装済みである。
- 観察（M4-0 baseline時点）: `cmd/tick-gateway`のusageは`init|run|status|reconcile|verify-local`だけであり、parent planに例示された`seal-wal`、`upload`、`prune-local`は未実装であった。
  根拠: M4-0 plan freeze時点のcommand switchを確認した。M4-3で`prune-local`が追加され、後続修正でstrict retention config、durable wall-clock、read-only R2 observerを接続した。remote uncertaintyは現在もfail-closedである。
- 観察: 現行`wal.Store`は起動時にsequence 1とzero chain rootから全sealed segmentを読み、全entryをmemoryへ保持する。
  根拠: `internal/wal/store_segments.go`の`loadSealedSegments`と`validateNextSegment`を確認した。
  判断: 最古segmentのfileだけを削除すると次回起動時のchain anchorが失われるため、pruningより先にversioned local checkpointとbounded retained inventoryが必要である。
- 観察: M2の`VerificationReceipt`とM3の`ReplayVerificationReceipt`はpublication時点の一致を示すが、将来のremote不変性またはlocal deletion eligibilityを証明しない。
  根拠: `protocol/v1/manifests.md`はfinal observation digestをpoint-in-time evidenceと定義している。
  判断: publisher receipt単独ではprune actionを承認せず、削除直前のread-only observationをbindした別のretention proofを要求する。
- 観察: M2 raw publisherはSQLite stageを再開情報として使い、M3 replay publisherはsealed bundleとfresh observationをauthorityにする。
  判断: pruningとhandoverは既存stageの順位をauthorityへ流用せず、検証済みinputとfresh observationから毎回planを再計算する。
- 観察: 現行ingest configは一つのproducer identity、campaign、exact source symbol、listen address、WAL root、journal pathを持つ。
  判断: M4の複数producer運用は、最初から一つのGateway processへmulti-tenant routingを追加せず、scopeごとの独立processを標準構成にする。
- 観察: `ArchiveReaderV1`はrawとreplayのlist、resolve、fetch-plan、fetch、verifyをすでに提供する。
  判断: `tick-api`は新しいselector規則を作らず、同じreader methodのboundedなHTTP表現だけを所有する。
- 観察: forced reboot、terminal history retention、credential revoke、real R2 outageはunit testだけでは証明できない。
  判断: M4-7のlocal gateがpassしてもM4全体をcompletedにせず、M4-8の外部環境証拠とM4-9 final auditを必須にする。
- 観察（M4-7初回試行）: managed sandboxにはC compilerがなく、loopback socketも許可されなかった。また、repositoryにはreal R2 handoverをold/new credential phaseで実行するCLI/testとlive MT5 runnerがない。
  判断: Raceはroot不要の一時toolchainとloopback許可付き実行でLinux-equivalent evidenceを取得したが、real R2 handover、read-only credential、24時間soakはskipやfake testで代替せず、外部gateの未完了証跡とoperator runbookをtracked artifactとして残す。

## Decision Log

- Decision: raw WAL pruningは最古のsealed segmentから連続するprefixだけに許可し、inventory途中のholeを作らない。
  Rationale: retained chainの開始anchorを一つのcheckpointで検証でき、起動時に欠落segmentを正常pruneと誤認しないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: local deletion authorityはpublisher journalまたはpublication receiptではなく、candidate identity、fresh remote observation、unique manifest coverage、grace、recovery非依存性をbindしたretention proofにする。
  Rationale: uploadの完了記録と、現在local copyを削除してよいという判断は時間と観測範囲が異なるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: pruning plannerはfilesystem write、network write、clock、SQLite mutationを持たないpure functionにし、observer、planner、executorを分離する。
  Rationale: proof不足、branch、clock regression、disk pressureを削除許可へ変換しないことをtruth tableで検証するためである。
  Date/Author: 2026-07-16 / Codex
- Decision: grace判定は注入可能なclockとdurable wall-clock watermarkを使い、時刻が後退または不明な場合はpruneを停止する。
  Rationale: process restartを越えるgraceをwall clockだけで測りながら、clock regressionで待機時間を短縮しないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: disk criticalでもproof条件を緩和せず、prunable artifactがなければreadinessを落とし、new batchへdurable ACKを返さない。
  Rationale: 容量不足を理由にraw truthを失うと、availability failureがacknowledged data lossへ変わるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: handoverはstrictly increasing publisher epoch、prior claim digest、expected next claim digest、conditional transition artifact、旧process停止確認、旧write credential revokeを要求する。
  Rationale: 二つのwriterが異なるepochで同じcampaign namespaceへ同時に書ける時間を正常状態にしないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: transition作成後に失敗した場合は旧epochへ戻さず、同じtransitionとexpected next claimを使って新epochの開始だけを再試行する。
  Rationale: rollbackが旧credentialの再有効化またはsplit brainを要求しないようにするためである。
  Date/Author: 2026-07-16 / Codex
- Decision: external gateが未実施またはharness不在の場合は、実行手順を追加してもM4 acceptance checkboxをcheckしない。
  Rationale: safe-to-run documentationはreal R2、credential revoke、実broker、forced rebootの観測事実を代替しないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: multi-broker/symbolはscopeごとのMQL ServiceとGateway processを標準にし、listen、WAL、journal、outbox、receipt、lock、credential prefixの衝突を起動前に拒否する。
  Rationale: 現行の一producer identity境界を保ち、M4で新しいmulti-tenant ACK authorityを導入しないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `tick-api`は`ArchiveReaderV1`のread-only adapterに限定し、任意R2 key proxy、upload、delete、publisher操作、local pruning操作を公開しない。
  Rationale: 配信credentialからarchive mutation能力と任意object列挙能力を除くためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `tick-api`のdefault bindはloopbackまたはprivate networkとし、non-loopback bindには認証、rate limit、短期credential発行方針を明示したdeployment profileを必須にする。
  Rationale: read-onlyであってもdataset metadataとfetch planを無制限にInternet公開しないためである。
  Date/Author: 2026-07-16 / Codex
- Decision: Cloudflare Worker、large object proxy、active late-history audit、strategy固有JSONL、Rust rewriteはM4の既定scopeに含めない。
  Rationale: M4はproduction fault toleranceと既存delivery contractのHTTP写像を受け入れるmilestoneだからである。
  Date/Author: 2026-07-16 / Codex

## Outcomes & Retrospective

M4-0の成果は、M3対象外だったproduction operationを実装可能な順序へ分解し、local gateだけではM4を完了できないことを固定したことである。
M4-0ではruntime、Protocol、fixture、CLI、workflow、remote stateを変更していない。

M4-3の終了時には、dry-runとexecuteが同じcanonical plan digestを使い、crash後にcheckpointとtrash inventoryから安全に再開できなければならない。
M4-4の終了時には、old token active、old process running、prior claim mismatch、epoch regression、transition conflictのいずれかでremote actionがzeroにならなければならない。
M4-6の終了時には、CLIとHTTPが同じimmutable manifestを選択し、同じfetch object key、digest、sizeを返さなければならない。
M4-7の終了時には、local correctnessとresource boundを証明しても、real broker、forced reboot、credential revoke、real R2の外部証拠は未完了として残す。
M4-9の終了時には、M4-8の24時間以上の実測証拠を含むfinal auditがpassしなければM4全体をcompletedにしない。

## Context and Orientation

`internal/wal`はactive WALのappend、sync、seal、sealed inventory、hash-chain recoveryを所有する。
`internal/archive`はverified sealed WALをbyte-exactなcontent-addressed local outboxへpromoteし、raw/replay manifestを検証する。
`internal/r2`はpublisher claim、immutable raw/replay publication、bounded observation、receipt、campaign/epoch lockを所有する。
`internal/delivery`は`ArchiveReaderV1`とraw/replayのselector、fetch plan、cache、verificationを所有する。
`internal/ingest`は一つのconfigured producer scopeのTCP session、WAL先行記録、journal、cursor、ACK、metricsを所有する。
`cmd/tick-gateway`はGateway lifecycleとoperator commandの入口であり、`cmd/tick-api`はstrict configから`ArchiveReaderV1`を構築するread-only HTTP adapterである。M4-0時点のscaffold観察は上記のhistorical noteであり、現在状態ではない。

Retention proofとは、特定local artifactをremoteと別cacheから復元可能であることを、削除前のfresh observationとmanifest graphで示すcanonical recordである。
Prune checkpointとは、削除済みWAL prefixの最後のsequence、chain root、object digest、canonical retention proof本体とそのdigestを記録し、次のretained segmentの検証開始anchorにするlocal durable recordである。
Prune planとは、observerが集めたverified factsだけからplannerが作る、stable artifact ID順のdry-run可能な削除候補列である。executeはdry-run出力のfrozen wall-clock valueとdigestを再入力し、時刻の進行だけでplan identityが変わらないようにする。
Handover artifactとは、prior publisher claimとexpected next publisher claimをbindし、旧writer停止とcredential revokeのoperator evidenceを参照するimmutable conditional objectである。
Disk watermarkとは、filesystem容量をnormal、high、critical、emergencyへ分類する設定値であり、proof条件を変更せず運用actionとhealthを決める。

M4のtrust boundaryでは、caller supplied path、SQLite stage、publication receiptの存在だけ、古いremote observation、remote list順序、wall clock単独、disk pressure単独を削除またはhandover authorityにしない。
Trusted inputは、configured rootから導出したpath、再検証済みlocal bytes、bounded fresh remote observation、strict manifest graph、canonical retention proof、durable clock watermark、OSが返すcurrent filesystem identityである。

## Scope and Non-Goals

M4の実装対象はproof-gated local WAL/raw-outbox pruning、disk pressure、publisher handover、scope分離した複数producer運用、read-only `tick-api`、運用health、fault injection、24時間以上の実機soakである。
Local raw WAL/raw-outbox pruningはParquet生成に依存させない。replay outboxまたはcacheの削除は対応するreplay proofが実装されるまでblockedとし、WAL/raw proofを流用しない。
Local Parquetまたはreplay outboxを削除する場合は、raw retention proofとは別にreplay manifest、全part、Parquet hash、read-only verificationをbindしたproofを要求する。

Cloudflare Workerは正しさの必須要素にしない。
`tick-api`はlarge raw/Parquet body、generic key GET、R2 write、publisher control、prune controlをproxyしない。
能動的late-history discoveryは別campaignまたはfollow-upとし、M4 acceptanceへ暗黙に含めない。
Canonical JSONL export、DuckDB integration、strategy evaluation、新producer schema、Rust adapterは対象外である。
Goが10倍rateの測定基準を満たさない場合だけprofileをDecision Logへ記録し、Rust adapterまたはGateway構成を別計画として再評価する。

## Plan of Work

### M4-0 plan freeze and baseline inventory

M4-0ではM3 merge commit、dirty worktree、既存package、設定、CLI、workflow、M3 acceptance evidenceをread-onlyで確認する。
Parent ExecPlan、M3 ExecPlan、roadmap、本ExecPlanでM3 completed、M4 planned、M4 implementation not startedを同じ意味にする。
M4-0ではcode、Protocol、fixture、remote、credential、service、commit、pushを変更しない。

### M4-1 operational contracts and limits

M4-1ではremoteに残るhandover artifactのcanonical JSON、domain digest、key layout、strict decoderを`protocol/v1/`、Go、Python、golden fixtureで固定する。
Handover contractはdataset、campaign、scope key、prior epoch、next epoch、prior claim keyとdigest、expected next claim keyとdigest、transition key、operator evidence digestを持ち、credential value、endpoint、local path、自由形式secretを持たない。
Next epochはprior epochより大きくし、same epoch、regression、wrong scope、wrong key、zero digest、unknown field、duplicate field、noncanonical bytesを拒否する。

Local retention proofとprune checkpointはProtocol wire contractへ混ぜず、`internal/retention`のversioned canonical schemaとして固定する。
Retention proofはartifact kind、trusted relative path、size、content digest、WAL rangeまたはreplay identity、remote object observation、covering manifest keyとdigest、verification report digest、observed wall time、grace not-before、proof limitsを持つ。
Prune checkpointは連続prefixのend sequence、chain root、最後に削除したsegment digest、retention proof digest、previous checkpoint digestを持つappend-only chainにする。

HTTP contractは`docs/api/tick-api-v1.md`へendpoint、query、status、JSON field、error code、pagination、limit、timeout、cache、credential boundaryを固定する。
API responseはversion fieldを持ち、digestをlowercase hexadecimal、sizeとrevisionをJSON integer、object配列をcanonical stable orderで返す。
HTTP JSONのbyte-level canonical一致は要求しないが、同じ`ArchiveReaderV1` valueから同じ意味のresponseを返すcontract testを要求する。

M4-1で`MaxPruneCandidates`、`MaxProofObjects`、`MaxProofBytes`、`MaxManifestNodes`、`MaxHandoverObservationRequests`、`MaxHandoverObservationBytes`、`MaxAPIRequestBytes`、`MaxAPIResponseItems`、`MaxConcurrentRequests`、`RequestTimeout`を設定とcontractへ固定する。
Zeroまたはoverflowするlimit、limitを超えるmanifest graph、incomplete pagination、short read、unknown sizeはfail closedにする。

M4-1はfixture、Python、focused Go、strict negative case、`git diff --check`が成功するまでM4-2以降を開始しない。

### M4-2 fresh retention observer and pure pruning planner

M4-2では`internal/retention`にlocal inventory、read-only remote observer、manifest coverage verifier、pure plannerを実装する。
Observerは候補fileをconfigured rootから列挙し、symlinkとroot escapeを拒否し、size、digest、WAL trailer、sequence、chain anchor、outbox identityを再検証する。
Remote observerはread-only backendだけを受け取り、claim、raw object、raw-day revision graph、replay object、part graphをaggregate request/byte budget内でfreshに読む。

Raw segmentは、byte-identical remote raw objectがExactで、全accepted batchがbranchのない一意なhighest valid raw-day manifest集合から発見可能で、独立day/campaign verification reportが一致する場合だけproof候補になる。
Manifest coverageはsegment単位の存在確認ではなく、各accepted batch sequenceとselected rangeが少なくとも一つのaccepted raw snapshotから復元可能であることを検証する。
Branch、duplicate revision、manifest gap、claim mismatch、Different、Ambiguous、Unavailable、Oversizedはcandidate zeroにする。

Plannerはretention facts、current clock、durable clock watermark、active recovery floor、configured grace、disk classだけを入力にするpure functionとする。
WAL planは最古のsealed segmentから連続するeligible prefixだけを返し、active WAL、cursor recoveryに必要なsegment、grace前、proof不足を飛び越えない。
Raw outbox、replay outbox、cacheはartifact kindごとに別policyとproofを使い、WAL planの成立をParquetへ依存させない。
Disk classは実行優先度と最大batch数だけを変え、eligibility predicateを変えない。

同じfactsから同じcandidate order、同じplan canonical bytes、同じplan digestを返すことをproperty testとstateful testで確認する。
Candidate listの順序が変わってもstable artifact IDで同じplanになり、unknown candidate、duplicate identity、path collisionを拒否する。

### M4-3 crash-safe executor, disk pressure, and operator CLI

M4-3ではplannerが返したstable artifact IDだけを受けるnarrow executorを実装する。
Executorはcaller supplied pathを受けず、planとtrusted rootsから対象pathを再導出する。
削除直前にfile type、root containment、size、digest、WAL metadata、proof digest、plan digestを再検証し、local mutationがあれば何も削除しない。

WAL prefixの各stepは新checkpointをtemporary fileへwrite、sync、closeし、no-clobberでpublishしてdirectory durabilityを確認した後、対象segmentを同一filesystemのtrashへatomic renameする。
Crash recoveryはpublished checkpointとtrashを照合し、checkpointがないtrash、checkpointと異なるfile、retained inventoryのholeをintegrity stopにする。
Delete failureは再試行可能なavailability failureとしてtrashを保持し、対象をprunedとして重複計上しない。
Startup loaderはlatest valid checkpoint chainを検証し、そのend sequenceとchain rootからretained sealed WALを連続検証する。

`internal/ingest`へfilesystem usage providerとdisk state machineを追加する。
High watermarkではseal、publish、verify、pruneのoperator actionを要求し、criticalではreadinessをfalseにしてnew workを抑止する。
Emergencyまたは実際のappend/sync failureではACKを返さず、WALをpoisoned/unavailableとしてintegrityを保つ。
Disk spaceの回復だけでpoisoned WALを自動継続せず、close、reopen、WAL verificationを要求する。

raw outboxの実装対象では、`prune-completions/`をartifact inventoryから明示的に除外し、unlink前に
canonical retention proofを含むdurable completion recordをno-clobber publishする。再起動後はsourceが
不在（またはunlink前の同一bytes）であること、completionのartifact/proof identity、current planの
action identityを検証してからだけ同じactionをidempotentに完了扱いにする。completionの元plan digest
は監査用に保持する。recovered raw proofのremote object keyとcovering manifest keyはcurrent
immutable scope/date prefixへ再bindする。completion metadataが欠落・改変・source差し替えの場合はintegrity stop
とし、replay outbox/cacheにはこの例外を拡張しない。

意図した最終operator interfaceでは、`tick-gateway prune-local --config <path> --retention-config <path> --dry-run`がdefault動作としてcanonical planと理由、`plan_current_wall_time_unix_ms`を表示する。実削除は同じ値を`--plan-time-unix-ms`へ凍結して`--execute --plan-digest <sha256>`を要求する。dry-run後にfactsが変わればdigest mismatchで停止する。
現行CLIはstrict retention configからtrusted scope/layoutを作り、bounded remote observation、manifest coverage、retention proofを候補へ注入する。read-only R2 credential、durable wall-clock、dry-run digestが揃わない場合はfail-closedであり、隔離real R2でのoperator再検証が完了するまでproduction実行済みとは扱わない。
`tick-gateway status`はdisk class、free bytes、WAL bytes、oldest retained sequence、prunable bytes、blocked reason、source lagをsecretなしで返す。fresh remote proofを持たないstatus probeでは`prunable_bytes`をzeroとし、削除可能量のauthorityにしない。

M4-3ではcheckpoint publish前、publish後、trash rename後、unlink前、unlink後、directory sync失敗を注入し、再起動後の結果がretainまたは一回だけのpruneになることを確認する。

### M4-4 publisher handover

M4-4では`internal/r2`にhandover sealer、bounded observer、pure reconciler、conditional executorを実装する。
Handover sealerはtrusted `r2.Layout`からprior claim key、transition key、next claim keyを導出し、caller supplied remote keyをcanonical identityへ含めない。
Observerはprior claim、既存transition、next claim、candidate namespaceをbounded fresh readし、Absent、Exact、Different、Ambiguous、Unavailable、Oversizedを区別する。

Operator flowはpreflight、旧process drain、旧process停止確認、旧write credential revoke確認、transition conditional create、transition Exact再観測、next claim conditional create、new publisher startの順にする。
停止確認とcredential revokeは自動推測せず、adapterが返すtyped evidenceとoperator confirmation recordの両方を要求する。
Credential evidenceはprovider credential IDのdigest、revoked-at、scope digestだけを持ち、token valueを保存しない。

Reconcilerはold process running、old token active、prior claim mismatch、next epoch regression、transition Different、next claim Different、candidate namespace ambiguityでzero actionを返す。
TransitionがExactでnext claimがAbsentの場合だけexpected next claimのconditional createを承認する。
Same-content retryは同じtransitionとclaimを受理し、transition作成後のcrashからnew epochだけを再開する。
New claimがExactになった後はold epochへのrollback actionを提供しない。

Fake backendではold writer race、revoke失敗、transition timeoutのunknown outcome、same-key different-content、old process再出現、next claim collision、restart after every stepを検証する。
隔離real R2では二つのscope-limited credentialを使い、旧credential revoke後に旧writerのwriteが失敗し、新writerだけがimmutable publicationを継続できることをM4-8で確認する。

### M4-5 multi-scope operation and health

M4-5では一つのinventory fileから複数の独立Gateway configを検証し、service manager用の起動単位とstatus集約を生成する薄いoperator layerを実装する。
各scopeは一つのprovider、stable feed、exact source symbol、campaign、publisher epoch、listen address、WAL root、journal path、outbox root、lock root、receipt rootを持つ。
二つのscopeがlisten address、writable root、scope key、publisher identity、credential prefixを共有する場合は起動前に拒否する。

MQL Serviceはscopeごとに一つ起動し、built-in TCPだけで対応するloopback listenerへ接続する。
一つのMQL Serviceが複数symbolを混ぜず、一つのGateway sessionが別scopeのproducer identityを受理しない現行境界を維持する。
Supervisor exampleはWindows serviceまたはoperator scriptの責務を説明するが、secretをrepositoryへ置かない。

Aggregate statusはscopeごとのlast durable source time、current source time、uncommitted lag、Gateway downtime、WAL free space、terminal synchronization、oldest retrievable tick、publisher epoch、last verified snapshotを返す。
一つのscopeのdisk、network、R2、publisher failureが別scopeのACK pathまたはlockをblockしないことをintegration testで確認する。

### M4-6 read-only tick-api

M4-6では`cmd/tick-api`を`internal/delivery.ArchiveReaderV1`へだけ依存するGo HTTP serviceとして実装する。
Endpointは次を提供する。

    GET /v1/datasets
    GET /v1/datasets/{dataset}/campaigns
    GET /v1/snapshots/raw
    GET /v1/snapshots/replay
    GET /v1/manifests/{sha256}
    POST /v1/fetch-plans
    GET /v1/health

Raw/replay listはdataset、campaign、date、replay contract、conversionをtyped queryとして検証し、bounded paginationとstable orderを使う。
Manifest endpointは任意keyを受けず、strict SHA-256 selectorからverified manifest inventoryを解決し、canonical manifest bytesとidentity metadataだけを返す。
Fetch-plan endpointはrawまたはreplayのimmutable manifest selectorを受け、`ArchiveReaderV1`が構築したrelative key、digest、size、credential scopeだけを返す。

API processは`r2.ReadBackend`相当のread-only capabilityだけを構築し、`S3Backend`、rclone writer、publisher、handover、retention executorをimportしない。
Responseは`Cache-Control: no-store`、`X-Content-Type-Options: nosniff`、request IDを持ち、secret、endpoint credential、local cache path、absolute pathを含めない。
Request body、query length、response item count、concurrency、reader bytes、timeoutをboundedにし、client cancelをdownstream contextへ伝播する。

Loopback profileは認証なしを許可する。
Non-loopback profileは実体のあるauthentication middleware、rate limit hook、trusted proxy設定、短期path-scoped credential providerがない場合に起動を拒否する。各hookは起動時のattestation booleanではなく、request pathで実際に実行されるadapterでなければならない。
M4のrepository acceptanceではfake credential providerを使い、実Internet公開またはCloudflare Worker deploymentを要求しない。

HTTP contract testはCLIとAPIが同じmanifest selectorから同じmanifest digest、revision、object keys、object digests、sizesを返すことを確認する。
Malformed selector、branch、duplicate revision、unknown manifest、oversized request、reader timeout、client cancelでwrite actionがzeroであることを確認する。

### M4-7 network-free fault, load, and repository gates

M4-7ではfake producer、fake read backend、fake conditional backend、fake credential revoker、fake clock、fake filesystem usageを使うend-to-endを追加する。
End-to-endはingest、WAL seal、M2 raw publication、M3 replay publication、independent verification、retention proof、dry-run、prune、restart、HTTP discovery/fetch planを一つのidentity graphで接続する。

Fault matrixはprocess crash、partial WAL、checkpoint crash、delete failure、clock regression、remote branch、manifest gap、publisher conflict、handover unknown outcome、credential revoke failure、R2 outage、rclone timeout、disk high-water、HTTP timeoutを含む。
各faultでACK済みentry loss、unproven delete、split brain、arbitrary key exposure、unbounded retryがないことを確認する。

想定最大rateは運用configにrecords/secondとaverage frame bytesで明示する。
Fake producerはその10倍を一定時間入力し、memory、goroutine、channel、WAL bytes、ACK latency、GC pause、source lag、recovery timeを測定する。
Pass条件はmemoryとqueueが設定上限内、WAL recoveryが完全、Parquet/R2停止がTCP ingestをdisk capacityより前に直接blockしない、critical diskでACKが安全に停止することである。
測定時間、hardware、Go version、config、最大値、p95/p99をverification recordへ残す。

Repository gateはfocused Go、`mise run fixture`、`mise run test-python`、`mise run check`、`mise exec -- go vet ./...`、空の`gofmt -l cmd internal`、`git diff --check`、Linux相当のrace、Windows Race workflowを含む。
Long testとdestructive simulationには明示的なbuild tagまたはenvironment enableを要求し、通常の`mise run check`を24時間blockしない。

### M4-8 isolated real R2 and live MT5 soak

M4-8は外部環境gateであり、隔離bucketまたはprefix、read/write credential、別read-only credential、handover用の二つのwriter credential、pinned rclone、Windows/MT5、実broker接続、forced rebootを許可できるhostを要求する。
必要なcredentialまたは明示確認がない場合はskip理由を記録できるが、M4全体は未完了のままにする。

Real R2 smokeはproduction `v1/`を使わず、synthetic dataset専用scopeでimmutable raw/replay publication、read-only fetch、same-content retry、different-content rejection、timeout recovery、handover、old token revokeを確認する。
Bucket lock対象でdeleteできないproduction prefixをdestructive fault testへ使わない。

Live soakは1 broker・1 exact symbol・1 MQL Service・1 Gatewayから開始し、連続24時間以上を一つのrun identityで記録する。
注入eventはGo process restart、MQL Service restart、MT5 terminal restart、forced OS reboot、network/R2停止、rclone timeout/retry、disk high-water、Gateway長時間停止、publisher handoverを含む。
各eventは開始時刻、終了時刻、期待状態、observed status、operator action、recovery rootを記録する。

Soak後は別hostまたは少なくとも空の別cacheとread-only credentialだけを使い、raw-day manifest、全参照sealed WAL object、campaign chain、replay manifest、part manifest、Parquet schema/hash/row-chain、API fetch planを検証する。
ACK済みdata loss、unexplained chain gap、manifest branch、publisher split brain、proofなしprune、unbounded resource、secret露出が一つでもあればM4-8はfailとする。
Terminal historyがGateway停止期間を回収できなかった場合は事実をsource errorまたはgapとして記録し、synthetic Tickで埋めず、停止時間の運用上限を更新する。

### M4-9 runbooks and final audit

M4-9では`docs/operations/`へday operation、disk pressure、pruning、restore、handover、credential rotation、R2 outage、forced reboot、soakのrunbookを追加する。
Runbookはprecondition、read-only preflight、command、expected output、stop condition、rollback不可のstep、recovery、escalationを持つ。
保存期限はraw WAL、raw outbox、replay outbox、cache、receipt、checkpoint、diagnostic logごとに既定値、最小grace、容量見積もりを定義する。

Final auditはM4-1からM4-8のcontract、implementation、fault、resource、external evidence、scope exclusionを再監査する。
Audit receiptは`delivery_status: completed`と`final_audit: pass`を、required actionがzeroの場合だけ記録する。
Real R2または24時間live soakがskip、短縮、失敗の場合は`delivery_status: incomplete`を維持する。

## Concrete Steps

作業開始時にbaselineとdirty stateを記録する。

    cd /home/akira/proj/utaki-tick-data-platform
    git status --short --branch
    git rev-parse HEAD
    git diff --check
    git diff --cached --check

M4-1ではcontract文書、fixture、strict decoderから先に実装する。

    mise exec -- go test ./internal/protocol ./internal/retention ./internal/r2 -count=1
    mise run fixture
    mise run test-python

M4-2とM4-3ではretentionのfocused testを実行する。

    mise exec -- go test ./internal/retention ./internal/wal ./internal/archive ./internal/delivery -count=1
    mise exec -- go test ./internal/retention -run 'Prune|Retention|Checkpoint|Clock|Disk' -count=1

M4-4ではhandoverのfocused testを実行する。

    mise exec -- go test ./internal/r2 -run 'Handover|PublisherClaim|Conditional|Credential' -count=1

M4-5とM4-6ではmulti-scopeとHTTPのfocused testを実行する。

    mise exec -- go test ./internal/ingest ./internal/operations ./internal/delivery ./internal/httpapi ./cmd/tick-api -count=1

Package名は実装時に責務を確認して確定する。
`internal/operations`または`internal/httpapi`を追加しない場合は、同じ責務を置いた実packageへcommandを更新し、このExecPlanのInterfaces and Dependenciesへ判断を記録する。

Repository gateを実行する。

    mise run check
    mise exec -- go vet ./...
    mise exec -- gofmt -l cmd internal
    git diff --check
    git diff --cached --check

Race gateはM4で追加したpackageを含める。

    mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/retention ./internal/operations ./internal/httpapi -count=1

LinuxまたはWindows workflow実行時は、それぞれ`linux-race-<run_id>`または`windows-race-<run_id>` artifactのrace JSON、commit/ref、runner、Go/CGO/CC情報を保存し、成功結果をM4-7 evidenceへ添付する。Linux workflowは`build-essential`をrunner上で導入する。

10倍rate testは通常testと分離し、明示的なenableと出力先を要求する。

    TICKDATA_ENABLE_LOAD=1 mise exec -- go test ./internal/ingest ./internal/retention -run 'TenX|DiskPressure' -count=1 -v

Real R2 smokeは既存の安全条件にhandover専用credential条件を追加して実行する。raw smokeは
`real_r2_smoke`、handoverは`m4_real_r2_smoke`の`prepare`/`verify` phase分離harnessを使う。
environment variable名とcommandは`docs/operations/real-r2-smoke.md`へ固定し、secret valueを
ExecPlanまたはtest logへ書かない。

Live soakは短いcommand例だけで開始せず、M4-8 runbookのpreflight checklist、run identity、event schedule、保存先、abort条件をoperatorが確認してから実行する。

## Validation and Acceptance

M4-0の受入条件は、本ExecPlanがPurpose、Progress、Surprises & Discoveries、Decision Log、Outcomes & Retrospective、Context and Orientation、Scope and Non-Goals、Plan of Work、Concrete Steps、Validation and Acceptance、Idempotence and Recovery、Artifacts and Notes、Interfaces and Dependencies、Revision noteを持ち、単独でM4-1を開始できることである。

M4-1の受入条件は、handover remote identity、retention local identity、HTTP semantics、resource limits、negative classificationが文書、Go、Pythonまたはcontract testで一致することである。
M4-2の受入条件は、fresh Exact proofとunique coverageがあるcandidateだけをpure plannerが返し、proof不足、branch、clock regression、recovery依存segmentではaction zeroになることである。
M4-3の受入条件は、dry-runのplan digestとfrozen wall-clockをexecuteへ再入力したdigestが一致し、各crash point後にACK済みchainを保持したままretainまたは一回だけのprefix pruneへ収束することである。
M4-4の受入条件は、旧process停止と旧credential revokeなしではtransitionを作らず、transition後はexpected next epochへだけ再開できることである。
M4-5の受入条件は、二つ以上の独立scopeがresourceとfailureを共有せず、設定collisionを起動前に拒否することである。
M4-6の受入条件は、HTTPとCLIが同じimmutable snapshotとfetch planを返し、API processがwrite capabilityまたはarbitrary key proxyを持たないことである。
M4-7の受入条件は、fault matrix、10倍rate、repository check、vet、format、diff、Raceがpassし、resource測定記録が残ることである。
M4-8の受入条件は、隔離real R2 handoverと24時間以上のlive MT5 soakがpassし、別cache/read-only credential verificationが成功することである。
M4-9の受入条件は、runbookと保存期限が検証環境で実行され、final auditのrequired actionがzeroになることである。

M4全体はM4-1からM4-9がすべて完了した場合だけcompletedとする。
Network-free gateだけのpass、Real R2 skip、24時間未満のsoak、fault未注入、read/write credentialを使った最終verificationはM4完了証拠にしない。

## Idempotence and Recovery

Retention observationとplanは副作用を持たず、同じverified factsから同じplan digestを生成する。
Dry-runはfilesystemを変更せず、何度実行しても同じfactsなら同じ候補とblocked reasonを返す。
Executorはplanにないpathを削除せず、各対象を削除直前に再検証する。

Checkpoint publish前のcrashではsegmentを保持する。
Checkpoint publish後かつtrash rename前のcrashでは、再開時にcheckpointが示すsegmentを再検証してrenameを継続する。
Trash rename後のcrashでは、checkpointとtrash digestが一致する場合だけunlinkを再試行する。
Checkpoint chain、retained chain、trashのどれかが矛盾すれば自動repairせずintegrity stopにする。

Outbox cleanupはcontent-addressed object単位でidempotentにし、missing fileは対応するcompleted prune eventとproofがある場合だけ成功済みとして扱う。
Unknown missing file、different bytes、symlink、root escapeはintegrity stopにする。
Cache evictionはarchive truthまたはretention proofにしない。

Handover preflightとobservationは再実行可能である。
Same transition bytesとsame next claim bytesのconditional retryは成功扱いにできる。
Different transitionまたはclaim bytes、old credential active、old process runningは停止する。
Transition作成後のcrashでは旧epochを再開せず、expected next claimの作成とExact確認から続ける。

`tick-api`はread-onlyであり、request retryはremote mutationを起こさない。
List responseのpagination tokenはscope、filter、stable last key、expiryへbindし、別queryへの再利用を拒否する。
Reader timeoutまたはclient cancelはpartial success responseを返さず、typed errorとrequest IDを返す。

Disk pressureからの回復はproofなしdeleteで行わない。
空き容量が回復してもWAL append/sync failure後はprocess restartとWAL verificationを要求し、未検証のままACKを再開しない。

## Artifacts and Notes

M4で追加するtracked artifactはcontract文書、source、test、small fixture、設定例、runbook、synthetic verification recordである。
実Tick、WAL、SQLite、Parquet、R2 object、credential、rclone config、token IDの原文、host固有absolute pathをcommitしない。

Verification recordにはrun identity、commit、tool versions、platform、config digest、synthetic scope、開始/終了時刻、fault event、result、artifact digestを記録する。
実broker名、account ID、credential ID、endpointに機密性がある場合はstable redacted digestだけを記録する。
Log excerptはsecret scannerを通し、自由形式exceptionにcredentialまたはquery tokenが含まれないことを確認する。

M4-8の24時間soakで生成する大容量logとdataはrepository外に置き、tracked summaryから保存場所、hash、retention期限を参照する。
外部artifactが期限切れになる場合は、最終audit receiptが何を再現できなくなるかをrunbookへ明記する。

## Interfaces and Dependencies

`internal/retention`は次と意味的に等価な境界を持つ。

    type Observer interface {
        Observe(ctx context.Context, request ObservationRequest, budget *Budget) (RetentionFacts, error)
    }

    type Planner interface {
        Plan(facts RetentionFacts, policy Policy, now TimeEvidence) (PrunePlan, error)
    }

    type Executor interface {
        Execute(ctx context.Context, sealed SealedPrunePlan) (ExecutionReport, error)
    }

Observerはremote write backend、publisher journal、prune executorを受け取らない。
Plannerはcontext、filesystem、network、SQLite、randomnessを受け取らない。
Executorはcaller supplied path、remote key、credential、unsealed candidateを受け取らない。

`internal/wal`はpruned prefixを表すcheckpoint anchorからretained segmentを検証できる境界を追加する。
Checkpointを指定しない既存WALはsequence 1とzero rootから始まる現行挙動を維持する。
Checkpoint writerとloaderは同じcanonical bytesとdigestを使い、old versionまたはunknown fieldを黙って無視しない。

Handoverは次と意味的に等価な境界を持つ。

    type CredentialRevoker interface {
        Status(ctx context.Context, credentialID CredentialRef) (CredentialStatus, error)
        Revoke(ctx context.Context, credentialID CredentialRef) (RevocationEvidence, error)
    }

    type ProcessStopVerifier interface {
        ObserveStopped(ctx context.Context, instance RuntimeInstanceRef) (StopEvidence, error)
    }

    type HandoverReconciler interface {
        Reconcile(bundle HandoverBundle, observation HandoverObservation) (HandoverDecision, error)
    }

Credential adapterはtoken valueを返さず、statusとredacted stable identityだけを返す。
Reconcilerはadapter、clock、backend、event storeを直接呼ばない。
Executorはapproved transition IDまたはnext claim IDだけを扱い、任意remote keyを受け取らない。

`tick-api`は`internal/delivery.ArchiveReaderV1`とclock、logger、authenticator、rate limiterだけに依存する。
HTTP handlerへ`r2.ObjectBackend`、`r2.S3Backend`、`r2.Publisher`、`r2.ReplayPublisher`、retention executorを渡さない。
Manifest bodyまたはfetch planを作るために必要なremote readは`ArchiveReaderV1`内の既存bounded verificationを通す。

新しい外部Go dependencyは、標準`net/http`では満たせない具体的要件がある場合だけ追加する。
Dependencyを追加する場合は現行Go toolchain、Windows、Race Detector、license、module checksumをM4 ProgressとDecision Logへ記録する。
Credential provider SDKはcore contractへ直接埋め込まず、small adapterの後ろへ置く。

## Revision note

改訂記録（2026-07-16 M4-0-PLAN-FREEZE）: M3のmerge済みbaseline `cb72752a651c88c3027b409f6f205ac9236f28b8`から、production operationとHTTP delivery adapterの専用ExecPlanを作成した。

Proof-gated pruningはpublisher receiptの存在だけに依存せず、fresh read-only remote observation、unique manifest coverage、grace、recovery非依存性、contiguous prefix checkpointを要求する。

Publisher handoverはstrictly increasing epoch、prior claim digest、conditional transition、old process stop、old credential revoke、expected next claimへbindする。

M4-0はdocs-onlyであり、runtime、Protocol、fixture、remote R2、credential、service、24時間soak、commit、pushを実行しない。

M4全体はnetwork-free gateだけでは完了せず、隔離real R2、24時間以上のlive MT5 soak、別cacheとread-only credentialによる最終verification、M4 final audit passを要求する。

2026-07-16 M4-7/M4-8/M4-9 update: network-free evidence、Linux-equivalent Race pass、strict retention CLI、durable wall-clock、保存期限・容量baseline、
phase分離real R2 handover harness、external R2/MT5 runbook、M4 external gate record、secret-free
external evidence template、final audit checklistを追加した。real R2 handover、read-only credential、
24時間soakが未実施で、Race raw artifactのdurable retentionもfinal audit待ちのため、delivery statusはincompleteのまま維持する。
