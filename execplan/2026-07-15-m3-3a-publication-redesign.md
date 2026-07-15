# M3-3A Replay Publication Redesign

このExecPlanは生きた文書である。
リポジトリにはExecPlan方法論の`PLANS.md`がないため、この文書自体に再開に必要な前提、設計、手順、検証方法を保持する。
実装と検証が進むたびに、`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective`を更新する。

M3-2Gを受け入れ済みの前提にして、現行M3-3A publication実装を一部破棄し、再設計したpublication境界を段階的に実装する。

このR0では文書だけを変更し、Go実装、Python fixture、golden生成、DB schema変更、M3-4 delivery、remote R2操作、commit、pushを行わない。

## Purpose / Big Picture

利用者は、検証済みM2 raw-day snapshotから、同じreplay bundleと現在のremote観測を再現可能な手順で評価し、Parquet、part manifest、replay manifest、verification receiptを不変に公開できるようになる。

今回の再設計では、upload順序をjournal stageの数値に置かず、完全なremote観測を純粋なreconcilerへ渡して、許可されたactionだけをexecutorへ渡す。

最終receiptは処理が完了したという記録ではなく、boundedな一回のfinal observationがbundleと一致したという証拠になる。

旧M3-3Aは実装量を削減するために破棄するのではなく、remote状態をstageと混ぜたpolicy couplingを除去し、検証済みbundle、観測、純粋な判断、実行、event、receiptを別境界へ置くために再構成する。

## Progress

- [x] (2026-07-15) Advisor preflight passとM3-2G acceptedを前提として、現行M3-3Aのstage、claim、lock、bounded backend、receipt境界を調査した。
- [x] (2026-07-15) 旧M3-3Aの初回実装、review correction、advisor BLOCK remediationの証拠を、accepted evidenceではなくbehavior/failure inventoryへ分類した。
- [x] (2026-07-15) bundle digest、final observation digest、observation class、pure reconciler、bounded resource、非権威のappend-only diagnostic event store、fault matrix、R1からR7の順序をこの文書へ固定した。
- [x] (2026-07-15) Protocol V1 manifest、hash domain、parent ExecPlan、M3 living ExecPlan、roadmapへM3-3A reopenとM3-4 blockを反映した。
- [x] (2026-07-15、G0 read-only inventory) branch `agent/m3-replay-parquet-delivery`、full HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 24、untracked 26、staged 0を確認した。
  `git diff --check`と`git diff --cached --check`は成功し、remote I/OとGitHub CI確認は行わなかった。
- [x] (2026-07-15、G0A docs alignment) M3-1とM3-2G、四正本文書、境界縮小して再利用するprimitive、useful-case移行後に置換削除する旧M3-3A runtime、workflow定義を分類した。
  G0Aは四文書だけを更新し、コード、test、fixture、DB、remote、commitを変更しなかった。
- [x] (2026-07-15、G1-R1) Protocol V1 canonical bundle、complete final observation、二つのdomain digest、10個のresource limit、M2 claim relationshipをGoとPythonで独立実装した。
  Golden fixtureは13件のnegative classificationを固定し、`mise run fixture`は23 fixtures、`mise run test-python`は33 tests、focused Goは`internal/protocol`と`internal/archive`で成功した。
- [x] (2026-07-15、G2-R2) local bundle sealerとfilesystem、network、clock、SQLite、journal、event、rcloneから独立したpure reconcilerを実装し、decision truth tableを通した。
  Sealerとreconcilerのfocused test、`internal/r2`全test、gofmt、targeted diff、両diff checkを新設計の受入証拠として使用した。
- [x] (2026-07-15、G3-R3) `ObjectBackend`をembedしないreplay専用bounded read interface、共有aggregate budget、fresh observer、part graph verifier、replay revision graph verifierを実装した。
  Timeout、short read、stale list、incomplete pagination、duplicate、negative size、unknown key、missing candidate、check mismatch、branch、missing predecessor、lock failureのnegative testとcomplete final observation生成testを成功させた。
- [x] (2026-07-15、G4-R4) sealed ObjectIDだけを解決するnarrow executor、copy直前のlocal snapshot再検証、`copyto --immutable`と`check --download`だけのtool seam、非権威のappend-only diagnostic event storeを実装した。
  Approved action、unknown／mismatched ID、local mutation、timeout、collision、check mismatch、allow-list、missing event、idempotent duplicate、conflicting duplicate、unknown event／action／object、digest mismatch、secret／local-path exclusionのtestを成功させた。
- [x] (2026-07-15、G1E contract correction) R5途中監査でR1のreplay edgeがzero rootsの根拠manifestを保持せず、R2のlock前見積りがedge stringsとescaped final observation bytesを含めないことを確認した。
  R1を再開して各edgeへtrusted `full_key`、strict `canonical_json`、`part_count`を固定し、empty terminal／empty predecessorの正例とzero／mixed／unproven／mismatchの負例をGoとPythonへ追加した。
  R2 sealerはdigestとlockの前に全edge aggregateをoverflow-safeに見積もり、trusted `r2.Layout`だけからfull keyを導出する。
  G5Cはpartial G5由来の4 compile errorだけをforward-fixし、M2-only remote claim writeとM3 Exact-only observationの境界を維持した。
- [x] (2026-07-15、G1RC regression reclose) Empty terminal／predecessor、zero／mixed／unproven、terminal mismatch、trusted Layout adapter、pre-lock exact／aggregate／overflow／exhaustion、lock-not-acquired、claim ownershipを個別再検証した。
  `internal/r2`全80 test、fixture 23件、Python 34件、Protocol／archive Go、gofmt、両diff checkが成功し、G1Eを再closeした。
  Aggregate resource enforcementは全campaignで共有し、canonical final observationは最後のfresh pass counterをbindする判断へ固定した。
  G5 legacy removalはunblockしたが未着手であり、次のwriter taskはpartial R5の完了とuseful-case移行確認から再開する。
  旧M3-3A test countはG1RCの受入証拠へ流用していない。
- [x] (2026-07-15、G5-R5) Thin publisher、canonical bundle／terminal observation receipt、no-clobber store、event非authority、全round shared budget、MaxRoundsを統合した。
  最初のremote observationからreceipt保存完了までcampaign lockを保持し、実行ごとにapproved ObjectID一件だけをexecutorへ渡してfresh reobserveする。
  M2だけがremote claimをconditional writeし、M3はexpected claimをbundleへ固定してExact observationだけを受理する。
  旧`ReplayStage*`、rank、`AdvanceReplayStage`、replay SQLite intent／object／transition table、journal-intent receipt authority、4-field legacy limitsを削除した。
  Stage restartのuseful caseはfresh reobserve、same-content retry、MaxRounds、event nonauthority、receipt no-clobber testsへ移行した。
  Focused R5 testsと`internal/r2`全83 test、fixture 23件、Python 34件、Protocol／archive Go、gofmt、両diff checkを現行受入証拠とし、旧test countを流用しなかった。
- [x] (2026-07-15、R6 completed) R6で現行R1からR5 testを正本fault matrixへ対応付け、fake action後crash、observation後crash、final observation後のreceipt保存crash、stale observation中remote collision、aggregate request／byte exhaustionを追加した。
  Focused fault test、`internal/r2`全88 test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff checkは成功した。
  Repository外のuser scopeへ導入したWinLibs POSIX/UCRT GCC 16.1.0を使い、Go 1.24.13 windows/amd64、CGO_ENABLED=1、CC=gccで指定8 packageのlocal Windows Race Detectorをexit 0で完了した。
  `mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/catalog ./internal/continuity ./internal/parquet -count=1`はingest、wal、archive、r2、delivery、continuity、parquetでpassし、catalogはno test filesだった。
  GCCはrepoまたはmiseのdependencyではなく、GitHub CIとremoteは未確認である。
  Real R2はcredentialと明示確認がないためoptional skipとし、旧M3-3A test countをR6の受入証拠へ流用していない。
- [x] (2026-07-15、R7 changes_required) Expected descriptor bytesのI/O前課金、terminal二回目readの`nil` collapse、実消費bytesを返さないrclone Parquet checkにより、R3 resource gateを再openした。
  Redesign文書内のevent fail-closed表現も、store-local validationとpublisher-level append failure非vetoを混同していたため修正対象にした。
- [x] (2026-07-15、G3E bounded actual bytes classification) RequestをI/O前、remote body bytesを実消費後に共有aggregate budgetへ課金するcap+1 bounded streamへ全object readを統一した。
  Terminal二回目readはUnavailable、Oversized、Ambiguous、Differentをtyped resultで保持し、非Exactからaction、FinalDigest、receiptを作らない。
  R3から`ReplayCheckDownloader`を除去し、Parquet remote verificationを`OpenLimited`とstreaming SHA-256へ移した。
  Focused R3／R5／fault、uncached `internal/r2`全test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功した。
  旧M3-3A test countはG3Eの受入証拠へ流用していない。
- [x] (2026-07-15、G3F final uplift／list descriptor) Final observationの未課金upliftとParquet list descriptor未照合を修正し、focused／repository／local Race gateを再実行した。
- [x] (2026-07-15、R7 third audit pass) AdvisorがR1からR6、G3F remediation、M3-4未着手境界を再監査し、phase `r7_m3_3a_third_audit`へverdict `pass`を記録した。
- [x] (2026-07-15、G7A docs-only unblock) M3-3A R1からR7をcompletedとし、M3-4を明示的にunblockedとした。
- [x] (2026-07-15、G8 downstream completion) R7 receiptを変更せず、read-only `r2.ReadBackend`だけを使うM3-4 selector／fetch／verify／CLIを完了した。

## Surprises & Discoveries

現行internal/r2/replay_publisher.goは、bundleの準備、remote list、graph検証、rclone実行、journal stage、receipt生成を一つのPublish経路へ結合している。

この結合は、stageを再開位置として扱うつもりでも、stageの存在をremote事実の代わりに読み替える経路を作る。

現行のM3-3A focused testは初回publish、same-content retry、各stage後のrestart、mutation、branch、orphan、claim、receiptを含むが、旧stage実装が正しいことを証明する証拠ではない。

旧実装の直近証拠はinternal/r2 86 passed、fixture 22 verified、Python 32 passed、repository check、vet、gofmt、diff checkの履歴であり、M3-3Aの受入証拠からquarantineする。

R6以前のlocal Race DetectorはCGOまたはgccの環境制約で未実行だった。

R6ではrepository外のuser scopeへ導入したWinLibs POSIX/UCRT GCC 16.1.0を使い、Go 1.24.13 windows/amd64、CGO_ENABLED=1、CC=gccで指定8 packageのlocal Race Detectorをexit 0で完了した。

GCCはrepoまたはmiseのdependencyではなく、GitHub CIとremoteは未確認である。

Repository gate、fault matrix、local Race gateが成功したためR6を完了し、R7をunblockした。

R7初回監査では、expected descriptor sizeをI/O前に課金していたため、実bodyがexpectedより大きい場合と複数objectの累積でaggregate byte budgetを迂回できることが判明した。

`mustReplayBytes`はterminal二回目readのtimeout、resource exhaustion、invalid bodyをすべて`nil`へ変換し、Ambiguousへ潰していた。

R3のrclone `check --download` seamはrequest countしか観測できず、Parquet remote bytesのcapと実消費量を証明できなかった。

G0のlocal workflow確認では`C:\Users\AKIRA\.codex\agents\semble-search.toml`が存在し、role名が`semble_search`であることを確認した。

G0とG0Aは既知pathとread-only Git確認だけで完了したため、`semble_search` childを起動せずsemantic searchも使わなかった。

R1のstrict canonical decoderはunknown field、duplicate field、invalid UTF-8、noncanonical integer、zero digest、wrong domain、wrong key、scope collision、raw claim missing、resource overflowをGoとPythonで同じerror codeへ分類できた。

R1のfinal observation validatorはExact claim、Exact raw inventory、full keyで整列した完全なderivative inventory、canonical replay manifestで証明された連続edge、request counter、byte counterが一つでも不足する場合にdigestを生成しない。

R2のsealerは同じverified bytesを異なるlocal pathとreceipt pathへ置いても、同じcanonical bytes、object ID、bundle digestを生成した。

R2のreconcilerは観測配列の順序が異なってもstable object IDでactionを整列し、同じaction-plan digestを生成した。

R2ではcandidate derivativeのunknown object、duplicate observation、missing observationをwinner selectionせずAmbiguousなintegrity stopへ分類した。

R3では既存`BoundedObjectBackend`がwriteを含む`ObjectBackend`をembedしていたため、observerへその型を渡さず、`GetLimited`、complete flag付き`ListLimited`、`OpenLimited`だけを公開するadapter境界が必要になった。

R3のcomplete derivative listで候補keyがない場合だけAbsentを生成し、list後のGet失敗、timeout、short read、incomplete paginationをAmbiguousまたはUnavailableへ保った。

R3のfinal observationはclaim、raw、Parquet、part、part graph、replay manifest、revision graphがすべてExactで、campaign／epoch lockをfinal digest生成直前にも再assertできた場合だけ作成できた。

R4ではcanonical metadataにcaller-supplied local pathがないため、sealed canonical bytesをdomain検証した後にexecutor自身が一時snapshotをmaterializeし、Parquetもreopenしたsourceから同じ形式の検証済みsnapshotを作る必要があった。

R4のevent payloadを固定enumとdigestだけへ限定すると、credential、endpoint、local path、自由形式retry errorをschema上から除外できた。

R4のevent storeはmissing eventでもapproved executorを妨げず、同一ID同一bytesだけをidempotentに扱い、lineage conflictと同一ID異内容を拒否した。

M2のpublisher claimはraw publicationがIf-None-Match:*で所有するidentityであり、M3 replayが同じclaimを作成するとraw事実とderivative事実の所有者が混ざる。

最終観測はpoint-in-time evidenceであり、receiptがfuture mutation、admin-resistant WORM、failover、handoverを証明しないことを明示しないと、receiptの意味を過大評価する。

## Decision Log

- Decision: M3-3Aは現行stage-based runtimeを部分破棄し、bundle、observation、pure plan、executor、event、receiptの境界へ置き換える。
  Rationale: remote stateをstage順位へ圧縮すると、stale observationやjournal行がupload権限として誤用されるためである。
  Date/Author: 2026-07-15 / Codex

- Decision: M3 replayはpublisher claimを作成せず、M2 raw publicationが作成した既存claimのExact observationだけを要求する。
  Rationale: claimはcampaignのpublisher identityであり、Parquet derivativeの処理状態や分散mutexではないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: reconcilerはReplayPublicationBundleとReplayRemoteObservationだけを受け取る純粋関数にし、context、filesystem、network、clock、randomness、SQLite、rcloneを依存にしない。
  Rationale: policy判断をI/Oと切り離すことで、同じ入力から同じaction列をtruth tableとstateful testで再現できるためである。
  Date/Author: 2026-07-15 / Codex

- Decision: actionはcaller-supplied full keyではなく、bundleのimmutable object IDだけを指す。
  Rationale: full keyはtrusted r2.Layoutがbundle sealing時に導出し、executorが任意keyへ書き込めないようにするためである。
  Date/Author: 2026-07-15 / Codex

- Decision: eventはappend-onlyの説明資料であり、monotonic stageやeventの存在だけではactionを承認しない。
  Rationale: event欠落、重複、衝突、停電途中の記録をremote事実より強い権限にしないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: resource limitはbundleへ固定し、MaxObservationRequestsを一回のPublish全passで共有する。
  Rationale: passごとの個別limitでは、retry、post-action observation、final observationが合算されたbounded budgetを越えるためである。
  Date/Author: 2026-07-15 / Codex

- Decision: R0からR7を独立gateにし、R1からR6の実装とR7 final auditが終わるまでM3-4をblockedのままにする。
  Rationale: publication redesignのcontract、policy、execution、failure testを同時に進めると、旧stage実装が新設計へ戻るためである。
  Date/Author: 2026-07-15 / Codex

- Decision: G0で確認した50件のdirty差分を一括操作せず、M3-1とM3-2Gは保持し、旧M3-3Aはuseful-case移行後に置換削除する。
  Rationale: baselineの実装と置換対象を同じ未分類差分として扱うと、受入済みcontractを失うか旧runtimeを誤って受け入れるためである。
  Date/Author: 2026-07-15 / Codex

- Decision: R1の10個のimplementation boundとcanonical field集合を`protocol/v1/hash-domains.md`、Go、Python、golden fixtureで同じ変更単位に固定する。
  Rationale: 後続sealerまたはobserverが独自上限や省略fieldを導入すると、contract driftとresource bypassが発生するためである。
  Date/Author: 2026-07-15 / Codex

- Decision: R2のlocal path、receipt path、rclone binary path、metadata bytesは`ReplayLocalSources`へ隔離し、Protocol V1 contractだけからcanonical bytesとdigestを計算する。
  Rationale: filesystem上の配置を変えてbundle identityが変わると、同じverified inputのretryが別publicationとして扱われるためである。
  Date/Author: 2026-07-15 / Codex

- Decision: 同じbarrierに複数objectがある場合はOversized、DifferentまたはAmbiguous、Unavailable、Absent uploadの順で評価し、upload actionはstable object IDで整列する。
  Rationale: 同じbarrier内のcollisionまたは取得不能を無視して一部のAbsent objectだけを公開すると、最初の未充足barrierを越えるためである。
  Date/Author: 2026-07-15 / Codex

- Decision: R3 observerはwrite-capable backend、journal、event、rcloneを受け取らず、replay専用`ListLimited`／`OpenLimited` interfaceと共有`ReplayObservationBudget`だけをremote観測能力にする。
  Rationale: Parquetを含む全bodyをcap+1 bounded streamで読むと、requestと実消費bytesを同じaggregate budgetへ課金できるためである。
  Date/Author: 2026-07-15 / Codex

- Decision: RequestはI/O前に課金し、body bytesは成功、short read、reader error、cap+1の各経路で実消費後に課金する。
  Rationale: Expected descriptor sizeの事前課金では、unexpected larger bodyと複数readの累積を制限できないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: Absentはcomplete derivative inventory内の候補欠落だけに限定し、dependency欠落、stale list、取得不能、不完全pagination、不正descriptor、graph矛盾へ使用しない。
  Rationale: 不確定なremote状態をupload許可へ変換しないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: R4 executorはcallerからfull key、rclone key、local path、credential、operation名を受け取らず、bundle内ObjectIDからsealed local sourceとtrusted rclone keyを解決する。
  Rationale: 任意key、任意path、任意rclone operationをapproved actionへ混入させないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: executorはlocal sourceを検証済み一時snapshotへstreamしてからrcloneへ渡し、copyとdownload checkの結果をCompleted、Different、Unavailableへ分類する。
  Rationale: seal後mutationと検証後copyのlocal TOCTOUをremote publicationへ持ち込まず、AbsentまたはExactのremote観測をexecutorへ再実装しないためである。
  Date/Author: 2026-07-15 / Codex

- Decision: diagnostic eventはfixed kind、digest、ObjectID、result／error enumだけをcanonical化し、observer、reconciler、executorはevent Loadをauthorityとして参照しない。
  Rationale: missing、duplicate、conflicting eventがaction、observation省略、receipt保存を承認しないようにするためである。
  Date/Author: 2026-07-15 / Codex

## Outcomes & Retrospective

R0の成果は、旧M3-3Aを受入済みと扱わず、何を保持し、何をadapterへ包み、何を削除するかをファイルと型の粒度で固定したことである。

R0ではruntimeの挙動を変更していないため、現行コードは動作中の機能ではなく、R1以降の置換で参照するfailure inventoryとして残っている。

R1の終了時点で、bundle identityとfinal observation identityをGoとPythonが同じcanonical bytesから計算できなければ、R2以降へ進まない。

R5の終了時点で旧stage rank、stage transition table、journal intent hash receipt authorityを削除または置換できなければ、M3-3Aは未完了のままとする。

G0Aの成果は、dirty worktreeの全50差分とlocal workflow定義の扱いを固定し、R1開始前の文書境界を四正本文書で一致させたことである。

G1E後のR1成果は、旧publisherを呼ばない独立contract gateがbundle digest `3bbd26b16fa1ba327b2d85fbec18eb2fc92771e1018796cd69c0ea0c0a183712`とfinal observation digest `999db1e5948455cf66911ba2ec44023545af1c66f72b479515ead580415de218`をGoとPythonで一致させたことである。

旧52件または86件のtest countはR1の証拠へ流用せず、R2はこのR1 gateのpassを前提として開始できる。

R2の成果は、M2 raw snapshot、Parquet reopen、part manifest、replay manifest、pinned rclone binaryをlockまたはremote I/Oなしで再検証し、trusted `r2.Layout`だけからfull keyとrclone keyを導出するsealerを作ったことである。

R2のpure reconcilerはclaim、raw manifest、raw object、Parquet、part manifest、part chain、replay manifest、replay graph、final observationの順に最初の未充足barrierだけを返す。

旧stage publisher、journal、receiptは変更または削除せず、R3はこのbundleとobservation値型を入力として開始できる。

R3の成果は、claim、raw manifest、全raw object、complete derivative namespace、Parquet、part chain、replay revision graphを一つのaggregate budgetとlock保持条件でfreshに観測し、完全なExact状態だけをProtocol V1 final observationへ変換できるようにしたことである。

旧stage publisher、journal、receiptはR3でも変更または削除せず、R4はapproved object IDだけを実行する境界から開始できる。

R4の成果は、reconcilerが返す三種類のupload actionだけをbundle ObjectIDへ解決し、local bytesを再検証したsnapshotに対してimmutable copyとdownload checkだけを実行するnarrow executorを作ったことである。

R4のdiagnostic event storeはcanonical EventID、idempotent same-byte duplicate、conflict検出、Load時再検証を持つが、action authority、stage rank、receipt authorityを持たない。

旧stage publisher、journal、receiptはR4でも変更または削除せず、durable event adapterはR5へ要求しないscopeで完了した。

## Context and Orientation

M2のraw truthは、Gatewayがdurably受理したBatchFrameV1を含むsealed WAL objectと、M2のRawDayManifestである。

M2 raw publicationがpublisher claimをIf-None-Match:*で作成し、campaignとpublisher epochのlocal exclusive lockを使う。

M3 replay publicationは、M2 raw manifest、参照sealed WAL object、M3-2Gで検証済みのParquet artifact、part manifest、replay manifestを一つのReplayPublicationBundleへsealする。

ReplayPublicationBundleとは、公開対象の全canonical bytes、exact key、hash、size、range、scope、conversion、resource limit、tool identityを一つのcanonical identityへ束ねた論理契約である。

Observationとは、bounded read、list、stream、rclone checkで現在のremote namespaceを再確認した結果であり、journal stageを含めない。

Reconcilerとは、bundleとobservationからuploadまたは停止のpolicyだけを決める純粋な関数である。

Executorとは、reconcilerが承認したbundle object IDのactionを、trusted keyとallow-listされたtransferで実行する境界である。

Eventとは、bundle、observation、action、resultを後から説明するappend-only記録であり、remote事実やaction権限の代替ではない。

Final observationとは、全raw、全candidate derivative、全part graph、全replay revision graph、claim、resource budgetを再検証したcomplete observationである。

現行M3-3Aはinternal/r2/replay_publisher.go、internal/r2/replay_journal.go、internal/r2/replay_receipt.goにmonolithic flowを置き、ReplayStage*とSQLite transitionを再開判断に使う。

そのruntimeと過去のtest countはquarantined behavior/failure inventoryであり、R5で新しいcontractへ移行するまでの比較対象に限る。

M3-4はArchiveReader、tickctl、tick-verify、empty-cache local verificationを扱うため、M3-3A redesignとR7 final audit passの後にだけ開始する。

## Partial-discard Map and Trust Boundary

そのまま保持する候補は、`internal/protocol`と`internal/archive`のpart／replay manifest verifier、`internal/r2/layout.go`のtrusted key導出、`internal/r2/tool.go`のpinned rcloneとcommand allow-list、`internal/r2/lock.go`のcampaign／publisher-epoch local lock、M2のpublisher claim作成、local no-clobber保存primitiveである。

M3-1とM3-2GのProtocol、fixture、GoとPython conformance、verified replay source、continuity、Parquet、part／replay manifest、exact key bindingは保持する。

親ExecPlan、M3 living ExecPlan、本redesign ExecPlan、roadmapの四文書は保持更新し、各gateの進捗、発見、判断、証拠を同期する。

境界を狭めて再利用する候補は、`GetLimited`、`ListLimited`、`Open`、streaming SHA-256、secret-free receipt canonicalization、fake backend／fake rcloneが表現している障害事例である。

置換後に削除する対象は、現行`ReplayPublisher.Publish`と`preparedReplayPublication`、policyを含む`inspectDerivativeObjects`、`ReplayStage*`とstage rank、`AdvanceReplayStage`、replay intent／object／transitionのstage table、`journal_intent_hash`をreceipt authorityにする処理、stage-driven restart testである。

旧コードをcommit対象のlegacy packageへ移さず、新しいtruth-tableとfault testが有用な障害事例を覆った時点で旧runtimeを削除または置換する。

repo `AGENTS.md`、`.github/workflows/check.yml`、`.github/workflows/windows-race.yml`、`mise.toml`、global `advisor.toml`、`implementer.toml`、`semble-search.toml`、`delivery-orchestrator/SKILL.md`、`delivery-orchestrator/references/contracts.md`はworkflow定義として保持し、G0Aでは変更しない。

callerが渡すfull key、local path、journal stageとevent、remote listの順序とdescriptor、過去のobservation、backendまたはrcloneの失敗は未検証入力である。

Protocol V1のcanonical bytesとdomain digest、trusted `r2.Layout`が導出したrelative／full／rclone key、再検証済みlocal bundle facts、resource budget内で完了したfresh remote observationだけを判断入力として信頼する。

Event store自身はmalformed eventと同一EventIDのconflicting duplicateを拒否するが、publisher-level append failureはbest-effort diagnosticの欠落として扱い、publicationをvetoしない。

## Plan of Work

### R0 docs/design freeze

R0では、Protocol V1のmanifestとhash domain、parent ExecPlan、M3 living ExecPlan、roadmap、新しい本ExecPlanを同じtrust modelへ揃える。

R0の検証はgit diff --check、対象ファイルだけのdiff確認、見出し確認、一文一行確認であり、Go、Python、fixture、DB、remote、commitは実行しない。

G0AではR0の文書境界へbranch、HEAD、全dirty差分、保持、境界縮小再利用、置換削除、workflow定義のinventoryを同期し、旧test countをR1以降の受入証拠に使わない。

### R1 canonical contract and conformance

R1ではprotocol/v1/のbundleとfinal observation canonical JSON、二つのdigest、resource limit、claim relationshipをGoとPythonで独立実装する。

R1はunknown key、duplicate key、zero digest、wrong domain、wrong key、scope collision、raw claim missing、oversized aggregate、noncanonical bytesを拒否するgolden fixtureを持つ。

R1では現行M3-3A publisherを呼ばず、mise run fixture、mise run test-python、focused Go test、git diff --checkでcontractだけを検証する。

### R2 local bundle sealer and pure reconciler

R2ではinternal/r2/replay_bundle.goに、M2 raw verifier、Parquet reopen verifier、Protocol verifier、Layout key derivationの結果を一つのbundleへsealする処理を実装する。

R2のbundle sealerは候補bundleとcomplete final observationがaggregate request budgetとbyte budgetに収まることをlock取得前に検査する。

revision 1と2では利用可能な全revision edgeを検証し、revision 3以降では直前manifestのsuccessor関係を検証したうえで、未観測の過去edgeを推測せず保守的なbounded estimateを全revision数へ適用する。

remote observerはrevision 1からのcomplete graphだけをfinal acceptanceへ使い、sealerの直前predecessorは受入証拠へ流用しない。

R2ではinternal/r2/replay_reconcile.goにI/Oを持たないdecision algebraを実装し、claim/raw exact barrier、candidate orphan、part predecessor、replay predecessor、final receipt barrierをtruth tableで検証する。

claim、raw manifest、参照raw objectのAbsentはintegrity stopとし、candidate derivativeのAbsentだけを直前barrierが全てExactの場合のaction候補にする。

R2の終了条件は、同じbundleとobservationから同じdecision、同じaction order、同じaction-plan digestが得られることである。

一回のdecisionは最初の未充足barrierに属するactionだけを返し、executor完了後は旧observationを破棄して再観測する。

### R3 bounded observer and graph validation

R3ではinternal/r2/replay_observation.goとinternal/r2/replay_graph.goに、bounded backend、raw inventory、derivative namespace、part chain、replay revision graphのfresh observationを実装する。

R3ではListLimitedとOpenLimitedをlogical request budgetへI/O前に加算し、各streamの実消費bytesをmetadata、Parquet、observation aggregateへ加算する。

各read capはper-object limit、category残量、MaxObservationBytes残量の最小値であり、cap+1 bytesを読んだ時点でOversizedとして停止する。

Unknown size、short read、reader errorはUnavailable、readable invalidとdescriptor contradictionはAmbiguous、canonical identity mismatchはDifferentへ分類する。

R3ではfinal observation digestを完全なkey-sorted inventoryから計算し、欠落、差異、曖昧、上限超過、取得不能な状態からdigestやreceiptを作らない。

timeout、到達不能、不確定readはUnavailable、読取可能な不正、graph上の欠落、矛盾はAmbiguous、上限超過はOversizedとし、read失敗をAbsentへ変換しない。

### R4 narrow executor and append-only event store

R4ではinternal/r2/replay_executor.goに、bundle object IDだけを受けるParquet、part manifest、replay manifestのtransfer executorを実装する。

R4ではinternal/r2/replay_events.goにBundleRegistered、ObservationCompleted、ActionPlanned、ActionStarted、ActionFinished、ReceiptSavedのappend-only event storeを実装する。

R4 event storeはmalformed event、unknown action、bundle digest mismatch、同一EventID異内容をstore-local validationで拒否し、same-byte duplicateをidempotentに扱う。

Publisherはevent appendの欠落、衝突、timeoutをbest-effort diagnostic failureとして無視し、eventを根拠にactionまたはreceiptを許可せず、event failureをpublication vetoにもしない。

### R5 thin publisher and old implementation removal

R5ではinternal/r2/replay_publisher.goをbundle sealingとstatic preflight、lock、fresh observation、reconcile、execute、fresh observation、receipt saveの薄いloopへ書き換える。

bundle sealingと一回のfinal observationがbudget内に収まることの検査はlock取得前に完了する。

lock取得後はclaim Exactを最初に観測し、各approved action後に旧observationを破棄して、同じaggregate budgetと`MaxPublicationRounds`の範囲でfresh observationを作る。

publisher constructorは任意lock filenameを受け取らず、trusted Layoutのcampaign／publisher epoch scopeとcaller-supplied lock rootからcanonical `PublicationLockPath`を導出する。

part数を`N`とするとParquet、part manifest、replay manifestを一件ずつ実行するため必要round数は`2N+2`であり、sealerはlock取得前に不足を拒否する。

lockは最初のremote observationからcomplete final observationとreceipt no-clobber保存まで保持する。

R5では旧ReplayStage* rank、AdvanceReplayStage、replay intent/object/transition stage tables、journal_intent_hashをreceipt authorityとして使う経路、stage-driven restart testを削除または新APIのtestへ置換する。

R5ではcommitted legacy runtime packageを作らず、旧コードは新しいuseful-case testsが通った直後に削除または置換する。

### R6 fault, resource, stateful, and repository gates

R6ではfake backend、fake rclone、fake event storeへaction間のinterleaving、crash、timeout、mutation、resource exhaustionを注入する。

R6ではmise run check、mise exec -- go vet ./...、focused Go、fixture、Python、git diff --check、Windows Race workflowを通す。

R6の全証拠が揃うまで、real R2 smokeを実行せず、production bucketやprefixへ書き込まない。

### R7 final audit

R7ではbundle digest、observation digest、decision algebra、fault matrix、removal diff、全gate証拠を独立に再監査する。

R7がpassするまでM3-4 delivery、CLI、cache、HTTP、real R2 smoke、mergeを開始しない。

## Concrete Steps

作業ディレクトリはC:\projects\utaki-tick-data-platformとし、全編集はapply_patchで対象ファイルだけへ行う。

R0とG0Aでは次の対象を確認し、四文書以外を変更しない。

    git status --short --branch --untracked-files=all
    git rev-parse HEAD
    git diff -- .agent/tick-data-platform-execplan-revised.md execplan/2026-07-15-m3-replay-parquet-delivery.md execplan/2026-07-15-m3-3a-publication-redesign.md docs/plan/roadmap.md
    git diff --check
    git diff --cached --check

R1のProtocol contract gateでは次を実行し、fixture生成を行う場合もR0のdocs-only境界を越えたR1作業として記録する。

    mise exec -- go test ./internal/protocol ./internal/archive -count=1
    mise run fixture
    mise run test-python
    git diff --check

R2からR5では、各unitの対象packageだけを先にfocused testし、次にrepository checkを実行する。

    mise exec -- go test ./internal/r2 -count=1
    mise run check
    mise exec -- go vet ./...
    git diff --check

R6ではWindows workflowのgcc --version確認、CGO有効Race Detector、full Go test、fixture、Python、check、vet、gofmt、diff checkを実行する。

    mise exec -- go test -race ./internal/r2 -count=1
    mise run check
    mise exec -- go vet ./...
    mise exec -- gofmt -l cmd internal
    git diff --check

R0では上記R1以降の実装コマンドを実行せず、docs-onlyの検証結果だけを記録する。

real R2 smokeは、既存の明示的なisolated credential、isolated bucketまたはprefix、pinned rclone、non-production scopeが全て存在する場合に限る。

## Validation and Acceptance

R0の受入条件は、対象文書がM3-3A reopened、M3-4 blocked、旧M3-3A evidence quarantinedを同じ意味で表し、新しいExecPlanが単独でR1からR7を実行できることである。

新しいExecPlanの受入条件は、Purpose、Progress、Surprises & Discoveries、Decision Log、Outcomes & Retrospective、Context and Orientation、Plan of Work、Concrete Steps、Validation and Acceptance、Idempotence and Recovery、Artifacts and Notes、Interfaces and Dependencies、bottom revision noteを持つことである。

R1の受入条件は、GoとPythonがpublication_bundle_digestとfinal_observation_digestを同じcanonical JSONから同じdigestとして計算し、unknown、duplicate、zero、wrong-domain、oversized、noncanonical入力を同じ失敗分類で拒否することである。

R2の受入条件は、pure reconcilerが同じbundleとobservationへ同じdecisionを返し、Unavailableではactionゼロ、DifferentとAmbiguousではintegrity stop、Oversizedではresource stopになることである。

R3の受入条件は、bounded observerがraw manifest、全raw object、全derivative inventory、全part chain、全replay revision graphをfreshに確認し、list、get、stream、checkの失敗をAbsentへ変換しないことである。

R4の受入条件は、executorがapproved object ID以外を実行せず、copyto --immutableとcheck --downloadのallow-listを守り、eventがbundle、observation、plan、resultへbindすることである。

R5の受入条件は、receiptがfinal observation digestをbindし、旧stage rank、stage table、journal intent hash authority、stage-driven restartをruntime authorityとして残さないことである。

R6の受入条件は、fault matrixの全ケース、same-content retry、collision、candidate orphan、unrelated orphan、resource exhaustion、secret absenceをfakeで通し、repository gateとWindows Race gateを成功させることである。

R7の受入条件は、design、contract、implementation、fault、resource、CIの全gateを再監査し、M3-4をunblockできる明示的なpass判断を記録することである。

## Idempotence and Recovery

bundle sealingは同じ検証済みlocal inputとConversionTupleから同じcanonical bytes、object ID、bundle digestを生成し、local path、clock、credential、journal rowをdigestへ含めない。

bundle digestが同じなら同じpublication identityであり、local pathが変わってもsealed local sourceを再検証できれば同じbundleを使える。

lock取得前のcrashはremoteへの作用なしとして、次回Publishが同じbundleのfresh observationから再開する。

observation後のcrashはobservationを破棄し、eventが残っていても再観測する。

Parquet copyの前、途中、後のcrashは、次回のfresh observationでcandidate-only exact Parquetならresumable dataとして扱い、digest、bytes、full keyが違えばintegrity stopにする。

part manifest copyの前、途中、後のcrashは、ParquetがExactであることとcomplete part graphを再確認してから同じpart manifestだけを対象にする。

replay manifest copyの前、途中、後のcrashは、全part chainがExactであることを再確認し、replay manifestを最後のcandidate actionとして再計画する。

upload成功前のevent commit、event commit前のupload成功、timeout、unknown outcomeは、action後の旧observationを破棄し、fresh observationから再評価する。

final observation後のcrashはreceiptを保存せず、次回に新しいcomplete final observationを作り、同じfinal observation bytesなら同じreceiptをlocal no-clobberで保存する。

receipt保存後にReceiptSaved eventが欠けても、eventの存在だけでactionを許可せず、bundleとremoteを再観測してreceipt bytesを比較する。

same-content retryはExact observationと同じbundle digestのときだけ許可し、different-content collision、claim missing、claim different、branch、missing predecessorは停止する。

### Fault matrix

- crash before lockはremoteへの作用なしとして同じbundleのfresh observationから再開する。
- crash after observationは旧observationを無効化し、eventが残っていても再観測する。
- Parquet copy前、copy中、copy後のcrashはcandidate-only exact objectを再利用し、それ以外のobjectは停止する。
- part manifest copy前、copy中、copy後のcrashはParquet Exactを再確認し、part graphがcompleteになるまでpart manifestを実行しない。
- replay manifest copy前、copy中、copy後のcrashはcomplete part chain Exactを再確認し、replay manifestを最後に実行する。
- upload success before event commitはfresh observationで同一bytesをExactとして再利用し、異なるbytesならcollisionにする。
- timeoutまたはunknown outcomeはUnavailableとしてactionゼロにし、同じrequest budget内で可能なfresh observationだけを行う。
- claim、raw manifest、raw object、Parquet、part manifest、replay manifestの各observable boundaryでmutationを検出したらDifferentまたはAmbiguousとして停止する。
- final observation前のmutationはreceiptを作らず、complete final observationを再作成する。
- receipt saved before eventはreceiptをtransaction完了の証明とせず、remoteとbundleを再観測してeventを説明可能な場合だけappendする。
- same-content retryはExact、different-content collisionはintegrity stop、claim missing/differentはintegrity stopとする。
- branch、missing predecessor、duplicate descriptor、list/get mismatchはwinnerを選ばずintegrity stopとする。
- candidate-only exact data-before-manifest objectはbundleの候補と完全一致する場合だけresumableとする。
- unrelated orphan、unknown object、unreferenced part、unreferenced replayはscope collisionとして停止する。
- resource limit超過はOversizedのresource stopとしてactionゼロにし、receiptを保存しない。
- 一時的なread、list、stream、check失敗はUnavailableのretryable zero-actionとして扱い、Absentへ変換しない。

全ケースで、part manifestはExact Parquetより先に現れず、replay manifestはExactでcompleteなpart chainより先に現れない。

この不変条件は、campaignとpublisher epochの一台一hostというM3 operational preconditionと、trusted bucket prefixが他writerから同時変更されない脅威モデルの下で成立する。

## Artifacts and Notes

R0で追加するartifactは、このExecPlanとProtocol、parent、M3 living plan、roadmapの文書差分だけである。

R1で追加するgoldenはbundle canonical bytes、bundle digest、final observation canonical bytes、final observation digest、claim relationship、negative caseを含む。

R2で追加するtruth tableはObservationClass、barrier、action precedence、resource exhaustion、unknown outcomeを網羅する。

R4で追加するeventはsecret、credential、endpoint、local pathを含めず、bundle digest、observation digest、action-plan digest、result classだけを保存する。

receiptはpublisher claim keyとclaim domain digest、raw inventory、derivative inventory、replay graph、final observation digest、verification_complete=trueを持つが、credentials、ETag、clock、retry文字列、journal stateを持たない。

receiptはpoint-in-time evidenceであり、将来のremote mutation、administrator-resistant WORM、failover、handover、consumer deliveryを保証しない。

現行旧コードはR5まで削除せず、new runtime packageやcommitted legacy adapterを作らず、useful caseを新testへ移した後に削除または置換する。

## Interfaces and Dependencies

R2のinternal/r2/replay_bundle.goには、local pathをidentityに使わず、verified M2 raw snapshot、Parquet artifact、manifest、Layout、ConversionTuple、resource profileをsealする次の型を置く。

    type ReplayPublicationBundleInput struct { Scope archive.ScopeConfig; Conversion parquet.ConversionSpec; RawManifest []byte; RawObjects []ReplayLocalObject; Parts []parquet.PartArtifact; PartManifests [][]byte; ReplayManifest []byte; ReceiptPath string; Limits ReplayPublicationLimits; RcloneIdentity RcloneIdentity }

    type ReplayPublicationBundle struct { Version uint8; Digest [32]byte; CanonicalBytes []byte; Identity ReplayBundleIdentity; LocalSources map[ReplayObjectID]LocalArtifact }

    func SealReplayPublicationBundle(ctx context.Context, input ReplayPublicationBundleInput) (ReplayPublicationBundle, error)

ReplayPublicationBundle.Identityはcanonical bundle fieldsだけを持ち、LocalSourcesのpath、file handle、receipt pathはcanonical JSONへ含めない。

ReplayObjectIDはbundle内のkind、sequence、canonical relative keyから導出するstable identifierであり、callerが任意full keyを持ち込む入口にしない。

R3のinternal/r2/replay_observation.goには、read failureをAbsentへ変換しないbounded observerを置く。

    type ObservationClass string

    const ( ObservationAbsent ObservationClass = "Absent"; ObservationExact ObservationClass = "Exact"; ObservationDifferent ObservationClass = "Different"; ObservationAmbiguous ObservationClass = "Ambiguous"; ObservationOversized ObservationClass = "Oversized"; ObservationUnavailable ObservationClass = "Unavailable" )

    type ReplayRemoteObservation struct { BundleDigest [32]byte; Claim ObservationClass; RawManifest ObservationClass; RawObjects []ReplayObjectObservation; ParquetObjects []ReplayObjectObservation; PartManifests []ReplayObjectObservation; PartChain ObservationClass; ReplayManifest ObservationClass; ReplayGraph ObservationClass; RequestCount uint64; ObservationBytes uint64; Complete bool; FinalObservation *protocol.ReplayFinalObservation; FinalDigest [32]byte }

    func NewReplayBoundedObserver(remote ReplayRemoteReadBackend, lock ReplayObservationLock) (*ReplayBoundedObserver, error)

    func (o *ReplayBoundedObserver) Observe(ctx context.Context, bundle ReplayPublicationBundle) (ReplayRemoteObservation, error)

    func (o *ReplayBoundedObserver) ObserveWithBudget(ctx context.Context, bundle ReplayPublicationBundle, budget *ReplayObservationBudget) (ReplayRemoteObservation, error)

ReplayRemoteObservationのderivative inventoryはfull key、kind、digest、bytesでkey-sortし、unknown、duplicate、conflicting descriptor、missing predecessor、branchをCompleteへ含めない。

R3のinternal/r2/replay_graph.goには、part chainとreplay revision graphを独立に検証する次のAPIを置く。

    func VerifyReplayPartGraph(parts []protocol.PartManifest, scope protocol.ReplayScope, conversion archive.ConversionTuple, maxNodes uint64) (ReplayPartGraph, error)

    func VerifyReplayRevisionGraph(revisions []protocol.ReplayDayManifest, maxNodes uint64) (ReplayRevisionGraph, error)

ReplayRevisionEdgeはrevision、manifest digest、previous digest、part set root、row-chain rootを持ち、revision branchとmissing predecessorを表現しない。

R2のinternal/r2/replay_reconcile.goには、I/O依存を持たない次のpure APIを置く。

    type ReplayReconcileDecision struct { Kind ReplayDecisionKind; Actions []ReplayAction; ActionPlanDigest [32]byte; StopClass ObservationClass; ReasonCode string }

    type ReplayAction struct { Kind ReplayActionKind; ObjectID ReplayObjectID }

    func ReconcileReplayPublication(bundle ReplayPublicationBundle, observation ReplayRemoteObservation) (ReplayReconcileDecision, error)

ReconcileReplayPublicationは入力内のbundle digest、observation digest、resource countersを検証するが、journal events、local paths、clock、network、filesystem、SQLite、rcloneを読まず、同じ入力に同じdecisionを返す。

ReplayActionはUploadParquet、UploadPartManifest、UploadReplayManifestだけを表し、full key、local path、credential、winner selectionを含めない。

全remote依存がExactである場合、reconcilerは`ReadyForReceipt`を返し、publisherがpure receipt builderとno-clobber storeを呼び出す。

R4のinternal/r2/replay_executor.goには、reconcilerのactionだけを実行する次のseamを置く。

    type ReplayActionExecutor interface { Execute(ctx context.Context, bundle ReplayPublicationBundle, action ReplayAction) (ReplayActionResult, error) }

    type ReplayActionResult struct { BundleDigest [32]byte; Action ReplayAction; Class ReplayActionResultClass; Bytes uint64; Digest string; ErrorClass ReplayActionErrorClass }

    type ReplayActionTool interface { CopyToImmutable(ctx context.Context, localPath, remoteKey string) error; CheckDownload(ctx context.Context, localPath, remoteKey string) error }

executorはbundleのobject IDからtrusted Layoutで導出済みのremote keyを解決し、rclone copyto --immutableとcheck --downloadだけを実行する。

executorはcopy直前にlocal sourceのbytesとSHA-256をbundleへ再照合し、seal後のlocal mutationをDifferentとして停止する。

executorはremoteのAbsentまたはExactを観測判断せず、approved actionの実行結果だけをCompleted、Different、Unavailableで返す。

R4のinternal/r2/replay_events.goには、stage rankを持たない次のevent storeを置く。

    func (s *ReplayDiagnosticEventStore) Append(ctx context.Context, bundle ReplayPublicationBundle, event ReplayPublicationEvent) error

    func (s *ReplayDiagnosticEventStore) Load(ctx context.Context, bundle ReplayPublicationBundle) ([]ReplayPublicationEvent, error)

    type ReplayPublicationEvent struct { EventID [32]byte; Kind ReplayEventKind; BundleDigest [32]byte; ObservationDigest [32]byte; ActionPlanDigest [32]byte; ActionKind ReplayActionKind; ObjectID ReplayObjectID; ResultClass ReplayActionResultClass; ErrorClass ReplayActionErrorClass }

Event storeはappend-onlyであり、malformed event、same event ID with different bytes、unknown bundle digestをstore-local validationで拒否する。

Missing eventとpublisher-level append failureはactionのauthorizationにもpublication vetoにもならない。

R5で書き換えるinternal/r2/replay_publisher.goは、lock、bundle seal、observer、pure reconciler、executor、event store、fresh observer、receipt storeを接続する薄いloopだけを持つ。

R5のinternal/r2/replay_receipt.goは、journal intent hashをauthorityに使わず、次のpure builderとno-clobber storeへ分ける。

    func BuildReplayVerificationReceipt(bundle ReplayPublicationBundle, finalObservation ReplayRemoteObservation) (ReplayVerificationReceipt, error)

    type ReplayReceiptStore interface { SaveNoClobber(ctx context.Context, path string, receipt ReplayVerificationReceipt) error }

receipt builderはfinalObservation.Complete、bundle digest一致、final observation digest非zero、claim/raw/derivative/replay graphのExactを要求し、clock、retry error、ETag、credential、journal stageを受け取らない。

R3のbounded backendはM2のunbounded ObjectBackendをembedせず、次のreplay専用interfaceへする。

    type ReplayRemoteReadBackend interface { GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error); ListLimited(ctx context.Context, prefix string, maxObjects uint64) (ReplayRemoteObjectList, error); OpenLimited(ctx context.Context, key string, maxBytes uint64) (io.ReadCloser, int64, error) }

M3 replay observerへ`PutIfAbsent`、unbounded `Get`、unbounded `List`を公開せず、publisher claimのconditional createはM2 raw publisherだけが所有する。

RemoteObjectListはObjectsとCompleteを持ち、pagination、truncation、cursor異常が残る場合はCompleteをfalseにしてfinal observationを作らない。

GetLimitedはmax+1まで、ListLimitedはmax件を超える前に停止し、OpenLimitedはadvertised sizeとstream bytesを上限内で検証する。

ReplayObservationBudgetはMaxObservationRequestsを一つのPublish全体で共有し、GetLimited、ListLimited、Open、rclone CheckDownloadのlogical invocationを開始前に一件加算する。

SDK内部retryは一回のlogical invocationとして数えるが、context deadline、client timeout、body limitを持つため、budgetと実時間の両方を無制限にしない。

resource profileはbundleへ次のU64 fieldsとして固定する。

    MaxMetadataObjectBytes=16777216
    MaxTotalMetadataBytes=268435456
    MaxListObjects=50000
    MaxGraphNodes=50000
    MaxParts=10000
    MaxParquetObjectBytes=1099511627776
    MaxTotalParquetBytes=17592186044416
    MaxObservationBytes=70368744177664
    MaxObservationRequests=100000
    MaxPublicationRounds=20002

全limitはnonzero、finite、実装上限以下であり、各加算はnext > limitまたはtotal > limit-nextを先に検査してoverflowを拒否する。

`MaxPublicationRounds`の実装上限は`2*MaxParts+2`であり、bundleの実part数に対する必要round数をGoとPythonの両方で検査する。

bundle sealerは候補全体を一回final observationできるrequest数とbyte数が上限内に収まらない場合、lock取得とremote actionより前に拒否する。

M2のPublisherClaimは"tick-data-platform/publisher-claim/v1\\0" || publisher_claim_canonical_jsonのSHA-256 digestを持つ。

M3 bundleはclaim key、claim canonical bytesのdomain digest、claim bytesを固定し、M2 raw publicationが作成した既存claimのExact observationだけを受け入れる。

## Revision note

改訂記録（2026-07-15 G6-R6）: 現行R1からR5 testをfault matrixへ対応付け、action barrier crash／restart、stale observation interleaving、receipt保存crash、aggregate request／byte exhaustionを追加した。

Focused fault test、`internal/r2`全88 test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、指定8 packageのlocal Windows Race gateは成功した。

WorkflowはCGO、gcc、mise確認を維持し、race対象を指定8 packageへ固定した。

Race実行にはrepository外のuser scopeにあるWinLibs POSIX/UCRT GCC 16.1.0を使用し、repoまたはmise dependencyには追加しなかった。

GitHub CIとremoteは未確認であり、Real R2はcredentialと明示確認がないためoptional skipとした。

旧M3-3A test countはR6の受入証拠へ流用せず、R6 passによりR7だけをunblockした。

改訂記録（2026-07-15 M3-3A-R0-DOCS）: advisor preflight passとM3-2G acceptedを受け、旧M3-3Aのstage、SQLite transition、journal intent hash authorityをquarantineし、bundle digest、final observation digest、pure reconciler、bounded observation、append-only event、fault matrix、R0からR7の実装gateを新しい正本として追加した。

R0はdocs-onlyであり、Go、Python、fixture、DB、remote R2、M3-4、commit、pushを実行しない。

改訂記録（2026-07-15 G0A-DOC-ALIGN）: branchとfull HEAD、tracked modified 24、untracked 26、staged 0、両diff check成功を記録し、全dirty差分を保持、境界縮小再利用、useful-case移行後の置換削除へ分類した。

四正本文書だけを更新し、旧52件または86件を含むM3-3A test countを新設計の受入証拠へ流用しない境界を維持した。

改訂記録（2026-07-15 G2-R2）: local-only bundle sealer、stable object ID、trusted Layout key derivation、pure first-barrier reconciler、deterministic action-plan digest、resource preflight、truth tableの実装と検証結果を同期した。

改訂記録（2026-07-15 G3-R3）: replay専用bounded read interface、aggregate observation budget、fresh observer、part／revision graph verifier、Exact-only final observation変換、negative test、focused／full `internal/r2` testの成功を同期した。

改訂記録（2026-07-15 G4-R4）: ObjectID-only narrow executor、verified local snapshot、rclone二操作allow-list、canonical diagnostic event、append-only in-memory store、negative test、focused／full `internal/r2` testの成功を同期した。

旧stage publisher、journal、receiptはR5まで保持し、G2の受入証拠へ旧test countを流用しなかった。

改訂記録（2026-07-15 G3E-R7-BOUNDED-ACTUAL-BYTES-CLASSIFICATION）: R7 changes_requiredによりR3を再openし、request事前課金とactual body bytes事後課金、cap+1 bounded stream、terminal typed classificationを実装した。

Parquet observationからrclone依存を除き、event方針をstore-local validationとpublisher-level append failure非vetoへ統一した。

現行focused／full／repository／Race証拠を記録し、R7再監査までM3-4をblockedに保った。

改訂記録（2026-07-15 G3F-FINAL-UPLIFT-LIST-DESCRIPTOR）: R7二回目監査のchanges_requiredによりR3のfinalizationとParquet descriptor検証を再openした。

発見事項は、`makeProtocolFinalObservation`が必要bytesまでcounterを自己補正して共有campaign budgetを消費しないことと、listで得た`RemoteObject.Size`をParquet content検証へ結び付けていないことである。

判断として、final observation canonical bytesの必要量と最終pass実課金量との差分をdigest生成前に共有budgetへ一度だけ課金し、課金後snapshotを最終pass counterの正本とした。

過去roundのaggregate counterを保持し、exact-fit、実課金十分、uplift-only exhaustionを独立testで固定した。

Uplift exhaustionは`ObservationOversized`で停止し、`Complete`、final observation、final digest、reconcile action、receiptを生成しない。

Parquet observationはlist size、bundle expected bytes、Open advertised size、actual stream bytesの一致を要求し、list sizeのstale／誤りを`ObservationAmbiguous`としてactionとreceiptの前で停止する。

Focused G3F testと未キャッシュ`internal/r2`全test、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、Go 1.24.13、CGO_ENABLED=1、GCC 16.1.0による指定8 packageのlocal Windows Race Detectorは成功した。

GitHub CI、remote I/O、Real R2は実施していない。

旧M3-3A test countはG3Fの受入証拠へ流用せず、G3F完了後もR7再監査をpending、M3-4をblockedに保つ。

改訂記録（2026-07-15 G7A-R7-DOC-UNBLOCK）: R7第三回監査receiptのphaseは`r7_m3_3a_third_audit`、verdictは`pass`である。

監査scopeはR1 canonical schema／hash／claim／limits／conformance、R2 sealer／trusted Layout／pure reconciler／pre-lock budget、R3 bounded observer／graph／lock／Exact-only typed classification、R4 narrow executor／local revalidation／immutable allow-list／event nonauthority、R5 thin publisher／single action／fresh re-observation／receipt／M2-only claim／legacy authority removal、R6 repository gate、およびG3F remediationとM3-4未着手境界を含む。

監査evidenceはfixture 23件、Python 34件、全Go、未キャッシュ`internal/r2`、Protocol／Archive、vet、空のgofmt、両diff check、CGOとGCC 16.1.0によるcurrent-diff local Raceの成功、およびbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 26、untracked 43、staged 0である。

監査assumptionsはlocal gateでGitHub CIとoptionalなReal R2を必須にしないこと、G3EとG3Fで更新した受入証拠を使って旧52件または86件のtest countを流用しないこと、およびG7Aをdocs-onlyとすることである。

Known unresolvedはGitHub CI未確認、Real R2 optional skip、M3全体final audit未実施である。

R7 passにより本再設計のR1からR7はcompletedとなり、M3-3Aをcompleted、M3-4を明示的にunblockedとする。

R7 passはM3全体final audit passを意味せず、M3全体の完了判断は後続auditへ残す。

次taskはG8のM3-4 read-only deliveryであり、G7Aでは実装を開始しない。

改訂記録（2026-07-15 G8-M3-4-READ-ONLY-REPLAY-UX）: R7 pass後のdownstream taskとしてM3-4 read-only replay deliveryを実装した。

G8はpublication authority、bundle、observer、reconciler、executor、event、receiptを変更せず、R7第三回監査receiptを維持した。

Delivery production codeは`r2.ReadBackend`だけを保持し、remote write、publication journal、SQLite、stage、eventをauthorityまたは依存へ追加していない。

Selector／fetch／verify negative test、focused delivery／CLI、関連archive／Protocol／Parquet、fixture 23件、Python 34件、repository check、vet、gofmt、両diff check、delivery／CLI local Raceは成功した。

旧M3-3A test countはG8 evidenceへ流用せず、M3-3A R1からR7 completedの判断を維持する。

M3-5、Real R2、HTTP API、GitHub CI、M3全体final auditはG8 scope外である。

改訂記録（2026-07-15 G9-M3-5-NETWORK-FREE-E2E-FULL-GATES）: R7 pass後のdownstream taskとして、raw truthからfinal observation receiptとread-only deliveryまでをfakeだけで接続するnetwork-free E2Eを追加した。

G9はR1からR7のpublication authorityを変更せず、M2だけがclaimを作成し、M3が既存Exact claimだけを受理する境界を実行時graphで再確認した。

Diagnostic event append失敗はpublicationをvetoせず、approved object ID列、same-content retry、receipt identity、empty-cache fetch、cache reuse、reader remote write 0を検証した。

Focused E2E、関連10 package、fixture 23件、Python 34件、repository check、vet、空のgofmt、指定8 packageのlocal Windows Race Detectorは成功した。

旧52件または86件のtest countはG9 evidenceへ流用せず、R7第三回監査receiptのpassを変更または無効化しない。

Real R2はoptional skip、GitHub CIとremote I/Oは未確認であり、M3全体final auditは別gateとして未実施である。

改訂記録（2026-07-16 G9E-PRODUCTION-M2-PUBLISHER-E2E）: Whole-M3 final auditはM3-3A R7 receiptを変更せず、downstream G9のM2受入経路がproduction publisherを通らない点をchanges_requiredとした。

G9Eはdirect M2 injection helperを削除し、production `r2.NewPublisher`と`Publisher.Publish`をtemporary `PublicationJournal`およびcapability限定fake rcloneで実行した。

このjournalはM2のlocal再開状態だけを担い、M2 retry後にcloseしてからM3 publicationとread-only deliveryを実行するため、M3-3AのauthorityをSQLite、stage、eventへ戻さない。

First publishのM2 receipt、conditional claim、scope descriptor、raw object、raw-day manifestをremote exact bytesへ照合し、same-content retryがreceipt identityを維持してsuccessful claim writeとremote copy mutationを増やさないことを確認した。

M2 raw manifest key／domain digestとclaim canonical bytes／domain digestはM3 sealed bundle、complete final observation receipt、resolved replay manifest、empty-cache day verificationまで一貫する。

Fake M2 rcloneは`version`、`copyto --immutable`、`check --download`以外のoperation、異なるflag、trusted prefix外keyを拒否し、real processとnetworkを使用しない。

Focused M3 E2E、M2 E2E、関連10 package、fixture 23件、Python 34件、repository check、vet、空のgofmt、両diff check、指定8 packageのlocal Windows Race Detectorは成功した。

旧52件または86件のtest countはG9E evidenceへ流用せず、M3-3A R7第三回監査のpassを維持する。

Real R2、GitHub CI、HTTP、commit、push、mergeは未実施であり、M3全体final re-auditはpendingである。

改訂記録（2026-07-16 FINAL-DOCS-M3-COMPLETE）: Advisorのwhole-M3監査phase `final_m3_whole_reaudit`はverdict `pass`、required actionsなしとなった。

この監査は本再設計のR7第三回監査を置き換えず、R7をM3-3A publicationの独立gate、whole-M3監査をM3-4とG9Eまで含む最終gateとして分離する。

Whole-M3 evidenceはproduction M2 publisherからM3 bundle／receipt／read-only deliveryまでの単一identity graph、旧direct injection helper 0、focused E2E、関連10 package、fixture 23件、Python 34件、repository check、vet、空のgofmt、両diff check、指定8 packageのlocal Windows Race passである。

Real R2はoptional skip、GitHub CIは未確認であり、local Race passをGitHub CI passとは扱わない。

HTTP、live broker、commit、push、mergeはM3 scope外であり、旧52件または86件のtest countはwhole-M3 evidenceへ流用していない。

M3-3A R1からR7とM3全体はcompletedであり、whole-M3 `final_audit: pass`を記録する。

改訂記録（2026-07-16 M3 review findings remediation）: revision 3以降のlocal sealerがsuffixをfull graphとして誤検証する経路を、immediate predecessor successor検証とrevision数に対する保守的なpre-lock final-observation budgetへ置換した。

publication round上限を`2*MaxParts+2=20002`へ拡張し、bundle sealerとPython verifierがpart数に対する必要round数をlock前に拒否するようにした。

final observationのpre-lock request budgetはraw、derivative、revision graph edgeの各readを含めて検査する。

publisher constructorはlock rootからtrusted Layoutのcampaign／publisher epoch pathを導出し、任意filenameをlock identityとして受け付けない。

revision 3 successor、4-part fresh-observation publication、round不足のlock前拒否、canonical campaign／epoch lock pathをfocused Goで受入した。

`mise run test-python`は35 passed、`mise run fixture`は23 verified、`mise run check`、`mise exec -- go vet ./...`、指定8 packageのlocal Windows Race Detector、`git diff --check`、`git diff --cached --check`は成功した。

再確認後もbranch `agent/m3-replay-parquet-delivery`、HEAD `cc9fc2dfc114bcb394e8e58b616dfa3281c2d380`、tracked modified 32、untracked 48、staged 0を維持した。

Implementerのread-only R7 remediation auditは3件すべてをpassとし、remote I/O、GitHub CI、Real R2を未実施のlocal scope内で残存riskなしと判定した。
