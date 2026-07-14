# M2 raw off-host delivery living ExecPlan

## Purpose and Big Picture

このExecPlanは、verified sealed WALからimmutable raw snapshotを作り、fake-only環境で検証可能にし、後続のR2配信とread-only検証へ引き渡すM2全体の実装計画です。

M2のraw truthは、Gatewayがdurably acceptedしたBatchFrameV1を含むsealed WAL objectです。

M2R-1は、canonical raw-day manifest、chain-complete object記述、local semantic verifier、archive scopeをProtocol V1の正本へ固定します。

M2R-2は、local verified objectとmanifestをpinned rcloneでoptional R2へ公開し、claimだけをconditional backendで作成し、scope descriptorを含むarchive bytesをrcloneで再検証します。

M2R-3は、read-only ArchiveReader、tickctlのraw commands、tick-verify day/campaignを実装します。

M2R-4は、integrationとfake test、optional isolated real-R2 smoke、docs、CI、race、PR/review readinessを完成させます。

fake-only completionは、実R2、実broker、live MT5、production credentialがなくてもM2R-1の完了を判定できる境界です。

real R2 smokeはoptionalであり、isolated synthetic namespace、明示的credential、deleteまたはoverwriteをしない運用でのみ実施します。

Parquet、replay-day manifest、part manifest、handover、pruning、Worker、HTTP service、live broker collectionはM2の対象外です。

## Progress

- [x] 2026-07-15にbranch agent/m2-raw-offhost-deliveryがcommit 510c014でM2R-1初回実装を完了しました。
- [x] 510c014でScopeConfig、archive-config hash、campaign-scope descriptor、raw_set_root、revision、canonical JSON、Go/Python fixture verifierを追加しました。
- [x] 510c014でcross-day batch、same-object disjoint range、zero-record sentinel、CopyTicksError、campaign continuityの初期contractを追加しました。
- [x] 2026-07-15のcorrective auditで、raw WAL keyをobjects/raw/wal-<64 lowercase sha256>.rtwへ固定しました。
- [x] 2026-07-15のcorrective auditで、full chain sliceを表すchain_objectsをmanifestへ追加しました。
- [x] 2026-07-15のcorrective auditで、VerifyRawDaySnapshotとA/B/A chain fixture、missing、tamper、false boundary、forged keyのfocused testを追加しました。
- [x] 2026-07-15にfixture 19件、Go archive/protocol test、Python test 16件、repository check、go vet、git diff --checkを成功させました。
- [x] 2026-07-15にM2R-2のrclone lock、campaign layout、conditional publisher claim、local exclusive lock、独立SQLite journal、revision graph validator、出版reconcileを実装しました。
- [x] M2R-2のpublication orderはclaim、local semantic verify、scope descriptorのrclone transfer、raw copy、raw check、manifest直前recheck、manifest copy/check、receipt保存へ固定しました。
- [x] journal stageはAdvanceStageで同一または後続stageを冪等に扱い、各永続stage後のcrash/restart、claim競合、lock競合、manifest前停止、check失敗をfocused testで確認しました。
- [x] receipt保存はsync済みtemporary fileをhard-linkでfinalへno-clobber publishし、partial finalを公開せず、同内容retryと異内容拒否を確認しました。
- [ ] M2R-2の実R2 smoke、追加のremote mutation variants、CI race実行は未実施です。
- [x] M2R-2 corrective acceptanceで、既存raw keyのimmutable collision、secret非漏洩、context deadline後のreconcile成功を追加確認しました。
- [ ] local環境ではgccとclangが存在しないためrace testは実行せず、mise exec -- go test -race ./internal/r2 ./internal/archiveはGitHub Windows CIで実行します。
- [ ] M2R-3のArchiveReader、tickctl raw commands、tick-verify day/campaignは未実装です。
- [ ] M2R-4のintegration、optional real-R2 smoke、CI/race、PR/review readinessは未実装です。
- [ ] M2全体はM2R-1からM2R-4までの完了を確認するまで未完了です。

## Surprises & Discoveries

- sealed WALのobject SHA-256はtrailerを含むfile全体を対象にし、trailerのfile_sha256とは異なるため、両者を混同してはいけません。
- day-selected rangeだけではcross-day WAL objectを完全に再検証できないため、最初のselected entryから最後のselected entryまでのfull sealed object chainをmanifestへ保持する必要があります。
- A/B/A campaignではday AのobjectsがAとCだけを参照し、middle Bをchain_objectsだけへ保持することで、raw_set_rootとchain continuityの責務を分離できます。
- WALから導出できないlogical close time、terminal status、settle policy、publisher identityはBuildRawDayManifestの明示inputとし、wall clockを暗黙使用してはいけません。
- WALはdataset identityを暗号学的に保持しないため、archive ScopeConfigとno-clobber descriptorがoperator trust rootになります。
- Protocol V1のcanonical JSONはRFC 8785 JCSではなく、UTF-8 byte順key、integer-only、lowercase Unicode escape、BOMなし、空白なし、末尾改行なしの独自規則です。
- receiptのfinal pathへ直接O_EXCL writeすると、process crash後にpartial finalが残り、same-content retryを妨げるため、archive raw promotionと同じtemporary sync plus hard-link方式が必要です。

## Decision Log

- 決定: raw WAL object keyをcampaign-relativeなobjects/raw/wal-<64 lowercase sha256>.rtwへ固定します。
- 理由: keyのalias、absolute path、dot segment、backslash、drive、UNCを全て同じ厳密な文字列比較で拒否し、content hashとrangeをbindするためです。
- 決定: RawWALObjectKey([32]byte) stringをlocal promotionとmanifest buildの共通helperにします。
- 理由: local path、manifest key、strict decoder、Python verifierが同じkey derivationを使い、digest後の再mappingを禁止するためです。
- 決定: raw-day manifestにobjectsとchain_objectsを別々に持たせます。
- 理由: raw_set_rootはday-selected rangesだけにbindし、chain_objectsはfull sealed objectのsequenceとhash continuityをbindするためです。
- 決定: chain_objectsは最初のselected entryから最後のselected entryまでに交差する最小完全集合です。
- 理由: middle objectを省略したday snapshotをsemantic verifierが受理しないようにするためです。
- 決定: successor revisionはscope、publisher epoch、chain start、previous prefixを保持し、chain end、objects、chain_objects、counts、watermarksを同じか前方へ単調拡張できます。
- 理由: late source evidenceを追加するrevisionを許しながら、過去のselected evidenceを変更しないためです。
- 決定: empty manifestはobjectsとchain_objectsを空配列にし、chain sequenceとrootをzeroにします。
- 理由: dayに該当するrecordとzero-record sentinelがない状態を曖昧な未初期化値と区別するためです。
- 決定: canonical JSONのmanifest digestへmanifest digest自身を埋め込みません。
- 理由: digest再帰を避け、domain prefixとcanonical bytesだけでstable digestを得るためです。
- 決定: source identity bytesはnormalization、case folding、trimをせずにScopeConfigへ保持します。
- 理由: exact source symbolとbroker identityの意味を変えず、path componentだけをSHA-256 lowercase hexへ縮約するためです。
- 決定: M2R-1の完了判定はfake-only local sealed WALとgolden fixtureを必須にし、実R2を必須にしません。
- 理由: protocol、manifest、semantic verifierをnetworkとcredentialから独立して再現可能にするためです。
- 決定: M2R-2のR2 smokeはoptional isolated synthetic namespaceに限定します。
- 理由: 実データ、既存bucket、他publisherのobjectへ影響を与えずにrcloneとreceiptだけを確認するためです。
- 決定: rcloneはv1.74.4のGOOS、GOARCH、archive URL、archive SHA-256、executable SHA-256、byte lengthをtools/tick-data-tools.lock.tomlへ固定し、runtime platformの選択後に実行ファイルを再検証します。
- 理由: 配信処理の再現性を保ち、改竄されたbinary、別version、別platformの実行をpublication前に停止するためです。
- 決定: raw publicationのclaimはconditional backendのPutObject IfNoneMatch "*"で作り、precondition-exists時はGETしてbyte-identical retryだけを許可します。
- 理由: publisher epochごとのsingle writerをremote側でもno-clobberにし、異なるpublisherまたは内容のclaimを上書きしないためです。
- 決定: publication journalはinternal/journalのingestion stateと分離したSQLite storeにし、immutable intentとstage transitionをFULL durabilityで保存します。
- 理由: ingestion WALの状態や既存journalをpublication recoveryの根拠にせず、raw object、manifest、claim、receiptの再開点を独立に保持するためです。
- 決定: manifest copy直前に全unique ChainObjectのcheck --downloadを再実行し、remote mutationを検出した場合はmanifestを公開しません。
- 理由: raw objectのremote検証済み状態をjournal flagだけで信頼せず、data-before-manifestの不変条件を守るためです。
- 決定: local verification receiptはsync済みtemporary fileをfinal pathへhard-linkし、partial finalを残さないno-clobber publishにします。
- 理由: crash後に残るtemporary fileは安全に破棄でき、final pathは完全bytesだけを指すためです。
- 決定: ImmutableRootは設定値として一度だけcampaign prefixの上位へ付け、RcloneRootは同じcanonical relative keyから独立にlocatorを導出します。
- 理由: S3 keyとrclone locatorのどちらにもv1やcampaign prefixを二重連結せず、raw objectとmanifestのrelative keyを不変に保つためです。
- 決定: conditional backendのPutIfAbsentはpublisher claimだけに使用し、scope descriptor、raw object、manifestはpinned rcloneのcopyto --immutableとcheck --downloadで転送・検証します。
- 理由: claimのremote single-writer条件とarchive bytesのtransfer pathを分離し、二重の書き込み実装による検証漏れを防ぐためです。
- 決定: publication journalのstage更新はAdvanceStageでcurrent以上を成功扱いし、永続stageを後退させません。
- 理由: crash後のreconcileが既に完了したstageを再適用してもbackward transition errorにならないためです。
- 決定: raw objectの既存key異内容、secret sentinel、timeoutを独立したfake publication recovery contractとして検証します。
- 理由: immutable no-clobber、credential非漏洩、retryable transport failureを同じ成功経路の副作用で見落とさないためです。
- 決定: Parquet、replay-day、part manifest、handover、pruning、Worker、HTTP、live brokerはM2から除外します。
- 理由: raw evidence deliveryとderivative replay、運用handover、service runtimeの責務を混ぜないためです。

## Outcomes & Retrospective

510c014のM2R-1初回実装で、Protocol V1のcanonical JSON、raw_set_root、ScopeConfig、campaign-scope descriptor、raw-day buildとstrict decodeの基礎が完成しました。

今回のcorrective taskでは、local raw promotionのlegacy keyを廃止し、manifestとlocal object pathを同一のcanonical keyへ収束させます。

今回のcorrective taskでは、chain_objectsとVerifyRawDaySnapshotにより、day-selected rangeだけでは見えないmiddle WAL objectを含むself-contained snapshotを実装します。

今回のcorrective taskでは、Go semantic verifierが実bytes、segment bounds、entry chain、UTC-day range、sentinel、counts、watermarksを再導出します。

M2R-1のfake-only成果は、M2R-2のremote publicationがなくてもreview可能なprotocolとlocal archive contractです。

M2R-2では、fake conditional backendとfake rclone executorでexact argv、claim-only conditional write、rclone descriptor/raw/manifest transfer、fault recovery、stage restart、secret非漏洩境界を検証しました。

M2R-2のlocal verificationはfixture 19件、Python 16件、Go r2/archive、repository check、Go vet、git diff --checkを成功させました。

M2R-2 corrective verificationは異内容raw collision時のno-clobberとdata-before-manifest、environment secretの非漏洩、context deadline後の同一intent再開を成功させました。

実R2 smokeは明示的なenable flag、isolated bucket/prefix、credentialが同時に存在する実行環境を提供されていないため実施せず、remote mutationの実環境確認はM2R-4のoptional smokeへ残します。

local環境にgccとclangがないためrace testは実施せず、mise exec -- go test -race ./internal/r2 ./internal/archiveをGitHub Windows CIで実行する未解決検証項目として残します。

M2R-2以降の実装開始時には、このExecPlanのProgress、Surprises & Discoveries、Decision Log、Artifacts and Notesを先に更新します。

M2全体の完了時には、real R2 smokeを実施しなかった場合もその理由、scope、未実施境界をOutcomes & Retrospectiveへ記録します。

## Context and Orientation

protocol/v1はwire layout、message、WAL、hash domain、manifest、golden fixture、conformanceのlanguage-neutralな正本です。

internal/walはsealed WALのheader、entry、CRC、chain、trailer、file hash、object hashを検証します。

internal/archiveはverified RawObject、ScopeConfig、campaign-scope descriptor、raw-day manifest、local semantic verifierを所有します。

internal/protocolはProtocol V1 canonical JSONとmessage/frame codecを所有します。

tools/tick_protocol.pyとtools/tick_fixture_verify.pyはGo実装から独立したPython canonical JSON、hash、manifest、fixture verifierです。

testdata/tickdata/goldenはaccepted wire、WAL、canonical JSON、hash、rejected mutation、stateful scenarioを固定します。

M2R-1が扱う入力はwal.VerifySealedSegment済みのRawObjectだけであり、active WAL、unverified path、live broker responseは入力にできません。

BuildRawDayManifestはScopeConfig、UTC date、RawObjects、revision、previous manifest、status、logical close timeを受け取ります。

BuildRawDayManifestはwall clock、filesystem traversal、R2 response、environment variableを暗黙参照しません。

## Plan of Work

### M2R-1 canonical raw-day manifest and archive semantics

Protocol V1 canonical-json-v1をUTF-8、BOMなし、空白なし、末尾改行なし、UTF-8 byte順key、integer-only、lowercase Unicode escapeとして固定します。

strict decoderはunknown key、duplicate key、float、leading zero、negative zero、指数表記、invalid UTF-8、noncanonical bytes、schema range違反を拒否します。

raw_set_rootはdomain prefix、U32 element count、object SHA-256、object bytes、inclusive sequence、ordinalをordered objects rangeごとにlittle-endianでbindします。

manifest digestはraw-day domain prefixとcanonical manifest bytesだけから計算し、digest自身をJSONへ含めません。

RawWALObjectKeyはsealed object全体のSHA-256からexact ASCII keyを作ります。

local promotionはobjects/raw/wal-<64 lowercase sha256>.rtwへ直接publishし、post-digest remapをしません。

strict manifest decodeとBuildRawDayManifestはkey、SHA、canonical pathを一対一で照合します。

RawChainObjectはkey、sha256、bytes、start_ingest_sequence、end_ingest_sequenceだけを持つtop-level chain_objects elementです。

chain_objectsはfull sealed WAL objectの最小ordered setであり、sequence gap、overlap、duplicate、slice外、prefix違反を拒否します。

objects rangeは同じkey、hash、bytesのchain objectへ正確にbindし、そのobject boundsとchain slice bounds内に限定します。

zero-record batchはRequestedFromMSCのUTC dayへordinal 0 sentinelとして割り当て、accepted countには加えず、CopyTicksErrorはerror countへ加えます。

non-empty batchのordinalは0からN-1であり、同一objectの複数disjoint rangeをstrict ascending non-overlapで保持します。

chain slice start rootは最初のselected entryのPreviousEntryHashであり、end rootは最後のselected entryのEntryHashです。

revision genesisはrevision 1かつprevious nullであり、successorはprevious revision plus oneとprevious manifest digestを要求します。

revision successorはscope、publisher epoch、chain start、Objects prefix、ChainObjects prefixを保持します。

revision successorはchain endを維持または前方延長し、accepted count、error count、source watermark、capture watermarkを減少させません。

ScopeConfigはdataset_id、campaign_id、provider_id、stable_feed_id、exact_source_symbol、broker_server_fingerprint、gateway build identity、producer build identity、day_definition_id、settle_policy、publisher_id、publisher_epoch、Protocol limitsを持ちます。

archive config canonical documentはsecret、environment variable名、absolute pathを含めず、archive-config domain prefixを付けてSHA-256します。

EnsureCampaignScopeDescriptorはhash-derived safe pathへno-clobber descriptorを作り、same content retryを成功させ、different contentをarchive.ErrIntegrityにします。

VerifyRawDaySnapshotは全chain objectをpathから再openし、wal.VerifySealedSegment、SHA、bytes、bounds、cross-object continuity、entry chain、day ranges、sentinel、counts、watermarksを検証します。

VerifyRawDaySnapshotはmissing、tampered、false boundary、cross-array mismatchをarchive.ErrIntegrityとしてfail closedします。

### M2R-2 raw R2 publication

R2 publicationはM2R-1でlocal semantic verification済みのobjectとmanifestだけを入力にします。

rclone binaryはGOOS、GOARCH、official archive URL、archive SHA-256、executable SHA-256、byte lengthをlockし、runtime platformをstrictに選択してversion出力を検証します。

campaign prefixはhash-safe scope identityから導出し、configured ImmutableRootを一度だけ上位へ付け、S3 keyとrclone locatorは同じcampaign-relative keyから独立に導出します。

scope descriptorはcanonical local bytesを保存してrclone copyto --immutableとcheck --downloadで先に公開し、conditional backendのPutIfAbsentはpublisher claimに限定します。

publisher claimはcampaign scopeとpublisher epochをbindし、conditional createとtransitionをno-clobberで検証します。

local lockは同じcampaign scopeとpublisher epochの同時publisherを一つに制限し、stale lock recoveryをowner identityとlease evidenceへ限定します。

publication journalはobject upload、manifest upload、claim、receiptのintent、state、hash、remote key、error、retryをdurably記録します。

AdvanceStageは既存stage以上を冪等成功とし、reconcileは各永続stageから後続処理へ進み、後退遷移を発生させません。

reconcileはlocal journal、remote object metadata、downloaded bytes、manifest referencesをread-after-write検証し、不一致を自動成功にしません。

publication receiptはpublisher claim、scope config hash、rclone profile、object hashes、manifest digest、remote verification completionをbindします。

M2R-2はremote delete、overwrite、sync、unscoped copyを実行しません。

### M2R-3 read-only ArchiveReader and verification CLI

ArchiveReaderはread-only tokenとremote manifestからscope、campaign、revision、objects、chain_objectsを解決します。

ArchiveReaderはremote bytesをlocal temporary pathへ取得し、VerifyRawDaySnapshot相当のlocal semantic verificationを完了してからconsumerへ返します。

tickctl raw commandsはdatasets、campaigns、snapshots、fetchのread-only raw delivery contractを提供します。

tick-verify dayはmanifestのselected chain sliceとday rangesを検証し、campaign genesisからの完全chain検証とは主張しません。

tick-verify campaignは指定campaignのchain object setをsequenceとhash continuityに沿って検証します。

M2R-3は前日prefix、gateway SQLite、write credential、Parquet、replay-day manifestをread pathの前提にしません。

### M2R-4 integration and release readiness

integration testはfake producer、local WAL、local raw promotion、manifest build、read-only fetch、semantic verifyをnetworkなしで結線します。

fake testはcross-day batch、same-object disjoint range、zero-record error、A/B/A chain、revision extension、missing middle、tamper、false boundary、key traversalを再現します。

optional real-R2 smokeはsynthetic objectとisolated prefixだけを使い、claim、immutable upload、download verify、receipt、reconcileを確認します。

docsはProtocol V1、archive semantics、operator scope trust root、fake-only completion、optional real-R2 boundary、未実装対象を一致させます。

CIはGo test、Go vet、Python test、fixture、Ruff、gofmt、diff check、必要なrace testを実行します。

PR readinessはscope内差分、secretとruntime dataの不在、verification command、unresolved risk、review boundaryを記録します。

## Concrete Steps

作業開始時にgit status --short --branchでcleanまたは既存差分を記録し、branchがagent/m2-raw-offhost-delivery由来であることを確認します。

既存の510c014とユーザー変更をrevertせず、scope外変更が必要なら実装せずunresolvedへ返します。

M2R-1ではinternal/archive、internal/protocol、protocol/v1、tools、tests、testdata、ExecPlanだけを変更します。

RawWALObjectKeyとraw promotionのpathを一致させ、legacy raw-wal-segment-v1 pathのtest期待値を更新します。

manifest canonical map、strict parser、Go verifier、Python verifier、golden fixtureを同時に更新します。

Go focused testでlocal objectをBuildRawDayManifestへ渡し、manifest canonical bytes、raw_set_root、manifest digest、revision chainを再計算します。

Go focused testでVerifyRawDaySnapshotへkeyからlocal pathを渡し、success、missing、tamper、false boundary、cross-array mismatchを確認します。

Go focused testでA/B/Aの3 sealed objectを作り、day AのobjectsがAとCだけでchain_objectsがA、B、Cになることを確認します。

Go focused testでforged、traversal、backslash、drive、UNC、hash mismatch keyをarchive.ErrIntegrityとして確認します。

M2R-2ではrclone profile、claim、lock、journal、reconcile、receiptの各state transitionをcrash-safeに実装します。

M2R-3ではArchiveReaderとtickctl raw commandsをread-only contractへ接続し、dayとcampaignのverification resultを分離します。

M2R-4ではfake integration、optional isolated real-R2 smoke、docs、CI、race、PR review evidenceを追加します。

## Validation and Acceptance

M2R-1のfixture検証はmise run fixtureで実行します。

M2R-1のGo focused検証はmise exec -- go test ./internal/archive ./internal/protocolで実行します。

Python検証はmise run test-pythonで実行します。

repository全体の通常検証はmise run checkで実行します。

静的なGo検証はmise exec -- go vet ./...で実行します。

差分の空白検証はgit diff --checkで実行します。

canonical JSON fixtureはGoとPythonで同じbytes、manifest digest、raw_set_root、unknown key rejection、duplicate key rejectionを示します。

golden raw-day fixtureはrevision 1、previous null、canonical raw key、chain_objects、raw_set_rootを固定します。

golden A/B/A fixtureはday-selected objectsとchain-complete chain_objectsの差を固定します。

BuildRawDayManifestはverified sealed WAL以外、caller-forged key、discontinuous campaign chainを受け付けません。

VerifyRawDaySnapshotはmissing、tampered、false boundary、cross-array mismatchをarchive.ErrIntegrityで拒否します。

M2R-1はfake-only local verificationと全指定gateが成功し、実R2がなくてもcompletion判定できる場合に完了です。

M2R-2 focused検証はmise exec -- go test ./internal/r2 ./internal/archiveで実行し、初回、同内容retry、collision、publisher conflict、各stage restart、data-before-manifest、check failure、lock conflictを確認します。

M2R-2 corrective focused検証は既存raw keyの異内容collision時の原bytes保持とmanifest不在、secret sentinelのargv・error・journal・receipt非包含、context deadline後の再開成功を確認します。

M2R-2 corrective focused検証はmise exec -- go test ./internal/r2で実行し、上記三つの追加contractを成功させます。

M2R-2はmise run check、mise exec -- go vet ./...、可能ならinternal/r2とinternal/archiveのrace test、git diff --checkが成功した場合にlocal gateを完了とします。

race compilerがない環境ではコマンドを弱めず、mise exec -- go test -race ./internal/r2 ./internal/archiveをGitHub Windows CIの実行項目としてunresolvedへ記録します。

M2全体はM2R-1、M2R-2、M2R-3、M2R-4とその未実施境界が全て確認されるまで完了ではありません。

## Idempotence and Recovery

local raw promotionは同じsealed bytesのretryを同じkey、path、hashで成功させます。

local raw promotionは同じkeyの異なるbytesを上書きせずarchive.ErrIntegrityで停止します。

campaign-scope descriptorは同一canonical config retryを成功させ、異なるconfigをno-clobberで拒否します。

BuildRawDayManifestは同じverified object、scope、date、explicit input、previous manifestから同じcanonical bytesとdigestを作ります。

revision successorは既存manifestを変更せず、新しいrevision、previous digest、累積prefixだけを追加します。

VerifyRawDaySnapshotは毎回object pathを再openし、cached metadataだけをtrustしません。

M2R-2のjournal recoveryはintentを再読し、remote bytesを再検証し、未完了stateを同じimmutable operationへretryします。

M2R-2のrclone retryはclaim、object、manifest、receiptの順序とno-clobber条件を再確認します。

M2R-2のscope descriptor retryは同じcanonical local bytesをrclone checkで受理し、異なるbytesやremote mutationをmanifest公開前に停止します。

M2R-3のfetch failureはtemporary bytesをdiscardし、partial objectをconsumerへ返しません。

## Artifacts and Notes

M2R-1の主要artifactはprotocol/v1/hash-domains.md、protocol/v1/manifests.md、protocol/v1/fixtures/README.mdです。

M2R-1のGo artifactはinternal/archive/raw_key.go、internal/archive/raw.go、internal/archive/manifest.go、focused testsです。

M2R-1のPython artifactはtools/tick_protocol.py、tools/tick_fixture_verify.py、tests/unit、tests/statefulです。

M2R-1のgolden artifactはtestdata/tickdata/golden/raw-day-manifest-v1.json、raw-day-manifest-chain-slice-v1.json、index.jsonです。

M2R-2のGo artifactはinternal/r2/tool.go、layout.go、claim.go、backend.go、lock.go、revision.go、journal.go、publisher.go、receipt.goとfocused testsです。

M2R-2のtool artifactはtools/tick-data-tools.lock.tomlであり、credential、runtime journal、R2 object、実行用configはcommitしません。

parent plan .agent/tick-data-platform-execplan-revised.mdにはJCSではなくProtocol V1 canonical-json-v1であること、canonical key、chain_objects、monotonic revisionのDecision Logを残します。

commit 510c014はM2R-1初回実装の既存履歴であり、今回のcorrective taskはそれを保持して追加のscoped fix commitだけを作ります。

今回のcommit messageはfix: make raw-day chain slices self-containedです。

M2R-2以降のartifact pathは、実装着手時にこのExecPlanのscopeと既存repository構成を確認してから確定します。

## Interfaces and Dependencies

RawWALObjectKeyはarchive package内のlocal promotion、BuildRawDayManifest、strict validationが共有するkey interfaceです。

RawObjectはverified sealed WALのcanonical key、local path、complete-file SHA-256、byte size、VerifiedSegmentを保持します。

RawDayManifestはscope identity、date、revision、objects、chain_objects、counts、watermarks、chain roots、raw_set_root、previous digestを保持します。

RawChainObjectはselected chain sliceを覆うsealed WAL objectのcontent identityとsequence boundsを保持します。

VerifyRawDaySnapshotはmanifestとmap[string]stringのobject pathを受け、成功またはarchive.ErrIntegrityを返します。

ScopeConfig.ConfigHashはsecret、environment variable名、absolute pathを含まないcanonical config documentのarchive-config domain digestを返します。

EnsureCampaignScopeDescriptorはrootとScopeConfigを受け、no-clobber descriptor pathまたはerrorを返します。

GoとPythonのcanonical JSON実装はProtocol V1の同じ restricted value setを受け、同じcanonical bytesを返します。

M2R-2はrclone executable、pinned profile、R2 endpoint、read/write credential、local journal storageへ依存します。

M2R-3はM2R-2のpublished manifest、raw object、receiptまたはlocal equivalentへ依存します。

M2R-4はM2R-1からM2R-3のcontractとfake producerへ依存します。

operator configはWALだけでは証明できないdataset identity、campaign identity、provider identity、exact source identityのtrust rootであり、その限界をdocsへ明記します。
