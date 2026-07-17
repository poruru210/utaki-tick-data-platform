# M2のローカルraw WAL基盤を実装する

このExecPlanはliving documentである。

Progress、Surprises & Discoveries、Decision Log、Outcomes & Retrospectiveを作業中に更新し、常にこの文書だけで作業を再開できる状態を保つ。

repository内にPLANS.mdはない。

この文書はexecplan skillが参照するPLANS.mdの方法に従う。

## Purpose / Big Picture

M1のGatewayは受理したBatchFrameV1をactive WALへ保存できるが、WALをclose済みの不変segmentへ移し、別工程が安全に扱えるraw objectへ昇格する機能を持たない。

この変更後は、operatorまたは後続のpublication処理がactive WALをsealして新しいactive WALへ切り替え、seal済みbytesを検証してcontent-addressed outboxへ置ける。

Gatewayを再起動した場合も、seal済みsegmentとactive WALを合わせてaccepted batch inventory、gateway ingest sequence、entry hash chainを復元できる。

動作はGo testで確認する。

testは複数segmentを作成し、journalを削除してGatewayを再起動し、同じaccepted batch inventoryとcursorを復元する。

別のtestはseal済みsegmentをoutboxへ二回promoteし、同じkeyとbytesを返すことを確認する。

破損、partial trailer、same-key different-content、segment間chain不整合は成功扱いにせず、integrity failureとして観測できる。

## Progress

- [x] (2026-07-14 17:32Z) PR #1のmerge commit d9df267を取得し、origin/mainからagent/m2-local-raw-wal branchを作成した。
- [x] (2026-07-14 17:32Z) Protocol V1のWAL layout、M1のWAL実装、Gatewayのjournal rebuild経路、既存testを確認した。
- [x] (2026-07-14 17:32Z) seal、rotation、segment scan、outbox promoteの設計をこのExecPlanへ記録した。
- [x] (2026-07-14 17:33Z) 変更前のmise run checkを実行し、Go全package、Python 13件、Protocol V1 fixture 18件、format checkが成功するbaselineを確認した。
- [x] (2026-07-14 17:43Z) internal/walへsealed trailer、segment verifier、rotation、startup recoveryを実装した。
- [x] (2026-07-14 17:43Z) internal/archiveへbyte-exactなcontent-addressed outbox promoteを実装した。
- [x] (2026-07-14 17:43Z) segmentをまたぐGatewayのjournal rebuildとduplicate判定をtestした。
- [x] (2026-07-14 17:43Z) partial trailer、incomplete active header、link後crash、corruption、cross-segment chain mismatch、same-content retry、same-key different-contentをtestした。
- [x] (2026-07-14 17:49Z) README、roadmap、親ExecPlan、検証記録を実装結果へ更新し、M2全体が未完了である境界を残した。
- [x] (2026-07-14 17:54Z) mise run checkを成功させ、GitHub Actionsのpush起動run 29355558781とPull Request起動run 29355582088でWindows Race Detectorを成功させた。
- [x] (2026-07-14 17:49Z) zero-base code reviewを行い、partial next headerの復旧範囲とsealed inventoryのno-clobber処理を修正した。
- [x] (2026-07-14 17:57Z) 実装と検証記録をbranchへpushし、main向けPull Request #2を作成してReadyへ移行した。
- [x] (2026-07-14 18:03Z) Pull Request #2のP2 reviewを妥当と評価し、valid entry内部のTWTR bytesをtrailerと誤認しないentry-boundary判定とregression testを追加した。
- [x] (2026-07-14 18:06Z) review修正commit 7c5dcf8へmise run checkとgo vetを実行し、push起動run 29356434110とPull Request起動run 29356436500でWindows Race Detectorを成功させた。

## Surprises & Discoveries

- Observation: Protocol V1のTWTR trailerにあるfile_sha256は、名前に反してtrailerを含むfile全体ではなく、headerと全entryのbytesを対象にする。
  Evidence: protocol/v1/wal-layout.mdは「complete file before trailer」と明記し、wal-entry-v1 fixtureもその値を固定している。

- Observation: M1のStore.loadは最初のentryのprevious_entry_hashをzeroと仮定するため、二つ目以降のsegmentをそのまま検証できない。
  Evidence: internal/wal/wal.goのloadはpreviousをzero valueで初期化し、最初のentryと比較する。

- Observation: Gatewayのjournal rebuildとduplicate探索はStore.Entriesの全件走査へ依存する。
  Evidence: internal/ingest/gateway.goのfindWALIdentity、reconcileJournal、journalMatchesWALがStore.Entriesを使う。

- Observation: local Windows環境のGo toolchainはCGO_ENABLED=0であり、PATH上にGCC executableがない。
  Evidence: mise exec -- go env GOOS GOARCH CGO_ENABLED CCはwindows、amd64、0、gccを返し、Get-Command gccはpathを返さなかった。

- Observation: next active headerは固定30 byteを書いた後、gateway instance IDの途中で停止する場合がある。
  Evidence: 6 byteだけでなく32 byteまで書かれたheader prefixをtestしたところ、初期実装は後者をintegrity failureとしていた。

- Observation: valid WAL entryの内部でも、file末尾から96 byteの位置にTWTRと同じ4 byteが現れ得る。
  Evidence: RawMqlTickV1のFlagsを0x52545754にしたvalid BatchFrameV1で同じ配置を再現し、初期実装がsealed trailerと誤認することを確認した。

## Decision Log

- Decision: trailer内のhashをTrailerFileSHA256、sealed file全体のhashをObjectSHA256として別の値にする。
  Rationale: 前者はProtocol V1のwire contractであり、後者はraw objectの全bytesをcontent-addressingするために必要である。
  Date/Author: 2026-07-14 / Codex

- Decision: Storeはroot直下のactive.walとroot/sealed配下のseal済みsegmentを所有する。
  Rationale: WAL recoveryはoutboxやR2の状態に依存できず、local WAL inventoryだけでjournalを再構築する必要がある。
  Date/Author: 2026-07-14 / Codex

- Decision: seal済みsegmentはsequence範囲を含むdeterministic filenameで保存し、outbox objectだけをObjectSHA256でaddressingする。
  Rationale: WAL layerは収集順序を復旧し、archive layerはimmutable object identityを提供するため、責務が異なる。
  Date/Author: 2026-07-14 / Codex

- Decision: outbox promoteはseal済みsegmentを削除せず、同じfilesystem上のtemporary fileを検証してからno-clobberのatomic publishを行う。
  Rationale: local pruningはM4まで禁止されており、WAL recoveryをoutboxの存在へ依存させない。
  Date/Author: 2026-07-14 / Codex

- Decision: active WALからsealed inventoryへの公開にもhard linkの作成とactive pathの削除を使う。
  Rationale: os.Renameはplatformによって既存destinationを置換するため、同じsequence範囲の異なるbytesを上書きしない要件をportableに満たせない。
  Date/Author: 2026-07-14 / Codex

- Decision: Store.Sealは空のactive WALをsealしない。
  Rationale: entryを持たないsegmentにはfirst sequence、last sequence、chain rootの意味がなく、Protocol V1 trailerを正しく構成できない。
  Date/Author: 2026-07-14 / Codex

- Decision: standalone verifierは最初のentryが持つprevious_entry_hashをChainStartとして返し、Storeのstartup scanが直前segmentのChainRootとの一致を検証する。
  Rationale: trailerはpredecessor anchorを持たないが、各entryはprevious_entry_hashを持つため、segment内部の検証とscope内の連結検証を分離できる。
  Date/Author: 2026-07-14 / Codex

- Decision: active WALのsealed recoveryは、headerからentryを順にparseし、entry boundaryで残りが正確に96 byteかつTWTR magicを持つ場合だけ開始する。
  Rationale: file末尾からの固定offset判定では、valid BatchFrameV1 payload内の同じ4 byteをtrailerと誤認するためである。
  Date/Author: 2026-07-14 / Codex

- Decision: このGoalでは明示的なStore.Seal APIを実装し、自動rotation policyやR2 publication schedulerは追加しない。
  Rationale: byte上限や時刻によるrotation policyは運用設定であり、local sealと検証のcorrectnessから独立して後続作業で決められる。
  Date/Author: 2026-07-14 / Codex

## Outcomes & Retrospective

WAL seal、TWTR encoderとverifier、segment間chain、startup recovery、Gateway journal rebuild、content-addressed outbox promoteを実装した。

Protocol V1 golden fixture、partial trailer、incomplete header、hard-link途中状態、entryとtrailerのmutation、cross-segment chain mismatch、同時promoteをGo testで検証した。

Pull Request reviewで発見したpayload内TWTRの誤認を修正し、valid active WALと本物のsealed trailerをentry boundaryで区別するtestを追加した。

R2 publication、raw-day manifest、delivery CLI、Parquet、pruningはこの作業へ含めていない。

local repository gateは成功し、local workstationにないGCCを備えたGitHub ActionsのWindows runnerでRace Detectorも成功した。

実装と検証記録はPull Request #2として公開し、mergeは行っていない。

## Context and Orientation

Protocol V1の正本はprotocol/v1である。

protocol/v1/wal-layout.mdはGatewayWalSegmentV1のheader、entry、TWTR trailerをbyte単位で定義する。

**active WAL**はGatewayが新しいaccepted batchをappendするroot/active.walであり、trailerを持たない。

**sealed segment**はactive WALの末尾へ有効なTWTR trailerを書き、以後変更しないfileである。

**TrailerFileSHA256**はProtocol V1 trailerのfile_sha256 fieldであり、headerと全entryを合わせたtrailer直前までのbytesをhashする。

**ObjectSHA256**はTWTR trailerを含むsealed segment全体のbytesをhashする。

**ChainStart**はsegment内の最初のentryに記録されたprevious_entry_hashである。

**ChainRoot**はsegment内の最後のentryのwal_entry_hashである。

**content-addressed object**は内容のObjectSHA256をkeyへ含め、異なるbytesが同じkeyを共有しないfileである。

internal/wal/wal.goはM1のStoreを実装する。

現在のStoreはroot/active.walだけを開き、BatchFrameV1をappendしてfile syncし、partial entry tailを切り詰める。

Store.EntriesはGatewayのjournal rebuildとduplicate判定に使われるため、M2ではseal済みsegmentとactive WALのentryをglobal sequence順で返す必要がある。

internal/ingest/gateway.goはwal.StoreとSQLite journalを開く。

GatewayはStore.Entriesからjournalを再構築するため、Storeが全segmentを正しく復元すればGateway側のraw truth境界を維持できる。

internal/archive/doc.goはarchive packageの責務だけを宣言している。

M2では同packageへlocal outbox promoteを追加する。

outboxはR2 upload前のimmutable local object置き場であり、このGoalではnetworkへ接続しない。

testdata/tickdata/golden/wal-entry-v1.jsonは有効なheader、entry、trailerと期待hashを持つ。

segment verifierはこのfixtureも受理し、既存のProtocol V1 contractと一致することを証明する。

## Plan of Work

最初にinternal/walを、active fileだけを扱うStoreから複数segmentを復旧できるStoreへ拡張する。

新しいsegment用fileにはTWTRの定数、96 byte trailer encoder、sealed segment metadata、standalone verifierを置く。

verifierはfileを一度読み、header、entry length、entry version、flags、BatchFrameV1、commit marker、CRC32C、gateway_batch_sha256、wal_entry_hash、sequence連続性、entry chain、trailer fields、TrailerFileSHA256、trailer CRC32Cを検証する。

verifierは最後にsealed file全体のObjectSHA256を計算する。

次にStore.Openを変更し、root/sealed内のsegmentを検証してstart sequence順に並べる。

最初のsegmentはsequence 1とzero ChainStartを要求し、後続segmentは直前のlast sequenceの次から始まり、ChainStartが直前のChainRootと一致することを要求する。

active.walがなければ、最後のsealed segmentの次sequenceとChainRootを引き継ぐ新しいheaderを作る。

active.walが有効なtrailerを持つ場合は、rotationがtrailer sync後に停止した状態としてroot/sealedへ移し、新しいactive.walを作る。

active.walの末尾がTWTRのprefixだけを持つ場合は、最後のvalid entry末尾まで切り詰め、active WALとして再開する。

完全なtrailerが存在してhashまたはCRCが不正な場合と、committed entryやsegment間chainが不正な場合はErrIntegrityを返す。

Store.Sealはactive WALのentryが一件以上ある場合だけ実行する。

同methodはTrailerFileSHA256を計算してTWTRをappendし、file syncとcloseを行い、deterministic sealed pathへrenameする。

rename先が既に存在する場合は両fileを検証し、byte-identicalなretryだけを成功扱いにする。

seal後は直前のChainRootと次sequenceを引き継ぐ新しいactive.walを作り、Store.Entriesが全entryを保持する。

その後、internal/archive/raw.goへPromoteSealedSegmentを追加する。

同functionはwal.VerifySealedSegmentを必ず呼び、raw-wal-segment-v1/sha256/<先頭2文字>/<ObjectSHA256>.walというrelative keyを作る。

同functionはdestination directoryへtemporary fileを作成し、source bytesをcopyしてfile syncし、temporary fileを再検証する。

検証済みtemporary fileは既存destinationを上書きしないatomic operationで公開する。

destinationが既に存在する場合はsize、ObjectSHA256、sealed segment contractを再検証し、同じbytesなら既存objectを返し、異なるbytesならarchive.ErrIntegrityを返す。

testはWAL layer、archive layer、Gateway rebuildの三層に分ける。

WAL testはgolden fixture受理、二segment rotation、restart、valid trailerがactiveへ残った場合の完遂、partial trailer切り詰め、entryとtrailerのmutation、chain gapを確認する。

archive testはbyte-exact promote、同一sourceのretry、既存destination破損、active WAL拒否を確認する。

Gateway integration testは一つ目のbatch後にSealを呼び、二つ目のbatchを新active WALへ追加し、Gateway停止後にjournalを削除して再起動する。

再起動後は二件のinventory、二つ目までのcursor、同じChainRoot、両batchのduplicate判定を確認する。

最後にREADME.md、docs/plan/roadmap.md、.agent/tick-data-platform-execplan-revised.md、docs/verification/m2-local-raw-wal.mdを更新する。

文書はローカル基盤だけが完了したことを記録し、R2、publisher claim、raw-day manifest、delivery CLI、Parquet、pruningを未実施として残す。

## Concrete Steps

作業directoryはC:\projects\utaki-tick-data-platformである。

変更前と変更後に次を実行する。

    mise run check

WAL実装中は次を反復する。

    mise exec -- go test ./internal/wal

archive実装中は次を反復する。

    mise exec -- go test ./internal/archive

Gateway rebuildを追加した後は次を実行する。

    mise exec -- go test ./internal/ingest ./internal/wal ./internal/archive

raceを含む最終local検証は次を実行する。

    mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive

全repository gateは次を実行する。

    mise run check

expected resultは各commandのexit status 0である。

## Validation and Acceptance

wal.VerifySealedSegmentはgolden wal-entry-v1 fixtureから構成したfileを受理し、first sequence 1、last sequence 1、entry count 1、fixtureと同じTrailerFileSHA256とChainRootを返さなければならない。

Store.Sealの後、旧active bytesはroot/sealedの一fileへ移り、そのfileは有効なTWTRを一つ持ち、新しいroot/active.walは次sequenceをheaderへ記録しなければならない。

二つ目のsegmentの最初のentryは一つ目のsegmentのChainRootをprevious_entry_hashとして持たなければならない。

Storeをcloseして再openした後も、Entriesは全segmentのentryをglobal sequence順で返し、Lastは最後のsequenceとChainRootを返さなければならない。

active.walへpartial TWTR prefixを追加して再openした場合は、valid entryを失わずpartial trailerだけを除去しなければならない。

active.walへ完全で有効なTWTRを残して再openした場合は、そのfileをseal済みsegmentとして回収し、新しいactive.walを作らなければならない。

committed entry、trailer、TrailerFileSHA256、segment間ChainStartのいずれかを変更した場合はErrIntegrityで停止しなければならない。

PromoteSealedSegmentが返すkeyはfull ObjectSHA256を含み、destination bytesはsource sealed segmentとbyte単位で一致しなければならない。

同じsourceを二回promoteした場合は同じkeyを返し、destinationを変更しない。

計算済みkeyへ異なるbytesが存在する場合はarchive.ErrIntegrityで停止し、既存fileを上書きしない。

journal削除後にGatewayを再openした場合はsealed segmentとactive WALから二件のbatch inventoryとcursorを再構築し、再送されたbatchへduplicate ACKを返さなければならない。

mise run checkと対象packageのRace Detectorがexit status 0で完了しなければならない。

Pull Requestはmainをbaseとし、全check成功、unresolved review threadなし、mergeable状態であることを確認する。

実際のmergeは行わない。

## Idempotence and Recovery

Store.Openは繰り返し実行しても同じsealed inventoryとactive continuationを復元する。

Store.Sealは空active WALを拒否し、既に完了した同じrotationの回収時はbyte-identicalなsealed fileだけを受理する。

partial trailer recoveryはTWTR prefixがvalid entry末尾から始まる場合だけ切り詰める。

完全なtrailerや既存sealed fileの不整合を推測でrepairしない。

PromoteSealedSegmentはdestinationを上書きしない。

temporary fileが残った場合はfinal keyから参照されず、同じsourceの再実行で新しいtemporary fileを使える。

local pruningは行わないため、seal済みWALはoutbox promote後もWAL rootへ残る。

## Artifacts and Notes

基準commitはd9df26787e4da891b7a05b21fb11ccda6b943e96である。

作業branchはagent/m2-local-raw-walである。

M1時点のStoreはactive.walだけを開き、最初のentryのprevious_entry_hashをzeroと仮定する。

M1時点のarchive packageはdoc.goだけである。

## Interfaces and Dependencies

internal/walは標準libraryだけを使い、Protocol V1 decodeとhashにはinternal/protocolを使う。

internal/walにVerifiedSegmentを追加する。

VerifiedSegmentはPath、GatewayInstanceID、StartSequence、LastSequence、EntryCount、ChainStart、ChainRoot、TrailerFileSHA256、ObjectSHA256、FileBytes、Entriesを保持する。

internal/walに次のfunctionを追加する。

    func VerifySealedSegment(path string) (VerifiedSegment, error)

internal/wal.Storeに次のmethodを追加する。

    func (s *Store) Seal() (VerifiedSegment, error)

Store.Entries、Store.Last、Store.Countはseal済みsegmentとactive WALを合わせたglobal inventoryを返す。

internal/archiveはinternal/walをimportし、標準libraryだけでcopy、hash確認、atomic publishを行う。

internal/archiveにRawObjectを追加する。

RawObjectはKey、Path、SHA256、Bytes、Segmentを保持する。

internal/archiveに次のfunctionを追加する。

    func PromoteSealedSegment(outboxRoot string, sealedPath string) (RawObject, error)

archive.ErrIntegrityは既存objectのbytes不一致、key不一致、sealed contract不一致を表す。

R2 SDK、rclone、Cloudflare client、新しい外部dependencyは追加しない。

Revision note 2026-07-14: 初版を作成し、M1の実装調査からseal、rotation、segment連結、outbox promote、受入条件を固定した。

Revision note 2026-07-14: WALとarchiveの実装結果、crash recovery test、cross-segment chain test、no-clobber publishの判断、local CGO制約を反映した。

Revision note 2026-07-14: zero-base reviewで修正したpartial header recovery、文書更新、local repository gateの結果を反映した。

Revision note 2026-07-14: push起動とPull Request起動のWindows Race Detector成功を反映した。

Revision note 2026-07-14: Pull Request #2のReady化、check、review stateを反映し、このExecPlanの全項目を完了した。

Revision note 2026-07-14: Pull Request #2のP2 reviewを受け、sealed trailer検出をfixed offsetからentry-boundary parseへ修正した。

Revision note 2026-07-14: review修正commitに対するrepository gateとpush、Pull Request両起動のWindows Race Detector成功を反映した。
