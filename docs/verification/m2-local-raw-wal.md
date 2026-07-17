# M2ローカルraw WAL基盤の検証記録

実施日は2026-07-15です。

対象は、active WALのsealとrotation、seal済みsegmentの完全性検証、segment間chainの継続、content-addressed local outboxへのbyte-exactなpromoteです。

R2 upload、publisher claim、raw-day manifest、remote verification、delivery CLI、Parquet、local pruningは対象外です。

## Hashの境界

この実装は、Protocol V1 trailerのhashとraw objectのhashを区別します。

**TrailerFileSHA256**：TWTR trailerのfile_sha256 fieldであり、headerと全entryを含むtrailer直前までのbytesを対象にします。

**ObjectSHA256**：TWTR trailerを含むseal済みfile全体のbytesを対象にします。

raw-wal-segment-v1のlocal outbox keyはObjectSHA256の全64桁hexを含みます。

## WAL sealとrotation

internal/walのStore.Sealは、entryを持つactive WALだけをsealします。

Storeはheaderと全entryのSHA-256を計算し、first sequence、last sequence、entry count、chain root、file SHA-256、CRC32Cを持つ96 byteのTWTR trailerをappendします。

Storeはfileをsyncしてcloseした後、既存destinationを置換しないhard linkでroot/sealedのinventoryへ公開し、active pathを削除します。

次のactive WALは直前segmentのlast sequenceの次から始まり、最初のentryは直前segmentのChainRootをprevious_entry_hashとして保持します。

Store.Openはseal済みsegmentをstart sequence順に検証し、active WALと合わせてglobal accepted batch inventoryを復元します。

## Sealed segment verifier

VerifySealedSegmentは、次の項目を再openしたfileから検証します。

- file headerのmagic、version、length、flags、gateway identity、start sequence
- 全entryのlength、version、flags、gateway ingest sequence
- BatchFrameV1のwire contract
- commit markerとCRC32C
- gateway batch SHA-256とWAL entry hash
- segment内のentry chain
- TWTR trailerのversion、length、sequence range、entry count、chain root
- TrailerFileSHA256とtrailer CRC32C
- seal済みfile全体のObjectSHA256

standalone verifierは最初のentryのprevious_entry_hashをChainStartとして返します。

Store.OpenはChainStartを直前segmentのChainRootと比較し、segment間chainを検証します。

## Local outbox promote

internal/archiveのPromoteSealedSegmentは、sourceとtemporary copyの両方へVerifySealedSegmentを実行します。

source bytesは再encodeも圧縮もせずtemporary fileへcopyし、file sync後にsourceとのbyte一致を確認します。

検証済みtemporary fileはhard linkにより最終keyへatomicに公開します。

最終keyが既に存在する場合は、same-content retryだけを成功扱いにします。

同じkeyへ異なるbytesまたは不正なsealed fileが存在する場合はarchive.ErrIntegrityで停止し、既存fileを上書きしません。

WAL recoveryはoutboxへ依存せず、promote後もroot/sealedのsegmentを保持します。

## Crashと破損のscenario

Go testは次の状態をfile上で再現しました。

- active WAL末尾に残ったpartial TWTR
- 最終entry内部の末尾96 byte位置にTWTRと同じ4 byteを持つvalid active WAL
- trailer sync後かつsealed inventory公開前に残ったvalid trailer
- sealed inventoryのhard link作成後かつactive path削除前のduplicate path
- 次active WALのincomplete header
- Protocol V1 header prefixと一致しない短いactive file
- committed entryのmutation
- TWTR trailerのmutation
- Protocol V1 golden fixtureとTWTR encoderのbyte不一致
- standaloneでは有効だが直前segmentとChainStartが一致しないsegment
- 同じsequence範囲に存在する異なるvalid sealed segment
- journal削除後のsealed segmentとactive WALを合わせたGateway rebuild
- same-content outbox retry
- same-key different-content
- 八つのgoroutineによる同一raw objectの同時promote

partial trailerと正しいProtocol V1 prefixを持つincomplete next headerは、accepted entryを失わず再開しました。

sealed trailer recoveryはentry boundaryを順にparseし、残りが正確に96 byteの場合だけ実行します。

完全なtrailer、committed entry、segment間chain、header prefix、既存outbox objectの不整合は自動repairせずintegrity failureとして停止しました。

## Local validation

対象packageのtestを実行しました。

    mise exec -- go test ./internal/archive ./internal/wal ./internal/ingest

終了statusは0です。

concurrent promoteを25回、WALとGatewayのtestを10回反復しました。

    mise exec -- go test ./internal/archive -run TestPromoteSealedSegmentPublishesOnceUnderConcurrency -count=25
    mise exec -- go test ./internal/wal ./internal/ingest -count=10

終了statusは0です。

Goの静的検査を実行しました。

    mise exec -- go vet ./...

終了statusは0です。

repository全gateを実行しました。

    mise run check

Go全package、Python 13件、Protocol V1 fixture 18件、Ruff、gofmt、git diff checkが成功し、終了statusは0です。

## Windows Race Detector

local workstationではGoのdefaultがCGO_ENABLED=0であり、PATH上にGCCがありません。

CGO_ENABLED=1を指定したlocal実行は、test開始前に次のtoolchain errorで停止しました。

    cgo: C compiler "gcc" not found

GitHub ActionsのWindows workflowはGCCを確認し、CGO_ENABLED=1で次を実行します。

    go test -race ./internal/ingest ./internal/wal ./internal/archive

review修正commit 7c5dcf8に対するpush起動run 29356434110は53秒で成功しました。

https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29356434110

同じcommitに対するPull Request起動run 29356436500は59秒で成功しました。

https://github.com/poruru210/utaki-tick-data-platform/actions/runs/29356436500

## データとsecret

testは一時directoryとsynthetic fixtureだけを使います。

credential、実Tick、WAL runtime data、SQLite runtime data、Parquet、R2 object、実行用TOMLはrepositoryへ追加しません。

## 未実施境界

この変更は自動rotation policyを実装しません。

size、entry count、時刻によるrotation条件は後続の運用設定で決めます。

M2全体の完了には、private R2へのimmutable publication、publisher claim、raw-day manifest、remote byte verification、read-only fetch、day verifier、scope verifierが必要です。
