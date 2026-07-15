# M2 raw off-host delivery verification

この記録は2026-07-15時点のM2R-4実装、ローカル検証、GitHub Actions検証境界を記録します。

M2R-4のfake end-to-end testは、verified sealed WAL、local raw promotion、canonical raw-day manifest、conditional claim、fake rclone publication、read-only ArchiveReader、empty-cache fetch、day verification、campaign verificationをnetworkなしで接続します。

## 実行結果

`mise exec -- go test ./internal/r2 ./internal/delivery ./cmd/tickctl ./cmd/tick-verify ./internal/archive`は成功しました。

`mise run check`は成功しました。

`mise exec -- go vet ./...`は成功しました。

`git diff --check`は成功しました。

`mise exec -- go test -tags real_r2_smoke ./internal/delivery -run TestOptionalRealR2Smoke -count=1`は、enable flag、confirmation、isolated bucketまたはprefix、endpoint、credential、pinned rclone binaryを指定していないためskipしました。

実R2 smokeはdefault testや`mise run check`へ含めず、明示的なbuild tagと環境変数を同時に指定した場合だけ実行します。

local race testはgccとclangが存在しないため実行せず、`mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/catalog`をWindows CIの実行境界へ移しました。

GitHub ActionsのRepository checkはpush run `29380124381`とPR run `29380126333`で成功しました。

GitHub ActionsのWindows raceはpush run `29380124589`とPR run `29380126328`で成功しました。

実R2 smokeは必要なopt-in条件がないため未実施です。

## fake test coverage

初回publicationと同内容retryは`internal/r2/publisher_test.go`の`TestPublisherFirstAndIdempotentRetry`で確認します。

異内容collisionと原bytes保持は`TestPublisherRawImmutableCollisionPreservesOriginalAndOmitsManifest`で確認します。

publisher conflictとlocal lock conflictは`TestPublisherRejectsPublisherConflictAndLockConflict`で確認します。

data-before-manifestは`TestPublisherFailureBeforeManifestLeavesDataWithoutManifest`で確認します。

raw remote mutationは`TestPublisherRemoteRawMutationStopsBeforeManifest`で確認します。

scope descriptor mutationは`TestPublisherScopeDescriptorMutationStopsBeforeManifest`で確認します。

downloadまたはcheck failureは`TestPublisherCheckFailureStopsBeforeManifest`で確認します。

revision branchとmissing predecessorは`internal/r2/revision_test.go`およびreaderのrevision graph testsで確認します。

empty-cache fetch、実production APIを使うpublicationからreaderまでの接続、exact BatchFrameV1 bytes、zero-record error batch、day reportの`anchored_day_slice`、campaign reportの`campaign_genesis_to_root`は`TestM2RawOffhostDeliveryEndToEndFake`で確認します。

readerのremote mutation、corrupt cache、streaming failure、traversal rejection、revision collision、campaign gap拒否は`internal/delivery`のfocused testsで確認します。

dataset namespace collisionは`TestLayoutSeparatesDatasetsWithTheSameCampaignIdentity`で拒否します。

scope-specific `ProtocolLimits.MaxRecords`は`TestVerifyRawDaySnapshotUsesScopedRecordLimit`でsemantic verificationへ適用します。

fake backendのconditional writeはclaimだけに使用し、scope descriptor、raw object、manifestの転送と検証はfake rcloneの`copyto --immutable`と`check --download`で再現します。

## optional real-R2 smoke

smoke testは`-tags real_r2_smoke`でだけbuildされ、合成WAL bytesをisolated prefixへimmutableに公開します。

smoke testは`TICK_M2_REAL_R2_SMOKE=1`、`TICK_M2_REAL_R2_CONFIRM=I_UNDERSTAND_NO_OVERWRITE`、isolated `TICK_M2_REAL_R2_BUCKET`、`TICK_M2_REAL_R2_PREFIX`、`TICK_M2_REAL_R2_ENDPOINT`、`TICK_M2_REAL_R2_REMOTE`、`TICK_M2_RCLONE_BINARY`、`RCLONE_CONFIG`、AWS credential envの全てを要求します。

smoke prefixは`m2-smoke/`から始まり、実行ごとにランダムなrun suffixを追加します。

smoke testはremote objectをdelete、move、sync、overwriteせず、credentialの値をログ、error、artifact、tracked configへ書き込みません。

現在の環境では必要なopt-in条件を満たしていないため、実R2 smokeは未実施です。

## 証跡と残存境界

secret、credential、WAL、SQLite journal、publication receipt、R2 object、実行用configはcommitしていません。

M2R-4のlocal implementation evidenceはこの記録、fake end-to-end test、pinned-tool smoke harness、workflow定義、通常のfocused testで構成します。

GitHub Actionsのcheck workflowとWindows race workflowは追加し、pushおよびPRの実行成功を確認しました。

M2の対象外はParquet、replay-dayまたはpart manifest、handover、pruning、Worker、HTTP API、live brokerです。

`RcloneExecutorFunc`はnetwork-free integration testがproduction Publisher APIを使うためのin-process command seamであり、通常のrclone runnerは引き続きpinned executable、exact argv、version、hash、byte lengthを検証します。
