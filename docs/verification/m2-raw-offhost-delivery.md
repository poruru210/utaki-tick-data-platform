# M2 raw off-host delivery verification

この記録は2026-07-15時点のM2R-4実装、ローカル検証、GitHub Actions検証境界を記録します。

M2R-4のfake end-to-end testは、verified sealed WAL、local raw promotion、canonical raw-day manifest、conditional claim、fake R2 SDK publication boundary、read-only ArchiveReader、empty-cache fetch、day verification、scope verificationをnetworkなしで接続します。

## 実行結果

`mise exec -- go test ./internal/r2 ./internal/delivery ./cmd/tickctl ./cmd/tick-verify ./internal/archive`は成功しました。

`mise run check`は成功しました。

`mise exec -- go vet ./...`は成功しました。

`git diff --check`は成功しました。

`mise exec -- go test -tags r2_smoke ./internal/delivery -run TestR2Smoke -count=1`は、isolated bucket、`smoke/` immutable root、endpoint、credentialを指定していないためskipしました。

R2 smokeはdefault testや`mise run check`へ含めず、明示的なbuild tagとR2環境変数を同時に指定した場合だけ実行します。

local race testはgccとclangが存在しないため実行せず、`mise exec -- go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/catalog`をWindows CIの実行境界へ移しました。

review修正後のGitHub ActionsのRepository checkはpush run `29380482941`とPR run `29380484737`で成功しました。

review修正後のGitHub ActionsのWindows raceはpush run `29380482973`とPR run `29380484762`で成功しました。

R2 smokeは必要なR2環境変数がないため未実施です。

## fake test coverage

初回publicationと同内容retryは`internal/r2/publisher_test.go`の`TestPublisherFirstAndIdempotentRetry`で確認します。

異内容collisionと原bytes保持は`TestPublisherRawImmutableCollisionPreservesOriginalAndOmitsManifest`で確認します。

publisher conflictとlocal lock conflictは`TestPublisherRejectsPublisherConflictAndLockConflict`で確認します。

data-before-manifestは`TestPublisherFailureBeforeManifestLeavesDataWithoutManifest`で確認します。

raw remote mutationは`TestPublisherRemoteRawMutationStopsBeforeManifest`で確認します。

scope descriptor mutationは`TestPublisherScopeDescriptorMutationStopsBeforeManifest`で確認します。

downloadまたはcheck failureは`TestPublisherCheckFailureStopsBeforeManifest`で確認します。

revision branchとmissing predecessorは`internal/r2/revision_test.go`およびreaderのrevision graph testsで確認します。

empty-cache fetch、実production APIを使うpublicationからreaderまでの接続、exact BatchFrameV1 bytes、zero-record error batch、day reportの`anchored_day_slice`、scope reportの`scope_genesis_to_root`は`TestM2RawOffhostDeliveryEndToEndFake`で確認します。

readerのremote mutation、corrupt cache、streaming failure、traversal rejection、revision collision、scope gap拒否は`internal/delivery`のfocused testsで確認します。

dataset/source/symbolのnamespace分離は`TestLayoutPhysicalPrefixUsesExactPathComponents`で拒否します。

scope-specific `ProtocolLimits.MaxRecords`は`TestVerifyRawDaySnapshotUsesScopedRecordLimit`でsemantic verificationへ適用します。

thread-aware review readでは、上記2件のP2 threadを未解決として確認しました。
指摘内容は最新headで修正済みですが、threadへの返信とresolveは明示依頼がないため実施していません。

fake backendのconditional writeはclaimだけに使用し、scope descriptor、raw object、manifestの転送と検証はfake `PutFileIfAbsent`と`VerifyFile`で再現します。

## optional R2 smoke

smoke testは`-tags r2_smoke`でだけbuildされ、合成WAL bytesをisolated R2 immutable rootへ公開します。

smoke testはisolated `TICK_R2_BUCKET`、`TICK_R2_IMMUTABLE_ROOT`、`TICK_R2_ENDPOINT`、`TICK_R2_ACCESS_KEY_ID`、`TICK_R2_SECRET_ACCESS_KEY`、`TICK_GATEWAY_INSTANCE_ID`を要求します。

smoke testは通常のimmutable rootの下に`gateway=<gateway-id>/run=<UTC-based-run-id>`を自動生成し、
その下の共通R2 layoutで`source=smoke`を使います。

smoke testはremote objectをdelete、move、sync、overwriteせず、credentialの値をログ、error、artifact、tracked configへ書き込みません。

2026-07-17の実R2 smokeでは、`env.local`で`TICK_R2_SECRET_ACCESS_KEY`と
`TICK_GATEWAY_INSTANCE_ID`が同一行に連結されていた状態ではR2署名が`SignatureDoesNotMatch`で失敗しました。
`env.local`の改行修正後、`go test -tags r2_smoke ./internal/delivery -run TestR2Smoke -count=1 -v`を
再実行し、通常symbolと`#`を含むsymbolの両方でpassしました。
その後、root側に`smoke` path segmentを置く誤りを修正し、本番appのlayout生成も同じ
`gateway=<gateway-id>/run=<UTC-based-run-id>` helperを通すように変更しました。
`env.local`の`TICK_R2_IMMUTABLE_ROOT`を`v1`へ更新して通常コマンドで再実行しました。
修正後の実R2 smokeは、通常のimmutable root配下の
`gateway=<gateway-id>/run=<UTC-based-run-id>/source=smoke/...`階層でpassしました。

## 証跡と残存境界

secret、credential、WAL、SQLite journal、publication receipt、R2 object、実行用configはcommitしていません。

M2R-4のlocal implementation evidenceはこの記録、fake end-to-end test、SDK smoke harness、workflow定義、通常のfocused testで構成します。

GitHub Actionsのcheck workflowとWindows race workflowは追加し、pushおよびPRの実行成功を確認しました。

M2の対象外はParquet、replay-dayまたはpart manifest、pruning、Worker、HTTP API、live brokerです。

network-free integration testはproduction Publisher APIをfake `WriteBackend`へ接続し、通常経路と同じ条件付きPut、読戻しVerify、publication journalを検証します。
