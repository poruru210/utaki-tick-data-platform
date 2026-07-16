# R2 Publication BoundaryをAWS SDK for Go v2へ統一する

> **状態: 2026-07-16に失効。**
> この計画はR2への転送手段をAWS SDK for Go v2へ統一したが、`tick-gateway run`からWALのseal、raw promotion、manifest作成、R2 publicationを実行する常駐経路が存在しない事実を受入条件に含めていなかった。
> SDK境界、条件付きPUT、全量読戻し検証、`remote_verified`状態に関する決定と検証記録は履歴として保持する。
> R2 publicationの完了判定と今後の実装は、`execplan/2026-07-16-gateway-r2-runtime-fx.md`だけを正本とする。

このExecPlanは生きた文書である。
実装と検証が進むたびに、`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective`を更新する。
リポジトリにはExecPlan方法論の`PLANS.md`がないため、この文書自体に再開に必要な実装順、停止条件、検証方法、review gateを保持する。

この計画はM4の途中で判明したpublication境界の設計不整合を修正する。
現時点のworking treeにはSDK化の途中差分が存在するが、この文書を以後の正本として固定し、シニアおよびアーキテクト観点のレビューを通してから残りの実装を進める。

## Purpose / Big Picture

R2 publicationの唯一のremote I/O境界を、Cloudflare R2のS3互換APIに統一する。
Go実装はAWS SDK for Go v2の`github.com/aws/aws-sdk-go-v2/service/s3`を使い、Gateway/Publisher内部で外部CLI、外部profile、外部command argv、外部binary lockを前提にしない。

Tick受信はこれまで通りlocal WALとraw outboxへ先に確定する。
R2 publicationはsealed local spoolだけを対象にし、確定済みlocal fileを非同期に読み、S3 `PutObject`で条件付き作成し、R2から再読込したbytesをhash/sizeで検証する。
remote verificationが完了した事実は、Gateway local WALまたはWALにbindされたpublication state ledgerへdurably記録する。
`prune-local`はR2をその場で眺めて削除可否を決めるのではなく、durableな`remote_verified` record、WAL continuity、grace、disk policyを満たすlocal artifactだけを削除対象にする。
追加のremote再検証を行う場合でも、それは`remote_verified` recordなしの削除権限にはしない。

```text
MQL / Rust DLL
  -> Go Gateway
  -> local WAL / sealed raw spool
  -> R2 uploader
  -> remote verification
  -> REMOTE_VERIFIED state record
  -> prune-local
```

Publication stateは少なくとも次の遷移だけを許可する。

```text
sealed_local -> uploading -> remote_committed -> remote_verified -> local_pruned
```

Publication state recordは最低限、`object_key`、`local_path`、`size`、`sha256`、`md5`、`publication_method`、`remote_etag`、`remote_verified_at`、`state`を持つ。
`publication_method`は`aws_sdk_s3_put_object_v1`のようにR2 S3 API上の手順を表し、外部tool identityを表さない。

最終状態では、raw publication、replay publication、handover smoke、real R2 smoke、receipt、Protocol V1 publication bundle、active runbook、CI testがすべてR2 S3 APIを正本にする。
ユーザーや運用者がR2互換の任意ツールを手元で使う可能性はこのシステムの設計要素ではなく、文書にも実装にも依存として載せない。

## Progress

- [x] (2026-07-16) M4実装後の設計レビューで、production upload pathに外部CLI境界が残っていることを確認した。
- [x] (2026-07-16) 方針を、R2 S3互換APIとAWS SDK for Go v2だけをpublication write/read boundaryにする、と固定した。
- [x] (2026-07-16) 作業途中のdirty treeを確認した。`internal/r2`、`internal/protocol`、`internal/delivery`、`cmd/tick-gateway`、`internal/retention`にSDK移行途中の差分がある。
- [x] (2026-07-16) ユーザーのarchitect reviewで、構成図、`remote_verified`状態機械、412/timeout照合、v1全量Get検証、credential分離表現、Bucket Lock検討の不足を受けた。
- [x] (2026-07-16) 指摘4点をExecPlanへ反映し、シニア観点レビューを再passした。Protocol/fixture更新とpublication state ledger追加が大きい点はR3/R5の明示リスクとして残す。
- [x] (2026-07-16) 指摘4点をExecPlanへ反映し、アーキテクト観点レビューを再passした。R2 S3 APIだけをpublication境界にし、prune authorityを`remote_verified` local stateへ接続する判断を維持する。
- [x] (2026-07-16) raw Publisher、replay Publisher、optional real R2 smoke、delivery E2EをAWS SDK for Go v2境界へ移行した。
- [x] (2026-07-16) `remote_verified` state ledgerを`PublicationJournal`へ追加し、raw objectの`sealed_local -> uploading -> remote_committed -> remote_verified`を永続化するテストを追加した。
- [x] (2026-07-16) `PutObject`成功、412、timeout/unknown outcomeを分岐し、412/unknown outcomeでは既存remote objectの全量`GetObject`検証を要求する実装へ更新した。
- [x] (2026-07-16) Protocol V1 replay publication bundle、Python verifier、golden fixture、README、active runbook、verification docsからproduction publication依存としての外部CLI記述を除去した。
- [x] (2026-07-16) endpoint validationをHTTPS host-onlyからR2 host allowlist付きへ強化し、test configはR2 host形へ更新した。
- [x] (2026-07-16) 既存テストを全て復旧し、`mise run check`をpassした。

## Surprises & Discoveries

- 観察: 既存実装はpublisher claimやread-only readerでAWS SDK for Go v2をすでに使っている一方、large object publicationだけが別境界だった。
  判断: 同一remote systemに対する書込/読込/検証を別境界に分ける理由が弱く、credential、error分類、timeout、retry、証跡の説明責任を悪化させる。
- 観察: replay publication bundleとreceiptに、remote object identityとは別のtransport-specific keyとtool identityが入っていた。
  判断: Protocol/receiptは「R2上のどのimmutable objectを検証したか」をbindすべきであり、「どの転送手段を使ったか」をauthorityにしてはいけない。
- 観察: Gateway側local dataの増加問題はpublication methodとは別で、upload成功後も`prune-local`が証明付きで実行されなければlocal WAL/outboxは残る。
  判断: SDK化はupload境界の修正であり、local容量制御はretention proofとprune-local gateで別途検証する。
- 観察: 単純な`PutObject`だけでは、same key different bytesの事故を防ぐ証明として弱い。
  判断: `IfNoneMatch: "*"`で条件付き作成し、既存objectがある場合はremote bytesを再読込して同一内容だけをidempotent successにする。
- 観察: R2はstrong read-after-write consistencyを提供し、`PutObject`は`If-None-Match`と`Content-MD5`をサポートする。
  判断: v1ではこの性質を使って、`PutObject`成功、412、timeout/unknown outcomeを区別し、全量`GetObject`検証で`remote_verified`を作る。
- 観察: R2 tokenの通常権限はObject Read & WriteまたはObject Read onlyであり、厳密なwrite-only credentialは前提にできない。
  判断: uploader processはbucket/prefix限定のRead & Write tokenを使ってよく、独立verifier/pruner/API processはRead only tokenに分ける。単一process内のupload+verifyにread-only credential分離を必須化しない。
- 観察: `PublicationJournal`は既存intent hashを持っていたが、remote verification stateはstage名だけでは表現できなかった。
  判断: 既存intent hashを壊さず、別テーブル`publication_object_states`へobject key、local path、size、SHA-256、MD5、upload method、ETag、verified time、stateをappend/uniqueで記録する。
- 観察: timeout後は元のcontext deadlineが切れているため、そのcontextだけではremote outcomeを照合できない。
  判断: PUT結果不明時だけ30秒のbounded probe contextを使い、既存objectが一致すれば冪等成功、不在なら1回だけ再PUT、不一致ならcollisionにする。

## Decision Log

- Decision: R2 publication write pathはAWS SDK for Go v2のS3 clientだけを使う。
  Rationale: Cloudflare R2の公式Go例と同じAPI境界で、credential、endpoint、timeout、error分類、conditional write、multipart拡張をGo内で管理できるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: Publisher APIは外部command runner、external binary identity、tool-specific keyを受け取らない。
  Rationale: upload責任をGo process外へ逃がすと、receiptとfault testが実際のproduction boundaryを証明しなくなるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: object publicationは`PutObject` + `IfNoneMatch: "*"`を基本にする。
  Rationale: immutable namespaceでは上書きを正常操作にせず、same-content retryだけをremote verificationでidempotentに扱うためである。
  Date/Author: 2026-07-16 / Codex
- Decision: `PutObject`成功、412、timeout/unknown outcomeを別状態として扱う。
  Rationale: PUTがremote commit後にresponse受信前で切れると再試行は412になり得る。412を単純成功にせず、既存objectを取得してhash一致なら冪等成功、hash不一致なら`KEY_COLLISION`として停止する必要があるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: v1ではlocal削除前にSDK `GetObject` streamから全量sizeとSHA-256を検証し、`remote_verified` stateをdurably記録する。
  Rationale: Tick raw dataは再取得不能であり、初期版ではread-after-writeやmetadataだけに削除根拠を寄せず、安全側へ倒すためである。将来は運用実績と帯域測定に基づき、checksummed PUT、`HeadObject`、定期scrubへ緩和可能とする。
  Date/Author: 2026-07-16 / Codex
- Decision: immutable object keyはSHA-256を含めるか、keyごとのsingle-writerを別途証明する。
  Rationale: single `PutObject`からmultipartへ移る場合に同じ原子性が得られるとは限らないため、key identity自体またはwriter fencingでdifferent-content overwriteを防ぐ必要がある。
  Date/Author: 2026-07-16 / Codex
- Decision: endpointはAccount IDから組み立てるだけでなく、設定値として明示できる形を維持する。
  Rationale: jurisdiction-specific endpointやtest endpointに対応し、誤接続を防ぐためである。
  Date/Author: 2026-07-16 / Codex
- Decision: credential env名はconfigで指定する。ambient `AWS_ACCESS_KEY_ID`固定にはしない。
  Rationale: 同一processやhostがAWS本体へ接続する可能性がある場合に、R2用credentialを明確に選ぶためである。権限分離はprocess単位で行い、uploaderはbucket/prefix限定Read & Write、独立verifier/pruner/APIはRead onlyを使う。
  Date/Author: 2026-07-16 / Codex
- Decision: multipart uploadは今回のacceptance条件には含めず、single `PutObject`のサイズ上限とfail-closedを先に固定する。
  Rationale: 現行WAL/manifest/Parquet publicationを復旧する最小正本はsingle object conditional writeであり、multipartはlarge-object運用実測後に同じSDK境界内で追加できるためである。
  Date/Author: 2026-07-16 / Codex
- Decision: raw prefixのBucket LockはM4外部運用gateで有効化または明示的な非採用理由を記録する。
  Rationale: local prune後はR2が唯一のcopyになるため、誤削除やcredential侵害に対する防御層としてprefix retentionを検討する価値が高い。
  Date/Author: 2026-07-16 / Codex
- Decision: production endpoint validationは`https://*.r2.cloudflarestorage.com`だけを許可する。
  Rationale: test用のbackend injectionとproduction configを分け、誤接続や非R2 hostへのcredential送信を防ぐためである。
  Date/Author: 2026-07-16 / Codex

## Scope and Non-Goals

対象はraw publication、replay publication、publication receipt、replay bundle contract、real R2 smoke、network-free tests、active runbook、README、M4 verification docsである。

削除対象はproduction code pathの外部command execution、external binary validation、tool-specific lock file、tool-specific key、tool-specific receipt identity、tool-specific runbook requirementである。

対象外はCloudflare Worker、client-side direct upload、Presigned PUT URL、Temporary Credentials、multipart uploadの本実装、public HTTP body proxy、local prune条件の緩和である。
これらはAWS SDK for Go v2境界内のfollow-up候補だが、本計画のacceptanceには含めない。
ただしmultipart導入時は、本計画と同等の上書き防止条件を別途proofするまでproduction pathへ入れない。

歴史的ExecPlanに残る過去の実装記録は、現時点の設計authorityではない。
ただしactive docs、runbook、README、current verification docsでは、production publication dependencyとして外部CLIを記載しない。

## Plan of Work

### R0 ExecPlan and review gate

この文書を追加し、以後の修正scopeと停止条件を固定する。
R0ではコードの追加修正を進めず、現在の途中差分を棚卸ししたうえで計画だけをレビューする。
途中差分はR1以降でこの計画に合わせて採用、修正、または削除するが、R0 reviewの根拠として「すでにある差分だから正しい」とは扱わない。

シニアレビューでは、変更範囲、test復旧順、compatibility、data-loss risk、CI gate、rollback可能性を確認する。
アーキテクトレビューでは、authority境界、remote object immutability、credential separation、Protocol/receipt meaning、local pruneとの接続を確認する。

R0は両レビューがpassするまで完了しない。

### R1 R2 SDK write backend

`internal/r2`にwrite-capable backend interfaceを固定する。
interfaceはgeneric commandやarbitrary key mutationを公開せず、`PutIfAbsent`、bounded read、conditional file put、verified file readだけを公開する。

`S3Backend`はAWS SDK for Go v2でR2 endpoint、region `auto`、explicit credential env、optional session token、path-style settingを扱う。
Startup validationはendpointのschemeが`https`であること、hostnameが許可済みR2 host patternに一致すること、bucketとjurisdictionの設定が矛盾しないこと、末尾slashなどが正規化されることを確認する。

`PutFileIfAbsent`はlocal fileのsize、SHA-256、MD5をI/O前に検証し、`PutObject`に`IfNoneMatch: "*"`と`Content-MD5`を付与する。
`PutObject`成功時は`remote_committed`へ進み、続けて`VerifyFile`を実行する。
412/`ErrObjectExists`時は既存objectを`GetObject`で全量検証し、一致なら冪等成功、不一致なら`KEY_COLLISION`として停止する。
Timeout、context deadline、結果不明な通信エラーでは、bounded probe contextで`GetObject`により既存objectを確認し、一致なら成功、不在なら1回だけbounded retryへ進む。
明示的なprocess停止やcaller cancelは上位の実行制御で止める。remote commit済みか不明なPUT errorを、検証なしに成功扱いまたはlocal prune根拠にしない。
`VerifyFile`は`GetObject` streamをexpected size + 1で読み、actual bytesとSHA-256を検証する。
検証成功後だけ`remote_verified` state recordをdurably publishする。

R1のtestはlocal mutation、remote missing、same-content retry、different-content collision、412 existing-match、412 existing-different、timeout unknown outcome、cancel、permission classification、oversized local object、MD5 mismatchを含める。

### R2 raw Publisher migration

`NewPublisher`から外部runner引数を削除し、`WriteBackend`だけを受け取る。
`PublicationObject`、journal intent、receiptからtool-specific key/identityを削除し、R2 full key、SHA-256、bytesだけをbindする。
Publication journalまたはWAL-bound state ledgerに、`sealed_local`、`uploading`、`remote_committed`、`remote_verified`、`local_pruned`をdurably記録する。
state recordはartifact identity、object key、local path、size、SHA-256、MD5、method、remote ETag、verified time、state transition digestを含める。

Publication orderはclaim、scope descriptor Put+Verify、raw object Put+Verify、manifest graph validation、manifest Put+Verify、`remote_verified` record、receipt saveを維持する。
Same-content retryはremote bytes verificationで成功にし、same-key different-contentはmanifest未公開で停止する。

R2は既存`ArchiveReaderV1`やretention observerからも同じfull keyで読める必要がある。

### R3 replay Publisher and Protocol migration

Protocol V1 replay publication bundleからtool-specific prefix/key/identityを削除する。
Bundleはtrusted `Layout`から導出したR2 full key、relative key、digest、bytes、claim、conversion、limitsだけをbindする。

Replay executorはsealed ObjectIDをR2 full keyへ解決し、SDK writerのconditional Put+Verifyだけを実行する。
Remote observation、final receipt、diagnostic eventは引き続き非secret、non-authority、bounded resourceを維持する。

Go strict decoder、Python fixture verifier、golden fixture、testsを新contractへ更新する。

### R4 real R2 smoke and active docs

Real R2 smokeはR2 bucket、endpoint、writer credential、read-only credentialだけを要求する。
R2 smokeはwriterでsynthetic objectをconditional Put+Verifyし、`remote_verified` state recordを出し、別read-only credentialの独立processでempty-cache read/verifyし、read-only write deniedを確認する。
同一Gateway process内のupload+verifyはRead & Write credential一つでよいが、独立verifier/pruner/APIはRead only credentialで実行する。
Bucket Lockはraw prefixに対して有効化するか、M4 external evidenceで非採用理由と代替策を記録する。

Active runbook、verification template、README、roadmap current sections、M4 docsからproduction publication dependencyとしての外部CLI記述を削除する。
Evidence templateはGo version、AWS SDK module version、OS、commit、config digest、R2 endpoint digest、credential scope digest、object digest、remote ETag、publication state digest、verification digest、Bucket Lock decisionを保存対象にする。

### R5 validation and CI

`gofmt`、`git diff --check`、focused package tests、repository check、race workflow相当を実行する。
既存テストは削除ではなく、新しいSDK boundaryで同等の意味を持つように移行する。
旧boundaryだけを検証していたtestは、R2 API writer allowlist、conditional Put、VerifyFile、same-content retry、different-content collision、no secret leakageへ置換する。

最終的に既存の通常テストが全てpassし、必要なdocs/ExecPlanが現状態と矛盾しないことを確認する。

## External References

- Cloudflare R2 S3 API compatibility: `https://developers.cloudflare.com/r2/api/s3/api/`
- Cloudflare R2 consistency model: `https://developers.cloudflare.com/r2/reference/consistency/`
- Cloudflare R2 write behavior: `https://developers.cloudflare.com/r2/how-r2-works/`
- Cloudflare R2 Bucket Locks: `https://developers.cloudflare.com/r2/buckets/bucket-locks/`

## Validation

最低限のlocal validation:

```text
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test ./internal/protocol ./internal/r2 ./internal/delivery ./internal/retention ./cmd/tick-gateway ./cmd/tick-api
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test ./...
mise run check
git diff --check
```

必要に応じてPython fixture verifierとrace対象packageを再実行する。
Networkを要求するreal R2 smokeはcredentialと明示確認がない場合skipでよいが、skipはpassではなくexternal evidence incompleteとして記録する。

2026-07-16実行結果:

```text
GOCACHE=/tmp/tick-go-build-cache mise exec -- go test ./internal/r2 ./internal/delivery ./internal/protocol -count=1
ok

GOCACHE=/tmp/tick-go-build-cache mise exec -- go test ./internal/r2 ./internal/delivery ./internal/retention -count=1
ok

GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags real_r2_smoke ./internal/delivery -run TestOptionalRealR2Smoke -count=1
ok (opt-inなしのskip境界)

GOCACHE=/tmp/tick-go-build-cache mise exec -- go test -tags m4_real_r2_smoke ./internal/r2 -run TestOptionalM4RealR2HandoverSmoke -count=1
ok (opt-inなしのskip境界)

mise run check
pass
```

## Acceptance Criteria

- production upload pathに外部command execution、external binary validation、tool-specific config requirementが存在しない。
- `internal/r2`のraw/replay publicationがAWS SDK for Go v2 backed `WriteBackend`だけでR2へ書く。
- Same key same bytesはidempotent success、same key different bytesはfail closedになる。
- `PutObject`成功、412、timeout/unknown outcomeを区別し、412/unknown outcomeは既存remote objectのhash検証なしに成功扱いしない。
- v1では`GetObject`による全量SHA-256一致だけを`remote_verified`条件にする。
- `remote_verified` state recordだけを`prune-local`のremote-side削除条件にし、R2のその場確認だけでlocal deletionを許可しない。
- Receipt、journal、Protocol replay bundle、active verification docsがR2 full key、digest、bytes、scope、claimをbindし、transport-specific identityをbindしない。
- Gateway local dataはupload成功だけで削除されず、`remote_verified`、WAL continuity、grace、crash-safe `prune-local`の条件を維持する。
- Multipart導入時の不変条件は別途proofされるまでproduction pathへ入らない。
- Endpointはstartup時にHTTPS、許可host、bucket/jurisdiction整合、正規化を検証する。
- Bucket Lockはraw prefix運用要件として有効化または明示的な非採用理由を記録する。
- Existing tests are migrated, not removed for convenience, and the full normal test suite passes.
- Senior review and architect review are recorded as pass in this ExecPlan before the implementation is marked complete.

## Review Notes

### Senior Review

Status: pass after user architect review corrections, 2026-07-16.

Findings:

- 実装順はR1 backend、R2 raw Publisher、R3 replay/protocol、R4 docs/smoke、R5 validationに分かれており、コンパイル復旧の切り戻し点が明確である。
- `IfNoneMatch: "*"`、412/unknown outcome照合、remote byte verificationにより、same key different bytesをfail closedにできる。upload成功をlocal prune条件へ直結せず、`remote_verified` stateを要求するため、隠れたdata-loss pathは計画上追加していない。
- 既存テストは削除ではなく、旧transport境界の意味をR2 API writer、conditional Put、VerifyFile、collision、no secret leakageへ移す方針になっている。
- Real R2 smokeはcredentialなしのskipをpass扱いにしないと明記されている。

Residual risks:

- Protocol V1 replay publication bundle、publication state ledger、fixture verifierの変更は広範囲であり、R2/R3/R5でGo/Python/goldenの同時更新が必要である。
- Historical ExecPlanに残る過去記述は設計authorityではないが、active docsとの読み違いを避けるためR4で明示的に整理する。

### Architect Review

Status: pass after user architect review corrections, 2026-07-16.

Findings:

- R2 S3 APIを唯一のpublication境界にし、外部command、外部binary、tool-specific key、tool-specific receipt identityをauthorityから外す設計になっている。
- Protocol/receiptはR2 full key、relative key、digest、bytes、claim、scope、limitsをbindし、転送手段をbindしない。
- credential separationはprocess単位へ修正した。uploaderはbucket/prefix限定Read & Write、独立verifier/pruner/APIはRead onlyを使い、同一processのupload+verifyにread-only credential分離を過剰要求しない。
- local pruneはpublication receiptやR2のその場確認だけではなく、durableな`remote_verified` stateへ依存し、upload完了だけでは削除を許可しない。
- multipart、Presigned URL、Temporary Credentialsは将来同じSDK境界内で追加でき、現在のauthority modelを変える必要がない。

Residual risks:

- Single `PutObject`のobject size上限は運用実測で再評価が必要である。大容量化が確認された場合は、key SHA-256またはsingle-writer保証を含むmultipart不変条件を別計画でproofしてから追加する。
- Bucket LockはR2 S3 client codeの責務ではないが、local prune後の唯一copyを守る運用要件としてM4 external evidenceに残す必要がある。

## Outcomes & Retrospective

実装はR2 publication境界をAWS SDK for Go v2へ統一した。
`internal/r2/tool.go`、RClone tool lock、runner testsは削除され、production upload pathに外部command execution、external binary validation、tool-specific key、tool-specific receipt identityは残っていない。

Raw publisherは`WriteBackend`だけを受け取り、`PutFileIfAbsent`と`VerifyFile`でscope descriptor、raw object、manifestを転送・検証する。
Raw objectについては`PublicationJournal`の`publication_object_states`に`sealed_local`、`uploading`、`remote_committed`、`remote_verified`を永続化し、`remote_verified`にはremote ETagと検証時刻を持たせた。

Replay publication bundle、Go/Python verifier、golden fixtureからtransport-specific identityを外し、R2 full key、relative key、digest、bytes、claim、scope、limitsだけをauthorityにした。
Delivery E2Eとreal R2 smokeはfake/real backendの`WriteBackend`境界へ接続され、ユーザー任意toolはruntime設計要素ではなくなった。

残る運用判断はBucket Lockの採否と実R2/MT5外部証跡である。
これはcredentialと外部環境が必要なため、このlocal implementation完了とは分けてM4 external evidenceで扱う。

未完了。
R0 review gateが通るまで、この計画は実装修正の受入状態ではない。
