# M2R-1でraw-day manifest契約を実装する

このExecPlanは生きた文書であり、実装の進行に合わせて`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective`を更新する。

この計画は、リポジトリから独立して読めるように、用語、入力、出力、検証方法、未実装境界を記述する。

この計画は、`C:\Users\AKIRA\.codex\skills\execplan\references\PLANS.md`の方法論に従う。

## Purpose / Big Picture

M2R-1の完了後、verified sealed WALから同じ日付と同じscopeを持つraw-day snapshotを何度buildしても同じcanonical JSON、raw_set_root、manifest digestを得られる。

strict verifierは、canonical JSONの表現差、unknown key、duplicate key、float、非canonical integer、invalid UTF-8、range違反を受理しない。

raw-day manifestは、選択したrecord rangeとzero-record batch sentinel、accepted record count、CopyTicksErrorのbatch count、campaign chain slice、revision chainを検証可能な形で保存する。

M2R-1の入力は`wal.VerifySealedSegment`済みの`archive.RawObject`だけであり、active WAL、実tick、credential、wall clockは入力にしない。

同じlocal scope descriptorを再作成するretryは成功し、同じpathで異なるdescriptor bytesを作ろうとするretryは`archive.ErrIntegrity`になる。

fake producerのgolden fixtureとlocal sealed WALでこの結果を示し、実R2が存在しない環境でもM2R-1のcompletionを判定できる。

## Progress

- [x] (2026-07-15 16:00+09:00) `agent/m2-raw-offhost-delivery`のclean worktreeと`origin/main`起点を確認した。
- [x] (2026-07-15 16:10+09:00) Protocol V1の既存hash domain、manifest、WAL verifier、raw object promotion、GoとPython fixture verifierを調査した。
- [x] (2026-07-15 16:40+09:00) `internal/protocol/canonical_json.go`へcanonical encodeとstrict decodeを追加した。
- [x] (2026-07-15 17:20+09:00) `internal/archive/manifest.go`へScopeConfig、raw_set_root、campaign-scope descriptor、raw-day buildとverifyを追加した。
- [x] (2026-07-15 17:40+09:00) revision、same-object disjoint range、zero-record sentinel、chain discontinuity、scope descriptor retryのfocused testを追加した。
- [x] (2026-07-15 17:50+09:00) raw-day golden fixtureとGoとPythonのfixture verifierを更新した。
- [x] (2026-07-15 18:00+09:00) Protocol V1文書と親ExecPlanのcanonical JSON記述を更新し、JCS記述をDecision Logへ修正した。
- [x] (2026-07-15 18:15+09:00) `mise run bootstrap`でPython開発依存を同期した。
- [x] (2026-07-15 18:16+09:00) `mise run fixture`、`mise exec -- go test ./internal/archive ./internal/protocol`、`mise run test-python`、`mise run check`を最終状態で実行した。
- [x] (2026-07-15 18:20+09:00) 検証成功後にM2R-1 scopeだけをcommitし、commit SHAをhandoffへ記録する。

## Surprises & Discoveries

Goとgofmtは通常のPATHに存在しなかったため、repositoryのtoolchain規約どおり`mise exec --`を使う必要があった。

`mise run fixture`はPython環境のpytestに依存せず成功したが、初回のsystem Python相当環境にはpytestがなく、Python testは`mise run bootstrap`後に実行する必要がある。

既存のraw-day fixtureはmanifest digestだけを検証し、revisionとraw_set_rootのdomain bytesを検証していなかった。

既存のparent ExecPlanはcanonical-json-v1とRFC 8785 JCSを同時に要求していたため、Protocol V1の正本と矛盾していた。

sealed WAL verifierはsegment内のchainを検証するが、複数segment間のcampaign continuityはcallerの責務であるため、BuildRawDayManifestでsequence連続性とpredecessor rootを再検証する必要があった。

WAL entryにはdataset identityが暗号学的に含まれないため、ScopeConfigとno-clobber descriptorをoperator trust rootとして扱う限界が残る。

## Decision Log

- Decision: canonical JSONはRFC 8785 JCSではなく、Protocol V1の`canonical-json-v1`を使用する。
  Rationale: GoとPythonでUTF-8 byte順key、整数のみ、lowercase `\u` escape、末尾改行なし、strict decodeを同じfixtureで検証する必要がある。
  Date/Author: 2026-07-15 / Codex
- Decision: canonical JSONのdigestへdigest自身を含めず、既存domain prefixとcanonical bytesからmanifest digestを計算する。
  Rationale: digestの再帰埋め込みは自己参照になるため、canonical documentとdigestの入力を分離する。
  Date/Author: 2026-07-15 / Codex
- Decision: `raw_set_root`はobject keyではなくobject SHA-256、object bytes、inclusive coordinate rangeをbindする。
  Rationale: object keyの再配置を許しつつ、contentと選択範囲の改変を検出するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: non-empty batchはrecord ordinal runとして選択し、zero-record batchはRequestedFromMSC UTC dayのordinal 0 sentinelとして選択する。
  Rationale: recordがないsource errorを日付snapshotから消さず、same-object cross-day batchの選択範囲を正確に表すためである。
  Date/Author: 2026-07-15 / Codex
- Decision: genesis revisionは1とし、successorはprevious revision plus oneとprevious digestを要求する。
  Rationale: revision chainの分岐、欠落、scope変更をstrict verifierがfail closedで検出できるためである。
  Date/Author: 2026-07-15 / Codex
- Decision: BuildRawDayManifestはRawObjectのsource pathを`wal.VerifySealedSegment`で再検証し、verified segment metadataと一致しないobjectを拒否する。
  Rationale: callerが任意bytesやactive WALをmanifest inputへ混ぜないよう、promotion済みobjectの検証境界を維持するためである。
  Date/Author: 2026-07-15 / Codex
- Decision: ScopePathKeyはdataset IDとcampaign IDのexact UTF-8 bytesからlowercase SHA-256 hexを作る。
  Rationale: Unicode normalization、case folding、separator解釈をせずにWindows safe path componentを作り、同じcampaignのdescriptor衝突を検出するためである。
  Date/Author: 2026-07-15 / Codex

## Outcomes & Retrospective

実装済みの成果は、Protocol V1共通canonical JSON、raw_set_root、raw-day manifestのrevisionとrange契約、ScopeConfigのconfig hash、campaign-scope descriptorである。

fake-only fixtureとlocal sealed WALで、cross-day batch、same-object disjoint range、zero-record error batch、revision successor、campaign discontinuityを検証できる。

実R2 upload、remote immutable write、download verify、tickctl、tick-verify command、Parquet derivativeはM2R-1の成果ではなく、後続M2R-2からM2R-4の成果である。

最終検証でfixture 18件、Python test 15件、Go全test、Ruff、gofmt、diff checkが成功した。

残課題はscope-only commit SHAの記録だけであり、real R2、live MT5、実tick dataはM2R-1のcompletionに不要である。

## Context and Orientation

`protocol/v1/`はwire、message、hash domain、manifest、fixtureのlanguage-neutralな正本である。

`internal/protocol/`はGoのProtocol V1 codecとcanonical JSON実装を持つ。

`internal/wal/`の`VerifySealedSegment`は、header、entry length、CRC、BatchFrameV1、batch hash、entry hash、trailer、file hash、segment内sequenceを検証した`wal.VerifiedSegment`を返す。

`internal/archive/raw.go`の`PromoteSealedSegment`は、verified sealed WALをbyte-exactかつno-clobberでlocal raw objectへ移し、`archive.RawObject`を返す。

**RawObject**は、content-addressed key、path、SHA-256、byte数、verified sealed segmentをまとめた入力単位であり、BuildRawDayManifestはこれ以外のraw inputを受け付けない。

**ScopeConfig**は、dataset、campaign、provider、stable feed、exact source symbol、broker server fingerprint、gatewayとproducer build identity、day definition、settle policy、publisher IDとepoch、Protocol limitsを持つoperator configである。

ScopeConfigのcanonical documentはsecret、environment variable名、absolute pathを持たず、`tick-data-platform/archive-config/v1\0`を前置したSHA-256をconfig hashとする。

**raw-day manifest**は一つのdataset、campaign、UTC day、revisionにおけるverified raw objectのselected range snapshotである。

**campaign chain**はWAL entryのsequenceとPreviousEntryHashからEntryHashへ続く順序であり、segmentをまたぐ場合もsequence連続性とpredecessor root一致を要求する。

**raw_set_root**はmanifestのordered range列をcontent hashとcoordinateでまとめたSHA-256であり、object keyは入力に含めない。

**canonical JSON**はUTF-8、BOMなし、空白なし、末尾改行なし、UTF-8 byte順key、整数のみ、非ASCII lowercase `\u` escapeを持つJSON表現である。

## Milestones

M2R-1では、canonical JSON、ScopeConfig、raw_set_root、raw-day manifest buildとstrict verify、local descriptor、fixtureとfocused testsを完成させる。

M2R-2では、verified raw objectとraw-day manifestをlocal outboxからoptional R2へimmutable publishし、remote bytesをdownload verifyする。

M2R-3では、raw-day manifestからParquet derivative、part manifest、replay-day manifestを構築する。

M2R-4では、read-only fetch、day verifier、campaign verifier、必要なdelivery interfaceを実装する。

M2R-1のcompletion境界はfake producerとlocal verified WALであり、real R2 data、real broker、live MT5、external tick historyがなくても判定できる。

実R2はoptionalな後続検証であり、M2R-1のlocal contract completionを遅延させる条件ではない。

M2R-1の対象外はR2 client、rclone、Cloudflare Worker、Parquet、tickctl、tick-verify、MQL5 compile、live account、strategy logic、既存文書の無関係な改稿である。

## Plan of Work

`internal/protocol/canonical_json.go`にrestricted JSON valueのencoderを追加し、map keyをUTF-8 byte順に並べ、stringをlowercase `\u`へ変換し、integer以外を拒否する。

同ファイルのstrict decoderはduplicate keyをparse時に検出し、integer grammarを検査し、UTF-8とsurrogate pairを検査し、再encodeとのbyte比較で空白、raw non-ASCII、uppercase escape、key順違反を拒否する。

`internal/archive/manifest.go`にScopeConfigのcanonical documentとconfig hashを追加し、configのexact identity bytesを変更しない。

同ファイルの`IdentityPathKey`と`ScopePathKey`はlowercase SHA-256 hexだけをpath componentとして返す。

`EnsureCampaignScopeDescriptor`は`campaign-scope-v1/<scope-key>/descriptor.json`をO_EXCLで作成し、既存bytesが同じ場合だけ成功させる。

`RawSetRoot`はdomain prefix、U32 element count、各ordered rangeのH32、U64、U64、U64、U32、U32をlittle-endianで連結してSHA-256を計算する。

`BuildRawDayManifest`はobjectsをsequence順に並べ、各source pathをsealed verificationし、segment間のsequenceとchain rootを検証し、BatchFrameV1をdecodeしてUTC day別record rangeを生成する。

non-empty batchではrecordのTimeMSCからdayを決め、ordinal runをrangeへ変換し、zero-record batchではRequestedFromMSCからdayを決めてordinal 0 sentinelを生成する。

BuildRawDayManifestはselected record count、selected error batch count、first selected entryのprevious root、last selected entryのentry root、raw_set_root、revision、config hash、explicit logical close timeをmanifestへ入れる。

previous manifestがある場合はscope、publisher epoch、objects prefix、watermark、revision successor、previous digestを検証し、current manifestへprevious digestを一度だけ書く。

`VerifyRawDayManifest`はcanonical JSONをstrict decodeし、unknown key、field type、hash lowercase、date、schema、revision、range、raw_set_root、chain sliceの規則を検証する。

`tools/tick_protocol.py`と`tools/tick_fixture_verify.py`はGoと同じcanonical JSON、raw_set_root、raw-day fixture schemaを独立実装する。

`testdata/tickdata/golden/raw-day-manifest-v1.json`はrevisionと新domainのraw_set_rootを含むcanonical JSONとmanifest digestを固定する。

`.agent/tick-data-platform-execplan-revised.md`はJCSとLFの記述をProtocol V1のcanonical-json-v1へ修正し、その理由をDecision Logへ残す。

## Concrete Steps

作業directoryは`C:\projects\utaki-tick-data-platform`とする。

最初にbranchと既存変更を確認する。

    git status --short --branch
    git branch -vv

期待するbranchは`agent/m2-raw-offhost-delivery`であり、既存変更がある場合はrevertせず、scope外変更を追加しない。

Go整形はmise-managed toolchainで実行する。

    mise exec -- gofmt -w internal/archive/manifest.go internal/archive/raw.go internal/archive/manifest_test.go internal/protocol/canonical_json.go internal/protocol/canonical_json_test.go

fixture verifierを実行する。

    mise run fixture

期待する出力は`verified 18 Protocol V1 fixtures`であり、raw-day fixtureのcanonical bytes、manifest digest、raw_set_rootが一致することを示す。

archiveとprotocolのfocused Go testを実行する。

    mise exec -- go test ./internal/archive ./internal/protocol

Python依存が未導入の場合は一度だけbootstrapする。

    mise run bootstrap

Python unitとstateful testを実行する。

    mise run test-python

最終的にrepository指定の全checkを実行する。

    mise run check

全checkが成功した後、変更scopeを確認する。

    git status --short
    git diff --stat
    git diff --check

scope内だけが変更されていることを確認してから、M2R-1のscoped commitを作成する。

    git add execplan/2026-07-15-m2-raw-offhost-delivery.md .agent/tick-data-platform-execplan-revised.md protocol/v1/hash-domains.md protocol/v1/manifests.md protocol/v1/fixtures/README.md testdata/tickdata/golden/raw-day-manifest-v1.json internal/archive internal/protocol/canonical_json.go internal/protocol/canonical_json_test.go tools/tick_protocol.py tools/tick_fixture_verify.py tests/unit tests/stateful
    git commit -m "feat: build deterministic raw-day manifests"

commit後はSHAを取得する。

    git rev-parse HEAD

## Validation and Acceptance

canonical JSON encodeはASCII key順、空白なし、末尾改行なし、整数のみ、lowercase Unicode escapeを出力する。

canonical JSON strict decodeはunknown key、duplicate key、float、leading zero、plus sign、exponent、negative zero、invalid UTF-8、raw non-ASCII、BOM、末尾空白を拒否する。

raw-day manifest decodeはrequired key以外を拒否し、revisionを1以上に限定し、genesisのprevious nullとsuccessorのprevious digestを検証する。

object rangeはsame objectの複数rangeを許すが、inclusive coordinateとしてempty、reversed、overlap、順序違反を拒否する。

record dayは`RawMqlTickV1.TimeMSC`をUTCへ変換し、zero-record batchだけは`RequestedFromMSC`を使ってordinal 0 sentinelを割り当てる。

selected BatchFrameV1のrecord数をaccepted_record_countへ入れ、selected BatchFrameV1のCopyTicksError非zero数をerror_countへ入れる。

chain sliceは最初と最後のselected WAL entryへbindし、campaign segmentのsequence gapまたはpredecessor root mismatchをErrIntegrityで拒否する。

raw_set_rootはkeyを除外してGoとPythonとgolden fixtureで一致する。

ScopeConfigのconfig hashはidentity、build、publisher、day、settle policy、protocol limitsを含み、secret、env名、absolute pathを含まない。

同じScopeConfigのdescriptor retryは成功し、同じscope keyで異なるconfig bytesを作るretryはErrIntegrityになる。

`mise run fixture`、`mise exec -- go test ./internal/archive ./internal/protocol`、`mise run test-python`、`mise run check`がすべてexit code 0であることをM2R-1の検証結果とする。

実R2が未設定でも、fake producerとlocal sealed WALを使うfocused testが成功すればM2R-1のcompletion条件を満たす。

## Idempotence and Recovery

BuildRawDayManifestは入力RawObject、date、scope、status、logical close time、previousを同じにすればwall clockに依存せず同じbytesを生成する。

descriptor作成はO_EXCLであり、processが作成後に再実行しても同じbytesを読み取って成功する。

descriptor書き込み途中で失敗した場合はtemporary descriptorを残さず、次回retryが同じscope pathを再利用できる。

manifest digest mismatch、object range mutation、segment verification failure、chain discontinuityは既存objectを削除せずErrIntegrityとして返す。

作業途中でverificationが失敗した場合は、失敗したcommandとscopeをhandoffへ残し、scope外修正や既存変更のrevertを行わない。

## Artifacts and Notes

raw-day golden fixtureは`revision=1`、`previous_manifest_sha256=null`、一つのrange、`raw_set_root=8fa4796d55ef8800626ae4769e20b41e3a1c9dceeaa8da9990ed258244f8362d`を固定する。

更新後fixtureのmanifest digestは`3206bfd7fd5ef2b8dccf479c400322cbbbb249c92a291e79a0e55a80ecb6c29d`である。

focused archive testは同一WAL object内のordinal 0と2のdisjoint rangeを別要素で保持する。

focused archive testはzero-record CopyTicksError batchをsequence 2 ordinal 0 sentinelとして保持する。

## Interfaces and Dependencies

`internal/protocol.CanonicalJSON(value any) ([]byte, error)`はProtocol V1のrestricted JSON valueをcanonical bytesへ変換する。

`internal/protocol.DecodeCanonicalJSON(data []byte) (any, error)`はcanonical bytesだけを受理し、duplicate keyとnoncanonical bytesを拒否する。

`archive.ScopeConfig.CanonicalConfigJSON() ([]byte, error)`はsecret、env名、absolute pathを含まないcanonical config documentを返す。

`archive.ScopeConfig.ConfigHash() ([32]byte, error)`はarchive-config domain prefixとcanonical config bytesからSHA-256を返す。

`archive.EnsureCampaignScopeDescriptor(root string, scope ScopeConfig) (string, error)`はcampaign-scope-v1 descriptorのpathを返し、different contentをErrIntegrityで拒否する。

`archive.RawSetRoot(objects []RawObjectRange) ([32]byte, error)`はordered rangesからraw_set_rootを計算する。

`archive.BuildRawDayManifest(input RawDayManifestInput) (RawDayManifest, error)`はverified RawObjectだけからraw-day manifestとdigestを生成する。

`archive.ManifestCanonicalJSON(manifest RawDayManifest) ([]byte, error)`はdigestを埋め込まないcanonical manifest bytesを返す。

`archive.ManifestDigest(manifest RawDayManifest) ([32]byte, error)`はraw-day manifest domainとcanonical bytesからdigestを返す。

`archive.VerifyRawDayManifest(data []byte) (RawDayManifest, error)`はcanonical JSONとraw_set_rootをstrict verifyし、digestを計算して返す。

依存する外部runtimeはGo 1.24、Python 3.12、uv、Ruff、pytestであり、M2R-1のR2 clientやnetwork serviceは不要である。

改訂記録（2026-07-15）: Protocol V1のcanonical-json-v1、raw_set_root、revision chain、verified RawObject入力、ScopeConfig descriptorをM2R-1の実装と検証対象へ追加した。
