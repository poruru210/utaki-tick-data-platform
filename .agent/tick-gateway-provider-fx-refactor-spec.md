# Tick Gateway — Credential Provider／Uber Fx導入・テスト再構成仕様書

- 文書バージョン: 1.0
- 状態: 実装着手可能
- 対象フェーズ: 基盤リファクタリング
- 対象OS: Windows／Linux
- 実装言語: Go
- 最終更新: 2026-07-16

## 1. Codexへの指示

本フェーズでは、Credential Providerの導入、Uber Fxによる依存組立てとLifecycle管理への移行、およびそれに伴うテスト再構成だけを行うこと。

外部仕様、データ形式、通信プロトコル、R2アップロードの意味論、WAL／spool形式、prune条件は変更してはならない。既存挙動を保持したまま、次フェーズでR2 uploader／verifier等を安全に追加できるcomposition rootを構築することが目的である。

既存コードと本書が衝突する場合、黙って仕様を弱めたり大規模な置換を行ったりせず、次をExecPlanへ記載すること。

1. 衝突箇所
2. 現在の挙動
3. 本書へ合わせるための最小変更
4. 互換性リスク
5. 採用案と不採用案

## 2. 背景

Tick Gatewayは今後、少なくとも次のコンポーネントを持つ。

- 設定ローダー
- Credential Provider
- ローカルWAL／raw spool
- 状態カタログ
- TCP等のingest server
- R2 client
- uploader
- verifier
- pruner
- メトリクス／ログ
- Windows Service／Linux service host

手動DIのまま拡張すると、`main`、テストfixture、起動停止処理へ依存組立てが重複する。本フェーズではUber Fxをcomposition rootとして導入し、依存関係とLifecycleを一か所へ集約する。

一方、Credential Providerのv1実装は`file`だけとする。Windows、systemd credentials、Docker secrets等は、Gatewayから見れば「指定されたファイルを読む」という共通契約に落とす。

## 3. 決定事項

| 項目 | 決定 |
| --- | --- |
| DI／Lifecycle | `go.uber.org/fx` |
| Fxテスト | `go.uber.org/fx/fxtest` |
| Credential抽象 | 独自の小さい`Provider` interface |
| v1 Provider実装 | `file`のみ |
| 資格情報形式 | 単一JSON bundle |
| 資格情報reload | 行わない。交換後にサービス再起動 |
| Fx適用範囲 | composition root、module、Lifecycle登録、統合テスト |
| Fx非適用範囲 | ドメインロジック、Provider単体テスト、値オブジェクト |
| テスト差替え | production moduleとtest moduleを分ける |
| 外部R2接続 | 本フェーズのテストでは禁止 |
| OS対応 | Windows／LinuxのCI matrixで検証 |

Fxは通常のGoコンストラクタを依存グラフへ登録し、アプリケーションの開始・終了hookを管理する。本仕様では`fx.Provide`、`fx.Module`、`fx.Invoke`、`fx.Lifecycle`、`fx.ValidateApp`および`fxtest`の必要最小限だけを使用する。[Fx package](https://pkg.go.dev/go.uber.org/fx) / [Fx Lifecycle](https://uber-go.github.io/fx/lifecycle.html)

## 4. 目的

### 4.1 機能目的

1. 資格情報の取得を`credentials.Provider`の背後へ分離する。
2. v1では単一JSONファイルから資格情報を取得する。
3. 既存の依存組立てをFx moduleへ移す。
4. 既存の常駐コンポーネントをFx Lifecycleへ接続する。
5. Windows／Linuxで同じapplication graphを使用する。
6. production依存とtest fakeを明確に差し替えられるようにする。

### 4.2 品質目的

1. グローバル状態と`init()`による暗黙初期化を増やさない。
2. 起動失敗時に部分初期化されたリソースを安全に解放する。
3. 停止順序をテスト可能にする。
4. 依存欠落・重複・循環をgraph validationで検出する。
5. ドメイン単体テストを高速で明示的なまま維持する。
6. テストから実ネットワーク・実R2・ユーザー環境へ依存しない。

## 5. 非目的

本フェーズでは以下を行わない。

- R2 uploader／verifierの新規実装
- PutObject／GetObjectの挙動変更
- AWS SDKの入替え
- WAL／spool／raw segment形式の変更
- SQLite等の状態ストア新規導入
- prune条件の変更
- TCP／UDP／DLL間インターフェースの変更
- DPAPI Provider
- systemd専用Provider
- Vault、AWS Secrets Manager等のProvider
- credential hot reload／ファイル監視
- R2 Temporary Credentials
- 自動キーローテーション
- Windows Service／systemd unitの全面的な再設計
- 全テストの機械的な`fxtest`化

既存コードに上記機能がすでに存在する場合は削除しない。本フェーズの必要範囲だけFx graphへ接続し、意味論を維持する。

## 6. 設計原則

### 6.1 Fxはcomposition rootに閉じ込める

原則として`go.uber.org/fx`をimportできるのは次だけとする。

- `cmd/...`の起動処理
- `internal/app/...`のmodule／Lifecycle登録
- Fx application graphのテスト

ドメインパッケージへ`fx.In`、`fx.Out`、`fx.Lifecycle`を広げてはならない。既存コンポーネントは通常のGoコンストラクタと`Start`／`Stop`を公開する。

例:

```go
func NewGateway(cfg Config, store Store, logger Logger) (*Gateway, error)

func (g *Gateway) Start(ctx context.Context) error
func (g *Gateway) Stop(ctx context.Context) error
```

Lifecycleへの接続は`internal/app`で行う。

### 6.2 interfaceは利用側で定義する

Credential Providerは利用側の最小要件だけを表現する。将来Providerを増やす可能性だけを理由に、大きな汎用secret manager interfaceを設計しない。

### 6.3 コンストラクタに副作用を集中させない

コンストラクタは設定検証とオブジェクト構築を担当する。長時間動作するgoroutine、listener、workerは`Start`で起動し、`Stop`で停止する。

### 6.4 テスト戦略を二層に分ける

- ロジック単体テスト: Fxを使わず依存を直接渡す。
- composition／Lifecycle／統合テスト: `fxtest`を使う。

Fx公式も、`fxtest`をLifecycle利用コードおよびFx applicationのend-to-end test用として位置づけている。[Fx testing API](https://pkg.go.dev/go.uber.org/fx/fxtest)

## 7. 目標ディレクトリ構成

既存repoの命名を尊重しつつ、責務は概ね次へ分離する。

```text
cmd/
  tick-gateway/
    main.go

internal/
  app/
    app.go
    modules.go
    lifecycle.go
    app_test.go
    lifecycle_test.go

  credentials/
    provider.go
    bundle.go
    file.go
    file_test.go
    file_permissions_linux.go
    file_permissions_linux_test.go
    file_permissions_windows.go
    file_permissions_windows_test.go

  config/
  gateway/
  spool/
  catalog/
```

既存構成が異なる場合、ディレクトリを機械的に移動する必要はない。無関係なrenameやpackage再編を本PRへ混ぜない。

## 8. Credential Provider仕様

### 8.1 interface

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

条件:

- AWS SDK型へ直接依存させない。
- `Credentials`へendpoint、bucket、regionを含めない。
- `Credentials`に`String()`や`GoString()`を実装しない。
- エラーへ秘密値を埋め込まない。
- Providerはログ出力を必須責務にしない。

### 8.2 bundle形式

ファイルはUTF-8 JSONとする。

```json
{
  "format_version": 1,
  "access_key_id": "<R2_ACCESS_KEY_ID>",
  "secret_access_key": "<R2_SECRET_ACCESS_KEY>"
}
```

Go内部表現:

```go
type bundle struct {
    FormatVersion   int    `json:"format_version"`
    AccessKeyID     string `json:"access_key_id"`
    SecretAccessKey string `json:"secret_access_key"`
}
```

条件:

- `format_version`は必須で、v1以外を拒否する。
- Access Key IDとSecret Access Keyの空文字を拒否する。
- JSON object以外を拒否する。
- 重複key、未知field、末尾の別JSON値を拒否する。
- BOMの許可／拒否は一貫させ、テストで固定する。推奨はUTF-8 BOMなしのみ許可。
- ファイル最大サイズを64 KiBとし、超過時は読み込まない。
- 改行の有無は問わない。

### 8.3 FileProvider

```go
type FileConfig struct {
    Path       string
    Protection ProtectionMode
}

type ProtectionMode string

const (
    ProtectionNativeACL   ProtectionMode = "native-acl"
    ProtectionManagedMount ProtectionMode = "managed-mount"
)

type FileProvider struct {
    path       string
    protection ProtectionMode
}

func NewFileProvider(cfg FileConfig) (*FileProvider, error)
func (p *FileProvider) Load(ctx context.Context) (Credentials, error)
```

`ProtectionMode`はProviderを増やすための抽象ではなく、同じfile providerをネイティブファイルとsystemd／container管理mountの両方で安全に使うための最小区別である。

#### `native-acl`

- WindowsネイティブサービスのNTFS ACL付きファイル
- Linuxネイティブサービスの`0400`または`0600`ファイル
- 起動時にOS固有permission checkを行う
- 過剰許可を検出した場合はfail closedとする

#### `managed-mount`

- systemd credentials
- Docker／Kubernetes等のsecret mount
- mount基盤がアクセス制御を担当する
- Providerはregular file、読取り可能性、サイズ、JSON構造を検査する
- シンボリックリンクを許可する必要がある管理基盤が存在する場合、repoの実環境を確認して仕様差分として報告する

Protection modeを未指定にした場合は`native-acl`とする。未知値を黙って`managed-mount`へfallbackしてはならない。

### 8.4 ファイル読取り条件

`Load`は次の順序で処理する。

1. `ctx.Err()`を確認する。
2. pathが空でないことを確認する。
3. `Lstat`等で対象を確認する。
4. security policyを検査する。
5. サイズ上限付きで内容を読む。
6. strict JSON decodeする。
7. format versionと必須値を検証する。
8. `Credentials`を返す。

TOCTOUを完全に防げると仮定せず、可能ならopenしたfile handleに対してstatとreadを行う。実装が複雑化する場合は、最小の安全な実装と残余リスクをExecPlanへ記載する。

### 8.5 permission check

Windows:

- 一般ユーザー、`Users`、`Authenticated Users`、`Everyone`にread権限がないこと。
- 少なくとも`SYSTEM`、`Administrators`、GatewayサービスIDだけがアクセスできること。
- 本フェーズでサービスIDが未確定なら、validatorを差し替え可能にし、実際のACL fixtureでテストする。

Linux `native-acl`:

- regular fileであること。
- group／other bitsが0であること。
- 所有者が実行ユーザーまたはrootであること。
- 親ディレクトリが意図せずworld-writableでないこと。ただしsticky bit付きの標準一時領域はテスト用途だけで明示的に扱う。

`managed-mount`では、Docker secretsがread-onlyの異なるmodeで公開される場合などを考慮し、ネイティブfileと同じmodeを強制しない。

### 8.6 reload

v1ではhot reloadしない。

- application開始時に資格情報を読み込む。
- 稼働中のファイル変更を監視しない。
- キー交換時は新しいファイルを配置後、サービスを再起動する。
- 新旧credentialの検証・失効手順は別フェーズで扱う。

## 9. 設定仕様

TOML例:

Windowsネイティブ:

```toml
[credentials]
provider = "file"
path = 'C:\ProgramData\TickGateway\secrets\r2-writer.json'
protection = "native-acl"
```

Linuxネイティブ:

```toml
[credentials]
provider = "file"
path = "/etc/tick-gateway/r2-writer.json"
protection = "native-acl"
```

systemd credentials／container:

```toml
[credentials]
provider = "file"
path = "/run/credentials/tick-gateway.service/r2-writer"
protection = "managed-mount"
```

動的pathのため、次の環境変数を許可してよい。

```text
TICK_R2_CREDENTIALS_FILE
```

これは秘密ではなくファイルパスである。`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、独自の平文Secret環境変数は追加しない。

既存config loaderに環境変数overrideの規約がある場合は、その規約を優先する。新しい設定frameworkへの置換は本フェーズ外とする。

## 10. Fx application設計

### 10.1 module分割

最低限、次を分ける。

```go
var ConfigModule fx.Option
var FileCredentialModule fx.Option
var CoreModule fx.Option
var RuntimeModule fx.Option
```

例:

```go
var ConfigModule = fx.Module(
    "config",
    fx.Provide(config.Load),
)

func credentialFileConfig(cfg *config.Config) credentials.FileConfig {
    return credentials.FileConfig{
        Path:       cfg.Credentials.Path,
        Protection: credentials.ProtectionMode(cfg.Credentials.Protection),
    }
}

var FileCredentialModule = fx.Module(
    "credentials.file",
    fx.Provide(
        credentialFileConfig,
        fx.Annotate(
            credentials.NewFileProvider,
            fx.As(new(credentials.Provider)),
        ),
    ),
)

var CoreModule = fx.Module(
    "core",
    fx.Provide(
        gateway.New,
    ),
)

var RuntimeModule = fx.Module(
    "runtime",
    fx.Invoke(registerGatewayLifecycle),
)
```

実際のコンストラクタ名は既存repoへ合わせる。

### 10.2 production options

```go
func BaseOptions() fx.Option {
    return fx.Options(
        ConfigModule,
        CoreModule,
        RuntimeModule,
    )
}

func ProductionOptions() fx.Option {
    return fx.Options(
        BaseOptions(),
        FileCredentialModule,
    )
}

func NewProductionApp() *fx.App {
    return fx.New(ProductionOptions())
}
```

条件:

- `main`は原則として引数解析、config path確定、Fx app起動だけを行う。
- production codeで`fx.Populate`をservice locatorとして使わない。
- `fx.Invoke`はapplication rootを到達可能にする処理とLifecycle登録へ限定する。
- 依存を隠すために`optional:"true"`を濫用しない。
- 同型依存を区別するためのnamed valueは、明確な必要がある場合だけ使う。
- 起動停止順序が重要なworkerをunorderedなvalue groupへ入れない。

### 10.3 test options

production Providerとfake Providerを同じgraphへ同時登録して`fx.Replace`を多用しない。共通moduleとenvironment moduleを分ける。

```go
func TestOptions(
    provider credentials.Provider,
    overrides ...fx.Option,
) fx.Option {
    return fx.Options(
        BaseOptions(),
        fx.Supply(
            fx.Annotate(
                provider,
                fx.As(new(credentials.Provider)),
            ),
        ),
        fx.Options(overrides...),
    )
}
```

既存module構成に合わせて別方式を採ってよいが、同一interfaceの重複provideを避け、どの依存がproduction／testか明確にする。

## 11. Lifecycle仕様

### 11.1 登録

ドメインコンポーネントはFxを知らない。`internal/app`がhookを登録する。

```go
func registerGatewayLifecycle(
    lc fx.Lifecycle,
    gateway *gateway.Gateway,
) {
    lc.Append(fx.Hook{
        OnStart: gateway.Start,
        OnStop:  gateway.Stop,
    })
}
```

複数コンポーネントがある場合、責務ごとに登録関数を分けてよい。

### 11.2 OnStart

- listener、worker、goroutine等を開始する。
- 長時間動作を同期実行してblockしない。
- 起動完了を確認できない場合はerrorを返す。
- 部分起動後のerrorでは、停止可能な状態を保つ。
- 秘密情報をログへ出さない。

### 11.3 OnStop

- 新規受付を先に停止する。
- 実行中処理をcontextでcancelする。
- 永続状態を再開可能な地点へ収束させる。
- worker終了を有界時間で待つ。
- listener、file、DB等を閉じる。
- context deadlineを無視して無期限に待たない。

FxはOnStart hookを登録順、OnStop hookを逆順で実行するため、依存リソースを先に開始し、利用側を後から開始する。停止時は利用側が先、依存リソースが後になるようにする。[Fx Lifecycle](https://uber-go.github.io/fx/lifecycle.html)

### 11.4 Windows／Linux host

本フェーズでは既存host統合を維持する。

- コンソール／systemd: 原則`app.Run()`または既存signal handlingから`Start`／`Stop`
- Windows Service: SCM通知から`app.Start(ctx)`／`app.Stop(ctx)`

Windows Service wrapperの全面変更は本フェーズに含めない。現在未実装なら、Fx appが`Start`／`Stop`可能なところまでを受入範囲とする。

## 12. テスト戦略

### 12.1 テスト分類

| 分類 | Fx利用 | 目的 |
| --- | --- | --- |
| Credentials bundle unit | なし | decode／validation |
| FileProvider unit | なし | read／error／security |
| ドメインロジックunit | なし | 高速で明示的なロジック検証 |
| Constructor unit | 原則なし | 個別依存とerror検証 |
| Fx graph | `fx.ValidateApp` | 依存欠落、重複、循環 |
| Lifecycle | `fxtest` | Start／Stop／rollback |
| Application integration | `fxtest` | fake依存で全体起動 |

### 12.2 既存テストの扱い

既存テストを次の3群に分類してから変更する。

1. 変更不要: pure logic、parser、state transition等
2. constructor引数だけ変更: Provider導入に伴う明示的fake注入
3. `fxtest`へ移行: 手動でapplication全体を組み立て、Start／Stopしているテスト

テストファイルを一括変換してはならない。変更理由のないテストは維持する。

### 12.3 Provider unit test

最低限、次をテーブル駆動テストで検証する。

- 正常bundle
- ファイルなし
- pathがdirectory
- 空ファイル
- JSON object以外
- malformed JSON
- 未知field
- 重複field
- 末尾に別JSON値
- 未対応`format_version`
- Access Key ID欠落／空文字
- Secret Access Key欠落／空文字
- 最大サイズちょうど
- 最大サイズ超過
- cancel済みcontext
- error文字列にsecretが含まれない
- provider／bundleのformat出力にsecretが含まれない

OS固有:

- Windows `native-acl`: 許可ACL／過剰許可ACL
- Linux `native-acl`: `0400`／`0600`成功、group／other permission失敗
- `managed-mount`: ネイティブmode制約を誤適用しない

資格情報fixtureには実キーに似た値を使わず、明確なテスト値を使う。

### 12.4 Graph validation test

```go
func TestProductionGraph(t *testing.T) {
    err := fx.ValidateApp(
        app.ProductionOptions(),
    )
    require.NoError(t, err)
}
```

production config constructorが環境へ依存する場合、graph validation専用に純粋なconfig供給方法を用意する。`fx.ValidateApp`は依存グラフを検証するが、constructor／invokeを実行しないため、これだけで起動テストを代替しない。

負例として、必須Providerを外したgraphがvalidation errorになることも確認する。

### 12.5 Lifecycle test

`fxtest.New`と`RequireStart`／`RequireStop`を使用する。

```go
func TestApplicationStartStop(t *testing.T) {
    events := newEventRecorder()
    provider := newFakeProvider()

    app := fxtest.New(
        t,
        app.TestOptions(provider, fx.Supply(events)),
    )

    app.RequireStart()
    app.RequireStop()

    require.Equal(t, []string{
        "dependency.start",
        "gateway.start",
        "gateway.stop",
        "dependency.stop",
    }, events.All())
}
```

既存コンポーネントの実際の順序に合わせ、次を確認する。

- 正常Start／Stop
- 二重Start／二重Stopに対する既存契約
- 途中のOnStart失敗
- OnStart失敗後に開始済みhookが巻き戻されること
- OnStop errorの伝播
- context timeout
- worker／listenerの終了
- goroutine leakがないこと
- Stop後に新規処理を受け付けないこと

### 12.6 Application integration test

- temporary directoryを使用する。
- port `0`等でOSに空きportを割り当てさせる。
- fake Credential Providerを注入する。
- fake／in-memory external clientを使う。
- DNS、Cloudflare、AWS、インターネットへ接続しない。
- ユーザーの実config、home directory、Windows credential、systemd credentialを読まない。

### 12.7 Test fake

テスト専用Provider例:

```go
type StaticProvider struct {
    Credentials credentials.Credentials
    Err         error
    Calls       atomic.Int64
}

func (p *StaticProvider) Load(context.Context) (credentials.Credentials, error) {
    p.Calls.Add(1)
    return p.Credentials, p.Err
}
```

production packageへ安易に`StaticProvider`を公開しない。複数packageで必要なら`internal/testkit`等へ置く。

### 12.8 Secret leakage test

意図的に一意なcanary secretをfixtureへ設定し、次へ含まれないことを検査する。

- application log
- Fx event log
- returned error
- `%v`、`%+v`で整形した公開型
- panic message
- test failure outputで通常表示される構造

Fx event loggerもproduction loggerへ統合する場合、constructor parameterや返却値を無加工でdumpしない。

## 13. 移行手順

### Step 1: ベースライン固定

1. 現在の`go test ./...`結果を記録する。
2. 既存の既知失敗を、新規失敗と区別して一覧化する。
3. main／service host／Start／Stop／config／credential参照を調査する。
4. 現在の依存グラフを簡潔に記録する。

### Step 2: Provider導入

1. `credentials.Credentials`と`Provider`を追加する。
2. `FileProvider`とbundle parserを追加する。
3. 既存の資格情報直読箇所をProvider利用へ変更する。
4. 単体テストを追加する。
5. 外部挙動が変わっていないことを確認する。

### Step 3: コンストラクタ整備

1. package-level globalへの依存をconstructor引数へ移す。
2. 長時間処理をconstructorから`Start`へ移す。
3. cleanupを`Stop`へまとめる。
4. 既存テストへfakeを直接渡して復旧する。

本Stepで無関係なドメインAPI変更を行わない。

### Step 4: Fx module導入

1. `ConfigModule`を作成する。
2. `FileCredentialModule`を作成する。
3. 既存コンポーネントのmoduleを作成する。
4. Lifecycle登録を作成する。
5. production applicationをFxから構築する。
6. mainを薄くする。

### Step 5: テスト再構成

1. pure unit testは維持する。
2. application assembly testを`fxtest`へ移す。
3. graph validation testを追加する。
4. Lifecycle failure／rollback testを追加する。
5. test moduleとproduction moduleを分離する。
6. secret leakage testを追加する。

### Step 6: Cross-platform検証

1. Linuxでformat、test、race testを実行する。
2. Windowsでformat、test、buildを実行する。
3. OS固有permission testを各runnerで実行する。
4. build tagで片方のOS実装が他方へ混入していないことを確認する。

### Step 7: 最終レビュー

1. 外部仕様差分がないことを確認する。
2. Fx依存がドメインへ漏れていないことを確認する。
3. Provider以外が資格情報ファイルを直接読んでいないことを確認する。
4. testから外部ネットワークへ接続していないことを確認する。
5. 不要になった手動DI／global／重複fixtureだけを削除する。

## 14. 禁止事項

- 全packageのconstructorを`fx.In`へ機械的に変更しない。
- ドメイン型へ`fx.Out`を埋め込まない。
- application containerをservice locatorとして使わない。
- production codeで`fx.Populate`を使わない。
- 依存エラーを避けるために`optional:"true"`を追加しない。
- 起動順序が必要なcomponentをunordered value groupへ入れない。
- `fx.Invoke`で長時間処理を直接実行しない。
- testのためだけにproduction APIを不必要にexportしない。
- testでsleepに依存した同期を行わない。
- 実R2 credentialやユーザー環境変数をtestで読むことを禁止する。
- Secret Access KeyをCLI引数、環境変数、ログへ出さない。
- Provider導入と同時にR2やWALの意味論を変更しない。
- 無関係なrename、format全repo変更、dependency upgradeを混ぜない。

## 15. エラー処理

Credential関連errorは分類可能にする。

```go
var (
    ErrCredentialPathRequired = errors.New("credential path is required")
    ErrCredentialFileUnsafe   = errors.New("credential file permissions are unsafe")
    ErrCredentialTooLarge     = errors.New("credential file is too large")
    ErrCredentialMalformed    = errors.New("credential file is malformed")
    ErrCredentialVersion      = errors.New("unsupported credential format version")
    ErrCredentialIncomplete   = errors.New("credential fields are incomplete")
)
```

条件:

- `errors.Is`／`errors.As`で分類できること。
- pathはerrorへ含めてよいが、bundle内容を含めない。
- parser errorをそのまま返して入力断片を露出させない。
- Fx constructor errorはroot causeを保持しつつsecretを含めない。

## 16. ログ要件

ログへ出してよいもの:

- provider種別`file`
- protection mode
- credential file path
- load成功／失敗のerror class
- Fx component開始／停止
- 起動／停止時間

ログへ出してはならないもの:

- Access Key ID
- Secret Access Key
- JSON bundle
- 認証構造体のdump
- ファイル内容・長さから推測可能な詳細

Access Key IDも本仕様ではsecret相当としてredactする。

## 17. CI要件

最低限、次のmatrixを持つ。

```yaml
strategy:
  matrix:
    os: [ubuntu-latest, windows-latest]
```

各OS:

```text
go test ./...
go vet ./...
go build ./...
```

Linuxでは最低限、次も実行する。

```text
go test -race ./...
```

repoに既存lint／format／CI規約がある場合はそれを維持する。新しいCI vendor導入は不要。

## 18. 受入条件

以下をすべて満たしたとき完了とする。

### Provider

- `credentials.Provider`が導入されている。
- v1のproduction実装が`FileProvider`だけである。
- 資格情報が単一JSON bundleから読み込まれる。
- strict decode、version、必須field、size limitが検証される。
- Provider以外がcredential fileを直接読まない。
- 平文credential環境変数が追加されていない。
- credential hot reloadが暗黙に導入されていない。
- エラーとログに資格情報が含まれない。
- Windows／Linuxのsecurity policyがテストされている。

### Fx

- production applicationがFx graphから構築される。
- moduleが責務単位で分割されている。
- Fx依存が原則composition rootへ限定されている。
- 既存常駐componentがLifecycleへ接続されている。
- constructorが長時間処理を開始しない。
- Start／Stop順序が明示・テストされている。
- mainが薄くなっている。
- productionでservice locator patternを使っていない。

### Tests

- pure unit testはFxなしで維持されている。
- Provider unit testがある。
- production graph validation testがある。
- 必須依存欠落のnegative graph testがある。
- `fxtest`による正常Start／Stop testがある。
- 起動途中失敗とrollback testがある。
- test fakeでProviderを差し替えられる。
- testが実R2／外部networkへ接続しない。
- secret leakage canary testが通る。
- Windows／Linuxの既存テストが復旧している。

### Compatibility

- CLI、config、通信、ファイル、R2、WAL、pruneの外部意味論が変わっていない。
- 変更が本フェーズの責務へ限定されている。
- 既知の既存失敗と新規失敗が区別されている。
- Windows／Linux buildが成功する。

## 19. Codexが提出する最終報告

最終報告には次を含める。

1. 変更概要
2. Provider interfaceとFileProviderの配置
3. Fx module構成
4. Lifecycleの開始・停止順序
5. 変更したテスト／変更しなかったテストの分類
6. Windows／Linuxの実行コマンドと結果
7. 既存挙動が維持されている根拠
8. 未解決事項
9. 次フェーズへ持ち越す事項

テスト未実行を「成功」と表現しない。Windows runner等を利用できなかった場合は、buildのみ／未検証を明確に区別する。

## 20. ExecPlanテンプレート

Codexは実装開始前に次の形式でExecPlanを作成・更新する。

```markdown
# ExecPlan: Credential Provider / Uber Fx migration

## Baseline
- Current entrypoints:
- Current dependency wiring:
- Current lifecycle:
- Current credential access:
- Existing tests:
- Known failures:

## Scope
- In scope:
- Out of scope:

## Design
- Provider interface:
- File bundle:
- Fx modules:
- Lifecycle order:
- Test override strategy:

## Implementation steps
1. ...
2. ...

## Validation
- Linux:
- Windows:
- Race:
- Secret leakage:

## Risks and rollback
- ...
```

## 21. 参考資料

- [Uber Fx package](https://pkg.go.dev/go.uber.org/fx)
- [Uber Fx Lifecycle](https://uber-go.github.io/fx/lifecycle.html)
- [Uber Fx test utilities](https://pkg.go.dev/go.uber.org/fx/fxtest)
- [systemd System and Service Credentials](https://systemd.io/CREDENTIALS/)
- [Docker secrets](https://docs.docker.com/engine/swarm/secrets/)
