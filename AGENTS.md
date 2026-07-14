# Repository Guidelines

## 構成と責務

このrepositoryは、Gatewayと複数producerを同じモノレポで管理します。

**IF**はGatewayとproducerが通信するための独自プロトコルです。

`protocol/v1/`をIFの正本とし、wire layout、message、schema、golden fixture、conformance testを置きます。

`producers/<name>/`にはproducerを置き、初期producerは`producers/mt5/`と`producers/fake/`です。

`cmd/`はGo Gatewayとread-only CLIの入口であり、`internal/`はGatewayの内部実装です。

producerは`internal/`、WAL、R2、Parquetをimportしません。

## 開発環境とコマンド

ツールチェーンは`mise.toml`で管理します。

```text
mise trust
mise install
mise run bootstrap
mise run check
```

個別の検証には`mise run test-go`、`mise run test-python`、`mise run fixture`、`mise run format`を使います。

Go commandは`mise exec -- go run ./cmd/tick-gateway`のように実行します。

MQL5のcompileと実機実行にはMetaTrader 5のWindows環境を使います。

## 実装規約

Goは`gofmt`で整形し、PythonはRuffでformatとlintを実行します。

Goのexported identifierには`PascalCase`、Pythonの関数とtestには`snake_case`を使います。

protocol名は`HelloV1`や`BatchFrameV1`のようにversionを含めます。

source固有のschemaとcursorを共通transport messageへ混ぜません。

## テスト方針

Pythonのunit testは`tests/unit/`、stateful testとinvariant testは`tests/stateful/`に置きます。

テストファイルとテスト関数には`test_` prefixを付けます。

wire contractを変更するときは`protocol/v1/fixtures/`のgolden dataとconformance testを更新します。

レビュー前には`mise run check`を成功させます。

## 設定と安全性

credential、実Tick、WAL、SQLite、Parquet、R2 object、実行用`.toml`をcommitしません。

設定は`local/*.toml.example`を基に作成し、secretはローカルへ注入します。

## CommitとPull Request

既存のcommit履歴がないため、短い命令形とprefixを使います。

```text
feat: add batch decoder
test: add resume fixture
docs: clarify producer contract
```

Pull Requestには変更の目的、影響する境界、実行した検証コマンドと結果、関連issueを記載します。

secretとruntime dataを含めていないことも明記します。

## 技術文書の作成

日本語のREADME、設計書、ExecPlan、レビュー記録、AGENTS.mdを新規作成または改稿するときは、`japanese-tech-writing` skillを使用します。

同skillの`SKILL.md`に従い、一文ごとに改行し、初出の用語を定義し、事実と推定を区別します。

根拠のない断定、空虚な総括、同じ主張の反復を残しません。
