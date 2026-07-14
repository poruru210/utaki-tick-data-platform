# miseで開発環境を管理する

このリポジトリでは、miseを使ってGo、Python、uvと開発タスクを管理します。

**mise**：ツールのバージョンとプロジェクト単位のタスクを宣言的に管理する実行環境です。

## 初回セットアップ

リポジトリのルートで次のコマンドを実行します。

```powershell
mise trust
mise install
mise run bootstrap
```

`mise trust`は、このリポジトリの`mise.toml`を信頼する設定です。

`mise install`は、`mise.toml`に定義したGo、Python、uvを準備します。

`mise run bootstrap`は、uvを使ってPythonの開発依存関係を同期します。

## 主なタスク

```powershell
mise run test-go
mise run test-python
mise run fixture
mise run format
mise run format-check
mise run check
```

`test-go`は`go test ./...`を実行します。

`test-python`は契約テストとstateful invariant testを実行します。

`fixture`は共有tick fixtureを検証します。

`format`はGoとPythonの書式を整えます。

`format-check`は書式、lint、差分の空白を検査します。

`check`は、上記の検証をまとめて実行する基本チェックです。

## 実行時の境界

Gatewayの実装とテストはGoを使います。

Pythonはfixture生成や契約検証などの開発支援に使います。

MT5 producerはMQL5で実装するため、miseのGoやPython設定だけではコンパイルできません。

Goサービスを直接実行するときは、バージョンの揺れを避けるため次の形式を使います。

```powershell
mise exec -- go run ./cmd/tick-gateway
```
