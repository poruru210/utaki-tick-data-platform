# tick-data-platform

`tick-data-platform` は、MT5を含む複数のデータ源からティックデータを収集し、再利用可能なデータとして保存するモノレポです。

M1では、1 brokerと1 exact symbolを対象に、MT5 Serviceからlocalhost TCPでGo Gatewayへ送信し、WAL syncとSQLite journal commitの後にACKを返します。

M2のローカル基盤では、active WALをProtocol V1準拠のsegmentとしてsealし、完全性を再検証したbyte-exactなcopyだけをcontent-addressed outboxへ置きます。

**モノレポ**：Gateway、producer、Protocol、検証ツールを一つのリポジトリで管理する構成です。

**IF**：producerとGatewayの間で交換する、言語に依存しないデータ形式と通信規則です。

## 構成

- `protocol/v1/`：IFの正規仕様です。
- `producers/mt5/`：MQL5で実装するMT5 producerです。
- `producers/fake/`：決定的なテストデータを送るproducerです。
- `cmd/`：Go製Gatewayの実行エントリポイントです。
- `internal/`：Gatewayの受信、検証、WAL、アーカイブ、配信処理です。
- `tools/`：Pythonで実行するfixture検証などの開発支援ツールです。
- `testdata/tickdata/`：Go、MQL5、Pythonで共有するfixtureです。
- `docs/`：構成、ロードマップ、開発環境の説明です。
- `local/`：ローカル実行用の設定例です。
- `mise.toml`：Go、Python、uv、開発タスクの管理定義です。

## データ経路

```text
MT5 producer -> protocol/v1 -> localhost TCP -> Go Gateway -> WAL + durable ACK -> sealed WAL -> local raw outbox -> archive/R2 -> delivery
```

producerはデータ源固有の形式をIFへ変換します。

GatewayはIFを検証し、受信済みデータを永続化して後続処理へ渡します。

現在の実装は明示的なWAL sealとローカルoutboxへのpromoteまでを提供し、自動rotation policy、R2、raw-day manifest、Parquet、HTTP delivery、local pruningを実行しません。

Pythonは本番Gatewayの実装には使わず、fixtureと契約の検証に使います。

## 開発環境

```powershell
mise trust
mise install
mise run bootstrap
mise run check
```

`mise run check` はGoテスト、Pythonテスト、fixture検証、書式検査を実行します。

MT5 producerのコンパイルと実行には、別途MetaTrader 5のWindows環境が必要です。

## 参照先

IFを変更するときは、先に [`protocol/v1/README.md`](protocol/v1/README.md) と関連するwire、message、schema、fixtureの文書を確認してください。

構成の責務分担は [`docs/architecture/repository-layout.md`](docs/architecture/repository-layout.md) に定義しています。
