# Repositoryの構成

このリポジトリは、Gatewayと複数のproducerを同じモノレポで管理します。

**Gateway**：producerから受信したIFを検証し、永続化、アーカイブ、配信へつなぐGoサービスです。

**Producer**：データ源からティックデータを取得し、IFに従ってGatewayへ送る入力アダプターです。

**IF**：producerとGatewayの間で共有する、言語に依存しない契約です。

## 全体像

```text
protocol/v1/                  IFの正規仕様
producers/mt5/                MT5向けMQL5 producer
producers/fake/               決定的なテストproducer
cmd/tick-gateway/             Gatewayの実行エントリポイント
internal/ingest/               受信と接続管理
internal/protocol/             Go側のdecode、validate、encode
internal/wal/                  受信データの先行記録
internal/archive/              rawとcontinuityの保存
internal/parquet/               replay用データの生成
internal/catalog/               メタデータと索引
internal/r2/                    R2への配置と検証
internal/delivery/              後続利用者への配信
tools/                          fixtureと契約の検証
testdata/tickdata/              共有fixture
```

データはproducerからlocalhost TCPを通ってGatewayへ入り、Gateway内でProtocol検証、WAL sync、SQLite journal commitの順に処理します。

M1ではWAL syncとjournal commitがdurable ACKの境界です。

R2、Parquet、HTTP delivery、local pruningはM1のACK経路から除外します。

```text
producer -> protocol/v1 -> localhost TCP -> ingest -> protocol validation -> WAL -> archive/replay/delivery
```

## 主要ディレクトリ

**`protocol/v1/`**：wire layout、message、source schema、hash domain、manifest、conformanceの正規仕様です。

**`internal/protocol/`**：`protocol/v1/`を実装するGo側のdecoder、validator、encoderです。

**`producers/<name>/`**：データ源ごとの取得処理、source schemaへの変換、IF送信処理を置きます。

**`cmd/`**：サービスまたは運用ツールの起動処理だけを置きます。

**`internal/`**：外部から直接参照させないGateway内部の責務ごとの実装を置きます。

`internal/ingest/`はbounded frame decode、session lease、cursor、ACK、status metricsを実装します。

`internal/wal/`は`protocol/v1/wal-layout.md`に従うactive WALをappendし、entry commit marker、CRC、batch hash、entry chainを検証します。

`internal/journal/`はWALから再構築できるSQLite batch indexとcursor stateを保持します。

**`testdata/tickdata/`**：仕様に対応する合成fixtureとgolden bytesを置きます。

**`tools/`**：本番経路に依存しない検証や生成の補助処理を置きます。

## Protocolの境界

`protocol/v1/`がIFの正規情報源です。

`internal/protocol/`はGo実装であり、producerから参照する共有ライブラリではありません。

共通messageには、`CopyTicks`や`RawMqlTickV1`などMT5固有の型を持ち込みません。

MT5固有のフィールドは`mt5.mqltick.v1`のsource schemaに定義します。

## Producerを追加する規則

新しいデータ源は`producers/<name>/`に追加します。

共通の接続、frame、ACK、再送規則は`protocol/v1/`を共有します。

データ源固有のpayload、cursor、取得時刻、エラー情報はsource schemaとして分離します。

新しいproducerを追加するときは、対応するschema、golden fixture、conformance caseを同時に追加します。

共有messageをデータ源ごとの事情に合わせて拡張する変更は、先にProtocol V1の互換性を評価します。
