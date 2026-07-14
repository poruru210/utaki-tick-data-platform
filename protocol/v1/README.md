# Protocol V1の位置付け

`protocol/v1/`は、producerとGo Gatewayの間で使うIFの正規情報源です。

**正規情報源**：実装より優先して参照する、契約の基準となる文書とfixtureです。

## 分離する責務

Protocol V1は、共通transportとデータ源固有のsource schemaを分けて定義します。

共通transportは、frame、message type、cursor、ACK、Error、CRCの規則を定義します。

source schemaは、データ源のpayload、時刻、属性、取得状態、エラー情報を定義します。

MT5固有の値は`mt5.mqltick.v1`へ置き、共通Batchの型には埋め込みません。

## 実装との対応

`internal/protocol/`は、Protocol V1を実装するGo側のdecoder、validator、encoderです。

producerは`internal/protocol/`を直接importせず、各言語で同じProtocol V1を実装します。

`producers/fake/`は、Protocol V1に従う決定的なテスト入力を提供します。

## 文書

**[wire-layout.md](wire-layout.md)**：frameの配置、幅、最大値、version規則を定義します。

**[messages.md](messages.md)**：Hello、Resume、Batch、Ack、Errorの意味を定義します。

**[schemas/README.md](schemas/README.md)**：source schemaの境界と識別子を定義します。

**[hash-domains.md](hash-domains.md)**：各hashの入力範囲と用途を定義します。

**[manifests.md](manifests.md)**：raw-dayとreplay-dayの保存契約を定義します。

**[fixtures/README.md](fixtures/README.md)**：合成fixtureとgolden bytesの用途を定義します。

**[conformance/README.md](conformance/README.md)**：実装が契約に適合することを確認する方法を定義します。

## M0で固定する項目

wireのoffset、width、最大frame長、unknown versionの扱いを固定します。

messageの必須フィールド、canonical JSON、CRC、hash domain、manifestの不変性を固定します。

Go、MQL5、Pythonで同じfixtureを検証し、実装間の解釈差を検出します。
