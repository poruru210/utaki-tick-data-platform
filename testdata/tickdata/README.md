# Tick data fixture

`testdata/tickdata/`は、tick data contractを実装間で検証するための合成fixtureを置くディレクトリです。

## 目的

fixtureは、Go Gateway、MQL5 producer、Fake producer、Python検証ツールが同じ入力を解釈できることを確認します。

fixtureは、実際の市場データや接続先に依存せず、同じ入力から同じbytesとhashを得られるようにします。

## 対象ケース

- 正常なHello、Resume、Batch、Ack、Error
- ACK欠落、再接続、再送、重複
- 短いresponse、malformed frame、CRC不一致
- dense boundaryとcursor境界
- WAL recoveryとmanifestのhash検証

fixtureの各ケースには、入力、期待するwireまたはcanonical JSON、hash、検証結果を対応付けます。

## 変更規則

fixtureのbytesとhashは、Protocol V1の仕様変更と同じ変更単位で更新します。

実際のtick data、credential、口座情報、個人を特定できる値は追加しません。

fixtureを更新したときは、`tools/tick_fixture_verify.py`、Goテスト、Pythonテストを実行します。

共有fixtureのgolden bytesは[`golden/README.md`](golden/README.md)で扱いを定義します。
