# Golden fixture

このディレクトリは、Protocol V1のwire bytes、canonical JSON、hashを固定するための領域です。

golden fixtureは、Go、MQL5、Pythonの実装が同じ結果を生成することを確認する基準になります。

## 収録対象

M0では、Hello、Resume、Batch、Ack、Errorの正常系を収録します。

異常系では、短いframe、CRC不一致、ACK欠落、重複、boundary、WAL recoveryを収録します。

各fixtureには、schema identifier、入力、期待するbytes、hash、判定結果を対応付けます。

## 変更規則

golden bytesを変更するときは、wire、message、schema、hash domainの仕様変更理由を記録します。

実際のtick data、秘密情報、口座情報は収録しません。

変更後は、fixture verifierと全契約テストを実行します。
