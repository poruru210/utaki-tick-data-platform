# Protocol V1のconformance

**Conformance**：実装が`protocol/v1/`のwire、message、hash、エラー処理の規則を満たすことを確認する手順です。

## 対象

conformanceでは、Go Gateway、MQL5 producer、Fake producerが同じ契約を解釈できることを確認します。

確認対象には、正常なHello、Resume、Batch、Ack、Errorを含めます。

異常系には、unknown version、短いframe、CRC不一致、ACK欠落、重複、境界値、WAL復旧を含めます。

## 判定材料

判定は、golden bytes、canonical JSON、hash、期待するACKまたはErrorで行います。

実際のtickデータ、credential、broker接続を判定材料にしません。

同じfixtureをGo、MQL5、Pythonから読める構成にします。

## Producer追加時の手順

新しいproducerを追加するときは、source schemaを定義します。

次に、正常系と異常系のfixtureを追加します。

その後、producerとGatewayでconformance caseを実行します。

transportの共通規則とsource schemaの固有規則を、別々に失敗として報告できるようにします。
