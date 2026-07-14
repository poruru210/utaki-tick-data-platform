# Protocol V1のfixture

fixtureは、producerとGatewayの契約を固定値で検証するための合成データです。

## 収録するケース

golden fixtureには、正常なHello、Resume、Batch、Ack、Errorを収録します。

異常系には、ACK欠落、重複、短いresponse、dense boundary、malformed frame、WAL recoveryを収録します。

fixtureには、wire bytes、canonical JSON、期待するhash、期待するdecode結果を含めます。

## 共有規則

fixtureはGo、MQL5、Pythonから参照できる形式にします。

fixtureのbytesとhashは、仕様変更の承認なしに書き換えません。

実際のtickデータ、credential、個人や口座を特定できる値は収録しません。

fixtureを追加または更新したときは、対応するconformance caseと契約テストを更新します。

## 配置

共有fixtureの実体は`testdata/tickdata/`に置きます。

このディレクトリの文書は、Protocol V1でfixtureを使う目的と対象を説明します。
