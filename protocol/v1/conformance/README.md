# Protocol V1のconformance

**Conformance**：実装がProtocol V1のbytes、hash、decode結果、失敗コードに適合することを確認する判定です。

## 正常系

Go decoder、Python decoder、MQL5 encoder、fake producerは、valid fixtureのwire_hexを同じbytesとして扱います。

BatchのRawMqlTickV1は、全fieldのbit pattern、signedness、time unit、record orderを保持します。

hashはfixtureのexpected_hashesと一致しなければなりません。

## 異常系

truncated frameはTRUNCATED_FRAME、CRC mutationはCRC_MISMATCH、unknown versionはUNSUPPORTED_PROTOCOL_VERSIONとして拒否します。

unknown messageはUNKNOWN_MESSAGE_TYPE、oversized frameはOVERSIZED_FRAMEとして拒否します。

拒否したframeはdecode結果、hash、WAL entry、ACKを生成しません。

## Stateful scenario

duplicate retransmissionとACK loss retryは、同じproducer session、batch sequence、bytesを再入力してDUPLICATE Ackになることを検証します。

WAL recoveryは、commit marker、entry CRC、entry hash、file hashを検証したentryをREPLAY_COMMITTED_ENTRYとして扱います。

short responseとdense boundaryは、source errorまたはdense boundary flagを保持したvalid BatchFrameV1として検証します。

## 判定順序

まずfixture indexのpathとJSONを検証します。

次にwire bytesをdecoderへ入力し、expected_resultとexpected_error_codeを比較します。

valid fixtureではdecoded fields、canonical JSON、hashを比較します。

同一fixtureをGo、Python、MQL5、fake producerで比較し、実装ごとの期待値を作りません。

## Producer追加

新しいproducerはsource schema、valid fixture、invalid fixture、hash、conformance caseを同時に追加します。

共通transportの規則とsource schemaの規則を別々に判定できるようにします。
