# RawとReplayのmanifest

Manifestは、保存したデータの範囲、schema、件数、hash、immutable objectを記録します。

Manifestは、再取得、検証、replay生成の入力になります。

## Raw-day manifest

**Raw-day manifest**：あるsourceと日付範囲に対応するraw保存物を記録するmanifestです。

Raw-day manifestは、source schema、対象時刻範囲、cursor範囲、件数、object key、bytes、hashを参照します。

Raw-day manifestは、producerやGatewayの非公開型を参照しません。

## Replay-day manifest

**Replay-day manifest**：raw保存物から生成したreplay用データを記録するmanifestです。

Replay-day manifestは、replay schema、入力raw hash、対象時刻範囲、順序、件数、生成物のhashを参照します。

Replay-day manifestは、raw-day manifestと異なる契約として管理します。

## 不変性

manifestが参照するobjectは、hashで検証できるimmutable objectとして扱います。

同じkeyを内容の異なるobjectで上書きしません。

manifestのcanonical JSONとhashのdomainはfixtureで固定します。

実装型の名前をmanifestの公開契約にしないことで、Go、MQL5、Pythonの実装を独立して変更できます。
