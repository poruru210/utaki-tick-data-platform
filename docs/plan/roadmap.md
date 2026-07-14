# 実装ロードマップ

この文書は、`.agent/tick-data-platform-execplan-revised.md`に定義した実装計画を、段階ごとの到達状態として整理したものです。

ExecPlanの内容を変更する場合は、ExecPlan自体を更新し、この文書との対応を確認します。

## 開発環境

開発ツールはmiseで固定します。

```powershell
mise trust
mise install
mise run bootstrap
mise run check
```

MQL5のコンパイルと実機接続の確認は、MetaTrader 5が動作するWindows環境で実施します。

## M0の契約固定

M0では、実装より先にproducerとGatewayが共有する契約を固定します。

M0ではTCP runtime、live MT5 collection、R2、Parquet、SQLite journal runtime、crash injection、production operationを実行しません。

固定対象はwire framing、message layout、hash domain、canonical JSON、WAL、raw-day manifest、replay-day manifestです。

`part-manifest-v1`の仕様と実装はM3へ延期します。

`producers/fake/`には、決定的かつネットワークを使わないGo製のtest producer/packageを置きます。

Go、MQL5、Pythonが同じ結果を返すcross-language fixtureを作成します。

unknown version、短いframe、CRC不一致、重複、ACK欠落、WAL復旧をfixtureとconformance caseで表現します。

M0の完了条件は、仕様、fixture、Go側検証、MQL5側送信の解釈が一致していることです。

### 2026-07-14時点の進捗

protocol/v1/にwire、message、source schema、WAL、hash domain、canonical JSON、raw/replay manifestの契約を固定しました。

testdata/tickdata/golden/に18 fixtureを追加し、Go decoder、独立Python decoder、fake producerでbytes、hash、正常系、wire異常系、stateful scenarioを検証しました。

MQL5のHello/Batch encoderはMetaEditorで0 errors、0 warningsを確認しました。

MQL5実機でのfixture出力、TCP実通信、live MT5 collection、R2、Parquet、SQLite journal runtimeは実施していません。

## 2026-07-15時点のM1実装

`internal/ingest/`にloopback TCP listener、bounded frame reader、Hello/Resume handshake、session lease、Batch/Ack処理、cursor directive、status metricsを実装しました。

`internal/wal/`に`protocol/v1/wal-layout.md`準拠のactive WALを実装し、BatchFrameV1のappend、file sync、entry CRC、batch hash、entry chain、partial tail recoveryを検証します。

`internal/journal/`にCGo-free SQLite journalを実装し、batch inventoryとcursor stateをWALから再構築できるようにしました。

`producers/mt5/TickCaptureService.mq5`にCopyTicks、built-in TCP、Hello/Resume、in-flight frame保持、ACK判定、再接続、再送を実装しました。

fake producerのTCP integration testで、accepted batch、duplicate、same identity/different bytes、source error、dense boundary、partial frame、WAL sync前後、ACK loss、journal deletion後のrebuildを検証しました。

MetaEditor compileは`Result: 0 errors, 0 warnings`でした。

## M1のローカル収集

M1では、MT5 producerからGo Gatewayへlocalhost TCPでデータを送ります。

GatewayはHello、Resume、Batch、Ack、Errorのmessageを処理します。

producerはACKの欠落、再接続、再送に対応します。

Gatewayは受信済み範囲と重複を判定し、WALへ先行記録します。

M1の完了条件は、fake producerとMT5 producerの両方で、切断後の再接続と重複排除を再現できることです。

M1ではR2、Parquet、HTTP delivery、local pruning、複数producer、24時間soakを実施しません。

## M2のRaw公開

M2では、受信したrawデータを日次単位で保管し、R2へ配置します。

raw-day manifestはsource schema、対象範囲、件数、hash、immutable objectを参照します。

manifestはGoやMQL5の非公開型を参照しません。

M2の完了条件は、ローカル保存とR2配置の結果をmanifestとhashで検証できることです。

### 2026-07-15時点のM2 raw off-host delivery

internal/walはactive WALへTWTR trailerを追加し、seal済みsegmentへ切り替えた後、次のgateway ingest sequenceとentry hash chainを引き継ぐ新しいactive WALを作成します。

起動時にはseal済みsegmentをsequence順に検証し、active WALと合わせてaccepted batch inventoryを復元します。

検証対象はheader、entry length、BatchFrameV1、commit marker、CRC32C、batch SHA-256、entry hash chain、trailer、trailer直前までのfile SHA-256です。

raw objectのkeyには、trailerを含むseal済みfile全体のSHA-256を使います。

internal/archiveは検証済みsegmentを再encodeも圧縮もせず、既存objectを上書きしないatomic operationでローカルoutboxへpromoteします。

同じbytesのretryは同じobjectを返し、同じkeyに異なるbytesが存在する場合はintegrity failureとして停止します。

この段階では明示的なStore.Seal APIだけを提供し、自動rotation policyは実装しません。

Protocol V1 raw-day manifestはverified sealed WAL、day-selected ranges、full chain_objects、revision graph、raw_set_rootをcanonical bytesへ固定します。

M2R-2はpinned rclone、campaign publisher claim、local exclusive lock、独立publication journal、immutable transfer、remote recheck、verification receiptを提供します。

M2R-3はread-only ArchiveReader、tickctlのdatasets、campaigns、snapshots raw、fetch、tick-verifyのday、campaign commandsを提供します。

M2R-4はnetwork-free fake end-to-end test、optional isolated real-R2 smoke、repository check workflow、Windows race workflow、verification recordを追加します。

通常のM2検証はfake backendとfake rcloneだけで完結し、real R2 smokeは明示的なenable、confirmation、isolated bucketまたはprefix、endpoint、credential、pinned rclone binaryを要求します。

M2の対象外はParquet、replay-dayまたはpart manifest、handover、pruning、Worker、HTTP API、live brokerです。

M2の実装は完了しましたが、real R2 smokeとGitHub Actionsの実行成功は未確認であり、PRではその実行結果を別途提示します。

## M3のReplay配信

M3では、rawデータからreplay用データを生成します。

replay-day manifestは、replay用schema、時刻範囲、順序、件数、生成元hashを記録します。

Parquetの生成とcatalogの登録は、rawの完全性を確認した後に実行します。

M3の完了条件は、同じ入力から同じreplay結果を再生成できることです。

## M4の運用強化

M4では、日次運用、pruning、handover、長時間稼働、障害注入を整備します。

必要に応じて、HTTP adapterなどの後続利用者向け接続を追加します。

M4の完了条件は、復旧手順と保存期限を含む運用手順を検証環境で実行できることです。
