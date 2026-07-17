# M4 MT5 24-hour soak

これは1 broker・1 exact symbol・1 MQL Service・1 Go Gatewayだけを使う実機runbookです。
2026-07-16時点ではMT5 Windows host、実broker、forced reboot可能な環境がないため未実施です。
24時間未満の試運転やfake producerの成功をM4-8の証拠にしません。

## Preflight

run開始前にoperator 2名で次を確認し、チェック結果をrun recordへ残します。

- unique `run_id`、commit、Go/MT5/build identity、config digest、operator ID digestを固定した。
- broker、server fingerprint、exact symbol、timezone/day definition、source schemaを固定した。
- MQL ServiceとGatewayが1対1で、Gatewayはloopback listener、WAL、journal、outbox、lockを
  他scopeと共有しない。
- source、Gateway、R2 SDK upload、disk、terminal healthのbaselineがhealthyである。
- write credentialはrun専用scope、verificationは空の別cacheとread-only credentialである。
- logとmetricsの保存先はrepository外で、secret scannerを通る。実データとWALはcommitしない。
- abort担当、R2停止担当、OS reboot担当、証跡担当の連絡先が決まっている。

## Required duration and event schedule

clockはUTCで記録し、run identityは全eventで同じものを使います。eventの注入により
ACK済みentry、chain、manifest、source errorの意味を変えてはなりません。

| 時点 | event | 期待する観測 |
| --- | --- | --- |
| 00:00 | baseline開始 | ACK、WAL sync、journal commit、source lagを記録 |
| 02:00 | Go Gateway graceful restart | durable stateから再開し、重複ACKやgapなし |
| 04:00 | MQL Service restart | reconnectとbounded replay後にsource errorなし |
| 06:00 | MT5 terminal restart | producer identity/session replacementが記録される |
| 08:00 | networkまたはR2 read/write停止 | retryはbounded、ACKは証拠なしに進まない |
| 10:00 | R2 upload timeout/retry注入 | immutable same-content retry、different bytes拒否 |
| 12:00 | disk high-water | statusがblockedになり、proofなしpruneをしない |
| 14:00 | Gateway長時間停止 | 復旧後にsource gapをsynthetic Tickで埋めない |
| 16:00 | forced OS reboot | WAL/journal recovery後にchainを再検証 |
| 18:00 | market-closed credential operation | Gateway停止、credential設定更新、再起動後のstatus/R2確認 |
| 24:00以降 | soak終了 | event recovery、resource、verificationをfreeze |

停止や障害で連続稼働期間が切れた場合は、終了時刻を実際に記録し、runを24時間passに
繰り上げません。再開する場合は新しいrun identityにします。

## Metrics and abort conditions

最低限、ACK count、durable WAL entries、source lag、reconnect回数、error batch、WAL bytes、
disk free、goroutine/heap、R2 request failure、upload retry、API timeout、recovery timeを
時系列で保存します。p95/p99 ACK latencyと最大値をsummaryへ転記します。

次のいずれかで即時abortし、データを補修せずにintegrity stopとして記録します。

- ACK済みデータの欠落、chain gap、manifest branch、publisher split brain
- proofのないdelete/prune、scope外write、read-only credentialのwrite成功
- unbounded retry、memory/WAL/disk上限超過、secret露出
- R2障害から復旧後に、source errorとgapの境界が説明できない

## Post-soak verification

soak hostのcacheを再利用せず、別hostまたは空のcacheとread-only credentialだけで、
raw/replay manifest、全参照sealed WAL object、campaign/replay/part chain、Parquet schema・
hash・row chain、HTTP fetch planを確認します。実際のselectorはrun recordに記録した digest
から解決し、任意remote keyを直接指定しません。

利用可能なCLIの例:

```text
tick-verify day --config <read-only-config> --manifest <raw-manifest-digest-or-key>
tick-verify campaign --config <read-only-config> --dataset <dataset> --campaign <campaign> --through-root <root>
tick-verify replay-day --config <read-only-config> --manifest <replay-manifest-digest-or-key>
tickctl fetch --config <read-only-config> --manifest <manifest-digest-or-key> --output <external-output-root>
```

実行後に`delivery_status`、fault event、observed recovery、artifact digest、保存期限を
summaryへ記録します。受入条件の一つでも欠ける場合は`delivery_status: incomplete`です。
