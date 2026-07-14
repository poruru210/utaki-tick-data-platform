# Hash domain

Hashは、対象データと計算範囲を明確に分けて定義します。

同じbytesでも、domainが異なるhashを別の意味に使いません。

## 定義

**`source_payload_fingerprint`**：producerが生成したsource payloadの正規bytesを識別するhashです。

**`observation_hash`**：Gatewayが観測したsource payloadと取得時刻などの観測情報を識別するhashです。

**`gateway_batch_sha256`**：Gatewayが受理したBatchの正規表現を識別するSHA-256です。

**`wal_entry_hash`**：WALへ記録したentryの正規表現を識別するhashです。

各hashの入力bytes、canonicalization、アルゴリズム、出力表現はProtocol V1の実装とfixtureで固定します。

## 境界

producerが計算するhashとGatewayが計算するhashを混同しません。

WAL entryのhashは、raw objectやreplay objectのhashの代わりには使いません。

manifestには、対象objectと対象hashのdomainを明示します。

## 変更規則

hashの入力範囲またはcanonicalizationを変更すると、既存fixtureとの互換性が失われます。

変更時は、schema、wire、manifest、golden fixture、conformance caseをまとめて更新します。
