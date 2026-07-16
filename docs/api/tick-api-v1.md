# `tick-api` read-only HTTP API V1

M4-1で固定するHTTP表現です。M4-6の実装は`internal/delivery.ArchiveReaderV1`を唯一の
selector/fetch-plan authorityとして、この契約へ写像します。

設定はstrict TOMLで、`api_config_version = "tick-api-v1"`を必須とします。readerのremote
inventoryもbounded capabilityで読み、APIのmanifest-node limitを超える一覧はfail closedします。

## Boundary

API processはR2 read credentialだけを持ち、write、delete、publisher claim作成、handover、
prune、任意key GET/list、raw WALまたはParquet bodyのproxyを持ちません。default bindは
loopbackです。non-loopback bindは、実体のあるauthentication、rate limit、trusted proxy、
短期credential policy hookをすべて注入したdeployment profileなしには起動できません。

全responseは次のversion fieldを持ちます。

```json
{"api_version":"tick-api-v1","items":[],"next_cursor":null}
```

`items`は`ArchiveReaderV1`の返すimmutable descriptorと同じ意味を持ち、digestはlowercase
hex、sizeとrevisionはJSON integer、配列はcanonical stable orderです。HTTP JSONのbyte-level
canonical一致は要求しません。

## Endpoints

| Method | Path | Reader method | Required query |
| --- | --- | --- | --- |
| GET | `/v1/datasets` | `ListDatasets` | none |
| GET | `/v1/datasets/{dataset}/campaigns` | `ListCampaigns` | path `dataset` |
| GET | `/v1/snapshots/raw` | `ListRawSnapshots` | `dataset`, `campaign`, optional `date` |
| GET | `/v1/snapshots/replay` | `ListReplaySnapshots` | `dataset`, `campaign`, `date`, `stream`, `conversion`, optional `day_definition` |
| GET | `/v1/manifests/{sha256}` | strict manifest selector | lowercase SHA-256 path parameter |
| POST | `/v1/fetch-plans` | `Resolve*Snapshot`, `Build*FetchPlan` | bounded JSON body with `kind` and typed selector |
| GET | `/v1/health` | bounded operator health | none |

fetch-plan requestは次のいずれかだけを受け付けます。unknown field、任意key、unbounded
arrayは拒否します。

```json
{"kind":"raw","selector":{"manifest":"<lowercase SHA-256 digest>"}}
{"kind":"replay","selector":{"manifest":"<lowercase SHA-256 digest>"}}
```

fetch-plan responseはlarge object bodyではなく、readerが返したmanifest key、digest、size、
object key、object digest、object size、cache identityだけを返します。HTTP callerはこの
identityを用いて別の許可されたfetch経路を選び、APIへ任意keyを渡しません。

## Status and error

* `200`: valid request and bounded response
* `400`: missing/invalid query、unknown query、limit超過
* `404`: exact immutable selectorに一致するsnapshotがない
* `409`: scopeまたはrevisionがambiguous、branch、integrity conflict
* `413`: requestまたはresponseがconfigured byte/item limitを超えた
* `429`: `max_concurrent_requests`を超えた
* `502`: remote read unavailableまたはshort read
* `504`: `request_timeout_ms`を超えた

error responseも`api_version`を持ち、形は次の通りです。

```json
{"api_version":"tick-api-v1","error":{"code":"INVALID_QUERY","message":"..."}}
```

`message`へcredential、secret、local absolute pathを含めません。paginationを実装する場合、
cursorはopaqueなbounded tokenとし、`next_cursor=null`を終端にします。未実装endpoint、
unknown query、incomplete paginationはsuccessとして返しません。
