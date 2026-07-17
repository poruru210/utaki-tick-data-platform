# Multi-scope operation

M4-5のoperator layerは、scopeごとに独立したGatewayとMQL Serviceを起動するための
secret-free inventory、collision検査、supervisor plan、aggregate healthを提供する。

## Isolation contract

各inventory entryは次の境界を持つ。

- archive scope identity: dataset、source、exact source symbol、publisher epoch
- process identity: Gateway instance ID、Gateway/MQL config path、loopback listen address
- writable roots: WAL、journal、outbox、receipt、lock
- credential prefix: scope-limited credentialを解決するための非secretなprefix

起動前に次の共有を拒否する。

- scope key（dataset/source/symbol）
- Gateway instance identityまたはpublisher identity
- credential prefix
- 同じportのlisten address（wildcard `0.0.0.0`/`::`を含む）
- 同一または入れ子のconfig、journal、WAL、outbox、receipt、lock path

この検査はfilesystemを作成せず、credential value、environment variable name、R2 endpointを
inventoryへ置かない。scopeのarchive identityは`archive-config-v1`と同じcanonical objectから
導出され、local process pathの変更でpublisher scope keyが変わることはない。

## Inventory and supervisor plan

`operations.ScopeInventory`は`m4-scope-inventory-v1`のcanonical JSONを読み込む。unknown field、
不正なscope config、空inventory、implementation bound超過、canonicalでないbyte列を拒否する。
`BuildSupervisorPlan`は入力順によらずscope key順にservice unitを並べ、各scopeに一つのGateway unitと
一つのMQL unitを割り当てる。

実際のWindows serviceまたはoperator scriptはこのplanをservice managerの起動単位へ変換する。
service managerは各MQL Serviceを対応するGatewayのloopback listenerへ接続し、一つのMQL Serviceへ
複数scopeのsymbolを混在させない。service managerの実装とsecret injectionはこのrepositoryの
operator layerの責務ではない。

## Aggregate health

`AggregateScopeHealth`はscope keyで安定ソートしたstatusを返す。各statusはlast durable source time、
current source time、uncommitted lag、Gateway downtime、WAL free space、terminal synchronization、
oldest retrievable tick、publisher epoch、last verified snapshot digest、blocked reasonを持つ。
同一scopeの重複またはidentity欠落は集約を拒否する。

aggregate statusは観測結果を表示するだけであり、別scopeのACK、WAL、journal、lock、publisher claimを
操作しない。一つのscopeのdisk pressure、R2 outage、publisher failureは、そのscopeのstatusへ
反映し、他scopeのprocessまたはACK pathを共有しない構成を前提とする。
