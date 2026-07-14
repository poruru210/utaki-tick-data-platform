# Protocol V1のfixture形式

**Golden fixture**：Protocol V1のbytes、canonical JSON、hash、decode結果を固定する合成入力です。

fixtureは実際のtick data、credential、broker接続に依存しません。

## Fixture index

fixture indexは`testdata/tickdata/golden/index.json`に置きます。

indexのtop-levelは`fixture_version`と`fixtures`を持ちます。

fixturesの各要素は`fixture_id`、`path`、`kind`、`expected_result`を持ちます。

`kind`は`valid_frame`、`invalid_frame`、`stateful_scenario`、`canonical_json`、`wal_entry`、`manifest`のいずれかです。

`expected_result`は`accepted`または`rejected`です。

## Fixture file

fixture fileはJSON objectです。

valid frame fixtureは`wire_hex`、`decoded_message_type`、`expected_hashes`、`expected_result`を持ちます。

manifest fixtureは`canonical_json`、`manifest_sha256`、`expected_result`を持ちます。

raw-day manifest fixtureはrevision、raw_set_root、object range、canonical JSONのstrict decode結果を同時に固定します。

invalid frame fixtureは、変異前の`base_fixture_id`、変異方法、`expected_error_code`を持ちます。
`mutation`は`type`と必要な引数を持つobjectです。
`set_u16`と`set_u32`はwire offsetの値を置換し、`xor`は指定offsetの1 byteを反転し、`truncate`は指定長へ切り詰めます。
duplicate identityのようにframe自体はvalidなcaseでは、`stateful_scenario`と`expected_ack_status`で受理後の結果を固定します。

WAL fixtureは、file header、entry、commit marker、trailerのhex bytesと`expected_hashes`を持ちます。

## 必須case

indexにはHello、Resume、Batch、Ack、Error、WAL entry、raw-day manifest、replay-day manifestを登録します。

異常系にはtruncated frame、CRC mutation、unknown version、unknown message、oversized frameを登録します。

stateful scenarioにはduplicate identity、ACK loss、WAL recovery、dense boundaryを登録できます。

Batch fixtureには少なくとも一つのRawMqlTickV1を含めます。

## 変更規則

fixtureのbytes、canonical JSON、hash、expected error codeはProtocol V1の契約です。

fixtureの変更は仕様変更として扱い、wire、message、schema、hash、manifest、conformanceの影響を確認します。

fixtureのbytesとhashを、仕様変更の承認なしに書き換えません。

raw-day manifestのfixture変更時は、raw_set_rootのelement order、object keyの除外、revision chainの規則をGoとPythonで同時に検証します。
