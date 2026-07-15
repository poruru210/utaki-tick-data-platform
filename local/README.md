# ローカル設定の扱い

`local/`には、開発者の環境でGatewayやproducerを起動するための設定例を置きます。

## 設定の作成

設定例をコピーして、ローカル専用の値を設定します。

```powershell
Copy-Item local/tick-gateway.toml.example local/tick-gateway.toml
Copy-Item local/tick-reader.toml.example local/tick-reader.toml
```

実際のファイル名は、リポジトリに存在する`*.toml.example`に合わせます。

## 秘密情報

API token、R2 credential、アカウント情報、接続先の秘密値は、設定ファイルに直接コミットしません。

秘密情報は環境変数またはローカルのsecret storeから注入します。

実際のtickデータ、WAL、SQLite、Parquet、R2用のruntime TOMLもコミットしません。

## 実行前の確認

設定のschema、source schema、保存先、ログ出力先を確認してからサービスを起動します。

MT5 producerを実行するときは、MetaTrader 5の端末、対象symbol、localhost TCPの接続先が同じ環境を指すことを確認します。

設定例を変更した場合は、対応する文書とfixtureの前提も更新します。

## read-only ArchiveReader

`tick-reader.toml`は`reader_config_version = "tick-reader-v1"`のstrict設定として扱い、未知のキーを拒否します。

`endpoint`、`bucket_env`、`access_key_env`、`secret_key_env`、`region`、`immutable_root`、`cache_root`、サイズ上限を明示します。

`endpoint`は必須のHTTP(S) URLであり、R2のreaderは`region = "auto"`を使用します。

bucket名とcredential値は設定ファイルへ書かず、設定で指定した環境変数から実行時だけ読み取ります。

`tickctl`と`tick-verify`はimmutableなscope descriptor、raw manifest、sealed WALだけを読み取り、書き込み用credentialを要求しません。

readerのcacheは検証済みSHA-256とcanonical basenameから導出し、remote keyのpath componentをそのままローカルへ使用しません。
