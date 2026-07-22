# action-log-batch-controller

行動ログ分析の日次 Batch Controller Lambda 服务。Athena Task A/B、MySQL 反映、Delivery gzip TSV 生成、完整性確認、清理処理を制御する。

## Handler

```text
app.handler
```

## Environment Variables

| 変数 | 内容 |
| --- | --- |
| `RAW_BUCKET` | Raw Bucket |
| `RAW_PREFIX` | Raw Prefix |
| `INTERMEDIATE_BUCKET` | 中間 Bucket |
| `INTERMEDIATE_PREFIX` | 中間 Prefix |
| `DELIVERY_BUCKET` | 提供 Bucket |
| `DELIVERY_PREFIX` | 提供 Prefix |
| `ATHENA_DATABASE` | Athena database |
| `ATHENA_WORKGROUP` | Athena workgroup |
| `MYSQL_SECRET_ID` | MySQL 接続 Secret ID |
