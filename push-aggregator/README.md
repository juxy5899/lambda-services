# push-aggregator

Push 配信結果集約 Lambda 服务。Aggregator SQS の batch を execution 単位で合算し、MySQL 更新と無効端末更新を行う。

## Handler

```text
app.handler
```

## Environment Variables

| 変数 | 内容 |
| --- | --- |
| `APP_ENV` | 环境名 |
| `MYSQL_SECRET_ID` | MySQL 接続 Secret ID |
