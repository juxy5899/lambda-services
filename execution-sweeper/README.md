# execution-sweeper

滞留 execution を定期検知する Lambda 服务。EventBridge Schedule から起動し、running のまま残った配信実行を error または partial_success に収束させる。

## Handler

```text
app.handler
```

## Environment Variables

| 変数 | 内容 |
| --- | --- |
| `APP_ENV` | 环境名 |
| `MYSQL_SECRET_ID` | MySQL 接続 Secret ID |
| `STALE_EXECUTION_MINUTES` | 滞留判定分数 |
