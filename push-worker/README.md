# push-worker

Push 配信用 Worker Lambda サービス。Worker SQS から chunk を受け取り、Push 通知または notice_only の結果を Aggregator SQS へ送信する。

## Handler

```text
app.handler
```

## Environment Variables

| 変数                   | 内容                                  |
| ---------------------- | ------------------------------------- |
| `APP_ENV`              | 環境名                                |
| `PUSH_APPLICATION_ID`  | AWS End User Messaging application ID |
| `AGGREGATOR_QUEUE_URL` | Aggregator SQS URL                    |
