# push-worker

プッシュ通知配信の Worker Lambda。Worker SQS から配信チャンク（`send_type=0`）を受信し、AWS End User Messaging へ Push 送信したうえで、チャンク単位の結果と無効端末 ID を Aggregator SQS へ送信する。

## Handler

```text
app.handler
```

## Environment Variables

| 変数                   | 内容                                                 |
| ---------------------- | ---------------------------------------------------- |
| `APP_ENV`              | 環境名                                               |
| `PUSH_APPLICATION_ID`  | End User Messaging の配信アプリケーション ID         |
| `AGGREGATOR_QUEUE_URL` | 送信結果の集約先 Aggregator SQS の URL               |
| `SEND_BATCH_SIZE`      | 1 回の SendMessages で送信する最大エンドポイント数    |

## 仕様メモ

- Push のカスタムデータには開封ログ帰属のため `notification_id` と `execution_id` を必ず含める。
- `TokenInvalid` / `DeviceUnregistered` 等の端末無効エラーは `invalid_endpoint_ids` として Aggregator へ報告する。無効端末の DB 更新は Aggregator Lambda が行う。
- End User Messaging 呼び出しは指数バックオフで最大 3 回まで再試行し、上限超過時は当該メッセージのみ `batchItemFailures` として再配送させる（Push は at-least-once）。
- 端末トークン、Endpoint ID、MypageID はログに出力しない。
