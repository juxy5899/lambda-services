# push-aggregator

プッシュ通知配信の Aggregator Lambda。Aggregator SQS から Worker の送信結果を受信し、配信実行単位に集約して `t_delivery_execution` へ反映する。無効端末の `t_user_device.is_valid` 更新も担当する。

## Handler

```text
app.handler
```

## Environment Variables

| 変数            | 内容                   |
| --------------- | ---------------------- |
| `APP_ENV`       | 環境名                 |
| `DB_SECRET_ARN` | Aurora 接続 Secret ARN |

## 仕様メモ

- 予約済み同時実行数は 1 に固定し、`t_delivery_execution` の同一行更新を直列化する。
- 受信 Batch 全体を 1 トランザクションで処理する。チャンク結果の `INSERT IGNORE` と実行行への加算を同一トランザクションに含めることで、再配送時の二重加算と途中失敗時の件数欠落を防ぐ。
- 完了条件は `dispatch_completed=1` かつ `processed_chunk_count=expected_chunk_count` かつ `success+fail+skipped=total_count`。
- 正式配信は個別端末の失敗があっても `result_status=1`（success）とし、即時・予約送信では親通知を `send_status=3`（completed）へ更新する。繰り返し送信は `send_status=1`（waiting）を維持する。
- テスト送信は `total_count>0` かつ `success_count=total_count` の場合のみ成功とし、実行時の `content_version` が一致する場合のみテスト成功バージョンを更新する。
- 既に終了済みの実行（`result_status<>0`）は更新対象外とし、遅れて到着した結果で上書きしない。
