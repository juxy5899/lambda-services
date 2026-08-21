# timeout-monitor

プッシュ通知配信のタイムアウト監視 Lambda。EventBridge Scheduler から 5 分間隔で起動され、以下 3 ハンドラを順に実行する。各ハンドラは独立したトランザクション境界を持ち、一方が失敗しても他方を実行する。

1. CSV タスクタイムアウトハンドラ：`csv_status=0` のまま閾値を超えたタスクを `csv_status=2`（failed）へ更新する。
2. 配信実行タイムアウトハンドラ：`result_status=0` かつ `updated_at` が閾値を超えて更新されない実行を `result_status=2`（error）へ更新する。
3. 繰り返し終了ハンドラ：表示終了日時を過ぎた繰り返し通知のスケジュールを冪等に削除し、`send_status=3`（completed）へ更新する。

## Handler

```text
app.handler
```

## Environment Variables

| 変数                             | 内容                                                          |
| -------------------------------- | ------------------------------------------------------------- |
| `APP_ENV`                        | 環境名                                                        |
| `DB_SECRET_ARN`                  | Aurora 接続 Secret ARN                                        |
| `CSV_TASK_TIMEOUT_MINUTES`       | CSV 取込タスクのタイムアウト閾値（分）                        |
| `EXECUTION_TIMEOUT_MINUTES`      | 正式配信実行（manual / scheduler）のタイムアウト閾値（分）    |
| `TEST_EXECUTION_TIMEOUT_MINUTES` | テスト配信実行のタイムアウト閾値（分）                        |
| `RECURRING_DELETE_ALERT_MINUTES` | スケジュール削除失敗が解消しない場合に監視通知を発報する閾値（分） |
| `SCHEDULER_GROUP_NAME`           | 予約・繰り返しスケジュールの EventBridge Scheduler グループ名 |

## 仕様メモ

- 配信実行を error にする際、即時・予約送信では親通知を `send_status=4`（error）にする。繰り返し送信ではスケジュールを `DISABLED` へ更新してから親通知を error にする。
- テスト送信の滞留では実行のみ error 終了させ、親通知の状態は変更しない（内容編集と再テストを可能にする）。
- スケジュール削除は `ResourceNotFoundException` を成功として扱い、次回起動で冪等に再試行する。閾値を超えて解消しない場合のみ例外を送出し、CloudWatch Alarm で通知する。
