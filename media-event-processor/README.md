# media-event-processor

メディアアップロード処理用 Lambda サービス。SQS からメディア処理イベントと MediaConvert ステータスイベントを受け取り、S3・MediaConvert・Aurora の更新を制御する。

## Handler

```text
app.handler
```

## Environment Variables

| 変数                    | 内容                      |
| ----------------------- | ------------------------- |
| `APP_ENV`               | 環境名                    |
| `DB_SECRET_ARN`         | Aurora 接続 Secret ARN    |
| `VIDEO_BUCKET_NAME`     | メディア S3 バケット      |
| `VIDEO_UPLOAD_PREFIX`   | アップロード prefix       |
| `MEDIA_OUTPUT_PREFIX`   | 公開 / 出力 prefix        |
| `MEDIACONVERT_ROLE_ARN` | MediaConvert job role ARN |
