# media-event-processor

媒体上传处理用 Lambda 服务。SQS から媒体处理イベントと MediaConvert 状态イベントを受け取り、S3、MediaConvert、Aurora 更新を制御する。

## Handler

```text
app.handler
```

## Environment Variables

| 変数                    | 内容                      |
| ----------------------- | ------------------------- |
| `APP_ENV`               | 环境名                    |
| `DB_SECRET_ARN`         | Aurora 接続 Secret ARN    |
| `VIDEO_BUCKET_NAME`     | 媒体 S3 bucket            |
| `VIDEO_UPLOAD_PREFIX`   | 上传 prefix               |
| `MEDIA_OUTPUT_PREFIX`   | 发布 / 输出 prefix        |
| `MEDIACONVERT_ROLE_ARN` | MediaConvert job role ARN |
