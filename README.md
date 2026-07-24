# asahimyapp-lambda

本ディレクトリは asahimyapp-lambda の Lambda 業務コード構造を定義する。各サブディレクトリをサービスモジュールとして管理する。

## サービス一覧

| サービス                      | Lambda handler | 主要責務                                                                                  | 推奨 CDK Stack         |
| ----------------------------- | -------------- | ----------------------------------------------------------------------------------------- | ---------------------- |
| `media-event-processor`       | `app.handler`  | メディアアップロード処理・S3 公開・MediaConvert ジョブ送信・MediaConvert コールバック処理 | `MediaProcessingStack` |
| `push-worker`                 | `app.handler`  | Worker SQS 消費・Push 送信・notice_only 処理・Aggregator SQS 結果送信                     | `PushProcessingStack`  |
| `push-aggregator`             | `app.handler`  | Worker 結果集約・無効端末更新・配信結果更新                                               | `PushProcessingStack`  |
| `execution-sweeper`           | `app.handler`  | 滞留 execution 検知・error / partial_success 収束                                         | `PushProcessingStack`  |
| `action-log-batch-controller` | `app.handler`  | Athena Task A/B・MySQL 反映・Delivery TSV 生成・整合性確認                                | `ActionLogBatchStack`  |

## Python プロジェクト方式

各サービスは `pyproject.toml` を使用する。この方式は独立リポジトリに適しており、依存関係・テスト・ビルドメタデータを一元管理できる。Lambda デプロイパッケージにはルートの `app.py` をエントリポイントとして保持し、業務ロジックは `src/<package_name>/` に配置する。

推奨するビルド成果物の構造は以下の通り。

```text
package.zip
  app.py
  <package_name>/
  dependencies...
```

CDK はビルド成果物のみを参照し、業務コードを直接保持しない。

```ts
new lambda.Function(this, "Function", {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: "app.handler",
  code: lambda.Code.fromBucket(artifactBucket, artifactKey),
});
```

## ローカルテスト

各サービスディレクトリ内で以下のコマンドを実行する。

```bash
python -m venv .venv
python -m pip install -e .[dev]
python -m pytest
```

## CDK Stack 構成

### Stack 方針

CDK Stack は業務ドメイン単位で分割する。Lambda 業務コードは独立リポジトリでビルドした artifact を使用し、CDK Stack は AWS リソース・権限・イベントソース・環境変数・監視を担当する。

| Stack                  | 所属 Lambda                                           | 主要 AWS リソース                                                                                                                           |
| ---------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `MediaProcessingStack` | `media-event-processor`                               | Media Event SQS/DLQ・EventBridge ルール・Media Processor Lambda・MediaConvert 権限・S3 権限・Aurora Secret 読み取り権限                     |
| `PushProcessingStack`  | `push-worker`、`push-aggregator`、`execution-sweeper` | Worker SQS/DLQ、Aggregator SQS/DLQ、Worker Lambda、Aggregator Lambda、Sweeper Schedule、CloudWatch Alarm                                    |
| `ActionLogBatchStack`  | `action-log-batch-controller`                         | EventBridge Schedule・Batch Controller Lambda・Athena 権限・Raw/Intermediate/Delivery S3 権限・Aurora Secret 読み取り権限・CloudWatch Alarm |

### Artifact 接続

各 Stack は Lambda artifact bucket と artifact key を設定値として受け取る。

```ts
export interface LambdaArtifactConfig {
  bucketName: string;
  objectKey: string;
  objectVersion?: string;
}
```

CDK 側の Lambda 定義は以下を基本形とする。

```ts
new lambda.Function(this, "Function", {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: "app.handler",
  code: lambda.Code.fromBucket(artifactBucket, artifact.objectKey),
  timeout: cdk.Duration.seconds(timeoutSec),
  memorySize: memoryMiB,
  environment,
});
```

### Deploy 単位

Lambda artifact のみ更新する場合は、対象 Lambda を含む Stack だけを deploy する。

```bash
npx cdk deploy MTI-dev-MediaProcessingStack -c env=dev
npx cdk deploy MTI-dev-PushProcessingStack -c env=dev
npx cdk deploy MTI-dev-ActionLogBatchStack -c env=dev
```

VPC、Aurora、S3、CloudFront など共有基盤を変更する場合は、該当する基盤 Stack を deploy する。
