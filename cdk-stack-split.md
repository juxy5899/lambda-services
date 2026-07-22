# CDK Stack Split

## Stack 方針

CDK Stack は业务域単位で分割する。Lambda 业务代码由独立仓库构建 artifact，CDK Stack 负责 AWS 资源、权限、事件源、环境变量和监控。

| Stack | 所属 Lambda | 主要 AWS 资源 |
| --- | --- | --- |
| `MediaProcessingStack` | `media-event-processor` | Media Event SQS/DLQ、EventBridge 规则、Media Processor Lambda、MediaConvert 权限、S3 权限、Aurora Secret 读取权限 |
| `PushProcessingStack` | `push-worker`、`push-aggregator`、`execution-sweeper` | Worker SQS/DLQ、Aggregator SQS/DLQ、Worker Lambda、Aggregator Lambda、Sweeper Schedule、CloudWatch Alarm |
| `ActionLogBatchStack` | `action-log-batch-controller` | EventBridge Schedule、Batch Controller Lambda、Athena 权限、Raw/Intermediate/Delivery S3 权限、Aurora Secret 读取权限、CloudWatch Alarm |

## Artifact 接続

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
new lambda.Function(this, 'Function', {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: 'app.handler',
  code: lambda.Code.fromBucket(artifactBucket, artifact.objectKey),
  timeout: cdk.Duration.seconds(timeoutSec),
  memorySize: memoryMiB,
  environment,
});
```

## Deploy 単位

Lambda artifact のみ更新する場合は、対象 Lambda を含む Stack だけを deploy する。

```bash
npx cdk deploy MTI-dev-MediaProcessingStack -c env=dev
npx cdk deploy MTI-dev-PushProcessingStack -c env=dev
npx cdk deploy MTI-dev-ActionLogBatchStack -c env=dev
```

VPC、Aurora、S3、CloudFront など共有基盤を変更する場合は、該当する基盤 Stack を deploy する。
