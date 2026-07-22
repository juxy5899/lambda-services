# asahimyapp-lambda

本目录定义 asahimyapp-lambda 的 Lambda 业务代码结构。每个子目录作为一个服务模块进行管理。

## 服务清单

| 服务 | Lambda handler | 主要职责 | 推荐 CDK Stack |
| --- | --- | --- | --- |
| `media-event-processor` | `app.handler` | 媒体上传处理、S3 发布、MediaConvert 作业提交、MediaConvert 回调处理 | `MediaProcessingStack` |
| `push-worker` | `app.handler` | Worker SQS 消费、Push 发送、notice_only 处理、Aggregator SQS 结果发送 | `PushProcessingStack` |
| `push-aggregator` | `app.handler` | Worker 结果聚合、无效端末更新、配信结果更新 | `PushProcessingStack` |
| `execution-sweeper` | `app.handler` | 滞留 execution 检测、error / partial_success 收束 | `PushProcessingStack` |
| `action-log-batch-controller` | `app.handler` | Athena Task A/B、MySQL 反映、Delivery TSV 生成、完整性确认 | `ActionLogBatchStack` |

## Python 工程方式

各服务使用 `pyproject.toml`。该方式适合独立仓库，依赖、测试、构建元数据可以统一管理。Lambda 部署包中保留根目录 `app.py` 作为入口，业务逻辑位于 `src/<package_name>/`。

推荐构建产物结构如下。

```text
package.zip
  app.py
  <package_name>/
  dependencies...
```

CDK 仅引用构建产物，不直接保存业务代码。

```ts
new lambda.Function(this, 'Function', {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: 'app.handler',
  code: lambda.Code.fromBucket(artifactBucket, artifactKey),
});
```

## 本地测试

各服务目录内执行以下命令。

```bash
python -m venv .venv
python -m pip install -e .[dev]
python -m pytest
```
