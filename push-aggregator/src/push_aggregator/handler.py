import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SYSTEM_USER = "system:push-aggregator"

# 配信実行結果ステータス（t_delivery_execution.result_status）
RESULT_STATUS_RUNNING = 0
RESULT_STATUS_SUCCESS = 1
RESULT_STATUS_ERROR = 2

# 起動種別（t_delivery_execution.triggered_by）
TRIGGERED_BY_TEST = 2

# 通知種別（t_delivery_execution.notification_type）
NOTIFICATION_TYPE_IMMEDIATE = 0
NOTIFICATION_TYPE_SCHEDULED = 1

# 通知の送信ステータス（t_notification.send_status）
SEND_STATUS_RUNNING = 2
SEND_STATUS_COMPLETED = 3

# t_delivery_execution.error_summary の桁数上限
ERROR_SUMMARY_MAX_LENGTH = 1000

# 無効端末 Batch 更新の 1 文の最大件数
INVALID_ENDPOINT_UPDATE_BATCH_SIZE = 500

_secrets_client = None
_db_secret: dict[str, Any] | None = None


def handle_event(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Aggregator SQS から受信した Worker 結果を配信実行単位で集約し、MySQL へ反映する。

    受信 Batch 全体を 1 トランザクションで処理する。チャンク結果の登録と実行行への加算を
    同一トランザクションに含めることで、途中失敗時の再配送でも二重加算・件数欠落が発生しない。
    """
    records = event.get("Records") or []
    if not records:
        return {"applied_chunk_count": 0, "completed_execution_ids": []}

    results = [_parse_sqs_record(record) for record in records]

    connection = _connect_db()
    try:
        with connection:
            with connection.cursor() as cursor:
                applied = _insert_chunk_results(cursor, results)
                completed_execution_ids = _apply_execution_totals(cursor, applied)
                _invalidate_endpoints(cursor, results)
            connection.commit()
    except Exception:
        # Batch 全体を再配送させ、再試行上限超過時は DLQ と CloudWatch Alarm で検知する
        logger.exception("push aggregation failed", extra={"record_count": len(records)})
        raise

    logger.info(
        "push aggregation applied",
        extra={
            "record_count": len(records),
            "applied_chunk_count": len(applied),
            "completed_execution_count": len(completed_execution_ids),
        },
    )

    return {
        "applied_chunk_count": len(applied),
        "completed_execution_ids": completed_execution_ids,
    }


def _parse_sqs_record(record: dict[str, Any]) -> dict[str, Any]:
    """SQS record body を Worker 結果の JSON として取得する。"""
    body = record.get("body")
    if not isinstance(body, str):
        raise ValueError("SQS record body must be a string")

    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(f"failed to parse SQS record body: {error}") from error


def _insert_chunk_results(cursor: Any, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """チャンク結果を INSERT IGNORE し、新規登録できたチャンクだけを返す。

    (execution_id, chunk_id) の一意制約により、SQS 再配送時の二重加算を排除する。
    """
    applied: list[dict[str, Any]] = []

    for result in results:
        execution_id = _required_string(result.get("execution_id"), "execution_id")
        chunk_id = _required_string(str(result.get("chunk_id") or ""), "chunk_id")
        success_count = _int_value(result.get("success_count"))
        fail_count = _int_value(result.get("fail_count"))
        skipped_count = _int_value(result.get("skipped_count"))

        cursor.execute(
            """
            INSERT IGNORE INTO t_delivery_chunk_result
                (execution_id, chunk_id, success_count, fail_count, skipped_count, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (execution_id, chunk_id, success_count, fail_count, skipped_count),
        )

        if cursor.rowcount == 1:
            applied.append(
                {
                    "execution_id": execution_id,
                    "chunk_id": chunk_id,
                    "success_count": success_count,
                    "fail_count": fail_count,
                    "skipped_count": skipped_count,
                    "error_samples": result.get("error_samples") or [],
                }
            )
        else:
            # 既登録チャンクは SQS 再配送とみなし、集計対象から除外する
            logger.info(
                "duplicated chunk result ignored",
                extra={"execution_id": execution_id, "chunk_id": chunk_id},
            )

    return applied


def _apply_execution_totals(cursor: Any, applied: list[dict[str, Any]]) -> list[str]:
    """新規チャンクを実行単位に合算し、t_delivery_execution へ 1 回の UPDATE で反映する。"""
    totals: dict[str, dict[str, Any]] = {}

    for chunk in applied:
        total = totals.setdefault(
            chunk["execution_id"],
            {
                "success_count": 0,
                "fail_count": 0,
                "skipped_count": 0,
                "processed_chunk_count": 0,
                "error_samples": [],
            },
        )
        total["success_count"] += chunk["success_count"]
        total["fail_count"] += chunk["fail_count"]
        total["skipped_count"] += chunk["skipped_count"]
        total["processed_chunk_count"] += 1
        for sample in chunk["error_samples"]:
            if isinstance(sample, str) and sample and sample not in total["error_samples"]:
                total["error_samples"].append(sample)

    completed_execution_ids: list[str] = []

    for execution_id, total in totals.items():
        cursor.execute(
            """
            UPDATE t_delivery_execution
            SET success_count = success_count + %s
              , fail_count = fail_count + %s
              , skipped_count = skipped_count + %s
              , processed_chunk_count = processed_chunk_count + %s
              , updated_at = NOW()
            WHERE id = %s
              AND result_status = %s
            """,
            (
                total["success_count"],
                total["fail_count"],
                total["skipped_count"],
                total["processed_chunk_count"],
                execution_id,
                RESULT_STATUS_RUNNING,
            ),
        )

        if cursor.rowcount == 0:
            # タイムアウトハンドラ等で終了済みの実行は、遅れて届いた結果で上書きしない
            logger.info(
                "execution already finished, aggregation skipped",
                extra={"execution_id": execution_id},
            )
            continue

        if _finalize_execution_if_completed(cursor, execution_id, total["error_samples"]):
            completed_execution_ids.append(execution_id)

    return completed_execution_ids


def _finalize_execution_if_completed(
    cursor: Any,
    execution_id: str,
    error_samples: list[str],
) -> bool:
    """完了条件を満たした実行を終了状態へ遷移させる。"""
    cursor.execute(
        """
        SELECT notification_id
             , notification_type
             , triggered_by
             , content_version
             , total_count
             , success_count
             , fail_count
             , skipped_count
             , dispatch_completed
             , expected_chunk_count
             , processed_chunk_count
        FROM t_delivery_execution
        WHERE id = %s
          AND result_status = %s
        FOR UPDATE
        """,
        (execution_id, RESULT_STATUS_RUNNING),
    )
    execution = cursor.fetchone()
    if execution is None:
        return False

    counted = (
        int(execution["success_count"])
        + int(execution["fail_count"])
        + int(execution["skipped_count"])
    )
    is_completed = (
        int(execution["dispatch_completed"]) == 1
        and int(execution["processed_chunk_count"]) == int(execution["expected_chunk_count"])
        and counted == int(execution["total_count"])
    )
    if not is_completed:
        return False

    triggered_by = int(execution["triggered_by"])
    total_count = int(execution["total_count"])
    success_count = int(execution["success_count"])

    if triggered_by == TRIGGERED_BY_TEST:
        # テスト送信は全件成功のみを技術的成功とする
        result_status = (
            RESULT_STATUS_SUCCESS
            if total_count > 0 and success_count == total_count
            else RESULT_STATUS_ERROR
        )
    else:
        # 正式配信は個別端末の失敗があっても配信処理自体は完了とみなす
        result_status = RESULT_STATUS_SUCCESS

    cursor.execute(
        """
        UPDATE t_delivery_execution
        SET result_status = %s
          , finished_at = NOW()
          , updated_at = NOW()
          , error_summary = COALESCE(error_summary, %s)
        WHERE id = %s
          AND result_status = %s
        """,
        (
            result_status,
            _build_error_summary(error_samples),
            execution_id,
            RESULT_STATUS_RUNNING,
        ),
    )
    if cursor.rowcount == 0:
        return False

    _update_notification_state(cursor, execution, result_status)

    logger.info(
        "execution finalized",
        extra={
            "execution_id": execution_id,
            "notification_id": execution["notification_id"],
            "result_status": result_status,
            "success_count": success_count,
            "total_count": total_count,
        },
    )

    return True


def _update_notification_state(cursor: Any, execution: dict[str, Any], result_status: int) -> None:
    """実行結果に応じて親通知の状態を更新する。"""
    notification_id = execution["notification_id"]
    triggered_by = int(execution["triggered_by"])
    notification_type = int(execution["notification_type"])

    if triggered_by == TRIGGERED_BY_TEST:
        if result_status != RESULT_STATUS_SUCCESS:
            # 失敗時はテスト成功バージョンを更新せず、内容編集と再テストを可能にする
            return

        # 実行時に固定した内容バージョンと現在値が一致する場合のみテスト済みとする
        cursor.execute(
            """
            UPDATE t_notification
            SET last_test_success_version = %s
              , last_test_success_at = NOW()
              , updated_at = NOW()
              , updated_by = %s
            WHERE id = %s
              AND content_version = %s
              AND is_deleted = 0
            """,
            (
                execution["content_version"],
                SYSTEM_USER,
                notification_id,
                execution["content_version"],
            ),
        )
        return

    # 繰り返し送信はスケジュール有効状態を示す waiting を維持する
    if notification_type not in (NOTIFICATION_TYPE_IMMEDIATE, NOTIFICATION_TYPE_SCHEDULED):
        return

    if result_status != RESULT_STATUS_SUCCESS:
        return

    cursor.execute(
        """
        UPDATE t_notification
        SET send_status = %s
          , updated_at = NOW()
          , updated_by = %s
        WHERE id = %s
          AND send_status = %s
          AND is_deleted = 0
        """,
        (SEND_STATUS_COMPLETED, SYSTEM_USER, notification_id, SEND_STATUS_RUNNING),
    )


def _invalidate_endpoints(cursor: Any, results: list[dict[str, Any]]) -> None:
    """受信 Batch 内の無効端末 ID を重複排除し、t_user_device を Batch 更新する。"""
    endpoint_ids: set[str] = set()

    for result in results:
        for endpoint_id in result.get("invalid_endpoint_ids") or []:
            if isinstance(endpoint_id, str) and endpoint_id:
                endpoint_ids.add(endpoint_id)

    if not endpoint_ids:
        return

    ordered = sorted(endpoint_ids)
    for index in range(0, len(ordered), INVALID_ENDPOINT_UPDATE_BATCH_SIZE):
        batch = ordered[index : index + INVALID_ENDPOINT_UPDATE_BATCH_SIZE]
        placeholders = ", ".join(["%s"] * len(batch))
        cursor.execute(
            f"""
            UPDATE t_user_device
            SET is_valid = 0
              , updated_at = NOW()
            WHERE endpoint_id IN ({placeholders})
              AND is_valid = 1
            """,
            tuple(batch),
        )

    logger.info("invalid endpoints updated", extra={"invalid_endpoint_count": len(ordered)})


def _build_error_summary(error_samples: list[str]) -> str | None:
    """エラーサンプルから error_summary を組み立てる。"""
    if not error_samples:
        return None

    return " | ".join(error_samples)[:ERROR_SUMMARY_MAX_LENGTH]


def _connect_db() -> Any:
    """Secrets Manager の接続情報で Aurora MySQL に接続する。"""
    import pymysql

    secret = _load_db_secret()
    return pymysql.connect(
        host=_required_string(secret.get("host"), "secret.host"),
        user=_required_string(secret.get("username"), "secret.username"),
        password=_required_string(secret.get("password"), "secret.password"),
        database=_required_string(secret.get("dbname"), "secret.dbname"),
        port=int(secret.get("port") or 3306),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def _load_db_secret() -> dict[str, Any]:
    """Aurora 接続 Secret を読み込み、実行環境内でキャッシュする。"""
    global _db_secret

    if _db_secret is None:
        response = _secretsmanager().get_secret_value(SecretId=_required_env("DB_SECRET_ARN"))
        secret_string = _required_string(response.get("SecretString"), "SecretString")
        _db_secret = json.loads(secret_string)

    return _db_secret


def _secretsmanager() -> Any:
    """Secrets Manager client を遅延生成して再利用する。"""
    global _secrets_client

    if _secrets_client is None:
        import boto3

        _secrets_client = boto3.client("secretsmanager")

    return _secrets_client


def _required_env(name: str) -> str:
    """必須の環境変数を取得する。"""
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"environment variable is required: {name}")

    return value


def _required_string(value: Any, name: str) -> str:
    """必須の文字列項目を取得する。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"field is required: {name}")

    return value


def _int_value(value: Any) -> int:
    """件数項目を整数として取得する。未設定時は 0 とする。"""
    if value is None:
        return 0

    return int(value)
