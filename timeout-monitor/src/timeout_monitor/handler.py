import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SYSTEM_USER = "system:timeout-monitor"

# CSV 取込ステータス（t_csv_import_task.csv_status）
CSV_STATUS_PROCESSING = 0
CSV_STATUS_FAILED = 2

# 配信実行結果ステータス（t_delivery_execution.result_status）
RESULT_STATUS_RUNNING = 0
RESULT_STATUS_ERROR = 2

# 起動種別（t_delivery_execution.triggered_by）
TRIGGERED_BY_TEST = 2

# 通知種別（t_notification.notification_type）
NOTIFICATION_TYPE_IMMEDIATE = 0
NOTIFICATION_TYPE_SCHEDULED = 1
NOTIFICATION_TYPE_RECURRING = 2

# 通知の送信ステータス（t_notification.send_status）
SEND_STATUS_WAITING = 1
SEND_STATUS_RUNNING = 2
SEND_STATUS_COMPLETED = 3
SEND_STATUS_ERROR = 4

CSV_TIMEOUT_MESSAGE = "csv import task timed out"
EXECUTION_TIMEOUT_MESSAGE = "delivery execution timed out"

_secrets_client = None
_scheduler_client = None
_db_secret: dict[str, Any] | None = None


def handle_event(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """CSV 取込タスク、配信実行、繰り返し送信終了の 3 ハンドラを順に実行する。

    各ハンドラは独立したトランザクション境界を持ち、一方が失敗しても他方を実行する。
    失敗が残った場合は最後に例外を送出し、CloudWatch Alarm で運用担当へ通知する。
    """
    summary: dict[str, Any] = {
        "csv_task_failed_count": 0,
        "execution_timeout_count": 0,
        "recurring_completed_count": 0,
    }
    errors: list[str] = []

    connection = _connect_db()
    try:
        with connection:
            for name, handler in (
                ("csv_task_failed_count", _handle_csv_task_timeout),
                ("execution_timeout_count", _handle_execution_timeout),
                ("recurring_completed_count", _handle_recurring_end),
            ):
                try:
                    summary[name] = handler(connection)
                    connection.commit()
                except Exception as error:  # noqa: BLE001 - 1 ハンドラの失敗で他を止めない
                    connection.rollback()
                    logger.exception("timeout monitor handler failed", extra={"handler": name})
                    errors.append(f"{name}: {error}")
    finally:
        logger.info("timeout monitor finished", extra=summary)

    if errors:
        raise RuntimeError("; ".join(errors))

    return summary


def _handle_csv_task_timeout(connection: Any) -> int:
    """一定時間 processing のまま滞留した CSV 取込タスクを failed へ更新する。"""
    timeout_minutes = _positive_int_env("CSV_TASK_TIMEOUT_MINUTES", 30)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE t_csv_import_task
            SET csv_status = %s
              , error_message = COALESCE(error_message, %s)
              , updated_at = NOW()
            WHERE csv_status = %s
              AND created_at <= DATE_SUB(NOW(), INTERVAL %s MINUTE)
            """,
            (CSV_STATUS_FAILED, CSV_TIMEOUT_MESSAGE, CSV_STATUS_PROCESSING, timeout_minutes),
        )
        return cursor.rowcount


def _handle_execution_timeout(connection: Any) -> int:
    """進捗が停止した配信実行を error へ更新し、親通知とスケジュールを整合させる。"""
    execution_timeout_minutes = _positive_int_env("EXECUTION_TIMEOUT_MINUTES", 60)
    test_timeout_minutes = _positive_int_env("TEST_EXECUTION_TIMEOUT_MINUTES", 10)
    timeout_count = 0

    with connection.cursor() as cursor:
        # updated_at は Dispatch のチャンク投入と Aggregator の集計で更新されるため、進捗停止の判定に使用する
        cursor.execute(
            """
            SELECT id
                 , notification_id
                 , notification_type
                 , triggered_by
            FROM t_delivery_execution
            WHERE result_status = %s
              AND updated_at <= DATE_SUB(NOW(), INTERVAL IF(triggered_by = %s, %s, %s) MINUTE)
            """,
            (
                RESULT_STATUS_RUNNING,
                TRIGGERED_BY_TEST,
                test_timeout_minutes,
                execution_timeout_minutes,
            ),
        )
        executions = cursor.fetchall() or []

        for execution in executions:
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
                    RESULT_STATUS_ERROR,
                    EXECUTION_TIMEOUT_MESSAGE,
                    execution["id"],
                    RESULT_STATUS_RUNNING,
                ),
            )
            if cursor.rowcount == 0:
                continue

            timeout_count += 1
            logger.warning(
                "delivery execution timed out",
                extra={
                    "execution_id": execution["id"],
                    "notification_id": execution["notification_id"],
                    "triggered_by": int(execution["triggered_by"]),
                },
            )

            # テスト送信は実行のみ error 終了させ、内容編集と再テストを可能にする
            if int(execution["triggered_by"]) == TRIGGERED_BY_TEST:
                continue

            notification_type = int(execution["notification_type"])
            if notification_type == NOTIFICATION_TYPE_RECURRING:
                # 以降の自動実行を止めてから親通知を error にする
                _disable_recurring_schedule(execution["notification_id"])

            cursor.execute(
                """
                UPDATE t_notification
                SET send_status = %s
                  , updated_at = NOW()
                  , updated_by = %s
                WHERE id = %s
                  AND send_status IN (%s, %s)
                  AND is_deleted = 0
                """,
                (
                    SEND_STATUS_ERROR,
                    SYSTEM_USER,
                    execution["notification_id"],
                    SEND_STATUS_WAITING,
                    SEND_STATUS_RUNNING,
                ),
            )

    return timeout_count


def _handle_recurring_end(connection: Any) -> int:
    """表示終了日時を過ぎた繰り返し送信のスケジュールを削除し、通知を completed にする。"""
    alert_minutes = _positive_int_env("RECURRING_DELETE_ALERT_MINUTES", 60)
    completed_count = 0
    failed_notification_ids: list[Any] = []

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id
                 , display_end_at
                 , display_end_at <= DATE_SUB(NOW(), INTERVAL %s MINUTE) AS is_overdue
            FROM t_notification
            WHERE notification_type = %s
              AND send_status = %s
              AND display_end_at <= NOW()
              AND is_deleted = 0
            """,
            (alert_minutes, NOTIFICATION_TYPE_RECURRING, SEND_STATUS_WAITING),
        )
        notifications = cursor.fetchall() or []

        for notification in notifications:
            try:
                _delete_recurring_schedule(notification["id"])
            except Exception:
                # 削除は次回起動で冪等に再試行する。長時間解消しない場合のみ監視通知を発報する。
                logger.exception(
                    "recurring schedule delete failed",
                    extra={"notification_id": notification["id"]},
                )
                if int(notification["is_overdue"] or 0) == 1:
                    failed_notification_ids.append(notification["id"])
                continue

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
                (SEND_STATUS_COMPLETED, SYSTEM_USER, notification["id"], SEND_STATUS_WAITING),
            )
            completed_count += cursor.rowcount

    if failed_notification_ids:
        raise RuntimeError(
            f"recurring schedule delete kept failing: notification_ids={failed_notification_ids}"
        )

    return completed_count


def _schedule_name(notification_id: Any) -> str:
    """繰り返し送信スケジュールの決定的な名称を返す。"""
    return f"notification_{notification_id}_recurring"


def _delete_recurring_schedule(notification_id: Any) -> None:
    """繰り返し送信スケジュールを冪等に削除する。"""
    try:
        _scheduler().delete_schedule(
            Name=_schedule_name(notification_id),
            GroupName=_required_env("SCHEDULER_GROUP_NAME"),
        )
    except Exception as error:  # noqa: BLE001 - 既に削除済みの場合は成功とみなす
        if not _is_resource_not_found(error):
            raise


def _disable_recurring_schedule(notification_id: Any) -> None:
    """繰り返し送信スケジュールを DISABLED へ更新し、以降の自動実行を停止する。

    UpdateSchedule は全項目置換のため、現在の定義を取得してから State だけを変更する。
    """
    name = _schedule_name(notification_id)
    group_name = _required_env("SCHEDULER_GROUP_NAME")

    try:
        current = _scheduler().get_schedule(Name=name, GroupName=group_name)
    except Exception as error:  # noqa: BLE001 - 削除済みなら停止済みとみなす
        if _is_resource_not_found(error):
            return
        raise

    request: dict[str, Any] = {
        "Name": name,
        "GroupName": group_name,
        "State": "DISABLED",
        "ScheduleExpression": current["ScheduleExpression"],
        "FlexibleTimeWindow": current["FlexibleTimeWindow"],
        "Target": current["Target"],
    }
    for key in ("ScheduleExpressionTimezone", "StartDate", "EndDate", "Description", "KmsKeyArn"):
        value = current.get(key)
        if value is not None:
            request[key] = value

    _scheduler().update_schedule(**request)
    logger.warning("recurring schedule disabled", extra={"notification_id": notification_id})


def _is_resource_not_found(error: Exception) -> bool:
    """EventBridge Scheduler の ResourceNotFoundException かを判定する。"""
    if type(error).__name__ == "ResourceNotFoundException":
        return True

    response = getattr(error, "response", None)
    if isinstance(response, dict):
        return response.get("Error", {}).get("Code") == "ResourceNotFoundException"

    return False


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


def _scheduler() -> Any:
    """EventBridge Scheduler client を遅延生成して再利用する。"""
    global _scheduler_client

    if _scheduler_client is None:
        import boto3

        _scheduler_client = boto3.client("scheduler")

    return _scheduler_client


def _required_env(name: str) -> str:
    """必須の環境変数を取得する。"""
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"environment variable is required: {name}")

    return value


def _positive_int_env(name: str, default: int) -> int:
    """正の整数の環境変数を取得する。不正値の場合は既定値を使用する。"""
    raw = os.environ.get(name)
    if raw is None:
        return default

    try:
        value = int(raw)
    except ValueError:
        return default

    return value if value > 0 else default


def _required_string(value: Any, name: str) -> str:
    """必須の文字列項目を取得する。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"field is required: {name}")

    return value
