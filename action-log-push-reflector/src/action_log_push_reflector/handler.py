import json
import logging
import os
try:
    import resource
except ImportError:  # pragma: no cover - Lambda runtime is Linux
    resource = None
from datetime import datetime, timedelta, timezone

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class RetryableServiceError(RuntimeError):
    pass


class NonRetryableDataError(RuntimeError):
    pass


def _raise_classified(exc):
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    if code in {"AccessDenied", "AccessDeniedException", "InvalidAccessKeyId"}:
        raise NonRetryableDataError(str(exc)) from exc
    if exc.__class__.__name__ in {"ProgrammingError", "DataError", "IntegrityError", "InternalError"}:
        raise NonRetryableDataError(str(exc)) from exc
    raise RetryableServiceError(str(exc)) from exc


def _limits_check(context):
    if context and context.get_remaining_time_in_millis() < int(os.environ.get("MIN_REMAINING_MS", "120000")):
        raise NonRetryableDataError("Lambda remaining time is below the safety threshold")
    memory_mb = int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "0"))
    if memory_mb and resource and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 > memory_mb * 0.8:
        raise NonRetryableDataError("Lambda memory usage exceeds 80 percent")


def _load_secret(client):
    value = json.loads(client.get_secret_value(SecretId=os.environ["MYSQL_SECRET_ID"])["SecretString"])
    return {
        "host": value["host"],
        "port": int(value.get("port", 3306)),
        "user": value.get("username") or value["user"],
        "password": value["password"],
        "database": value.get("dbname") or value.get("database"),
        "charset": "utf8mb4",
        "connect_timeout": 10,
        "read_timeout": 60,
        "write_timeout": 60,
        "autocommit": False,
    }


def _athena_rows(client, query_execution_id):
    paginator = client.get_paginator("get_query_results")
    first = True
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        for row in page["ResultSet"].get("Rows", []):
            values = [item.get("VarCharValue", "") for item in row.get("Data", [])]
            if first:
                first = False
                if values[:3] == ["notification_id", "execution_id", "device_uuid"]:
                    continue
            if len(values) != 3:
                raise NonRetryableDataError("Unexpected Task A result columns")
            try:
                yield int(values[0]), values[1], values[2]
            except (TypeError, ValueError) as exc:
                raise NonRetryableDataError("Invalid Task A result row") from exc


def _check_warnings(cursor):
    cursor.execute("SHOW WARNINGS")
    unexpected = [row for row in cursor.fetchall() if int(row[1]) != 1062]
    if unexpected:
        raise NonRetryableDataError(f"Unexpected MySQL warning: {unexpected[0]}")


def handle_event(event, context, athena_client=None, secrets_client=None, connect=None):
    required = ("query_execution_id", "business_date")
    if any(not event.get(name) for name in required):
        raise NonRetryableDataError("Missing required workflow input")
    if athena_client is None or secrets_client is None:
        import boto3
        athena_client = athena_client or boto3.client("athena")
        secrets_client = secrets_client or boto3.client("secretsmanager")
    if connect is None:
        import pymysql
        connect = pymysql.connect

    connection = None
    inserted = 0
    try:
        _limits_check(context)
        rows = list(_athena_rows(athena_client, event["query_execution_id"]))
        _limits_check(context)
        connection = connect(**_load_secret(secrets_client))
        business_date = datetime.strptime(event["business_date"], "%Y-%m-%d")
        utc_start = (business_date - timedelta(hours=9)).replace(tzinfo=timezone.utc)
        utc_end = utc_start + timedelta(days=1)
        with connection.cursor() as cursor:
            if rows:
                cursor.executemany(
                    """
                    INSERT IGNORE INTO t_notification_open_devices
                      (notification_id, execution_id, device_uuid)
                    VALUES (%s, %s, %s)
                    """,
                    rows,
                )
                inserted = cursor.rowcount
                _check_warnings(cursor)

            cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_action_log_target_executions")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE tmp_action_log_target_executions (
                  notification_id BIGINT NOT NULL,
                  execution_id CHAR(36) NOT NULL,
                  PRIMARY KEY (notification_id, execution_id)
                ) ENGINE=InnoDB
                """
            )
            if rows:
                cursor.executemany(
                    "INSERT IGNORE INTO tmp_action_log_target_executions VALUES (%s, %s)",
                    [(notification_id, execution_id) for notification_id, execution_id, _ in rows],
                )
                _check_warnings(cursor)
            cursor.execute(
                """
                INSERT IGNORE INTO tmp_action_log_target_executions
                SELECT notification_id, id
                FROM t_delivery_execution
                WHERE finished_at >= %s AND finished_at < %s
                  AND triggered_by <> 2
                  AND result_status IN (1, 3)
                """,
                (utc_start.replace(tzinfo=None), utc_end.replace(tzinfo=None)),
            )
            _check_warnings(cursor)
            cursor.execute(
                """
                UPDATE t_delivery_execution e
                JOIN tmp_action_log_target_executions target
                  ON target.notification_id = e.notification_id AND target.execution_id = e.id
                LEFT JOIN (
                  SELECT notification_id, execution_id, COUNT(*) AS open_count
                  FROM t_notification_open_devices
                  GROUP BY notification_id, execution_id
                ) opened ON opened.notification_id = e.notification_id AND opened.execution_id = e.id
                SET e.open_count = COALESCE(opened.open_count, 0)
                WHERE e.triggered_by <> 2 AND e.result_status IN (1, 3)
                """
            )
            cursor.execute(
                """
                UPDATE t_notification n
                JOIN (SELECT DISTINCT notification_id FROM tmp_action_log_target_executions) target
                  ON target.notification_id = n.id
                LEFT JOIN t_delivery_execution latest
                  ON latest.id = n.latest_execution_id AND latest.notification_id = n.id
                LEFT JOIN (
                  SELECT notification_id, SUM(open_count) AS cumulative_open_count
                  FROM t_delivery_execution
                  WHERE notification_type = 2 AND triggered_by <> 2 AND result_status IN (1, 3)
                  GROUP BY notification_id
                ) cumulative ON cumulative.notification_id = n.id
                SET n.latest_open_count = COALESCE(latest.open_count, 0),
                    n.cumulative_open_count = CASE
                      WHEN n.notification_type = 2 THEN COALESCE(cumulative.cumulative_open_count, 0)
                      ELSE n.cumulative_open_count
                    END
                """
            )
        connection.commit()
        LOGGER.info("Push open statistics reflected", extra={"result_rows": len(rows), "inserted_rows": inserted})
        return {"result_rows": len(rows), "inserted_rows": inserted}
    except NonRetryableDataError:
        if connection:
            connection.rollback()
        raise
    except Exception as exc:
        if connection:
            connection.rollback()
        _raise_classified(exc)
    finally:
        if connection:
            connection.close()
