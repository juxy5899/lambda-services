import csv
import gzip
import json
import logging
import os
try:
    import resource
except ImportError:  # pragma: no cover - Lambda runtime is Linux
    resource = None
import tempfile
from contextlib import closing

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HEADER = ["uuid", "member_id", "sex", "zip_code", "birth_day", "created_at", "updated_at"] + [
    f"custom_parameter_{number:02d}_{suffix}"
    for number in range(1, 31)
    for suffix in ("title", "group", "value")
]


class RetryableServiceError(RuntimeError):
    pass


class NonRetryableDataError(RuntimeError):
    pass


def _raise_classified(exc):
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    if code in {"AccessDenied", "AccessDeniedException", "InvalidAccessKeyId", "NoSuchBucket"}:
        raise NonRetryableDataError(str(exc)) from exc
    if exc.__class__.__name__ in {"ProgrammingError", "DataError", "IntegrityError", "InternalError"}:
        raise NonRetryableDataError(str(exc)) from exc
    raise RetryableServiceError(str(exc)) from exc


def _format_datetime(value):
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S UTC") if hasattr(value, "strftime") else str(value)


def _remaining_time_check(context):
    if context and context.get_remaining_time_in_millis() < int(os.environ.get("MIN_REMAINING_MS", "120000")):
        raise NonRetryableDataError("Lambda remaining time is below the safety threshold")
    memory_mb = int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "0"))
    if memory_mb and resource and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 > memory_mb * 0.8:
        raise NonRetryableDataError("Lambda memory usage exceeds 80 percent")


def _load_secret(secret_client):
    response = secret_client.get_secret_value(SecretId=os.environ["MYSQL_SECRET_ID"])
    value = json.loads(response["SecretString"])
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


def _verify(s3, bucket, key):
    if s3.head_object(Bucket=bucket, Key=key)["ContentLength"] <= 0:
        raise NonRetryableDataError("Delivery object is empty")
    response = s3.get_object(Bucket=bucket, Key=key)
    with closing(response["Body"]), gzip.GzipFile(fileobj=response["Body"], mode="rb") as stream:
        actual = stream.readline().decode("utf-8").rstrip("\r\n")
    if actual != "\t".join(HEADER):
        raise NonRetryableDataError("Attributes TSV header verification failed")


def handle_event(event, context, s3_client=None, secrets_client=None, connect=None):
    if not event.get("business_date"):
        raise NonRetryableDataError("Missing required input: business_date")
    if s3_client is None or secrets_client is None:
        import boto3
        s3_client = s3_client or boto3.client("s3")
        secrets_client = secrets_client or boto3.client("secretsmanager")
    if connect is None:
        import pymysql
        connect = pymysql.connect

    business_date = event["business_date"]
    bucket = os.environ["DELIVERY_BUCKET"]
    key = f"{os.environ.get('DELIVERY_ATTRIBUTES_PREFIX', 'attributes/').rstrip('/')}/attributes_{business_date.replace('-', '')}.tsv.gz"
    page_size = int(os.environ.get("ATTRIBUTES_PAGE_SIZE", "5000"))
    max_bytes = int(os.environ.get("MAX_TMP_OUTPUT_BYTES", "1610612736"))
    path = None
    connection = None
    count = 0
    try:
        _remaining_time_check(context)
        connection = connect(**_load_secret(secrets_client))
        with connection.cursor() as cursor:
            cursor.execute("SET time_zone = '+00:00'")
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            with tempfile.NamedTemporaryFile(prefix="attributes-", suffix=".tsv.gz", delete=False) as temp:
                path = temp.name
            with gzip.open(path, "wt", encoding="utf-8", newline="") as output:
                writer = csv.writer(output, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
                writer.writerow(HEADER)
                last_mypage_id = None
                while True:
                    _remaining_time_check(context)
                    sql = """
                        SELECT device_uuid, mypage_id, created_at, updated_at
                        FROM t_user_device
                    """
                    params = []
                    if last_mypage_id is not None:
                        sql += " WHERE mypage_id > %s"
                        params.append(last_mypage_id)
                    sql += " ORDER BY mypage_id ASC LIMIT %s"
                    params.append(page_size)
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                    if not rows:
                        break
                    for device_uuid, mypage_id, created_at, updated_at in rows:
                        writer.writerow([
                            device_uuid or "", mypage_id or "", "", "", "",
                            _format_datetime(created_at), _format_datetime(updated_at), *([""] * 90),
                        ])
                    count += len(rows)
                    last_mypage_id = rows[-1][1]
                    output.flush()
                    if os.path.getsize(path) > max_bytes:
                        raise NonRetryableDataError("Attributes gzip exceeds MAX_TMP_OUTPUT_BYTES")
            connection.commit()

        if os.path.getsize(path) > max_bytes:
            raise NonRetryableDataError("Attributes gzip exceeds MAX_TMP_OUTPUT_BYTES")
        s3_client.upload_file(path, bucket, key, ExtraArgs={"ServerSideEncryption": "AES256"})
        _verify(s3_client, bucket, key)
        LOGGER.info("Attributes TSV generated", extra={"business_date": business_date, "record_count": count})
        return {"bucket": bucket, "key": key, "record_count": count}
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
        if path and os.path.exists(path):
            os.unlink(path)
