import gzip
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

HEADER = (
    "_id\tuuid\tidfa\taaid\tscreen_name\tscreen_name_id\tsource_screen_name\t"
    "source_screen_id\tevent_category\tevent_action\tevent_label\tevent_value\t"
    "timestamp\tos\tos_version\tapplication_version\tip_address\tuser_agent"
)


class RetryableServiceError(RuntimeError):
    pass


class NonRetryableDataError(RuntimeError):
    pass


def _raise_classified(exc):
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    if code in {"AccessDenied", "AccessDeniedException", "InvalidAccessKeyId", "NoSuchBucket"}:
        raise NonRetryableDataError(str(exc)) from exc
    if isinstance(exc, (gzip.BadGzipFile, UnicodeError, ValueError)):
        raise NonRetryableDataError(str(exc)) from exc
    raise RetryableServiceError(str(exc)) from exc


def _required(event, name):
    value = event.get(name)
    if value is None or value == "":
        raise NonRetryableDataError(f"Missing required input: {name}")
    return value


def _remaining_time_check(context):
    minimum = int(os.environ.get("MIN_REMAINING_MS", "120000"))
    if context and context.get_remaining_time_in_millis() < minimum:
        raise NonRetryableDataError("Lambda remaining time is below the safety threshold")
    memory_mb = int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "0"))
    if memory_mb and resource and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 > memory_mb * 0.8:
        raise NonRetryableDataError("Lambda memory usage exceeds 80 percent")


def _data_keys(s3, bucket, prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            name = key.rsplit("/", 1)[-1]
            if item.get("Size", 0) and not name.endswith(".metadata") and "manifest" not in name:
                keys.append(key)
    return sorted(keys)


def _copy_gzip_lines(body, output, context):
    with closing(body), gzip.GzipFile(fileobj=body, mode="rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)
            _remaining_time_check(context)


def _verify(s3, bucket, key):
    head = s3.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] <= 0:
        raise NonRetryableDataError("Delivery object is empty")
    response = s3.get_object(Bucket=bucket, Key=key)
    with closing(response["Body"]), gzip.GzipFile(fileobj=response["Body"], mode="rb") as stream:
        first_line = stream.readline().decode("utf-8").rstrip("\r\n")
    if first_line != HEADER:
        raise NonRetryableDataError("Events TSV header verification failed")


def _cleanup(s3, bucket, prefix):
    keys = _data_keys(s3, bucket, prefix)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if item["Key"] not in keys
            and (item["Key"].endswith(".metadata") or "manifest" in item["Key"].rsplit("/", 1)[-1])
        )
    for offset in range(0, len(keys), 1000):
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in keys[offset : offset + 1000]], "Quiet": True},
        )


def handle_event(event, context, s3_client=None):
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    s3 = s3_client
    business_date = _required(event, "business_date")
    source_prefix = _required(event, "intermediate_prefix")
    source_bucket = os.environ["INTERMEDIATE_BUCKET"]
    delivery_bucket = os.environ["DELIVERY_BUCKET"]
    destination_key = f"{os.environ.get('DELIVERY_EVENTS_PREFIX', 'events/').rstrip('/')}/events_{business_date.replace('-', '')}.tsv.gz"
    max_bytes = int(os.environ.get("MAX_TMP_OUTPUT_BYTES", "1610612736"))
    record_count = 0
    path = None

    try:
        _remaining_time_check(context)
        keys = _data_keys(s3, source_bucket, source_prefix)
        with tempfile.NamedTemporaryFile(prefix="events-", suffix=".tsv.gz", delete=False) as temp:
            path = temp.name
        with gzip.open(path, "wb") as output:
            output.write((HEADER + "\n").encode("utf-8"))
            for key in keys:
                _remaining_time_check(context)
                response = s3.get_object(Bucket=source_bucket, Key=key)
                _copy_gzip_lines(response["Body"], output, context)
                output.flush()
                if os.path.getsize(path) > max_bytes:
                    raise NonRetryableDataError("Events gzip exceeds MAX_TMP_OUTPUT_BYTES")
        if os.path.getsize(path) > max_bytes:
            raise NonRetryableDataError("Events gzip exceeds MAX_TMP_OUTPUT_BYTES")

        # Count after closing gzip so malformed source data fails before publication.
        with gzip.open(path, "rt", encoding="utf-8", newline="") as generated:
            next(generated)
            record_count = sum(1 for _ in generated)
        s3.upload_file(path, delivery_bucket, destination_key, ExtraArgs={"ServerSideEncryption": "AES256"})
        _verify(s3, delivery_bucket, destination_key)
        try:
            _cleanup(s3, source_bucket, source_prefix)
        except Exception:
            # Delivery is already verified. Remaining intermediate objects are removed by Lifecycle.
            LOGGER.exception("Intermediate cleanup failed", extra={"prefix": source_prefix})
        LOGGER.info("Events TSV generated", extra={"business_date": business_date, "record_count": record_count})
        return {"bucket": delivery_bucket, "key": destination_key, "record_count": record_count}
    except NonRetryableDataError:
        raise
    except Exception as exc:
        _raise_classified(exc)
    finally:
        if path and os.path.exists(path):
            os.unlink(path)
