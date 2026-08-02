import io
import json
import os

from action_log_attributes_tsv_generator.handler import HEADER, _format_datetime, handle_event
from datetime import datetime


def test_header_has_97_columns():
    assert len(HEADER) == 97


def test_datetime_is_utc_text():
    assert _format_datetime(datetime(2026, 8, 1, 2, 3, 4)) == "2026-08-01 02:03:04 UTC"


class Cursor:
    def __init__(self):
        self.page = 0

    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params=None):
        if "SELECT device_uuid" in sql:
            self.page += 1
    def fetchall(self):
        return [("device", "member", datetime(2026, 8, 1), datetime(2026, 8, 2))] if self.page == 1 else []


class Connection:
    def cursor(self): return Cursor()
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass


class Secrets:
    def get_secret_value(self, SecretId):
        return {"SecretString": json.dumps({"host": "db", "username": "user", "password": "pw", "dbname": "app"})}


class S3:
    def __init__(self): self.data = {}
    def upload_file(self, path, bucket, key, ExtraArgs): self.data[(bucket, key)] = open(path, "rb").read()
    def head_object(self, Bucket, Key): return {"ContentLength": len(self.data[(Bucket, Key)])}
    def get_object(self, Bucket, Key): return {"Body": io.BytesIO(self.data[(Bucket, Key)])}


def test_generates_attributes_file(monkeypatch):
    monkeypatch.setenv("MYSQL_SECRET_ID", "secret")
    monkeypatch.setenv("DELIVERY_BUCKET", "delivery")
    result = handle_event(
        {"business_date": "2026-08-01"}, None, S3(), Secrets(), lambda **kwargs: Connection()
    )
    assert result["record_count"] == 1
    assert result["key"] == "attributes/attributes_20260801.tsv.gz"
