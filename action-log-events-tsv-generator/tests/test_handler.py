import gzip
import io
import os

from action_log_events_tsv_generator.handler import HEADER, handle_event


class Body(io.BytesIO):
    def close(self):
        pass


class Paginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": k, "Size": len(v)} for k, v in self.client.objects.get(Bucket, {}).items() if k.startswith(Prefix)]}


class S3:
    def __init__(self):
        self.objects = {"intermediate": {"run/part-1": gzip.compress(b"a\tb\n")}}

    def get_paginator(self, name):
        return Paginator(self)

    def get_object(self, Bucket, Key):
        return {"Body": Body(self.objects[Bucket][Key])}

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.objects.setdefault(bucket, {})[key] = open(path, "rb").read()

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.objects[Bucket][Key])}

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            self.objects[Bucket].pop(item["Key"], None)


def test_generates_header_and_rows(monkeypatch):
    monkeypatch.setenv("INTERMEDIATE_BUCKET", "intermediate")
    monkeypatch.setenv("DELIVERY_BUCKET", "delivery")
    result = handle_event({"business_date": "2026-08-01", "intermediate_prefix": "run/"}, None, S3())
    assert result["record_count"] == 1
    assert result["key"] == "events/events_20260801.tsv.gz"


def test_header_has_18_columns():
    assert len(HEADER.split("\t")) == 18
