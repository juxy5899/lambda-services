import json

from action_log_push_reflector.handler import _athena_rows, handle_event


class Paginator:
    def paginate(self, QueryExecutionId):
        yield {"ResultSet": {"Rows": [
            {"Data": [{"VarCharValue": "notification_id"}, {"VarCharValue": "execution_id"}, {"VarCharValue": "device_uuid"}]},
            {"Data": [{"VarCharValue": "12"}, {"VarCharValue": "execution"}, {"VarCharValue": "device"}]},
        ]}}


class Athena:
    def get_paginator(self, name):
        return Paginator()


def test_athena_header_is_skipped():
    assert list(_athena_rows(Athena(), "query")) == [(12, "execution", "device")]


class Secrets:
    def get_secret_value(self, SecretId):
        return {"SecretString": json.dumps({"host": "db", "username": "user", "password": "pw", "dbname": "app"})}


class Cursor:
    rowcount = 1
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params=None): self.last_sql = sql
    def executemany(self, sql, rows): self.last_sql = sql
    def fetchall(self): return []


class Connection:
    def cursor(self): return Cursor()
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass


def test_reflects_task_a_rows(monkeypatch):
    monkeypatch.setenv("MYSQL_SECRET_ID", "secret")
    result = handle_event(
        {"query_execution_id": "query", "business_date": "2026-08-01"},
        None,
        Athena(),
        Secrets(),
        lambda **kwargs: Connection(),
    )
    assert result == {"result_rows": 1, "inserted_rows": 1}
