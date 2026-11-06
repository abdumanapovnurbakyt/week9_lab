import os
import statistics
import time

import psycopg


def conn_info() -> dict:
    return dict(host=os.environ.get("PGHOST", "localhost"), port=int(os.environ.get("PGPORT", "5432")),
                user=os.environ.get("PGUSER", "postgres"), password=os.environ.get("PGPASSWORD", "postgres"),
                dbname=os.environ.get("PGDATABASE", "lab6"))


def connect(autocommit: bool = True, **over) -> psycopg.Connection:
    info = {**conn_info(), **over}
    conn = psycopg.connect(**info, autocommit=autocommit)
    conn.prepare_threshold = None
    with conn.cursor() as cur:
        cur.execute("SET timezone = 'UTC'")
    return conn


def wait_for_db(timeout: float = 120.0) -> None:
    t0 = time.time()
    while True:
        try:
            connect().close()
            return
        except Exception as e:
            if time.time() - t0 > timeout:
                raise RuntimeError(f"БД недоступна: {e}")
            time.sleep(2)


def one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0] if row and len(row) == 1 else row


def rows(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def run(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)


def timed(conn, sql, params=None):
    with conn.cursor() as cur:
        t = time.perf_counter()
        cur.execute(sql, params)
        data = cur.fetchall() if cur.description else []
        return time.perf_counter() - t, data


def pct(values, p):
    s = sorted(values)
    if not s:
        return None
    k = (len(s) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def summarize(values):
    return {"n": len(values), "median": statistics.median(values), "min": min(values), "max": max(values),
            "p90": pct(values, 90)}
