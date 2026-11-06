import io
import os
import time
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

VARIANTS = {
    "snappy": dict(compression="snappy"),
    "zstd": dict(compression="zstd", compression_level=3),
    "zstd_sorted": dict(compression="zstd", compression_level=3, sort=True, use_dictionary=["device_id", "metric_id"],
                        column_encoding={"ts": "DELTA_BINARY_PACKED", "value": "BYTE_STREAM_SPLIT"},
                        data_page_version="2.0"),
}


def month_ranges(t_start: datetime, t_end: datetime):
    out, cur = [], t_start
    while cur < t_end:
        nxt = (cur.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        out.append((cur.strftime("%Y-%m"), cur, min(nxt, t_end)))
        cur = nxt
    return out


def _copy_csv(conn, select_sql, params):
    buf = bytearray()
    with conn.cursor() as cur:
        with cur.copy(f"COPY ({select_sql}) TO STDOUT WITH (FORMAT csv)", params) as cp:
            for chunk in cp:
                buf += bytes(chunk)
    return bytes(buf)


def fetch_narrow(conn, table, m0, m1) -> pa.Table:
    data = _copy_csv(conn, f"""SELECT (EXTRACT(EPOCH FROM ts) * 1000000)::bigint, device_id, metric_id, value
                               FROM {table} WHERE ts >= %s AND ts < %s""", (m0, m1))
    t = pacsv.read_csv(io.BytesIO(data), read_options=pacsv.ReadOptions(column_names=["ts", "device_id", "metric_id", "value"]),
                       convert_options=pacsv.ConvertOptions(column_types={"ts": pa.int64(), "device_id": pa.int32(),
                                                                          "metric_id": pa.int16(), "value": pa.float64()}))
    return t.set_column(0, "ts", t.column("ts").cast(pa.timestamp("us", tz="UTC")))


def fetch_wide_json(conn, table, m0, m1) -> pa.Table:
    data = _copy_csv(conn, f"""SELECT (EXTRACT(EPOCH FROM ts) * 1000000)::bigint, device_id, payload::text
                               FROM {table} WHERE ts >= %s AND ts < %s""", (m0, m1))
    t = pacsv.read_csv(io.BytesIO(data), read_options=pacsv.ReadOptions(column_names=["ts", "device_id", "payload"]),
                       convert_options=pacsv.ConvertOptions(column_types={"ts": pa.int64(), "device_id": pa.int32(),
                                                                          "payload": pa.string()}))
    return t.set_column(0, "ts", t.column("ts").cast(pa.timestamp("us", tz="UTC")))


def to_parquet_bytes(table: pa.Table, variant: str) -> bytes:
    kw = dict(VARIANTS[variant])
    if kw.pop("sort", False):
        table = table.sort_by([("device_id", "ascending"), ("metric_id", "ascending"), ("ts", "ascending")])
    buf = io.BytesIO()
    pq.write_table(table, buf, **kw)
    return buf.getvalue()


def wide_to_parquet_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table.sort_by([("device_id", "ascending"), ("ts", "ascending")]), buf, compression="zstd", compression_level=3)
    return buf.getvalue()


def minio_env():
    return dict(endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
                access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
                bucket=os.environ.get("MINIO_BUCKET", "telemetry-cold"))


def minio_client():
    from minio import Minio
    e = minio_env()
    return Minio(e["endpoint"], access_key=e["access_key"], secret_key=e["secret_key"], secure=False)


def minio_available(timeout=4.0) -> bool:
    try:
        import urllib3
        from minio import Minio
        e = minio_env()
        c = Minio(e["endpoint"], access_key=e["access_key"], secret_key=e["secret_key"], secure=False,
                  http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=timeout, read=timeout),
                                                  retries=urllib3.Retry(total=1)))
        c.list_buckets()
        return True
    except Exception:
        return False


def ensure_bucket(client, bucket):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def clear_prefix(client, bucket, prefix):
    for o in client.list_objects(bucket, prefix=prefix, recursive=True):
        client.remove_object(bucket, o.object_name)


def upload_bytes(client, bucket, key, data: bytes):
    client.put_object(bucket, key, io.BytesIO(data), length=len(data), content_type="application/octet-stream")


def list_size(client, bucket, prefix):
    n = size = 0
    for o in client.list_objects(bucket, prefix=prefix, recursive=True):
        n += 1
        size += o.size
    return n, size


def s3_filesystem():
    from pyarrow import fs
    e = minio_env()
    return fs.S3FileSystem(access_key=e["access_key"], secret_key=e["secret_key"], endpoint_override=e["endpoint"],
                           scheme="http", region="us-east-1")


def read_month(bucket, month, device_id, metric_id):
    t = time.perf_counter()
    tbl = pq.read_table(f"{bucket}/narrow/month={month}/part-0.parquet", filesystem=s3_filesystem(),
                        filters=[("device_id", "=", device_id), ("metric_id", "=", metric_id)])
    return tbl, time.perf_counter() - t
