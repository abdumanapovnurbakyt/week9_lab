import time

from .db import one, rows, run


DDL = {
    "wide": """CREATE TABLE {t} (
        device_id integer     NOT NULL,
        ts        timestamptz NOT NULL,
        payload   jsonb       NOT NULL)""",
    "narrow": """CREATE TABLE {t} (
        ts        timestamptz      NOT NULL,
        device_id integer          NOT NULL,
        metric_id smallint         NOT NULL,
        value     double precision NOT NULL)""",
    "flat": """CREATE TABLE {t} (
        ts          timestamptz      NOT NULL,
        device_id   integer          NOT NULL,
        temperature double precision NOT NULL,
        humidity    double precision NOT NULL,
        battery_mv  integer          NOT NULL,
        rssi_dbm    smallint         NOT NULL)""",
}
INDEX = {
    "wide": "CREATE INDEX {t}_dev_ts ON {t} (device_id, ts DESC)",
    "narrow": "CREATE INDEX {t}_dev_metric_ts ON {t} (device_id, metric_id, ts DESC)",
    "flat": "CREATE INDEX {t}_dev_ts ON {t} (device_id, ts DESC)",
}
COPY_COLS = {
    "wide": "(ts, device_id, payload)",
    "narrow": "(ts, device_id, metric_id, value)",
    "flat": "(ts, device_id, temperature, humidity, battery_mv, rssi_dbm)",
}
SEGMENTBY = {"wide": "device_id", "narrow": "device_id", "flat": "device_id"}

CAGG_NAMES = {"1m": "cagg_narrow_1m", "1h": "cagg_narrow_1h", "1d": "cagg_narrow_1d"}


def drop_all(conn, tables):
    for t in tables:
        run(conn, f"DROP TABLE IF EXISTS {t} CASCADE")


def drop_cagg(conn):
    for lvl in ("1d", "1h", "1m"):
        run(conn, f"DROP MATERIALIZED VIEW IF EXISTS {CAGG_NAMES[lvl]} CASCADE")


def create_dictionaries(conn):
    run(conn, "DROP TABLE IF EXISTS devices, metrics CASCADE")
    run(conn, "CREATE TABLE metrics (metric_id smallint PRIMARY KEY, name text NOT NULL, unit text NOT NULL)")
    run(conn, "CREATE TABLE devices (device_id integer PRIMARY KEY, site text NOT NULL, device_type text NOT NULL)")


def create_hypertable(conn, kind, table, chunk_interval="7 days"):
    drop_all(conn, [table])
    run(conn, DDL[kind].format(t=table))
    run(conn, f"SELECT create_hypertable('{table}', 'ts', chunk_time_interval => INTERVAL '{chunk_interval}')")
    run(conn, INDEX[kind].format(t=table))


def set_compression(conn, table, segmentby, orderby="ts DESC"):
    run(conn, f"ALTER TABLE {table} SET (timescaledb.compress, timescaledb.compress_segmentby = '{segmentby}', "
              f"timescaledb.compress_orderby = '{orderby}')")


def chunk_names(conn, table, only=None):
    cond = {"compressed": "AND is_compressed", "uncompressed": "AND NOT is_compressed"}.get(only, "")
    return [r[0] for r in rows(conn, f"""SELECT format('%%I.%%I', chunk_schema, chunk_name)::text
        FROM timescaledb_information.chunks WHERE hypertable_name = %s {cond} ORDER BY range_start""", (table,))]


def compress_all(conn, table):
    chunks = chunk_names(conn, table, "uncompressed")
    t0 = time.perf_counter()
    for c in chunks:
        one(conn, f"SELECT compress_chunk('{c}'::regclass, if_not_compressed => true)")
    return {"chunks": len(chunks), "seconds": time.perf_counter() - t0}


def sizes(conn, table):
    r = rows(conn, f"SELECT table_bytes, index_bytes, toast_bytes, total_bytes FROM hypertable_detailed_size('{table}')")[0]
    n = one(conn, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = %s", (table,))
    return {"table_bytes": int(r[0] or 0), "index_bytes": int(r[1] or 0), "toast_bytes": int(r[2] or 0),
            "total_bytes": int(r[3] or 0), "chunks": int(n)}


def compression_stats(conn, table):
    r = rows(conn, f"""SELECT total_chunks, number_compressed_chunks,
                       before_compression_table_bytes, before_compression_index_bytes, before_compression_toast_bytes,
                       before_compression_total_bytes, after_compression_table_bytes, after_compression_index_bytes,
                       after_compression_toast_bytes, after_compression_total_bytes
                       FROM hypertable_compression_stats('{table}')""")
    if not r:
        return None
    k = ("total_chunks", "compressed_chunks", "before_table", "before_index", "before_toast", "before_total",
         "after_table", "after_index", "after_toast", "after_total")
    return {a: (int(b) if b is not None else None) for a, b in zip(k, r[0])}


def analyze(conn, table):
    run(conn, f"VACUUM (ANALYZE) {table}")


def create_caggs(conn, base="telemetry_narrow"):
    drop_cagg(conn)
    run(conn, f"""CREATE MATERIALIZED VIEW {CAGG_NAMES['1m']} WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
        SELECT time_bucket(INTERVAL '1 minute', ts) AS bucket, device_id, metric_id,
               count(*) AS cnt, sum(value) AS sum_v, min(value) AS min_v, max(value) AS max_v
        FROM {base} GROUP BY 1, 2, 3 WITH NO DATA""")
    run(conn, f"""CREATE MATERIALIZED VIEW {CAGG_NAMES['1h']} WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
        SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, device_id, metric_id,
               count(*) AS cnt, sum(value) AS sum_v, min(value) AS min_v, max(value) AS max_v
        FROM {base} GROUP BY 1, 2, 3 WITH NO DATA""")
    run(conn, f"""CREATE MATERIALIZED VIEW {CAGG_NAMES['1d']} WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
        SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket, device_id, metric_id,
               sum(cnt)::bigint AS cnt, sum(sum_v) AS sum_v, min(min_v) AS min_v, max(max_v) AS max_v
        FROM {CAGG_NAMES['1h']} GROUP BY 1, 2, 3 WITH NO DATA""")


def refresh_cagg(conn, level, start=None, end=None):
    t0 = time.perf_counter()
    run(conn, f"CALL refresh_continuous_aggregate('{CAGG_NAMES[level]}', %s::timestamptz, %s::timestamptz)", (start, end))
    return time.perf_counter() - t0


def cagg_stats(conn, level):
    name = CAGG_NAMES[level]
    n = one(conn, f"SELECT count(*) FROM {name}")
    size = one(conn, """SELECT hypertable_size(format('%%I.%%I', materialization_hypertable_schema,
                          materialization_hypertable_name)::regclass)
                          FROM timescaledb_information.continuous_aggregates WHERE view_name = %s""", (name,))
    return {"rows": int(n), "bytes": int(size or 0)}


def add_policies(conn, keep_days, base="telemetry_narrow", wide="telemetry_wide"):
    out = {}
    out["cagg_1m"] = one(conn, f"""SELECT add_continuous_aggregate_policy('{CAGG_NAMES['1m']}',
        start_offset => INTERVAL '2 hours', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute',
        if_not_exists => true)""")
    out["cagg_1h"] = one(conn, f"""SELECT add_continuous_aggregate_policy('{CAGG_NAMES['1h']}',
        start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes',
        if_not_exists => true)""")
    out["cagg_1d"] = one(conn, f"""SELECT add_continuous_aggregate_policy('{CAGG_NAMES['1d']}',
        start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour',
        if_not_exists => true)""")
    out["retention_1m"] = one(conn, f"SELECT add_retention_policy('{CAGG_NAMES['1m']}', INTERVAL '30 days', if_not_exists => true)")
    out["compress_narrow"] = one(conn, f"SELECT add_compression_policy('{base}', INTERVAL '7 days', if_not_exists => true)")
    out["retention_narrow"] = one(conn, f"SELECT add_retention_policy('{base}', INTERVAL '{keep_days} days', if_not_exists => true)")
    if wide:
        out["compress_wide"] = one(conn, f"SELECT add_compression_policy('{wide}', INTERVAL '7 days', if_not_exists => true)")
        out["retention_wide"] = one(conn, f"SELECT add_retention_policy('{wide}', INTERVAL '{keep_days} days', if_not_exists => true)")
    return out


def list_jobs(conn):
    r = rows(conn, """SELECT job_id, proc_name, coalesce(hypertable_name, '-'), schedule_interval::text, config::text
                      FROM timescaledb_information.jobs WHERE job_id >= 1000 ORDER BY job_id""")
    return [{"job_id": a, "proc": b, "hypertable": c, "schedule": d, "config": e} for a, b, c, d, e in r]


def compressed_relations(conn, table):
    for sql in ("""SELECT cs.compress_relid::text FROM _timescaledb_catalog.compression_settings cs
                   JOIN _timescaledb_catalog.chunk c ON c.relid = cs.relid
                   JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
                   WHERE h.table_name = %s AND cs.compress_relid IS NOT NULL""",
                """SELECT format('%%I.%%I', cc.schema_name, cc.table_name) FROM _timescaledb_catalog.chunk c
                   JOIN _timescaledb_catalog.chunk cc ON cc.id = c.compressed_chunk_id
                   JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id WHERE h.table_name = %s"""):
        try:
            r = rows(conn, sql, (table,))
            if r:
                return [x[0] for x in r]
        except Exception:
            continue
    return []


def column_breakdown(conn, table, columns, by_metric=False):
    rels = compressed_relations(conn, table)
    if not rels:
        return None
    tot_rows, batches, row_bytes = 0, 0, 0
    col_bytes = {c: 0 for c in columns}
    per_metric = {}
    sel = ", ".join(f"sum(pg_column_size({c}))" for c in columns)
    for rel in rels:
        r = rows(conn, f"SELECT sum(_ts_meta_count), count(*), sum(pg_column_size(c.*)), {sel} FROM {rel} c")[0]
        tot_rows += int(r[0] or 0)
        batches += int(r[1] or 0)
        row_bytes += int(r[2] or 0)
        for c, v in zip(columns, r[3:]):
            col_bytes[c] += int(v or 0)
        if by_metric:
            for m in rows(conn, f"SELECT metric_id, sum(_ts_meta_count), {sel} FROM {rel} c GROUP BY metric_id"):
                e = per_metric.setdefault(int(m[0]), {"rows": 0, **{c: 0 for c in columns}})
                e["rows"] += int(m[1] or 0)
                for c, v in zip(columns, m[2:]):
                    e[c] += int(v or 0)
    out = {"rows": tot_rows, "batches": batches, "rows_per_batch": tot_rows / max(batches, 1),
           "bytes_per_row": {c: col_bytes[c] / max(tot_rows, 1) for c in columns},
           "meta_bytes_per_row": (row_bytes - sum(col_bytes.values())) / max(tot_rows, 1)}
    if by_metric:
        out["per_metric"] = {m: {c: e[c] / max(e["rows"], 1) for c in columns} for m, e in sorted(per_metric.items())}
    return out
