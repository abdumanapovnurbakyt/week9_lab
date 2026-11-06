import argparse
import csv
import json
import math
import os
import platform
import re
import time
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone

from . import db, export, load, queries, schema
from .gen import METRICS, UNITS, Dataset, DatasetSpec, device_meta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPERIMENTS = ["e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9"]


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def clean(o):
    if isinstance(o, dict):
        return {str(k): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    if isinstance(o, float):
        return None if (math.isnan(o) or math.isinf(o)) else o
    if isinstance(o, datetime):
        return o.isoformat()
    return o


class Ctx:
    def __init__(self, args):
        exp = json.load(open(args.exp, encoding="utf-8"))
        sc = exp["scales"][args.scale]
        d = exp["dataset"]
        self.scale, self.exp, self.out = args.scale, exp, args.out
        self.spec = DatasetSpec(devices=sc.get("devices", d["devices"]), interval_s=d["interval_s"],
                                days=sc.get("days", d["days"]), seed=d["seed"], anomaly_rate=d["anomaly_rate"],
                                jitter_s=d["jitter_s"])
        s = exp["sub"]
        self.sub_spec = DatasetSpec(devices=sc.get("sub_devices", s["devices"]), interval_s=d["interval_s"],
                                    days=sc.get("sub_days", s["days"]), seed=d["seed"], anomaly_rate=d["anomaly_rate"],
                                    jitter_s=d["jitter_s"])
        q = exp["queries"]
        self.reps, self.warmup = sc.get("reps", q["reps"]), sc.get("warmup", q["warmup"])
        self.lo, self.hi = q["anomaly_lo"], q["anomaly_hi"]
        self.device = min(q["device"], self.spec.devices)
        self.sub_device = min(q["device"], self.sub_spec.devices)
        self.chunk_interval, self.slice_days = exp["chunk_interval"], exp["slice_days"]
        self.chunk_intervals = sc.get("chunk_intervals", s["chunk_intervals"])
        self.keep_days = sc.get("keep_days", exp["cold"]["keep_days"])
        self.bucket = os.environ.get("MINIO_BUCKET", exp["cold"]["bucket"])
        self.no_minio = args.no_minio
        self.path = os.path.join(args.out, "results.json")
        self.R = {}
        if os.path.exists(self.path) and args.only:
            self.R = json.load(open(self.path, encoding="utf-8"))
        self._sub = None

    def save(self):
        os.makedirs(self.out, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(clean(self.R), f, ensure_ascii=False, indent=1, allow_nan=False)

    @property
    def anchor(self) -> datetime:
        return datetime.fromisoformat(self.R["meta"]["anchor"])

    def params(self, days, device) -> dict:
        a = self.anchor
        return dict(d=device, t_day0=a - timedelta(days=1), t_day1=a, t_mon0=a - timedelta(days=min(30, days)), t_mon1=a,
                    t_year0=a - timedelta(days=min(365, days)), t_year1=a, lo=self.lo, hi=self.hi)

    def sub_dataset(self, jitter=True):
        key = "j" if jitter else "n"
        if self._sub is None:
            self._sub = {}
        if key not in self._sub:
            spec = self.sub_spec if jitter else replace(self.sub_spec, jitter_s=1)
            self._sub[key] = Dataset(spec, int(self.anchor.timestamp()) - spec.days * 86400)
        return self._sub[key]


def _walk(plan, acc, visited):
    if "Relation Name" in plan:
        acc.add(plan["Relation Name"])
    if "Chunks Visited" in plan:
        visited.append(plan["Chunks Visited"])
    for ch in plan.get("Plans", []):
        _walk(ch, acc, visited)


def explain(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
        doc = cur.fetchone()[0]
    doc = doc[0] if isinstance(doc, list) else json.loads(doc)[0]
    names, visited = set(), []
    _walk(doc["Plan"], names, visited)
    chunks = {re.sub(r"_compressed$", "", n) for n in names if re.match(r"^(compress)?_hyper_\d+_\d+_chunk(_compressed)?$", n)}
    comp = {n for n in names if n.endswith("_compressed") or n.startswith("compress_hyper_")}
    p = doc["Plan"]
    return {"planning_ms": doc.get("Planning Time"), "execution_ms": doc.get("Execution Time"),
            "chunks": max(len(chunks), sum(visited)), "compressed_chunks": len(comp),
            "shared_hit": p.get("Shared Hit Blocks"), "shared_read": p.get("Shared Read Blocks"),
            "workers": p.get("Workers Launched", 0)}


def bench_query(conn, sql, params, reps, warmup):
    for _ in range(warmup):
        db.timed(conn, sql, params)
    ts, data = [], []
    for _ in range(reps):
        t, data = db.timed(conn, sql, params)
        ts.append(t * 1000.0)
    res = {"ms": db.summarize(ts), "rows": len(data), "explain": explain(conn, sql, params)}
    return res, data


def bench_config(ctx, conn, label, kind, table, P, extra=()):
    out = {}
    for q in ("q1", "q1b", "q2", "q3", "q4") + tuple(extra):
        res, data = bench_query(conn, queries.raw_query(kind, table, q), P, ctx.reps, ctx.warmup)
        res["fingerprint"] = queries.fingerprint(q.split("_")[0], data)
        out[q] = res
        log(f"    {label:14s} {q:7s} медиана {res['ms']['median']:9.2f} мс  строк {res['rows']:6d}  "
            f"чанков {res['explain']['chunks']:3d}  планирование {res['explain']['planning_ms']:.1f} мс")
    return out


def fp_equal(a, b, tol=1e-9):
    if a["rows"] != b["rows"]:
        return False
    if a["check"] is None or b["check"] is None:
        return a["check"] == b["check"]
    return abs(a["check"] - b["check"]) <= tol * max(1.0, abs(a["check"]))


def cross_check(configs):
    ref_name = next(iter(configs))
    bad = []
    for name, res in configs.items():
        for q in ("q1", "q1b", "q2", "q3", "q4"):
            if not fp_equal(configs[ref_name][q]["fingerprint"], res[q]["fingerprint"]):
                bad.append((name, q))
    return {"reference": ref_name, "mismatches": bad, "ok": not bad}


def environment(ctx, conn):
    def show(name):
        return db.one(conn, f"SHOW {name}")
    cpu = "?"
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    mem = None
    try:
        for line in open("/proc/meminfo"):
            if line.startswith("MemTotal"):
                mem = int(line.split()[1]) // 1024
                break
    except OSError:
        pass
    return {"cpu_model": cpu, "cpu_count": os.cpu_count(), "mem_mb": mem, "platform": platform.platform(),
            "python": platform.python_version(), "postgres": db.one(conn, "SELECT version()").split(",")[0],
            "timescaledb": db.one(conn, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"),
            "settings": {k: show(k) for k in ("shared_buffers", "work_mem", "effective_cache_size", "max_wal_size",
                                              "jit", "max_parallel_workers_per_gather", "timescaledb.max_background_workers")},
            "scale": ctx.scale, "spec": asdict(ctx.spec), "sub_spec": asdict(ctx.sub_spec),
            "chunk_interval": ctx.chunk_interval, "reps": ctx.reps, "warmup": ctx.warmup, "keep_days": ctx.keep_days,
            "s3": os.environ.get("MINIO_ENDPOINT", "localhost:9000")}


def e1(ctx):
    log("E1: схемы, hypertable, загрузка")
    conn = db.connect()
    schema.create_dictionaries(conn)
    schema.drop_cagg(conn)
    anchor = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    ctx.R["meta"] = {**environment(ctx, conn), "anchor": anchor.isoformat(), "started": time.strftime("%Y-%m-%d %H:%M:%S")}
    t = time.perf_counter()
    ds = Dataset(ctx.spec, int(anchor.timestamp()) - ctx.spec.days * 86400)
    gen_s = time.perf_counter() - t
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO metrics VALUES (%s, %s, %s)", [(i + 1, m, UNITS[m]) for i, m in enumerate(METRICS)])
        cur.executemany("INSERT INTO devices VALUES (%s, %s, %s)", [(d, *device_meta(d)) for d in range(1, ctx.spec.devices + 1)])
    out = {"gen_seconds": gen_s, "readings": ctx.spec.readings, "values": ctx.spec.values, "anchor": anchor.isoformat(),
           "anomalies": int(ds.is_anomaly.sum())}
    log(f"  данные: {ctx.spec.devices} устройств × {ctx.spec.days} сут × {86400 // ctx.spec.interval_s} в сутки = "
        f"{ctx.spec.readings:,} показаний ({ctx.spec.values:,} значений), выбросов {out['anomalies']}; генерация {gen_s:.1f} с")
    for kind in ("wide", "narrow"):
        table = f"telemetry_{kind}"
        schema.create_hypertable(conn, kind, table, ctx.chunk_interval)
        r = load.load(db.connect, kind, table, ds, ctx.slice_days,
                      progress=lambda i, n, k=kind: log(f"    {k}: {n:,} строк") if i and i % 90 == 0 else None)
        schema.analyze(conn, table)
        sz = schema.sizes(conn, table)
        n = db.one(conn, f"SELECT count(*) FROM {table}")
        assert n == r["rows"], (table, n, r["rows"])
        idx = {name: int(db.one(conn, f"SELECT hypertable_index_size('{name}')"))
               for (name,) in db.rows(conn, "SELECT indexname FROM pg_indexes WHERE tablename = %s ORDER BY indexname", (table,))}
        out[kind] = {"load": r, "size": sz, "rows": n, "bytes_per_reading": sz["total_bytes"] / ctx.spec.readings,
                     "bytes_per_row": sz["total_bytes"] / n, "indexes": idx}
        log(f"  {kind:6s}: {n:,} строк за {r['seconds']:.1f} с ({r['rows_per_s']:,.0f} строк/с; БД {r['db_seconds']:.1f} с), "
            f"{sz['total_bytes'] / 1e6:,.0f} МБ, чанков {sz['chunks']}, {out[kind]['bytes_per_reading']:.0f} Б/показание")
    pj = db.rows(conn, """SELECT avg(pg_column_size(payload)), avg(length(payload::text)) FROM
        (SELECT payload FROM telemetry_wide WHERE ts >= %s ORDER BY ts DESC LIMIT 20000) s""", (anchor - timedelta(days=1),))[0]
    out["jsonb_bytes"], out["json_text_chars"] = float(pj[0]), float(pj[1])
    ctx.R["e1"] = out
    ctx.save()


def e2(ctx):
    log("E2: запросы на несжатых данных (конфигурации 1 и 2)")
    conn = db.connect()
    P = ctx.params(ctx.spec.days, ctx.device)
    cfg = {"wide_raw": bench_config(ctx, conn, "wide, raw", "wide", "telemetry_wide", P, extra=("q3_opt",)),
           "narrow_raw": bench_config(ctx, conn, "narrow, raw", "narrow", "telemetry_narrow", P)}
    ctx.R["e2"] = {"configs": cfg, "params": {k: v for k, v in P.items()}, "check": cross_check(cfg)}
    log(f"  сверка результатов wide/narrow: {'OK' if ctx.R['e2']['check']['ok'] else ctx.R['e2']['check']['mismatches']}")
    ctx.save()


def compress_table(ctx, conn, kind, table, segmentby=None, orderby="ts DESC"):
    before = schema.sizes(conn, table)
    schema.set_compression(conn, table, segmentby or schema.SEGMENTBY[kind], orderby)
    r = schema.compress_all(conn, table)
    schema.analyze(conn, table)
    after = schema.sizes(conn, table)
    stats = schema.compression_stats(conn, table)
    if r["chunks"] == 0 and stats and stats["before_total"]:
        before = {**before, "total_bytes": stats["before_total"]}
    return {"before": before, "after": after, "stats": stats, "compress": r,
            "ratio": before["total_bytes"] / after["total_bytes"] if after["total_bytes"] else None}


def e3(ctx):
    log("E3: компрессия (segmentby=device_id, orderby=ts DESC)")
    conn = db.connect()
    out = {}
    readings = ctx.spec.readings
    for kind in ("wide", "narrow"):
        table = f"telemetry_{kind}"
        r = compress_table(ctx, conn, kind, table)
        r["bytes_per_reading_before"] = r["before"]["total_bytes"] / readings
        r["bytes_per_reading_after"] = r["after"]["total_bytes"] / readings
        out[kind] = r
        log(f"  {kind:6s}: {r['before']['total_bytes'] / 1e6:,.0f} МБ -> {r['after']['total_bytes'] / 1e6:,.0f} МБ, "
            f"коэффициент {r['ratio']:.1f}, сжатие заняло {r['compress']['seconds']:.1f} с, {r['bytes_per_reading_after']:.1f} Б/показание")
    P = ctx.params(ctx.spec.days, ctx.device)
    cfg = {"wide_comp": bench_config(ctx, conn, "wide, compr.", "wide", "telemetry_wide", P, extra=("q3_opt",)),
           "narrow_comp": bench_config(ctx, conn, "narrow, compr.", "narrow", "telemetry_narrow", P)}
    allc = {**ctx.R.get("e2", {}).get("configs", {}), **cfg}
    out["configs"] = cfg
    out["check"] = cross_check({k: v for k, v in allc.items()})
    log(f"  сверка результатов всех 4 конфигураций: {'OK' if out['check']['ok'] else out['check']['mismatches']}")
    ctx.R["e3"] = out
    ctx.save()


def _bucket_floor(ts, level):
    if level == "1m":
        return ts.replace(second=0, microsecond=0)
    if level == "1h":
        return ts.replace(minute=0, second=0, microsecond=0)
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)


def verify_cagg(conn, level, P, table="telemetry_narrow"):
    v = schema.CAGG_NAMES[level]
    res = {}
    for q, w0, w1 in (("q2", "t_day0", "t_day1"), ("q4", "t_year0", "t_year1")):
        if q == "q4" and level == "1m" and (P["t_year1"] - P["t_year0"]).days > (P["t_mon1"] - P["t_mon0"]).days:
            continue
        dev = "AND device_id = %(d)s" if q == "q4" else ""
        a = db.rows(conn, f"SELECT sum(sum_v), sum(cnt) FROM {v} WHERE metric_id = 1 {dev} AND bucket >= %({w0})s AND bucket < %({w1})s", P)[0]
        b = db.rows(conn, f"SELECT sum(value), count(*) FROM {table} WHERE metric_id = 1 {dev} AND ts >= %({w0})s AND ts < %({w1})s", P)[0]
        res[q] = {"cagg": [float(a[0]), int(a[1])], "raw": [float(b[0]), int(b[1])],
                  "ok": a[1] == b[1] and abs(float(a[0]) - float(b[0])) <= 1e-9 * max(1.0, abs(float(b[0])))}
    raw = db.rows(conn, f"SELECT device_id, ts FROM {table} WHERE metric_id = 1 AND ts >= %(t_mon0)s AND ts < %(t_mon1)s "
                        f"AND (value > %(hi)s OR value < %(lo)s)", P)
    want = {(d, _bucket_floor(t, level)) for d, t in raw}
    got = {(d, b) for d, b in db.rows(conn, f"SELECT device_id, bucket FROM {v} WHERE metric_id = 1 AND bucket >= %(t_mon0)s "
                                            f"AND bucket < %(t_mon1)s AND (max_v > %(hi)s OR min_v < %(lo)s)", P)}
    res["q3"] = {"raw_buckets": len(want), "cagg_buckets": len(got), "ok": want == got}
    return res


def e4(ctx):
    log("E4: continuous aggregates 1 мин / 1 ч / 1 сут")
    conn = db.connect()
    P = ctx.params(ctx.spec.days, ctx.device)
    a = ctx.anchor
    win_mon = min(30, ctx.spec.days)
    schema.create_caggs(conn)
    out = {"levels": {}, "queries": {}, "verify": {}, "window_1m_days": win_mon}
    raw_rows = ctx.spec.values
    plan = (("1m", a - timedelta(days=win_mon), a), ("1h", None, None), ("1d", None, None))
    for lvl, s, e in plan:
        sec = schema.refresh_cagg(conn, lvl, s, e)
        st = schema.cagg_stats(conn, lvl)
        covered = raw_rows if lvl != "1m" else ctx.spec.devices * len(METRICS) * (86400 // ctx.spec.interval_s) * win_mon
        out["levels"][lvl] = {"refresh_seconds": sec, **st, "raw_rows_covered": covered,
                              "reduction": covered / st["rows"] if st["rows"] else None}
        log(f"  {lvl}: обновление {sec:.1f} с, строк {st['rows']:,} ({st['bytes'] / 1e6:.1f} МБ), сжатие числа строк ×{out['levels'][lvl]['reduction']:.1f}")
    for lvl in ("1m", "1h", "1d"):
        out["queries"][lvl] = {}
        for q in ("q2", "q3", "q4"):
            if q == "q4" and lvl == "1m" and ctx.spec.days > win_mon:
                continue
            res, data = bench_query(conn, queries.cagg_query(lvl, q), P, ctx.reps, ctx.warmup)
            out["queries"][lvl][q] = res
            log(f"    CAGG {lvl} {q}: медиана {res['ms']['median']:8.2f} мс, строк {res['rows']}, планирование {res['explain']['planning_ms']:.1f} мс")
        out["verify"][lvl] = verify_cagg(conn, lvl, P)
        ok = all(v["ok"] for v in out["verify"][lvl].values())
        log(f"    сверка уровня {lvl} с сырыми данными: {'OK' if ok else out['verify'][lvl]}")
    ctx.R["e4"] = out
    ctx.save()


VARIANTS = [("wide", "s_wide", "device_id", "wide: JSONB, segmentby=device_id", True),
            ("flat", "s_flat", "device_id", "wide: типизированные колонки, segmentby=device_id", True),
            ("narrow", "s_narrow_dev", "device_id", "narrow: segmentby=device_id", True),
            ("narrow", "s_narrow_devmet", "device_id,metric_id", "narrow: segmentby=device_id,metric_id", True),
            ("wide", "s_wide_nj", "device_id", "wide: JSONB, метки времени без джиттера", False),
            ("narrow", "s_narrow_devmet_nj", "device_id,metric_id", "narrow: segmentby=device_id,metric_id, без джиттера", False)]
BREAKDOWN_COLS = {"wide": ["ts", "payload"], "narrow": ["ts", "value"], "flat": ["ts", "temperature", "humidity", "battery_mv", "rssi_dbm"]}


def e5(ctx):
    log("E5: разбор причин сжатия на подмножестве (схемы, segmentby, метки времени)")
    conn = db.connect()
    sp = ctx.sub_spec
    out = {"spec": asdict(sp), "variants": {}}
    for kind, table, seg, title, jitter in VARIANTS:
        ds = ctx.sub_dataset(jitter)
        schema.create_hypertable(conn, kind, table, ctx.chunk_interval)
        r = load.load(db.connect, kind, table, ds, ctx.slice_days)
        schema.analyze(conn, table)
        c = compress_table(ctx, conn, kind, table, segmentby=seg)
        c.update({"title": title, "kind": kind, "segmentby": seg, "jitter": jitter, "load": r, "readings": sp.readings,
                  "bytes_per_reading_before": c["before"]["total_bytes"] / sp.readings,
                  "bytes_per_reading_after": c["after"]["total_bytes"] / sp.readings})
        c["columns"] = schema.column_breakdown(conn, table, BREAKDOWN_COLS[kind], by_metric=(kind == "narrow" and "metric_id" in seg))
        if table in ("s_wide", "s_flat", "s_narrow_dev"):
            c["queries"] = bench_config(ctx, conn, kind, kind, table, ctx.params(sp.days, ctx.sub_device))
        out["variants"][table] = c
        log(f"  {title:62s} {c['before']['total_bytes'] / 1e6:7.1f} -> {c['after']['total_bytes'] / 1e6:6.1f} МБ  "
            f"×{c['ratio']:.1f}  {c['bytes_per_reading_after']:.1f} Б/показание")
    ctx.R["e5"] = out
    ctx.save()
    schema.drop_all(conn, [v[1] for v in VARIANTS])


def e6(ctx):
    log("E6: размер чанка (chunk_time_interval) на подмножестве, узкая схема")
    conn = db.connect()
    ds = ctx.sub_dataset()
    sp = ctx.sub_spec
    P = ctx.params(sp.days, ctx.sub_device)
    out = {"spec": asdict(sp), "runs": {}}
    tables = []
    for ci in ctx.chunk_intervals:
        table = "s_chunk_" + re.sub(r"\W+", "", ci)
        tables.append(table)
        schema.create_hypertable(conn, "narrow", table, ci)
        r = load.load(db.connect, "narrow", table, ds, ctx.slice_days)
        schema.analyze(conn, table)
        sz = schema.sizes(conn, table)
        log(f"  чанк {ci}: загрузка {r['seconds']:.1f} с, чанков {sz['chunks']}, {sz['total_bytes'] / 1e6:.0f} МБ")
        raw_q = bench_config(ctx, conn, f"{ci} raw", "narrow", table, P)
        c = compress_table(ctx, conn, "narrow", table)
        comp_q = bench_config(ctx, conn, f"{ci} compr.", "narrow", table, P)
        out["runs"][ci] = {"table": table, "load": r, "chunks": sz["chunks"], "size_raw": sz, "compress": c,
                           "queries_raw": raw_q, "queries_comp": comp_q,
                           "rows_per_chunk": sp.values / max(sz["chunks"], 1),
                           "raw_mb_per_chunk": sz["total_bytes"] / 1e6 / max(sz["chunks"], 1)}
        log(f"    сжатие ×{c['ratio']:.1f} ({c['compress']['seconds']:.1f} с)")
    ctx.R["e6"] = out
    ctx.save()
    schema.drop_all(conn, tables)


def e7(ctx):
    log("E7: холодный срез -> Parquet -> MinIO")
    conn = db.connect()
    keep = ctx.keep_days
    row = db.rows(conn, """SELECT min(range_start), max(range_end) FROM timescaledb_information.chunks
                           WHERE hypertable_name = 'telemetry_narrow' AND range_end < now() - make_interval(days => %s)""", (keep,))[0]
    if row[0] is None:
        ctx.R["e7"] = {"skipped": "нет чанков старше keep_days"}
        log("  нет чанков старше порога - пропуск")
        ctx.save()
        return
    cold_start, cold_end = row
    t_first = db.one(conn, "SELECT min(ts) FROM telemetry_narrow")
    n_db, sum_db = db.rows(conn, "SELECT count(*), sum(value) FROM telemetry_narrow WHERE ts < %s", (cold_end,))[0]
    nw_db = db.one(conn, "SELECT count(*) FROM telemetry_wide WHERE ts < %s", (cold_end,))
    dbsz = {}
    for tbl in ("telemetry_narrow", "telemetry_wide"):
        r = db.rows(conn, f"""SELECT coalesce(sum(cs.before_compression_total_bytes), 0), coalesce(sum(cs.after_compression_total_bytes), 0),
                              count(*) FROM chunk_compression_stats('{tbl}') cs
                              JOIN timescaledb_information.chunks c ON c.chunk_schema = cs.chunk_schema AND c.chunk_name = cs.chunk_name
                              WHERE c.range_end < now() - make_interval(days => %s)""", (keep,))[0]
        dbsz[tbl] = {"raw_bytes": int(r[0]), "compressed_bytes": int(r[1]), "chunks": int(r[2])}
    out = {"cold_start": cold_start, "cold_end": cold_end, "keep_days": keep, "rows_narrow": int(n_db), "rows_wide": int(nw_db),
           "db": dbsz, "variants_bytes": {v: 0 for v in export.VARIANTS}, "wide_json_parquet_bytes": 0, "arrow_bytes": 0,
           "months": []}
    use_s3 = False
    if not ctx.no_minio:
        for _ in range(15):
            if export.minio_available():
                use_s3 = True
                break
            time.sleep(2)
    client = None
    if use_s3:
        client = export.minio_client()
        export.ensure_bucket(client, ctx.bucket)
        export.clear_prefix(client, ctx.bucket, "narrow/")
    else:
        log("  S3/MinIO недоступен - выгрузка только в память (размеры считаются, загрузки нет)")
    t0 = time.perf_counter()
    rows_pq, sum_pq = 0, 0.0
    uploaded = 0
    for label, m0, m1 in export.month_ranges(t_first, cold_end):
        tbl = export.fetch_narrow(conn, "telemetry_narrow", m0, m1)
        rows_pq += tbl.num_rows
        sum_pq += float(__import__("pyarrow.compute", fromlist=["sum"]).sum(tbl.column("value")).as_py())
        out["arrow_bytes"] += tbl.nbytes
        best = None
        for v in export.VARIANTS:
            data = export.to_parquet_bytes(tbl, v)
            out["variants_bytes"][v] += len(data)
            if v == "zstd_sorted":
                best = data
        wide = export.fetch_wide_json(conn, "telemetry_wide", m0, m1)
        out["wide_json_parquet_bytes"] += len(export.wide_to_parquet_bytes(wide))
        if client is not None:
            export.upload_bytes(client, ctx.bucket, f"narrow/month={label}/part-0.parquet", best)
            uploaded += len(best)
        out["months"].append({"month": label, "rows": tbl.num_rows, "parquet_bytes": len(best)})
        log(f"    {label}: {tbl.num_rows:,} строк, Parquet {len(best) / 1e6:.1f} МБ")
    out["export_seconds"] = time.perf_counter() - t0
    out["rows_parquet"], out["sum_parquet"], out["sum_db"] = rows_pq, sum_pq, float(sum_db)
    out["rows_match"] = rows_pq == int(n_db)
    out["sum_match"] = abs(sum_pq - float(sum_db)) <= 1e-9 * max(1.0, abs(float(sum_db)))
    if client is not None:
        n_obj, size = export.list_size(client, ctx.bucket, "narrow/")
        out["s3"] = {"objects": n_obj, "bytes": size, "uploaded_bytes": uploaded, "bucket": ctx.bucket}
        m = out["months"][-1]["month"] if len(out["months"]) < 2 else out["months"][-2]["month"]
        tbl, sec = export.read_month(ctx.bucket, m, ctx.device, 1)
        import pyarrow.compute as pc
        mean_pq = float(pc.mean(tbl.column("value")).as_py())
        y, mo = int(m[:4]), int(m[5:])
        m0 = datetime(y, mo, 1, tzinfo=timezone.utc)
        m1 = (m0 + timedelta(days=32)).replace(day=1)
        mean_db = db.one(conn, "SELECT avg(value) FROM telemetry_narrow WHERE device_id = %s AND metric_id = 1 AND ts >= %s AND ts < %s",
                         (ctx.device, max(m0, t_first), min(m1, cold_end)))
        out["readback"] = {"month": m, "device": ctx.device, "rows": tbl.num_rows, "seconds": sec, "mean_parquet": mean_pq,
                           "mean_db": float(mean_db), "ok": abs(mean_pq - float(mean_db)) < 1e-9 * max(1.0, abs(float(mean_db)))}
        log(f"  чтение обратно из S3 ({m}, устройство {ctx.device}): {tbl.num_rows} строк за {sec * 1000:.0f} мс, "
            f"среднее совпало: {out['readback']['ok']}")
    log(f"  срез: {n_db:,} строк; БД (несжатая) {dbsz['telemetry_narrow']['raw_bytes'] / 1e6:,.0f} МБ, сжатая {dbsz['telemetry_narrow']['compressed_bytes'] / 1e6:,.0f} МБ; "
        f"Parquet {out['variants_bytes']['zstd_sorted'] / 1e6:,.0f} МБ; строк/сумма совпали: {out['rows_match']}/{out['sum_match']}")
    ctx.R["e7"] = out
    ctx.save()


def _chunk_info(conn, table):
    r = db.rows(conn, "SELECT count(*), min(range_start), max(range_end) FROM timescaledb_information.chunks WHERE hypertable_name = %s", (table,))[0]
    return {"chunks": int(r[0]), "range_start": r[1], "range_end": r[2]}


def e8(ctx):
    log(f"E8: retention (хранить {ctx.keep_days} сут.) и фоновые политики")
    conn = db.connect()
    out = {"keep_days": ctx.keep_days, "tables": {}}
    before = {t: {**_chunk_info(conn, t), "size": schema.sizes(conn, t), "min_ts": db.one(conn, f"SELECT min(ts) FROM {t}")}
              for t in ("telemetry_narrow", "telemetry_wide")}
    cagg_before = schema.cagg_stats(conn, "1d") if db.one(conn, "SELECT to_regclass('cagg_narrow_1d')") else None
    cagg_min = db.one(conn, "SELECT min(bucket) FROM cagg_narrow_1d") if cagg_before else None
    pol = schema.add_policies(conn, ctx.keep_days)
    out["policies"] = pol
    out["jobs"] = schema.list_jobs(conn)
    log(f"  создано политик: {len(pol)}")
    for t in ("telemetry_narrow", "telemetry_wide"):
        jid = db.one(conn, "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention' AND hypertable_name = %s", (t,))
        t0 = time.perf_counter()
        db.run(conn, "CALL run_job(%s)", (jid,))
        sec = time.perf_counter() - t0
        after = {**_chunk_info(conn, t), "size": schema.sizes(conn, t), "min_ts": db.one(conn, f"SELECT min(ts) FROM {t}")}
        out["tables"][t] = {"job_id": jid, "run_seconds": sec, "before": before[t], "after": after,
                            "dropped_chunks": before[t]["chunks"] - after["chunks"]}
        log(f"  {t}: чанков {before[t]['chunks']} -> {after['chunks']}, самые старые данные: {before[t]['min_ts']:%Y-%m-%d} -> {after['min_ts']:%Y-%m-%d}, "
            f"размер {before[t]['size']['total_bytes'] / 1e6:,.0f} -> {after['size']['total_bytes'] / 1e6:,.0f} МБ")
    if cagg_before:
        cagg_after = schema.cagg_stats(conn, "1d")
        out["cagg_1d"] = {"before": cagg_before, "after": cagg_after, "min_bucket_before": cagg_min,
                          "min_bucket_after": db.one(conn, "SELECT min(bucket) FROM cagg_narrow_1d"),
                          "intact": cagg_before["rows"] == cagg_after["rows"]}
        log(f"  агрегат 1 сут: строк {cagg_before['rows']:,} -> {cagg_after['rows']:,} (история сохранена: {out['cagg_1d']['intact']})")
    P = ctx.params(ctx.spec.days, ctx.device)
    raw_rows = db.timed(conn, queries.raw_query("narrow", "telemetry_narrow", "q4"), P)[1]
    out["q4_after"] = {"raw_rows": len(raw_rows), "year_from_cagg_rows": len(db.timed(conn, queries.cagg_query("1d", "q4"), P)[1]) if cagg_before else None}
    ctx.R["e8"] = out
    ctx.save()


def dashboard_queries(path):
    d = json.load(open(path, encoding="utf-8"))
    out = []
    for p in d.get("panels", []):
        for t in p.get("targets", []):
            out.append({"panel": p["title"], "sql": t["rawSql"]})
    return out


def render_grafana_sql(sql, t0, t1, device):
    sql = re.sub(r"\$__timeFilter\((\w+)\)", lambda m: f"{m.group(1)} >= '{t0.isoformat()}' AND {m.group(1)} < '{t1.isoformat()}'", sql)
    sql = sql.replace("${device}", str(device)).replace("$device", str(device))
    return sql


def e9(ctx):
    log("E9: запросы панелей Grafana (проверка на данных)")
    conn = db.connect()
    path = os.path.join(ROOT, "grafana", "dashboards", "telemetry.json")
    t1 = ctx.anchor
    t0 = t1 - timedelta(days=min(ctx.keep_days, ctx.spec.days))
    out = {"range": [t0, t1], "panels": []}
    for q in dashboard_queries(path):
        sql = render_grafana_sql(q["sql"], t0, t1, ctx.device)
        times = []
        for _ in range(3):
            t, data = db.timed(conn, sql)
            times.append(t * 1000)
        out["panels"].append({"panel": q["panel"], "rows": len(data), "ms": min(times)})
        log(f"  панель «{q['panel']}»: строк {len(data)}, {min(times):.1f} мс")
    ctx.R["e9"] = out
    ctx.save()


def _c(x, d=2):
    return "" if x is None else f"{x:.{d}f}".replace(".", ",")


def write_csv(ctx):
    R = ctx.R
    out = ctx.out

    def w(name, header, rows):
        with open(os.path.join(out, name), "w", encoding="utf-8-sig", newline="") as f:
            wr = csv.writer(f, delimiter=";")
            wr.writerow(header)
            wr.writerows(rows)
    if "e1" in R and "e3" in R:
        rows = []
        for kind in ("wide", "narrow"):
            e1_, e3_ = R["e1"][kind], R["e3"][kind]
            rows.append([kind, e1_["rows"], _c(e1_["load"]["seconds"], 1), _c(e1_["load"]["rows_per_s"], 0),
                         _c(e3_["before"]["total_bytes"] / 1e6, 1), _c(e3_["after"]["total_bytes"] / 1e6, 1), _c(e3_["ratio"], 2),
                         _c(e3_["bytes_per_reading_before"], 1), _c(e3_["bytes_per_reading_after"], 1), _c(e3_["compress"]["seconds"], 1)])
        w("size.csv", ["схема", "строк", "загрузка, с", "строк/с", "до сжатия, МБ", "после, МБ", "коэффициент", "Б/показание до",
                       "Б/показание после", "сжатие, с"], rows)
    cfgs = {}
    for k in ("e2", "e3"):
        cfgs.update(R.get(k, {}).get("configs", {}))
    if cfgs:
        rows = []
        for name, res in cfgs.items():
            for q, r in res.items():
                rows.append([name, q, _c(r["ms"]["median"], 2), _c(r["ms"]["min"], 2), r["rows"], r["explain"]["chunks"],
                             _c(r["explain"]["planning_ms"], 2), _c(r["explain"]["execution_ms"], 2)])
        w("queries.csv", ["конфигурация", "запрос", "медиана, мс", "минимум, мс", "строк", "чанков", "планирование, мс", "выполнение, мс"], rows)
    if "e4" in R:
        rows = []
        for lvl, qs in R["e4"]["queries"].items():
            for q, r in qs.items():
                rows.append([lvl, q, _c(r["ms"]["median"], 2), r["rows"], r["explain"]["chunks"]])
        w("cagg_queries.csv", ["уровень", "запрос", "медиана, мс", "строк", "чанков"], rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", default="full", choices=["smoke", "quick", "full"])
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default=os.path.join(ROOT, "results"))
    ap.add_argument("--exp", default=os.path.join(ROOT, "experiments.json"))
    ap.add_argument("--no-minio", action="store_true")
    ap.add_argument("--no-plots", action="store_true")
    a = ap.parse_args()
    db.wait_for_db()
    ctx = Ctx(a)
    todo = [s.strip() for s in a.only.split(",") if s.strip()] or EXPERIMENTS
    funcs = {"e1": e1, "e2": e2, "e3": e3, "e4": e4, "e5": e5, "e6": e6, "e7": e7, "e8": e8, "e9": e9}
    t_start = time.time()
    if "e1" not in todo and "meta" not in ctx.R:
        raise SystemExit("нет results.json с метой: сначала запустите e1 (или полный прогон)")
    for k in todo:
        funcs[k](ctx)
    ctx.R.setdefault("meta", {})["elapsed_s"] = ctx.R["meta"].get("elapsed_s", 0) + time.time() - t_start if a.only else time.time() - t_start
    ctx.save()
    write_csv(ctx)
    if not a.no_plots:
        try:
            from . import plots
            plots.make_all(ctx.R, a.out)
        except ImportError:
            pass
    log(f"Готово за {time.time() - t_start:.0f} с. Результаты: {a.out}")


if __name__ == "__main__":
    main()
