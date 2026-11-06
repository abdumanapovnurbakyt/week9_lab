import io
import os
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import numpy as np
import pyarrow.parquet as pq

from tsdb import bench, db, export, load, queries, schema
from tsdb.gen import Dataset, DatasetSpec, METRICS

TEST_DB = "lab6_test"
OK = []


def check(name, cond, info=""):
    assert cond, f"НЕ ПРОЙДЕНО: {name} {info}"
    OK.append(name)
    print(f"  ок  {name} {info}")


def setup_db():
    admin = db.connect(dbname="postgres")
    db.run(admin, f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
    db.run(admin, f"CREATE DATABASE {TEST_DB} TEMPLATE template0 ENCODING 'UTF8'")
    admin.close()
    os.environ["PGDATABASE"] = TEST_DB
    c = db.connect()
    db.run(c, "CREATE EXTENSION IF NOT EXISTS timescaledb")
    return c


def teardown_db():
    admin = db.connect(dbname="postgres")
    db.run(admin, f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")


def test_generator():
    spec = DatasetSpec(devices=6, days=12, seed=7)
    a, b = Dataset(spec, 1_700_000_000 - 1_700_000_000 % 86400), Dataset(spec, 1_700_000_000 - 1_700_000_000 % 86400)
    check("генератор детерминирован (одинаковые seed и параметры -> одинаковые данные)",
          np.array_equal(a.temperature, b.temperature) and np.array_equal(a.ts, b.ts))
    check("форма данных: устройства × показания", a.ts.shape == (6, spec.per_device) and spec.readings == 6 * 12 * 144)
    check("метки времени каждого устройства возрастают и лежат внутри периода",
          bool((np.diff(a.ts, axis=1) > 0).all()) and a.ts.min() >= a.t0 and a.ts.max() < a.t_end)
    ok = ~a.is_anomaly
    check("обычные показания внутри нормы температуры (-10..45 °C), влажность 5..99 %",
          bool(((a.temperature[ok] >= -10) & (a.temperature[ok] <= 45)).all()) and a.humidity.min() >= 5 and a.humidity.max() <= 99)
    bad = a.temperature[a.is_anomaly]
    check("выбросы всегда вне нормы (иначе запрос «аномалии» нельзя проверить)",
          bool(((bad > 45) | (bad < -10)).all()) if bad.size else True, f"(выбросов {bad.size})")
    big = Dataset(DatasetSpec(devices=50, days=60, seed=1), 1_700_000_000 - 1_700_000_000 % 86400)
    rate = big.is_anomaly.mean()
    check("доля выбросов близка к заданной 2e-4", 0.5e-4 < rate < 5e-4, f"({rate:.2e})")
    total = sum(len(s["ts"]) for s in a.slices(144))
    check("куски загрузки покрывают все показания, внутри куска ts отсортированы",
          total == spec.readings and all((np.diff(s["ts"]) >= 0).all() for s in a.slices(144)))


def test_database():
    c = setup_db()
    anchor = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    spec = DatasetSpec(devices=5, days=12, seed=11)
    ds = Dataset(spec, int(anchor.timestamp()) - spec.days * 86400)
    P = dict(d=3, t_day0=anchor - timedelta(days=1), t_day1=anchor, t_mon0=anchor - timedelta(days=12), t_mon1=anchor,
             t_year0=anchor - timedelta(days=12), t_year1=anchor, lo=-10, hi=45)
    tables = {"wide": "t_wide", "narrow": "t_narrow", "flat": "t_flat"}
    for kind, t in tables.items():
        schema.create_hypertable(c, kind, t, "7 days")
        load.load(db.connect, kind, t, ds)
        schema.analyze(c, t)
    n = {k: db.one(c, f"SELECT count(*) FROM {t}") for k, t in tables.items()}
    check("загрузка: wide = flat = числу показаний, narrow = 4 × показаний",
          n["wide"] == spec.readings == n["flat"] and n["narrow"] == 4 * spec.readings, f"({n})")
    check("t_wide / t_narrow / t_flat - hypertable с чанками по 7 суток",
          all(db.one(c, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = %s", (t,)) >= 2 for t in tables.values()))
    for mid, m in enumerate(METRICS, 1):
        bad = db.one(c, f"""SELECT count(*) FROM t_wide w JOIN t_narrow n ON n.ts = w.ts AND n.device_id = w.device_id AND n.metric_id = {mid}
                            WHERE (w.payload ->> '{m}')::float8 IS DISTINCT FROM n.value""")
        bad2 = db.one(c, f"""SELECT count(*) FROM t_flat f JOIN t_narrow n ON n.ts = f.ts AND n.device_id = f.device_id AND n.metric_id = {mid}
                             WHERE f.{m}::float8 IS DISTINCT FROM n.value""")
        check(f"данные метрики {m} совпадают в wide, narrow и flat", bad == 0 and bad2 == 0)
    js = db.one(c, "SELECT payload FROM t_wide ORDER BY ts LIMIT 1")
    check("payload - JSON с четырьмя ключами", set(js) == set(METRICS))

    def run_all(cfg):
        out = {}
        for kind, t in tables.items():
            out[f"{kind}_{cfg}"] = {}
            for q in ("q1", "q1b", "q2", "q3", "q4"):
                _, data = db.timed(c, queries.raw_query(kind, t, q), P)
                out[f"{kind}_{cfg}"][q] = {"fingerprint": queries.fingerprint(q, data)}
        return out
    raw = run_all("raw")
    check("результаты q1-q4 (и q1b) одинаковы в трёх схемах", bench.cross_check(raw)["ok"], str(bench.cross_check(raw)["mismatches"]))
    a3 = db.timed(c, queries.raw_query("wide", "t_wide", "q3_opt"), P)[1]
    b3 = db.timed(c, queries.raw_query("wide", "t_wide", "q3"), P)[1]
    check("оптимизированный q3 для JSONB даёт тот же результат", a3 == b3 and len(a3) > 0, f"(аномалий {len(a3)})")

    sizes_before = {k: schema.sizes(c, t)["total_bytes"] for k, t in tables.items()}
    for kind, t in tables.items():
        schema.set_compression(c, t, "device_id")
        schema.compress_all(c, t)
        schema.analyze(c, t)
    st = {k: schema.compression_stats(c, t) for k, t in tables.items()}
    check("все чанки сжаты", all(s["compressed_chunks"] == s["total_chunks"] for s in st.values()), str({k: (s['compressed_chunks'], s['total_chunks']) for k, s in st.items()}))
    check("после сжатия объём меньше (во всех схемах)", all(schema.sizes(c, t)["total_bytes"] < sizes_before[k] for k, t in tables.items()))
    comp = run_all("comp")
    check("результаты запросов после сжатия те же, что до", bench.cross_check({**raw, **comp})["ok"])
    br = schema.column_breakdown(c, "t_narrow", ["ts", "value"])
    check("разбор сжатых пакетов по колонкам даёт ненулевые размеры (ts, value)", br is None or (br["bytes_per_row"]["ts"] > 0 and br["bytes_per_row"]["value"] > 0))

    schema.create_caggs(c, "t_narrow")
    for lvl in ("1m", "1h", "1d"):
        schema.refresh_cagg(c, lvl, None, None)
    for lvl in ("1m", "1h", "1d"):
        v = bench.verify_cagg(c, lvl, P, "t_narrow")
        check(f"агрегат {lvl}: среднее, число значений и аномалии совпадают с сырыми данными", all(x["ok"] for x in v.values()), str(v))
    n1h, n1d = schema.cagg_stats(c, "1h")["rows"], schema.cagg_stats(c, "1d")["rows"]
    check("число строк агрегатов = число корзин: 1 ч -> 5·4·24·12, 1 сут -> 5·4·12",
          n1h == 5 * 4 * 24 * 12 and n1d == 5 * 4 * 12, f"({n1h}, {n1d})")
    last = anchor - timedelta(minutes=5)
    before = db.rows(c, "SELECT sum(cnt), sum(sum_v) FROM cagg_narrow_1h WHERE device_id = 1 AND metric_id = 1 AND bucket = date_trunc('hour', %s::timestamptz)", (last,))[0]
    db.run(c, "INSERT INTO t_narrow (ts, device_id, metric_id, value) VALUES (%s, 1, 1, 100.0)", (last,))
    schema.refresh_cagg(c, "1h", last - timedelta(hours=2), anchor)
    after = db.rows(c, "SELECT sum(cnt), sum(sum_v) FROM cagg_narrow_1h WHERE device_id = 1 AND metric_id = 1 AND bucket = date_trunc('hour', %s::timestamptz)", (last,))[0]
    check("после вставки и обновления окна агрегат подхватывает новую точку", after[0] == before[0] + 1 and abs(after[1] - before[1] - 100.0) < 1e-9)

    cb = db.one(c, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 't_narrow'")
    min_before = db.one(c, "SELECT min(ts) FROM t_narrow")
    jid = db.one(c, "SELECT add_retention_policy('t_narrow', INTERVAL '2 days')")
    d1d = schema.cagg_stats(c, "1d")["rows"]
    db.run(c, "CALL run_job(%s)", (jid,))
    ca = db.one(c, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 't_narrow'")
    oldest = db.one(c, "SELECT min(ts) FROM t_narrow")
    check("retention: старые чанки удалены, граница данных сдвинулась", ca < cb and oldest > min_before, f"(чанков {cb} -> {ca}, старейшая точка {oldest:%Y-%m-%d})")
    check("retention не трогает агрегаты (история сохранена)", schema.cagg_stats(c, "1d")["rows"] == d1d)
    jobs = schema.add_policies(c, 2, base="t_narrow", wide=None)
    check("политики созданы (обновление агрегатов, сжатие, retention)", len(jobs) >= 6 and len(schema.list_jobs(c)) >= 6)

    tbl = export.fetch_narrow(c, "t_narrow", datetime.now(timezone.utc) - timedelta(days=20), datetime.now(timezone.utc) + timedelta(days=1))
    exp_n, exp_sum = db.rows(c, "SELECT count(*), sum(value) FROM t_narrow")[0]
    check("выгрузка: число строк и сумма значений совпадают с БД",
          tbl.num_rows == exp_n and abs(float(__import__('pyarrow.compute', fromlist=['sum']).sum(tbl.column('value')).as_py()) - float(exp_sum)) < 1e-6 * abs(float(exp_sum)))
    sizes = {}
    for v in export.VARIANTS:
        data = export.to_parquet_bytes(tbl, v)
        r = pq.read_table(io.BytesIO(data))
        sizes[v] = len(data)
        ok = r.num_rows == tbl.num_rows and abs(r.column("value").to_numpy().sum() - tbl.column("value").to_numpy().sum()) < 1e-6 * abs(float(exp_sum))
        check(f"Parquet ({v}): чтение обратно даёт те же строки и сумму", ok, f"({len(data) / 1e3:.0f} КБ)")
    check("Parquet занимает меньше, чем те же данные в памяти (Arrow)", max(sizes.values()) < tbl.nbytes, f"({max(sizes.values()) / 1e3:.0f} КБ против {tbl.nbytes / 1e3:.0f} КБ)")
    if export.minio_available():
        client = export.minio_client()
        bucket = "lab6-test-" + str(int(time.time()))
        export.ensure_bucket(client, bucket)
        data = export.to_parquet_bytes(tbl, "zstd_sorted")
        export.upload_bytes(client, bucket, "narrow/month=test/part-0.parquet", data)
        n_obj, size = export.list_size(client, bucket, "narrow/")
        check("MinIO/S3: объект загружен, размер совпадает", n_obj == 1 and size == len(data))
        os.environ["MINIO_BUCKET"] = bucket
        t2 = pq.read_table(f"{bucket}/narrow/month=test/part-0.parquet", filesystem=export.s3_filesystem())
        check("MinIO/S3: чтение через pyarrow даёт те же строки", t2.num_rows == tbl.num_rows)
        client.remove_object(bucket, "narrow/month=test/part-0.parquet")
        client.remove_bucket(bucket)
    else:
        print("  --  MinIO/S3 недоступен - проверка загрузки в объектное хранилище пропущена")

    db.run(c, "CREATE VIEW telemetry_narrow AS SELECT * FROM t_narrow")
    path = os.path.join(HERE, "grafana", "dashboards", "telemetry.json")
    qs = bench.dashboard_queries(path)
    check("в дашборде 4 панели с SQL-запросами", len(qs) == 4, f"({[q['panel'][:28] for q in qs]})")
    t1 = datetime.now(timezone.utc)
    for q in qs:
        sql = bench.render_grafana_sql(q["sql"], t1 - timedelta(days=20), t1 + timedelta(days=1), 3)
        _, data = db.timed(c, sql)
        check(f"панель «{q['panel'][:40]}»: запрос выполняется и возвращает данные", len(data) > 0, f"(строк {len(data)})")
    c.close()


if __name__ == "__main__":
    db.wait_for_db()
    try:
        for name, fn in list(globals().items()):
            if name.startswith("test_") and callable(fn):
                print(f"[{name}]")
                fn()
    finally:
        try:
            teardown_db()
        except Exception as e:
            print("не удалось удалить временную базу:", e)
    print(f"\nПроверок: {len(OK)}")
    print("ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
