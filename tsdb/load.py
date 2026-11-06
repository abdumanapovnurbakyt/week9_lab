import time

import numpy as np

from .gen import METRICS, iso_utc, DAY
from .schema import COPY_COLS


def _fmt_wide(s):
    ts = iso_utc(s["ts"])
    t, h, b, r = (s[k].tolist() for k in ("temperature", "humidity", "battery_mv", "rssi_dbm"))
    dev = s["device_id"].tolist()
    return [f'{a}\t{d}\t{{"temperature":{x},"humidity":{y},"battery_mv":{z},"rssi_dbm":{w}}}'
            for a, d, x, y, z, w in zip(ts, dev, t, h, b, r)]


def _fmt_narrow(s):
    ts = iso_utc(s["ts"])
    dev = s["device_id"].tolist()
    cols = [s[m].astype(np.float64).tolist() for m in METRICS]
    out = []
    ap = out.append
    for i, (a, d) in enumerate(zip(ts, dev)):
        pre = f"{a}\t{d}\t"
        ap(f"{pre}1\t{cols[0][i]}")
        ap(f"{pre}2\t{cols[1][i]}")
        ap(f"{pre}3\t{cols[2][i]}")
        ap(f"{pre}4\t{cols[3][i]}")
    return out


def _fmt_flat(s):
    ts = iso_utc(s["ts"])
    t, h, b, r = (s[k].tolist() for k in ("temperature", "humidity", "battery_mv", "rssi_dbm"))
    dev = s["device_id"].tolist()
    return [f"{a}\t{d}\t{x}\t{y}\t{z}\t{w}" for a, d, x, y, z, w in zip(ts, dev, t, h, b, r)]


FORMATTERS = {"wide": _fmt_wide, "narrow": _fmt_narrow, "flat": _fmt_flat}
ROWS_PER_READING = {"wide": 1, "narrow": len(METRICS), "flat": 1}


def load(conn_factory, kind, table, dataset, slice_days=1, progress=None):
    per_slice = max(1, slice_days * DAY // dataset.spec.interval_s)
    fmt = FORMATTERS[kind]
    conn = conn_factory(autocommit=False)
    rows_total = 0
    t_fmt = 0.0
    t_all = time.perf_counter()
    try:
        for i, s in enumerate(dataset.slices(per_slice)):
            t = time.perf_counter()
            data = ("\n".join(fmt(s)) + "\n").encode()
            t_fmt += time.perf_counter() - t
            with conn.cursor() as cur:
                with cur.copy(f"COPY {table} {COPY_COLS[kind]} FROM STDIN") as cp:
                    cp.write(data)
            conn.commit()
            rows_total += len(s["ts"]) * ROWS_PER_READING[kind]
            if progress:
                progress(i, rows_total)
    finally:
        conn.close()
    total = time.perf_counter() - t_all
    return {"rows": rows_total, "seconds": total, "format_seconds": t_fmt, "db_seconds": total - t_fmt,
            "rows_per_s": rows_total / total}
