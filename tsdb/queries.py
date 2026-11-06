import json

from .schema import CAGG_NAMES

RAW = {
    "wide": {
        "q1": "SELECT ts, payload FROM {t} WHERE device_id = %(d)s ORDER BY ts DESC LIMIT 1",
        "q1b": "SELECT ts, payload FROM {t} WHERE device_id = %(d)s AND ts >= %(t_day0)s ORDER BY ts DESC LIMIT 1",
        "q2": """SELECT device_id, avg((payload ->> 'temperature')::float8) AS avg_t FROM {t}
                 WHERE ts >= %(t_day0)s AND ts < %(t_day1)s GROUP BY device_id ORDER BY device_id""",
        "q3": """SELECT device_id, ts, (payload ->> 'temperature')::float8 AS temperature FROM {t}
                 WHERE ts >= %(t_mon0)s AND ts < %(t_mon1)s
                   AND ((payload ->> 'temperature')::float8 > %(hi)s OR (payload ->> 'temperature')::float8 < %(lo)s)
                 ORDER BY ts, device_id""",
        "q4": """SELECT ts, (payload ->> 'temperature')::float8 AS temperature FROM {t}
                 WHERE device_id = %(d)s AND ts >= %(t_year0)s AND ts < %(t_year1)s ORDER BY ts""",
        "q3_opt": """SELECT device_id, ts, temperature FROM (
                       SELECT device_id, ts, (payload ->> 'temperature')::float8 AS temperature FROM {t}
                       WHERE ts >= %(t_mon0)s AND ts < %(t_mon1)s OFFSET 0) AS s
                     WHERE temperature > %(hi)s OR temperature < %(lo)s ORDER BY ts, device_id""",
    },
    "narrow": {
        "q1": """SELECT l.ts, m.metric_id, l.value FROM (VALUES (1), (2), (3), (4)) AS m(metric_id)
                 CROSS JOIN LATERAL (SELECT ts, value FROM {t} WHERE device_id = %(d)s AND metric_id = m.metric_id
                                     ORDER BY ts DESC LIMIT 1) AS l ORDER BY m.metric_id""",
        "q1b": """SELECT l.ts, m.metric_id, l.value FROM (VALUES (1), (2), (3), (4)) AS m(metric_id)
                  CROSS JOIN LATERAL (SELECT ts, value FROM {t} WHERE device_id = %(d)s AND metric_id = m.metric_id
                                      AND ts >= %(t_day0)s ORDER BY ts DESC LIMIT 1) AS l ORDER BY m.metric_id""",
        "q2": """SELECT device_id, avg(value) AS avg_t FROM {t}
                 WHERE metric_id = 1 AND ts >= %(t_day0)s AND ts < %(t_day1)s GROUP BY device_id ORDER BY device_id""",
        "q3": """SELECT device_id, ts, value AS temperature FROM {t}
                 WHERE metric_id = 1 AND ts >= %(t_mon0)s AND ts < %(t_mon1)s AND (value > %(hi)s OR value < %(lo)s)
                 ORDER BY ts, device_id""",
        "q4": """SELECT ts, value AS temperature FROM {t}
                 WHERE device_id = %(d)s AND metric_id = 1 AND ts >= %(t_year0)s AND ts < %(t_year1)s ORDER BY ts""",
    },
    "flat": {
        "q1": "SELECT ts, temperature, humidity, battery_mv, rssi_dbm FROM {t} WHERE device_id = %(d)s ORDER BY ts DESC LIMIT 1",
        "q1b": "SELECT ts, temperature, humidity, battery_mv, rssi_dbm FROM {t} WHERE device_id = %(d)s AND ts >= %(t_day0)s ORDER BY ts DESC LIMIT 1",
        "q2": """SELECT device_id, avg(temperature) AS avg_t FROM {t}
                 WHERE ts >= %(t_day0)s AND ts < %(t_day1)s GROUP BY device_id ORDER BY device_id""",
        "q3": """SELECT device_id, ts, temperature FROM {t}
                 WHERE ts >= %(t_mon0)s AND ts < %(t_mon1)s AND (temperature > %(hi)s OR temperature < %(lo)s)
                 ORDER BY ts, device_id""",
        "q4": """SELECT ts, temperature FROM {t}
                 WHERE device_id = %(d)s AND ts >= %(t_year0)s AND ts < %(t_year1)s ORDER BY ts""",
    },
}


def raw_query(kind, table, q):
    return RAW[kind][q].format(t=table)


def cagg_query(level, q):
    v = CAGG_NAMES[level]
    if q == "q2":
        return f"""SELECT device_id, sum(sum_v) / sum(cnt) AS avg_t FROM {v}
                   WHERE metric_id = 1 AND bucket >= %(t_day0)s AND bucket < %(t_day1)s GROUP BY device_id ORDER BY device_id"""
    if q == "q3":
        return f"""SELECT device_id, bucket, max_v, min_v FROM {v}
                   WHERE metric_id = 1 AND bucket >= %(t_mon0)s AND bucket < %(t_mon1)s AND (max_v > %(hi)s OR min_v < %(lo)s)
                   ORDER BY bucket, device_id"""
    if q == "q4":
        return f"""SELECT bucket, sum_v / cnt AS avg_t FROM {v}
                   WHERE device_id = %(d)s AND metric_id = 1 AND bucket >= %(t_year0)s AND bucket < %(t_year1)s ORDER BY bucket"""
    raise KeyError(q)


def fingerprint(q, data, source="raw"):
    n = len(data)
    if q.startswith("q1"):
        q = "q1"
    if q == "q1":
        if source == "raw" and n == 1 and isinstance(data[0][1], (dict, str)):
            p = data[0][1] if isinstance(data[0][1], dict) else json.loads(data[0][1])
            return {"rows": 4, "check": round(sum(float(p[k]) for k in ("temperature", "humidity", "battery_mv", "rssi_dbm")), 6)}
        if n == 1:
            return {"rows": 4, "check": round(sum(float(x) for x in data[0][1:5]), 6)}
        return {"rows": n, "check": round(sum(float(r[2]) for r in data), 6)}
    if q == "q2":
        return {"rows": n, "check": round(sum(float(r[1]) for r in data), 6)}
    if q == "q3":
        if source == "raw":
            return {"rows": n, "check": round(sum(float(r[2]) for r in data), 6)}
        return {"rows": n, "check": None}
    if q == "q4":
        return {"rows": n, "check": round(sum(float(r[1]) for r in data), 4)}
    raise KeyError(q)
