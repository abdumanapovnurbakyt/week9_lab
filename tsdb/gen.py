from dataclasses import dataclass

import numpy as np

METRICS = ("temperature", "humidity", "battery_mv", "rssi_dbm")
METRIC_ID = {name: i + 1 for i, name in enumerate(METRICS)}
UNITS = {"temperature": "°C", "humidity": "%", "battery_mv": "mV", "rssi_dbm": "dBm"}
SITES = 8
TYPES = ("temp-hum-A", "temp-hum-B", "temp-hum-C")
DAY = 86400


@dataclass(frozen=True)
class DatasetSpec:
    devices: int = 100
    interval_s: int = 600
    days: int = 365
    seed: int = 20261031
    anomaly_rate: float = 2e-4
    jitter_s: int = 10

    @property
    def per_device(self) -> int:
        return self.days * DAY // self.interval_s

    @property
    def readings(self) -> int:
        return self.devices * self.per_device

    @property
    def values(self) -> int:
        return self.readings * len(METRICS)


def device_meta(device_id: int):
    return f"site-{(device_id - 1) % SITES}", TYPES[(device_id - 1) % len(TYPES)]


class Dataset:
    def __init__(self, spec: DatasetSpec, t0: int):
        self.spec, self.t0 = spec, int(t0)
        D, n = spec.devices, spec.per_device
        self.device_ids = np.arange(1, D + 1, dtype=np.int32)
        self.ts = np.empty((D, n), dtype=np.int64)
        self.temperature = np.empty((D, n))
        self.humidity = np.empty((D, n))
        self.battery_mv = np.empty((D, n), dtype=np.int64)
        self.rssi_dbm = np.empty((D, n), dtype=np.int64)
        self.is_anomaly = np.zeros((D, n), dtype=bool)
        k = np.arange(n)
        t_days = k * spec.interval_s / DAY
        hour = (k * spec.interval_s % DAY) / 3600.0
        kernel = 0.9 ** np.arange(40)
        for i, dev in enumerate(self.device_ids):
            rng = np.random.default_rng([spec.seed, int(dev)])
            u1, u2 = rng.random(2)
            phase = (int(dev) * 37) % max(spec.interval_s - spec.jitter_s, 1)
            self.ts[i] = self.t0 + phase + k * spec.interval_s + rng.integers(0, spec.jitter_s, n)
            noise = np.convolve(rng.standard_normal(n + 39), kernel, "valid") * 0.22
            clean = (12 + 8 * u1) + 9 * np.sin(2 * np.pi * (t_days - 105) / 365.25) \
                + 3.5 * np.sin(2 * np.pi * (hour - 9) / 24) + noise
            temp = clean.copy()
            bad = rng.random(n) < spec.anomaly_rate
            high = rng.random(n) < 0.7
            temp[bad & high] = 55 + 15 * rng.random(int((bad & high).sum()))
            temp[bad & ~high] = -25 - 10 * rng.random(int((bad & ~high).sum()))
            self.is_anomaly[i] = bad
            self.temperature[i] = np.round(temp, 2)
            hum = 62 - 1.5 * (clean - 15) + 3 * rng.standard_normal(n)
            self.humidity[i] = np.round(np.clip(hum, 5, 99), 1)
            batt = (4150 - 30 * u2) - 850 * (k / n) + rng.normal(0, 3, n)
            self.battery_mv[i] = np.round(batt).astype(np.int64)
            rssi = -70 + 12 * (u1 - 0.5) + rng.normal(0, 4, n)
            self.rssi_dbm[i] = np.clip(np.round(rssi), -115, -40).astype(np.int64)

    @property
    def t_end(self) -> int:
        return self.t0 + self.spec.days * DAY

    def slices(self, readings_per_device: int):
        n = self.spec.per_device
        for k0 in range(0, n, readings_per_device):
            k1 = min(k0 + readings_per_device, n)
            ts = self.ts[:, k0:k1].ravel()
            order = np.argsort(ts, kind="stable")
            dev = np.repeat(self.device_ids, k1 - k0)[order]
            yield {"ts": ts[order], "device_id": dev,
                   "temperature": self.temperature[:, k0:k1].ravel()[order],
                   "humidity": self.humidity[:, k0:k1].ravel()[order],
                   "battery_mv": self.battery_mv[:, k0:k1].ravel()[order],
                   "rssi_dbm": self.rssi_dbm[:, k0:k1].ravel()[order]}


def iso_utc(ts_seconds: np.ndarray) -> list:
    return [s + "Z" for s in ts_seconds.astype("datetime64[s]").astype("U19").tolist()]
