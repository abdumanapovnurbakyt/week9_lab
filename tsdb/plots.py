import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Patch
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, NullLocator

SURFACE, INK, INK2, MUTED, GRID, BASE = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
LIGHT = {CAT[0]: "#a9c8ef", CAT[1]: "#f6bfa6", CAT[2]: "#a3dcc6", CAT[3]: "#f6dc9b"}
C_WIDE, C_NARROW, C_FLAT = CAT[1], CAT[0], CAT[2]
NB = " "

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 9.5, "text.color": INK,
    "axes.labelcolor": INK2, "axes.labelsize": 9.5, "xtick.color": MUTED, "ytick.color": MUTED,
    "xtick.labelcolor": INK2, "ytick.labelcolor": INK2, "axes.edgecolor": BASE,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "axes.grid.axis": "y",
    "grid.color": GRID, "grid.linewidth": 0.7, "axes.axisbelow": True,
    "axes.titlesize": 10.5, "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlepad": 9,
    "legend.frameon": False, "legend.fontsize": 9, "lines.linewidth": 1.7,
    "xtick.major.size": 3, "ytick.major.size": 3,
})


def ru(x, d=1):
    return f"{x:.{d}f}".replace(".", ",")


def n_fmt(v, _=None):
    return f"{int(round(v)):,}".replace(",", NB)


def ms_fmt(v, _=None):
    if v >= 100:
        return f"{v:.0f}"
    if v >= 10:
        return f"{v:.0f}"
    if v >= 1:
        return f"{v:g}".replace(".", ",")
    return f"{v:g}".replace(".", ",")


def log_axis(ax):
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=20))
    ax.yaxis.set_major_formatter(FuncFormatter(ms_fmt))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_minor_formatter(NullFormatter())


def save(fig, out, name):
    d = os.path.join(out, "figures")
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    fig.savefig(p, dpi=160)
    plt.close(fig)
    return p


def clean_axes(ax, grid_x=False):
    ax.spines["left"].set_color(BASE)
    ax.spines["bottom"].set_color(BASE)
    if grid_x:
        ax.grid(axis="x", color=GRID, lw=0.7)


def mb(x):
    return x / 1e6


def fig_architecture(R, out):
    fig = plt.figure(figsize=(11.4, 4.3))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 114)
    ax.set_ylim(0, 43)
    ax.axis("off")

    def box(x, y, w, h, txt, fc="#e8f0fb", ec=CAT[0], fs=8.6, bold=False):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.3,rounding_size=1.2", fc=fc, ec=ec, lw=1.2))
        ax.text(x + w / 2, y + h / 2, txt, ha="center", va="center", fontsize=fs, color=INK, linespacing=1.3,
                fontweight="bold" if bold else "normal")

    def arrow(x0, y0, x1, y1, txt=None, dx=0, dy=1.5):
        ax.add_patch(FancyArrowPatch((x0, y0), (x1, y1), arrowstyle="-|>", mutation_scale=11, color=INK2, lw=1.1))
        if txt:
            ax.text((x0 + x1) / 2 + dx, (y0 + y1) / 2 + dy, txt, ha="center", fontsize=8, color=INK2)

    box(1, 17, 21, 12, "bench\n(генератор + загрузчик)\nCOPY по суткам\nзамеры, сверка", fc="#e8f0fb")
    ax.add_patch(FancyBboxPatch((32, 4), 49, 36, boxstyle="round,pad=0.3,rounding_size=1.5", fc="none", ec=MUTED, lw=1.0, ls=(0, (4, 3))))
    ax.text(33.5, 37, "PostgreSQL 16 + TimescaleDB", fontsize=9, color=INK2, fontweight="bold")
    box(34, 24, 21, 10, "hypertable (чанки 7 сут)\nwide: payload JSONB\nnarrow: ts, device, metric, value", fc="#fdeee7", ec=CAT[1])
    box(58, 24, 21, 10, "компрессия\nsegmentby = device_id\norderby = ts DESC", fc="#fdeee7", ec=CAT[1])
    box(34, 7, 21, 13, "continuous aggregates\n1 мин → (сырые)\n1 ч → (сырые)\n1 сут → (из 1 ч)", fc="#e3f5ee", ec=CAT[2])
    box(58, 7, 21, 13, "retention\nadd_retention_policy\nудаление старых чанков", fc="#f6f4ea", ec=CAT[3])
    box(90, 24, 22, 12, "MinIO (S3)\nхолодный слой\nParquet по месяцам", fc="#e3f5ee", ec=CAT[2])
    box(90, 6, 22, 12, "Grafana\nдашборд из 4 панелей\n(SQL к агрегатам и таблице)", fc="#f6f4ea", ec=CAT[3])
    arrow(22.4, 23, 33.6, 29, "INSERT / COPY", dx=-3.5, dy=3.2)
    arrow(81.4, 30, 89.6, 30, "Parquet", dy=1.6)
    arrow(89.6, 12, 81.4, 12, "SQL", dy=1.6)
    return save(fig, out, "fig0_architecture.png")


CONF = [("wide_raw", "wide", False), ("wide_comp", "wide", True), ("narrow_raw", "narrow", False), ("narrow_comp", "narrow", True)]
CONF_LABEL = {"wide_raw": "wide\nбез сжатия", "wide_comp": "wide\nсжатая", "narrow_raw": "narrow\nбез сжатия", "narrow_comp": "narrow\nсжатая"}


def _color(kind, comp):
    c = {"wide": C_WIDE, "narrow": C_NARROW, "flat": C_FLAT}[kind]
    return c if comp else LIGHT[c]


def fig_size(R, out):
    e1, e3 = R["e1"], R["e3"]
    readings = e1["readings"]
    fig, axs = plt.subplots(1, 3, figsize=(12.4, 4.7), gridspec_kw={"width_ratios": [1.25, 1, 1]})
    ax = axs[0]
    sizes = {"wide_raw": e3["wide"]["before"], "wide_comp": e3["wide"]["after"], "narrow_raw": e3["narrow"]["before"],
             "narrow_comp": e3["narrow"]["after"]}
    x = np.arange(4)
    bottom = np.zeros(4)
    for key, lab, col in (("table_bytes", "данные (heap)", CAT[0]), ("index_bytes", "индексы", CAT[3]), ("toast_bytes", "TOAST", CAT[2])):
        v = np.array([mb(sizes[c][key]) for c, _, _ in CONF])
        ax.bar(x, v, 0.62, bottom=bottom, color=col, edgecolor=SURFACE, linewidth=1.2, label=lab)
        bottom += v
    for xi, tot in zip(x, bottom):
        ax.text(xi, tot * 1.01, f"{n_fmt(tot)}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([CONF_LABEL[c] for c, _, _ in CONF], fontsize=8.8)
    ax.set_ylabel("МБ (hypertable_size)")
    ax.set_title("A. Объём таблицы")
    ax.yaxis.set_major_formatter(FuncFormatter(n_fmt))
    ax.legend(loc="upper left")
    ax.set_ylim(0, bottom.max() * 1.12)
    clean_axes(ax)
    ax = axs[1]
    bpr = [sizes[c]["total_bytes"] / readings for c, _, _ in CONF]
    ax.bar(x, bpr, 0.62, color=[_color(k, comp) for _, k, comp in CONF], edgecolor=SURFACE)
    for xi, v in zip(x, bpr):
        ax.text(xi, v * 1.01, ru(v, 0 if v >= 100 else 1), ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax.set_xticks(x)
    lab = [CONF_LABEL[c] for c, _, _ in CONF]
    lab[1] += f"\n(×{ru(e3['wide']['ratio'], 1)})"
    lab[3] += f"\n(×{ru(e3['narrow']['ratio'], 1)})"
    ax.set_xticklabels(lab, fontsize=8.8)
    ax.set_ylabel("байт на показание (4 значения)")
    ax.set_title("B. Байт на одно показание")
    ax.set_ylim(0, max(bpr) * 1.14)
    clean_axes(ax)
    ax = axs[2]
    w, n = e1["wide"]["load"], e1["narrow"]["load"]
    xs = np.arange(2)
    v1 = [w["rows_per_s"], n["rows_per_s"]]
    v2 = [e1["wide"]["rows"] / w["seconds"], e1["readings"] / n["seconds"]]
    ax.bar(xs - 0.19, v1, 0.36, color=[LIGHT[C_WIDE], LIGHT[C_NARROW]], edgecolor=SURFACE)
    ax.bar(xs + 0.19, v2, 0.36, color=[C_WIDE, C_NARROW], edgecolor=SURFACE)
    for xi, a, b in zip(xs, v1, v2):
        ax.text(xi - 0.19, a, n_fmt(a), ha="center", va="bottom", fontsize=8.8)
        ax.text(xi + 0.19, b, n_fmt(b), ha="center", va="bottom", fontsize=8.8, fontweight="bold")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"wide\n{ru(w['seconds'], 0)} с", f"narrow\n{ru(n['seconds'], 0)} с"])
    ax.set_ylabel("в секунду")
    ax.set_title("C. Скорость загрузки (COPY)")
    ax.yaxis.set_major_formatter(FuncFormatter(n_fmt))
    ax.set_ylim(0, max(v1 + v2) * 1.15)
    ax.legend(handles=[Patch(color=MUTED, alpha=0.45, label="строк"), Patch(color=MUTED, label="показаний")], loc="upper left")
    clean_axes(ax)
    fig.subplots_adjust(left=0.065, right=0.99, top=0.9, bottom=0.14, wspace=0.28)
    return save(fig, out, "fig1_size.png")


def _cfgs(R):
    cfg = {}
    cfg.update(R["e2"]["configs"])
    cfg.update(R["e3"]["configs"])
    return cfg


def fig_queries(R, out):
    cfg = _cfgs(R)
    titles = {"q1": "Q1. Последнее значение (все метрики устройства)", "q2": "Q2. Среднее за сутки (по устройствам)",
              "q3": "Q3. Аномалии за месяц (весь парк)", "q4": "Q4. Один датчик за год"}
    fig, axs = plt.subplots(2, 2, figsize=(11.6, 7.4))
    x = np.arange(4)
    for ax, q in zip(axs.flat, ("q1", "q2", "q3", "q4")):
        vals = [cfg[c][q]["ms"]["median"] for c, _, _ in CONF]
        ax.bar(x, vals, 0.62, color=[_color(k, comp) for _, k, comp in CONF], edgecolor=SURFACE)
        for xi, v, (c, _, _) in zip(x, vals, CONF):
            ax.text(xi, v * 1.08, ru(v, 2 if v < 10 else (1 if v < 100 else 0)), ha="center", va="bottom", fontsize=9, fontweight="bold")
        if q == "q3" and "q3_opt" in cfg["wide_raw"]:
            for c, xi in (("wide_raw", 0), ("wide_comp", 1)):
                v = cfg[c]["q3_opt"]["ms"]["median"]
                ax.plot([xi], [v], marker="D", color=INK, ms=6, ls="none", zorder=5)
            ax.plot([], [], marker="D", color=INK, ls="none", label="wide, запрос с одним извлечением из JSONB")
            ax.legend(loc="upper right", fontsize=8.3)
        ax.set_yscale("log")
        ax.set_ylim(min(vals) * 0.45, max(vals) * 4)
        log_axis(ax)
        ax.set_xticks(x)
        ax.set_xticklabels([CONF_LABEL[c] for c, _, _ in CONF], fontsize=8.8)
        ax.set_ylabel("мс (лог. шкала)")
        ax.set_title(titles[q])
        rows = {c: cfg[c][q]["rows"] for c, _, _ in CONF}
        ax.set_xlabel(f"строк в ответе: {n_fmt(max(rows.values()))}", fontsize=8.8)
        clean_axes(ax)
    fig.legend(handles=[Patch(color=C_WIDE, label="wide"), Patch(color=C_NARROW, label="narrow"),
                        Patch(color=MUTED, alpha=0.45, label="без сжатия (светлые)"), Patch(color=MUTED, label="сжатая (яркие)")],
               loc="lower center", ncol=4, bbox_to_anchor=(0.5, 0.0))
    fig.subplots_adjust(left=0.07, right=0.99, top=0.95, bottom=0.12, wspace=0.2, hspace=0.4)
    return save(fig, out, "fig2_queries.png")


def fig_cagg(R, out):
    cfg = _cfgs(R)
    e4 = R["e4"]
    titles = {"q2": "Q2. Среднее за сутки", "q3": "Q3. Аномалии за месяц", "q4": "Q4. Один датчик за год"}
    labels = ["narrow\nбез сжатия", "narrow\nсжатая", "агрегат\n1 мин", "агрегат\n1 ч", "агрегат\n1 сут"]
    cols = [LIGHT[C_NARROW], C_NARROW, LIGHT[C_FLAT], "#5cc59e", C_FLAT]
    fig, axs = plt.subplots(1, 3, figsize=(12.4, 4.6))
    for ax, q in zip(axs, ("q2", "q3", "q4")):
        vals, rows = [cfg["narrow_raw"][q]["ms"]["median"], cfg["narrow_comp"][q]["ms"]["median"]], [cfg["narrow_raw"][q]["rows"], cfg["narrow_comp"][q]["rows"]]
        for lvl in ("1m", "1h", "1d"):
            r = e4["queries"].get(lvl, {}).get(q)
            vals.append(r["ms"]["median"] if r else np.nan)
            rows.append(r["rows"] if r else None)
        x = np.arange(5)
        ax.bar(x, np.nan_to_num(vals), 0.64, color=cols, edgecolor=SURFACE)
        for xi, v in zip(x, vals):
            if np.isnan(v):
                ax.text(xi, 0.6, "нет\n(30 сут)", ha="center", fontsize=8, color=MUTED)
            else:
                ax.text(xi, v * 1.08, ru(v, 2 if v < 10 else (1 if v < 100 else 0)), ha="center", va="bottom", fontsize=9, fontweight="bold")
        ax.set_yscale("log")
        fin = [v for v in vals if not np.isnan(v)]
        ax.set_ylim(min(fin) * 0.4, max(fin) * 4)
        log_axis(ax)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8.5)
        ax.set_ylabel("мс (лог. шкала)")
        ax.set_title(titles[q])
        t = cfg["narrow_raw"][q]["ms"]["median"]
        best = min(fin)
        ax.text(0.99, 0.97, f"быстрее в {ru(t / best, 0 if t / best >= 10 else 1)} раз\nстрок в ответе: {n_fmt(rows[0])} → {n_fmt(rows[4] if rows[4] else rows[3])}",
                transform=ax.transAxes, ha="right", va="top", fontsize=8.3, color=INK2)
        clean_axes(ax)
    fig.subplots_adjust(left=0.065, right=0.99, top=0.9, bottom=0.17, wspace=0.24)
    return save(fig, out, "fig3_cagg.png")


def fig_decomp(R, out):
    v = R["e5"]["variants"]
    order = [("s_wide", "wide (JSONB)"), ("s_flat", "wide (типизированные\nколонки)"), ("s_narrow_dev", "narrow\nsegmentby=device_id"),
             ("s_narrow_devmet", "narrow\nsegmentby=device_id,\nmetric_id"), ("s_wide_nj", "wide (JSONB),\nбез джиттера ts"),
             ("s_narrow_devmet_nj", "narrow (dev,metric),\nбез джиттера ts")]
    order = [(k, t) for k, t in order if k in v]
    fig, axs = plt.subplots(1, 2, figsize=(12.4, 4.9), gridspec_kw={"width_ratios": [1.6, 1]})
    ax = axs[0]
    y = np.arange(len(order))[::-1]
    segs = {"ts": [], "val": [], "oth": []}
    for k, _ in order:
        r = v[k]
        c = r["columns"]
        per = 4 if r["kind"] == "narrow" else 1
        total = r["bytes_per_reading_after"]
        if c:
            ts = c["bytes_per_row"]["ts"] * per
            val = sum(b for col, b in c["bytes_per_row"].items() if col != "ts") * per
        else:
            ts = val = 0
        segs["ts"].append(ts)
        segs["val"].append(val)
        segs["oth"].append(max(total - ts - val, 0))
    left = np.zeros(len(order))
    for key, lab, col in (("ts", "метки времени ts", CAT[0]), ("val", "значения метрик", CAT[1]), ("oth", "служебное: метаданные пакетов,\nTOAST, индексы, пустые страницы", CAT[3])):
        a = np.array(segs[key])
        ax.barh(y, a, 0.6, left=left, color=col, edgecolor=SURFACE, linewidth=1.2, label=lab)
        for yy, vv, ll in zip(y, a, left):
            if vv >= 3:
                ax.text(ll + vv / 2, yy, ru(vv, 1), ha="center", va="center", fontsize=8.6, fontweight="bold")
        left += a
    for yy, tot in zip(y, left):
        ax.text(tot + 0.8, yy, ru(tot, 1), va="center", fontsize=9, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels([t for _, t in order], fontsize=8.8)
    ax.set_xlabel("байт на одно показание (после сжатия)")
    ax.set_title("A. Из чего состоит сжатый объём")
    ax.set_xlim(0, left.max() * 1.1)
    ax.grid(axis="x", color=GRID, lw=0.7)
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower right", fontsize=8.3)
    clean_axes(ax)
    ax = axs[1]
    ratios = [v[k]["ratio"] for k, _ in order]
    cols = [{"wide": C_WIDE, "flat": C_FLAT, "narrow": C_NARROW}[v[k]["kind"]] for k, _ in order]
    ax.barh(y, ratios, 0.6, color=cols, edgecolor=SURFACE)
    for yy, r in zip(y, ratios):
        ax.text(r + 0.15, yy, f"×{ru(r, 1)}", va="center", fontsize=9, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels([])
    ax.set_xlabel("коэффициент: до сжатия / после")
    ax.set_title("B. Коэффициент сжатия")
    ax.set_xlim(0, max(ratios) * 1.18)
    ax.grid(axis="x", color=GRID, lw=0.7)
    ax.grid(axis="y", visible=False)
    clean_axes(ax)
    fig.subplots_adjust(left=0.15, right=0.99, top=0.92, bottom=0.14, wspace=0.05)
    return save(fig, out, "fig4_decomp.png")


def fig_chunks(R, out):
    runs = R["e6"]["runs"]
    cis = list(runs)
    labels = [f"{c.replace(' days', ' сут').replace(' day', ' сут')}\n{runs[c]['chunks']} чанков" for c in cis]
    x = np.arange(len(cis))
    fig, axs = plt.subplots(2, 2, figsize=(11.8, 7.6))
    ax = axs[0, 0]
    ratio = [runs[c]["compress"]["ratio"] for c in cis]
    comp = [mb(runs[c]["compress"]["after"]["total_bytes"]) for c in cis]
    ax.bar(x, comp, 0.6, color=C_NARROW, edgecolor=SURFACE)
    for xi, m, r in zip(x, comp, ratio):
        ax.text(xi, m * 1.02, f"{ru(m, 0)} МБ\n×{ru(r, 1)}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax.set_ylim(0, max(comp) * 1.3)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8.8)
    ax.set_ylabel("МБ после сжатия")
    ax.set_title("A. Объём после сжатия и коэффициент")
    clean_axes(ax)
    ax = axs[0, 1]
    for key, lab, mk, col in (("queries_raw", "без сжатия", "o", CAT[0]), ("queries_comp", "сжатые", "s", CAT[1])):
        ax.plot(x, [runs[c][key]["q4"]["explain"]["planning_ms"] for c in cis], marker=mk, ms=6, color=col, label=f"Q4, {lab}")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8.8)
    ax.set_ylabel("мс")
    ax.set_title("B. Время планирования запроса Q4")
    ax.legend(loc="upper right")
    clean_axes(ax)
    for ax, key, ttl in ((axs[1, 0], "queries_raw", "C. Время запросов, без сжатия"), (axs[1, 1], "queries_comp", "D. Время запросов, сжатые чанки")):
        for q, mk, col, lab in (("q2", "o", CAT[0], "Q2 сутки"), ("q3", "s", CAT[1], "Q3 месяц"), ("q4", "^", CAT[2], "Q4 один датчик")):
            ax.plot(x, [runs[c][key][q]["ms"]["median"] for c in cis], marker=mk, ms=6, color=col, label=lab)
        ax.set_yscale("log")
        log_axis(ax)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8.8)
        ax.set_ylabel("мс (лог. шкала)")
        ax.set_title(ttl)
        if key == "queries_raw":
            ax.legend(loc="upper left")
        else:
            ax.set_ylim(1.2, 110)
            ax.legend(loc="lower center", ncol=3, columnspacing=1.0, handletextpad=0.3)
        clean_axes(ax)
    fig.subplots_adjust(left=0.07, right=0.99, top=0.95, bottom=0.08, wspace=0.2, hspace=0.38)
    return save(fig, out, "fig5_chunks.png")


def fig_cold(R, out):
    e7 = R["e7"]
    if "skipped" in e7:
        return None
    d = e7["db"]
    items = [("БД, narrow, без сжатия", d["telemetry_narrow"]["raw_bytes"], LIGHT[C_NARROW]),
             ("БД, narrow, сжатая", d["telemetry_narrow"]["compressed_bytes"], C_NARROW),
             ("БД, wide, без сжатия", d["telemetry_wide"]["raw_bytes"], LIGHT[C_WIDE]),
             ("БД, wide, сжатая", d["telemetry_wide"]["compressed_bytes"], C_WIDE),
             ("Parquet narrow, snappy", e7["variants_bytes"]["snappy"], LIGHT[C_FLAT]),
             ("Parquet narrow, zstd", e7["variants_bytes"]["zstd"], "#5cc59e"),
             ("Parquet narrow, zstd + сортировка\n+ delta/byte-stream-split", e7["variants_bytes"]["zstd_sorted"], C_FLAT),
             ("Parquet wide (payload строкой), zstd", e7["wide_json_parquet_bytes"], "#7fd0b1")]
    fig, ax = plt.subplots(figsize=(10.6, 4.9))
    y = np.arange(len(items))[::-1]
    ax.barh(y, [mb(v) for _, v, _ in items], 0.62, color=[c for _, _, c in items], edgecolor=SURFACE)
    top = max(mb(v) for _, v, _ in items)
    for yy, (_, v, _) in zip(y, items):
        ax.text(mb(v) + top * 0.01, yy, f"{n_fmt(mb(v)) if mb(v) >= 10 else ru(mb(v), 1)} МБ", va="center", fontsize=9, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels([n for n, _, _ in items], fontsize=8.8)
    ax.set_xlabel(f"МБ; холодный срез: {n_fmt(e7['rows_narrow'])} значений (narrow) / {n_fmt(e7['rows_wide'])} показаний (wide), старше {e7['keep_days']} сут")
    ax.set_xlim(0, top * 1.14)
    ax.set_title("Объём холодного среза в БД и в Parquet")
    ax.grid(axis="x", color=GRID, lw=0.7)
    ax.grid(axis="y", visible=False)
    clean_axes(ax)
    fig.subplots_adjust(left=0.3, right=0.98, top=0.9, bottom=0.14)
    return save(fig, out, "fig6_cold.png")


def fig_retention(R, out):
    e8 = R["e8"]
    tabs = [("telemetry_narrow", "narrow", C_NARROW), ("telemetry_wide", "wide", C_WIDE)]
    fig, axs = plt.subplots(1, 2, figsize=(10.8, 4.4))
    for ax, key, ttl, unit in ((axs[0], "chunks", "A. Чанков в hypertable", ""), (axs[1], "size", "B. Размер, МБ", "")):
        x = np.arange(2)
        for i, (t, name, col) in enumerate(tabs):
            b, a = e8["tables"][t]["before"], e8["tables"][t]["after"]
            vb = b["chunks"] if key == "chunks" else mb(b["size"]["total_bytes"])
            va = a["chunks"] if key == "chunks" else mb(a["size"]["total_bytes"])
            xs = x[i]
            ax.bar(xs - 0.18, vb, 0.34, color=LIGHT[col], edgecolor=SURFACE)
            ax.bar(xs + 0.18, va, 0.34, color=col, edgecolor=SURFACE)
            ax.text(xs - 0.18, vb, n_fmt(vb), ha="center", va="bottom", fontsize=9)
            ax.text(xs + 0.18, va, n_fmt(va), ha="center", va="bottom", fontsize=9, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(["narrow", "wide"])
        ax.set_title(ttl)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.3)
        clean_axes(ax)
    axs[0].legend(handles=[Patch(color=MUTED, alpha=0.45, label="до retention"), Patch(color=MUTED, label=f"после (хранить {e8['keep_days']} сут)")], loc="upper center", ncol=2)
    fig.subplots_adjust(left=0.07, right=0.99, top=0.9, bottom=0.12, wspace=0.2)
    return save(fig, out, "fig7_retention.png")


FIGS = [("fig_architecture", None), ("fig_size", "e3"), ("fig_queries", "e3"), ("fig_cagg", "e4"), ("fig_decomp", "e5"),
        ("fig_chunks", "e6"), ("fig_cold", "e7"), ("fig_retention", "e8")]


def make_all(R, out):
    for name, need in FIGS:
        if need and need not in R:
            continue
        p = globals()[name](R, out)
        if p:
            print(f"    {os.path.relpath(p, out)}")


if __name__ == "__main__":
    here = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
    make_all(json.load(open(os.path.join(here, "results.json"), encoding="utf-8")), here)
