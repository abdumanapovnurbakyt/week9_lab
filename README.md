# Lab 6 – база временных рядов (PostgreSQL 16 + TimescaleDB)

Схема хранения телеметрии, загрузка 5,26 млн показаний, измерение объёма, компрессии, скорости запросов,
continuous aggregates, retention, холодный слой Parquet в MinIO и дашборд Grafana.

```
tsdb.bench (загрузчик + измерения) ──▶ PostgreSQL + TimescaleDB
                                         │  hypertable (чанки 7 суток), compression, retention
                                         ├──▶ continuous aggregates: 1 мин / 1 ч / 1 сут
                                         ├──▶ MinIO: холодный слой, Parquet (месячные файлы)
                                         └──▶ Grafana: дашборд из 4 панелей
```

## Быстрый старт (Windows, PowerShell, Docker Desktop запущен)

```powershell
cd lab6-tsdb
docker compose build
docker compose up -d timescaledb minio
docker compose run --rm tests                      # проверки, в конце «ВСЕ ТЕСТЫ ПРОЙДЕНЫ»
docker compose run --rm bench --scale smoke        # ~2 минуты на малых данных
docker compose run --rm bench --scale full         # задание: 100 устройств × 365 суток × 10 мин = 5,26 млн показаний
docker compose up -d grafana                       # http://localhost:3000, admin / admin
docker compose down -v                             # остановить всё и удалить данные
```

MinIO больше не публикуется в Docker Hub (и quay.io), поэтому `docker compose build` собирает его из исходников GitHub
(`docker/minio/Dockerfile`, тег `RELEASE.2025-10-15T17-29-55Z`). Первая сборка занимает 5–15 минут, дальше берётся из кэша.
Если сборка не удалась, остальное работает без MinIO: `docker compose up -d timescaledb` и `docker compose run --rm --no-deps bench --scale full --no-minio` –
размеры Parquet будут посчитаны, но файлы в бакет не загрузятся.

Полный прогон занимает около 15 минут на стенде отчёта и 20–45 минут в Docker Desktop; пока он идёт, **не запускайте на компьютере ничего тяжёлого** – это искажает
измерения. Результаты: `.\results\results.json` (все данные), `*.csv` (для Excel, разделитель `;`),
`figures\*.png` (графики для отчёта). Если прогон прервался, продолжите с нужного опыта:
`docker compose run --rm bench --scale full --only e4,e5,e6,e7,e8,e9` (результаты дописываются).

Посмотреть данные вручную: `docker compose exec timescaledb psql -U postgres -d lab6`, например
`SELECT * FROM hypertable_compression_stats('telemetry_narrow');` или `SELECT * FROM timescaledb_information.jobs;`.
Консоль MinIO: http://localhost:9001 (minioadmin / minioadmin), бакет `telemetry-cold`.

## Что делает прогон

| Опыт | Содержание |
|---|---|
| e1 | две схемы: **wide** `(device_id, ts, payload JSONB)` и **narrow** `(ts, device_id, metric_id, value DOUBLE)`; `create_hypertable`, чанк 7 суток; загрузка COPY по суткам; размер `hypertable_size` |
| e2 | 4 запроса (последнее значение, среднее за сутки, аномалии за месяц, один датчик за год) на несжатых данных |
| e3 | компрессия `segmentby=device_id, orderby=ts DESC`; объём, коэффициент, те же 4 запроса (всего 4 конфигурации: схема × компрессия) |
| e4 | continuous aggregates 1 мин / 1 ч / 1 сут (иерархические), время обновления, запросы до и после, сверка с сырыми данными |
| e5 | почему сжатие ведёт себя так: типизированная wide-схема, segmentby с `metric_id`, метки времени без джиттера, байты по колонкам |
| e6 | размер чанка 1 / 7 / 30 / 90 суток: планирование, запросы, компрессия |
| e7 | выгрузка холодного среза в Parquet и MinIO, сравнение размеров, чтение обратно |
| e8 | `add_retention_policy`, удаление старых чанков, политики обновления агрегатов и сжатия |
| e9 | запросы панелей Grafana на реальных данных |

Все результаты запросов сверяются между конфигурациями (число строк и контрольная сумма), агрегаты – с сырыми данными.
Время запроса – медиана 7 прогонов после 2 прогревочных, включая получение всех строк клиентом; кэш «тёплый».

## Файлы

| Путь | Назначение |
|---|---|
| `tsdb/gen.py` | генератор: 4 метрики, суточный и годовой ход, шум, редкие выбросы; детерминирован (seed) |
| `tsdb/schema.py` | DDL, hypertable, компрессия, continuous aggregates, политики, размеры |
| `tsdb/load.py` | загрузка COPY из клиента, замер времени |
| `tsdb/queries.py` | 4 запроса в трёх схемах и по корзинам агрегатов |
| `tsdb/bench.py` | оркестратор опытов e1–e9, CSV и JSON |
| `tsdb/export.py` | Parquet и MinIO |
| `tsdb/plots.py` | графики |
| `grafana/` | провижининг источника данных и дашборд (4 панели) |
| `sql/00_init.sql` | роль `grafana_reader` (создаётся при первом старте БД) |
| `experiments.json` | параметры опытов и масштабов (`smoke`, `quick`, `full`) |
| `tests.py` | проверки на временной базе |

## Модель и ограничения

* Данные синтетические: 100 устройств, показание раз в 10 минут (метки времени с джиттером 0–9 с), 4 метрики
  (температура, влажность, заряд батареи, RSSI), редкие выбросы температуры (≈ 0,02 %). Генератор детерминирован,
  поэтому обе схемы получают одни и те же значения. Размеры сжатия **зависят от данных**: другие ряды дадут другие числа.
* Агрегат 1 мин при периоде опроса 10 мин не уменьшает число строк (в минутной корзине ≤ 1 показание), он нужен как
  основа иерархии и имеет смысл при опросе чаще раза в минуту; обновляется за 30 суток (как в реальной схеме
  «минуты – недавно, часы – год, сутки – всегда»).
* Агрегаты строятся по узкой схеме. Для wide-схемы они делаются так же через выражения `payload ->> 'temperature'`.
* Абсолютные числа зависят от процессора, диска и версии TimescaleDB (`SELECT extversion FROM pg_extension`);
  сравнивайте формы и соотношения. Образ `timescale/timescaledb:latest-pg16` может быть новее версии, на которой
  сделан отчёт – версия записана в `results.json` (`meta`).
* Один сервер, без репликации; кэш PostgreSQL «тёплый» (холодный кэш в Docker Desktop сбросить нельзя).
