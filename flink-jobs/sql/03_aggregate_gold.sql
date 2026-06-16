-- =============================================================
-- 03_aggregate_gold.sql
-- Job 3 : SILVER → GOLD
-- Tumbling Window 1min + Sliding Window 5min/1min
-- Double sink : Kafka gold_output + PostgreSQL gold.aggregates
-- =============================================================


-- =========================
-- SOURCE : SILVER EVENTS avec Watermark
-- =========================

DROP TABLE IF EXISTS silver_windowed;

CREATE TABLE silver_windowed (
    event_id       STRING,
    event_time     BIGINT,
    icao24         STRING,
    callsign       STRING,
    origin_country STRING,
    longitude      DOUBLE,
    latitude       DOUBLE,
    altitude_ft    DOUBLE,
    speed_kmh      DOUBLE,
    heading_deg    DOUBLE,
    cardinal       STRING,
    is_valid       BOOLEAN,
    ts             AS TO_TIMESTAMP_LTZ(event_time, 0),
    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'silver_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-silver-consumer',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'scan.startup.mode'            = 'earliest-offset'
);


-- =========================
-- SINK 1 : Kafka gold_output (Gold)
-- =========================

DROP TABLE IF EXISTS gold_output;

CREATE TABLE gold_output (
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    window_type     STRING,
    origin_country  STRING,
    aircraft_count  BIGINT,
    avg_speed_kmh   DOUBLE,
    avg_altitude_ft DOUBLE,
    max_speed_kmh   DOUBLE
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'gold_output',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format'                       = 'json'
);


-- =========================
-- SINK 2 : PostgreSQL gold.aggregates (JDBC)
-- =========================

DROP TABLE IF EXISTS gold_aggregates_pg;

CREATE TABLE gold_aggregates_pg (
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    window_type     STRING,
    origin_country  STRING,
    aircraft_count  BIGINT,
    avg_speed_kmh   DOUBLE,
    avg_altitude_ft DOUBLE,
    max_speed_kmh   DOUBLE
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://postgres:5432/gold',
    'table-name' = 'gold.aggregates',
    'username'   = 'gold_user',
    'password'   = 'gold_password',
    'driver'     = 'org.postgresql.Driver'
);


-- =========================
-- TUMBLING WINDOW 1 min → Kafka
-- =========================

INSERT INTO gold_output
SELECT
    window_start,
    window_end,
    'tumbling_1min'             AS window_type,
    origin_country,
    COUNT(*)                    AS aircraft_count,
    ROUND(AVG(speed_kmh), 2)   AS avg_speed_kmh,
    ROUND(AVG(altitude_ft), 0) AS avg_altitude_ft,
    MAX(speed_kmh)              AS max_speed_kmh
FROM TABLE(
    TUMBLE(TABLE silver_windowed, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, origin_country;


-- =========================
-- TUMBLING WINDOW 1 min → PostgreSQL
-- =========================

INSERT INTO gold_aggregates_pg
SELECT
    window_start,
    window_end,
    'tumbling_1min'             AS window_type,
    origin_country,
    COUNT(*)                    AS aircraft_count,
    ROUND(AVG(speed_kmh), 2)   AS avg_speed_kmh,
    ROUND(AVG(altitude_ft), 0) AS avg_altitude_ft,
    MAX(speed_kmh)              AS max_speed_kmh
FROM TABLE(
    TUMBLE(TABLE silver_windowed, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, origin_country;


-- =========================
-- SLIDING WINDOW 5 min / 1 min → Kafka
-- =========================

INSERT INTO gold_output
SELECT
    window_start,
    window_end,
    'sliding_5min'              AS window_type,
    origin_country,
    COUNT(*)                    AS aircraft_count,
    ROUND(AVG(speed_kmh), 2)   AS avg_speed_kmh,
    ROUND(AVG(altitude_ft), 0) AS avg_altitude_ft,
    MAX(speed_kmh)              AS max_speed_kmh
FROM TABLE(
    HOP(TABLE silver_windowed, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, origin_country;


-- =========================
-- SLIDING WINDOW 5 min / 1 min → PostgreSQL
-- =========================

INSERT INTO gold_aggregates_pg
SELECT
    window_start,
    window_end,
    'sliding_5min'              AS window_type,
    origin_country,
    COUNT(*)                    AS aircraft_count,
    ROUND(AVG(speed_kmh), 2)   AS avg_speed_kmh,
    ROUND(AVG(altitude_ft), 0) AS avg_altitude_ft,
    MAX(speed_kmh)              AS max_speed_kmh
FROM TABLE(
    HOP(TABLE silver_windowed, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, origin_country;