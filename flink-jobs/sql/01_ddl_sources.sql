-- =============================================================
-- 01_ddl_sources.sql
-- Déclaration des tables sources et sinks Flink SQL
-- Documentation de référence — les DDL sont embarqués dans
-- 02_transform_silver.sql et 03_aggregate_gold.sql pour
-- permettre leur soumission indépendante via sql-client.sh -f
-- =============================================================


-- =========================
-- SOURCE : RAW EVENTS (Bronze — Kafka)
-- Watermark : tolérance 10s pour les late events
-- =========================

DROP TABLE IF EXISTS raw_events;

CREATE TABLE raw_events (
    event_id       STRING,
    event_time     BIGINT,
    source         STRING,
    icao24         STRING,
    callsign       STRING,
    origin_country STRING,
    longitude      DOUBLE,
    latitude       DOUBLE,
    baro_altitude  DOUBLE,
    velocity       DOUBLE,
    true_track     DOUBLE,
    on_ground      BOOLEAN,
    ts             AS TO_TIMESTAMP_LTZ(event_time, 0),
    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'raw_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-raw-consumer',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'scan.startup.mode'            = 'earliest-offset'
);


-- =========================
-- SINK : SILVER EVENTS (Silver — Kafka)
-- ts exclu : recompuité depuis event_time dans le job Gold
-- =========================

DROP TABLE IF EXISTS silver_events;

CREATE TABLE silver_events (
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
    is_valid       BOOLEAN
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'silver_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format'                       = 'json'
);


-- =========================
-- SINK : LATE EVENTS (side output — DAT section 7.2)
-- Événements arrivés > 30s après leur event_time
-- =========================

DROP TABLE IF EXISTS late_events_sink;

CREATE TABLE late_events_sink (
    event_id       STRING,
    event_time     BIGINT,
    icao24         STRING,
    callsign       STRING,
    origin_country STRING,
    longitude      DOUBLE,
    latitude       DOUBLE,
    delay_seconds  BIGINT,
    detected_at    TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'late_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format'                       = 'json'
);


-- =========================
-- SINK : GOLD OUTPUT (Gold — Kafka)
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
-- SINK : GOLD AGGREGATES (PostgreSQL — gold.aggregates)
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
