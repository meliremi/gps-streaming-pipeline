-- =============================================================
-- 01_ddl_sources.sql
-- Déclaration des tables sources et sinks Flink SQL
-- =============================================================


-- =========================
-- SOURCE : RAW EVENTS (Bronze — Kafka)
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
-- =========================

DROP TABLE IF EXISTS silver_events;

CREATE TABLE silver_events (
    event_id       STRING,
    event_time     BIGINT,
    ts             TIMESTAMP(3),
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