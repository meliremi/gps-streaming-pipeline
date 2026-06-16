-- =============================================================
-- 02_transform_silver.sql
-- Job 2 : RAW (Bronze) → SILVER
-- Filtrage + Normalisation + Watermark event_time
-- =============================================================

SET 'execution.runtime-mode' = 'streaming';

-- =========================
-- SOURCE : RAW EVENTS (Kafka)
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
-- DESTINATION : SILVER EVENTS (Kafka)
-- ts exclu : le job Gold le recompute depuis event_time
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
-- TRANSFORMATION : RAW → SILVER
-- Normalisation unités + calcul cardinal + filtre invalides
-- =========================

INSERT INTO silver_events
SELECT
    event_id,
    event_time,
    icao24,
    TRIM(callsign)                      AS callsign,
    origin_country,
    longitude,
    latitude,
    ROUND(baro_altitude * 3.28084, 0)   AS altitude_ft,
    ROUND(velocity * 3.6, 1)            AS speed_kmh,
    true_track                          AS heading_deg,
    CASE
        WHEN true_track >= 315 OR  true_track < 45  THEN 'N'
        WHEN true_track >= 45  AND true_track < 135 THEN 'E'
        WHEN true_track >= 135 AND true_track < 225 THEN 'S'
        ELSE 'W'
    END                                 AS cardinal,
    TRUE                                AS is_valid
FROM raw_events
WHERE latitude      IS NOT NULL
  AND longitude     IS NOT NULL
  AND baro_altitude >= 0
  AND velocity      <= 330
