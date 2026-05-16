-- =========================
-- SOURCE : RAW EVENTS (Kafka)
-- =========================

CREATE TABLE raw_events (
    icao24 STRING,
    callsign STRING,
    longitude DOUBLE,
    latitude DOUBLE,
    altitude DOUBLE,
    velocity DOUBLE,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);


-- =========================
-- DESTINATION : SILVER EVENTS (Kafka)
-- =========================

CREATE TABLE silver_events (
    icao24 STRING,
    callsign STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    altitude_ft DOUBLE,
    speed_kmh DOUBLE,
    direction STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'silver_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);


-- =========================
-- TRANSFORMATION (RAW → SILVER)
-- =========================

INSERT INTO silver_events
SELECT
    icao24,
    callsign,
    latitude,
    longitude,
    altitude * 3.28084 AS altitude_ft,
    velocity * 1.852 AS speed_kmh,

    CASE
        WHEN longitude >= 0 AND latitude >= 0 THEN 'NE'
        WHEN longitude < 0 AND latitude >= 0 THEN 'NW'
        WHEN longitude < 0 AND latitude < 0 THEN 'SW'
        ELSE 'SE'
    END AS direction,

    event_time
FROM raw_events
WHERE
    latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND altitude IS NOT NULL
    AND altitude >= 0
    AND velocity <= 330;