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
    'properties.group.id' = 'flink-group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);