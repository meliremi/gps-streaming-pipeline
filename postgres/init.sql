-- init.sql — exécuté automatiquement au premier démarrage de PostgreSQL
-- Crée le schéma gold et la table gold.aggregates

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.aggregates (
    window_start    TIMESTAMPTZ     NOT NULL,
    window_end      TIMESTAMPTZ     NOT NULL,
    window_type     VARCHAR(20)     NOT NULL,  -- 'tumbling_1min' ou 'sliding_5min'
    origin_country  VARCHAR(100)    NOT NULL,
    aircraft_count  INTEGER,
    avg_speed_kmh   NUMERIC(8, 2),
    avg_altitude_ft NUMERIC(10, 2),
    max_speed_kmh   NUMERIC(8, 2),
    computed_at     TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (window_start, window_type, origin_country)
);

-- Index pour accélérer les requêtes du dashboard Tableau
CREATE INDEX IF NOT EXISTS idx_aggregates_window_start ON gold.aggregates (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_aggregates_country      ON gold.aggregates (origin_country);
