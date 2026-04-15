PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS species (
    id INTEGER PRIMARY KEY,
    species_code TEXT NOT NULL UNIQUE,
    scientific_name TEXT NOT NULL,
    common_name TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY,
    species_id INTEGER NOT NULL,
    event_timestamp TEXT NOT NULL,
    week_start TEXT NOT NULL,
    deployment_id TEXT,
    latitude REAL NOT NULL CHECK (latitude >= -90 AND latitude <= 90),
    longitude REAL NOT NULL CHECK (longitude >= -180 AND longitude <= 180),
    source_file TEXT,
    FOREIGN KEY (species_id) REFERENCES species(id) ON UPDATE CASCADE ON DELETE RESTRICT,
    UNIQUE (species_id, event_timestamp, deployment_id, latitude, longitude)
);

CREATE INDEX IF NOT EXISTS idx_species_code
    ON species (species_code);

CREATE INDEX IF NOT EXISTS idx_observations_species_id
    ON observations (species_id);

CREATE INDEX IF NOT EXISTS idx_observations_week_start
    ON observations (week_start);

CREATE INDEX IF NOT EXISTS idx_observations_timestamp
    ON observations (event_timestamp);
