
PRAGMA foreign_keys = ON;

-- Core study table from Movebank studies payload.
CREATE TABLE IF NOT EXISTS studies (
    id INTEGER PRIMARY KEY,
    main_location_lat REAL,
    main_location_long REAL,
    taxon_ids TEXT NOT NULL DEFAULT '',
    sensor_type_ids TEXT NOT NULL DEFAULT '',
    contact_person_name TEXT
);

-- Core individual table from Movebank individuals payload.
-- study_id is optional at ingest time and can be backfilled later.
CREATE TABLE IF NOT EXISTS individuals (
    id INTEGER PRIMARY KEY,
    study_id INTEGER REFERENCES studies(id) ON UPDATE CASCADE ON DELETE SET NULL,
    local_identifier TEXT,
    nick_name TEXT,
    ring_id TEXT,
    sex TEXT CHECK (sex IN ('m', 'f', 'u') OR sex IS NULL OR sex = ''),
    taxon_canonical_name TEXT,
    timestamp_start TEXT,
    timestamp_end TEXT,
    number_of_events INTEGER NOT NULL DEFAULT 0 CHECK (number_of_events >= 0),
    number_of_deployments INTEGER NOT NULL DEFAULT 0 CHECK (number_of_deployments >= 0),
    sensor_type_ids TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_individuals_study_id
    ON individuals (study_id);

CREATE INDEX IF NOT EXISTS idx_individuals_taxon
    ON individuals (taxon_canonical_name);

CREATE INDEX IF NOT EXISTS idx_studies_contact_person
    ON studies (contact_person_name);
