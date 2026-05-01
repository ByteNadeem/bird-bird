PRAGMA foreign_keys = ON;

-- Normalized lookup tables derived from comma-separated columns.
CREATE TABLE IF NOT EXISTS study_taxa (
	study_id INTEGER NOT NULL,
	taxon_id TEXT NOT NULL,
	PRIMARY KEY (study_id, taxon_id),
	FOREIGN KEY (study_id) REFERENCES studies(id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS study_sensors (
	study_id INTEGER NOT NULL,
	sensor_type_id TEXT NOT NULL,
	PRIMARY KEY (study_id, sensor_type_id),
	FOREIGN KEY (study_id) REFERENCES studies(id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS individual_sensors (
	individual_id INTEGER NOT NULL,
	sensor_type_id TEXT NOT NULL,
	PRIMARY KEY (individual_id, sensor_type_id),
	FOREIGN KEY (individual_id) REFERENCES individuals(id) ON UPDATE CASCADE ON DELETE CASCADE
);

BEGIN;

-- Refresh normalized rows from current core tables.
DELETE FROM study_taxa;
DELETE FROM study_sensors;
DELETE FROM individual_sensors;

WITH RECURSIVE split(study_id, rest, token) AS (
	SELECT id, trim(COALESCE(taxon_ids, '')) || ',', ''
	FROM studies
	WHERE trim(COALESCE(taxon_ids, '')) <> ''
	UNION ALL
	SELECT
		study_id,
		substr(rest, instr(rest, ',') + 1),
		trim(substr(rest, 1, instr(rest, ',') - 1))
	FROM split
	WHERE rest <> ''
)
INSERT OR IGNORE INTO study_taxa (study_id, taxon_id)
SELECT study_id, token
FROM split
WHERE token <> '';

WITH RECURSIVE split(study_id, rest, token) AS (
	SELECT id, trim(COALESCE(sensor_type_ids, '')) || ',', ''
	FROM studies
	WHERE trim(COALESCE(sensor_type_ids, '')) <> ''
	UNION ALL
	SELECT
		study_id,
		substr(rest, instr(rest, ',') + 1),
		trim(substr(rest, 1, instr(rest, ',') - 1))
	FROM split
	WHERE rest <> ''
)
INSERT OR IGNORE INTO study_sensors (study_id, sensor_type_id)
SELECT study_id, lower(token)
FROM split
WHERE token <> '';

WITH RECURSIVE split(individual_id, rest, token) AS (
	SELECT id, trim(COALESCE(sensor_type_ids, '')) || ',', ''
	FROM individuals
	WHERE trim(COALESCE(sensor_type_ids, '')) <> ''
	UNION ALL
	SELECT
		individual_id,
		substr(rest, instr(rest, ',') + 1),
		trim(substr(rest, 1, instr(rest, ',') - 1))
	FROM split
	WHERE rest <> ''
)
INSERT OR IGNORE INTO individual_sensors (individual_id, sensor_type_id)
SELECT individual_id, lower(token)
FROM split
WHERE token <> '';

CREATE INDEX IF NOT EXISTS idx_study_taxa_taxon_id
	ON study_taxa (taxon_id);

CREATE INDEX IF NOT EXISTS idx_study_sensors_sensor
	ON study_sensors (sensor_type_id);

CREATE INDEX IF NOT EXISTS idx_individual_sensors_sensor
	ON individual_sensors (sensor_type_id);

COMMIT;

SELECT COUNT(*) AS study_taxa_rows FROM study_taxa;
SELECT COUNT(*) AS study_sensors_rows FROM study_sensors;
SELECT COUNT(*) AS individual_sensors_rows FROM individual_sensors;
