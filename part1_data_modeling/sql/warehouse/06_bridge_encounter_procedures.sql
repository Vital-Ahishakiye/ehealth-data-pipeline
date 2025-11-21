DROP TABLE IF EXISTS bridge_encounter_procedures CASCADE;

CREATE TABLE bridge_encounter_procedures (
    bridge_id SERIAL PRIMARY KEY,
    encounter_key INTEGER NOT NULL REFERENCES fact_encounters(encounter_key) ON DELETE CASCADE,
    procedure_key INTEGER NOT NULL REFERENCES dim_procedure(procedure_key) ON DELETE CASCADE,
    UNIQUE(encounter_key, procedure_key)
);

CREATE INDEX idx_bridge_proc_encounter ON bridge_encounter_procedures(encounter_key);
CREATE INDEX idx_bridge_proc_procedure ON bridge_encounter_procedures(procedure_key);

COMMENT ON TABLE bridge_encounter_procedures IS 'Bridge table for encounter-procedure many-to-many relationship';
