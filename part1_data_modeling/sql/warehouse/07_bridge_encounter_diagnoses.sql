DROP TABLE IF EXISTS bridge_encounter_diagnoses CASCADE;

CREATE TABLE bridge_encounter_diagnoses (
    bridge_id SERIAL PRIMARY KEY,
    encounter_key INTEGER NOT NULL REFERENCES fact_encounters(encounter_key) ON DELETE CASCADE,
    diagnosis_key INTEGER NOT NULL REFERENCES dim_diagnosis(diagnosis_key) ON DELETE CASCADE,
    diagnosis_type VARCHAR(50),                   -- Primary, Secondary, Complication
    UNIQUE(encounter_key, diagnosis_key)
);

CREATE INDEX idx_bridge_diag_encounter ON bridge_encounter_diagnoses(encounter_key);
CREATE INDEX idx_bridge_diag_diagnosis ON bridge_encounter_diagnoses(diagnosis_key);
CREATE INDEX idx_bridge_diag_type ON bridge_encounter_diagnoses(diagnosis_type);

COMMENT ON TABLE bridge_encounter_diagnoses IS 'Bridge table for encounter-diagnosis many-to-many relationship';
