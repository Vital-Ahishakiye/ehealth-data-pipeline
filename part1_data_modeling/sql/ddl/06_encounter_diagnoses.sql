-- =============================================
-- Table: encounter_diagnoses
-- Description: Junction table linking encounters to diagnoses
-- =============================================

DROP TABLE IF EXISTS encounter_diagnoses CASCADE;

CREATE TABLE encounter_diagnoses (
    encounter_diagnosis_id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(20) NOT NULL REFERENCES encounters(encounter_id) ON DELETE CASCADE,
    diagnosis_id VARCHAR(20) NOT NULL REFERENCES diagnoses(diagnosis_id) ON DELETE CASCADE,
    diagnosis_rank INTEGER NOT NULL,            -- 1=primary, 2+=secondary
    is_primary BOOLEAN NOT NULL,                -- True for primary diagnosis
    diagnosis_confidence NUMERIC(3, 2) CHECK (diagnosis_confidence >= 0 AND diagnosis_confidence <= 1),
    diagnosed_by VARCHAR(255),                  -- Clinician name
    diagnosis_datetime TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(encounter_id, diagnosis_id)          -- Prevent duplicate diagnosis per encounter
);

-- Indexes
CREATE INDEX idx_encounter_diagnoses_encounter ON encounter_diagnoses(encounter_id);
CREATE INDEX idx_encounter_diagnoses_diagnosis ON encounter_diagnoses(diagnosis_id);
CREATE INDEX idx_encounter_diagnoses_primary ON encounter_diagnoses(is_primary);
CREATE INDEX idx_encounter_diagnoses_rank ON encounter_diagnoses(diagnosis_rank);

-- Comments
COMMENT ON TABLE encounter_diagnoses IS 'Links encounters to diagnoses (many-to-many)';
COMMENT ON COLUMN encounter_diagnoses.diagnosis_rank IS '1=primary diagnosis, 2+=secondary';
COMMENT ON COLUMN encounter_diagnoses.diagnosis_confidence IS 'Confidence score 0.0-1.0 (for ML predictions)';