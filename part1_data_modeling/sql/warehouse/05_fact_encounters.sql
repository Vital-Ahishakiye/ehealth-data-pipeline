DROP TABLE IF EXISTS fact_encounters CASCADE;

CREATE TABLE fact_encounters (
    encounter_key SERIAL PRIMARY KEY,             -- Surrogate key
    encounter_id INTEGER NOT NULL UNIQUE,         -- Natural key mapped from operational VARCHAR

    -- Dimension foreign keys
    patient_key INTEGER NOT NULL REFERENCES dim_patient(patient_key),
    date_id INTEGER NOT NULL REFERENCES dim_time(date_id),

    -- Degenerate dimensions
    facility_id INTEGER,
    encounter_type VARCHAR(100),

    -- Measures
    procedure_count INTEGER DEFAULT 0,
    diagnosis_count INTEGER DEFAULT 0,
    report_count INTEGER DEFAULT 0,

    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_encounters_patient ON fact_encounters(patient_key);
CREATE INDEX idx_fact_encounters_date ON fact_encounters(date_id);
CREATE INDEX idx_fact_encounters_facility ON fact_encounters(facility_id);
CREATE INDEX idx_fact_encounters_type ON fact_encounters(encounter_type);

COMMENT ON TABLE fact_encounters IS 'Central fact table for encounter-based analytics';
