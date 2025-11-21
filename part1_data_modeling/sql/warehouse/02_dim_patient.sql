DROP TABLE IF EXISTS dim_patient CASCADE;

CREATE TABLE dim_patient (
    patient_key SERIAL PRIMARY KEY,               -- Surrogate key for warehouse
    patient_id INTEGER NOT NULL UNIQUE,           -- Natural key mapped from operational VARCHAR
    external_patient_id VARCHAR(100),
    age INTEGER,
    age_group VARCHAR(50),                        -- Pediatric, Young Adult, etc.
    sex VARCHAR(10),
    location VARCHAR(255),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_patient_id ON dim_patient(patient_id);
CREATE INDEX idx_dim_patient_age_group ON dim_patient(age_group);
CREATE INDEX idx_dim_patient_sex ON dim_patient(sex);
CREATE INDEX idx_dim_patient_location ON dim_patient(location);

COMMENT ON TABLE dim_patient IS 'Patient dimension for analytics (SCD Type 1)';
