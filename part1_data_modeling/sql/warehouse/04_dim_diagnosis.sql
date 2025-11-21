DROP TABLE IF EXISTS dim_diagnosis CASCADE;

CREATE TABLE dim_diagnosis (
    diagnosis_key SERIAL PRIMARY KEY,             -- Surrogate key for warehouse
    diagnosis_id INTEGER NOT NULL UNIQUE,         -- Natural key mapped from operational VARCHAR
    diagnosis_code VARCHAR(50),
    diagnosis_name VARCHAR(255),
    description TEXT,
    category VARCHAR(100),
    severity VARCHAR(50),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_diagnosis_id ON dim_diagnosis(diagnosis_id);
CREATE INDEX idx_dim_diagnosis_code ON dim_diagnosis(diagnosis_code);
CREATE INDEX idx_dim_diagnosis_category ON dim_diagnosis(category);
CREATE INDEX idx_dim_diagnosis_severity ON dim_diagnosis(severity);

COMMENT ON TABLE dim_diagnosis IS 'Diagnosis dimension for analytics';
