-- =============================================
-- Table: diagnoses
-- Description: Master diagnosis catalog (ICD-10 codes)
-- =============================================

DROP TABLE IF EXISTS diagnoses CASCADE;

CREATE TABLE diagnoses (
    diagnosis_id VARCHAR(20) PRIMARY KEY,       -- e.g., 'DIAG001'
    diagnosis_code VARCHAR(50) UNIQUE NOT NULL, -- ICD-10 code
    diagnosis_name VARCHAR(255) NOT NULL,
    diagnosis_category VARCHAR(100),            -- Respiratory, Cardiovascular, Infectious, etc.
    severity VARCHAR(50) CHECK (severity IN ('Mild', 'Moderate', 'Severe', 'Critical', 'N/A')),
    is_chronic BOOLEAN DEFAULT FALSE,           -- Chronic condition
    is_reportable BOOLEAN DEFAULT FALSE,        -- Must be reported to health authorities
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_diagnoses_code ON diagnoses(diagnosis_code);
CREATE INDEX idx_diagnoses_category ON diagnoses(diagnosis_category);
CREATE INDEX idx_diagnoses_severity ON diagnoses(severity);
CREATE INDEX idx_diagnoses_name ON diagnoses(diagnosis_name);

-- Comments
COMMENT ON TABLE diagnoses IS 'Master catalog of diagnoses with ICD-10 codes';
COMMENT ON COLUMN diagnoses.diagnosis_code IS 'Standard ICD-10 diagnosis code';
COMMENT ON COLUMN diagnoses.severity IS 'Mild, Moderate, Severe, Critical, or N/A';
COMMENT ON COLUMN diagnoses.is_reportable IS 'True if condition must be reported (e.g., infectious diseases)';