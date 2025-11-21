-- =============================================
-- Table: patients
-- Description: Patient demographics (Rwanda context)
-- =============================================

DROP TABLE IF EXISTS patients CASCADE;

CREATE TABLE patients (
    patient_id VARCHAR(20) PRIMARY KEY,         -- e.g., 'PAT0000001'
    date_of_birth DATE NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('M', 'F', 'Other')),
    ethnicity VARCHAR(50),                      -- Rwanda context: optional
    primary_language VARCHAR(50),               -- Kinyarwanda, English, French, Swahili
    contact_email VARCHAR(255),
    contact_phone VARCHAR(20),
    address_line1 VARCHAR(200),
    address_city VARCHAR(100),
    address_state VARCHAR(50),                  -- Province
    address_zipcode VARCHAR(10),
    insurance_provider VARCHAR(100),            -- RAMA, MMI, Private, etc.
    insurance_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Indexes
CREATE INDEX idx_patients_dob ON patients(date_of_birth);
CREATE INDEX idx_patients_gender ON patients(gender);
CREATE INDEX idx_patients_city ON patients(address_city);
CREATE INDEX idx_patients_state ON patients(address_state);
CREATE INDEX idx_patients_insurance ON patients(insurance_provider);
CREATE INDEX idx_patients_active ON patients(is_active);

-- Comments
COMMENT ON TABLE patients IS 'Patient master records';
COMMENT ON COLUMN patients.gender IS 'M=Male, F=Female, Other';
COMMENT ON COLUMN patients.primary_language IS 'Kinyarwanda, English, French, Swahili';
COMMENT ON COLUMN patients.insurance_provider IS 'RAMA, MMI, Private Insurance, Self-Pay, Community Health';