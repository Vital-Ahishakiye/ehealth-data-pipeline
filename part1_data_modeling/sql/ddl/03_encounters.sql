-- =============================================
-- Table: encounters
-- Description: Patient visits to healthcare facilities
-- =============================================

DROP TABLE IF EXISTS encounters CASCADE;

CREATE TABLE encounters (
    encounter_id VARCHAR(20) PRIMARY KEY,       -- e.g., 'ENC00000001'
    patient_id VARCHAR(20) NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
    facility_id VARCHAR(50) NOT NULL REFERENCES facilities(facility_id) ON DELETE CASCADE,
    encounter_date DATE NOT NULL,
    encounter_datetime TIMESTAMP NOT NULL,
    encounter_type VARCHAR(50) CHECK (
    encounter_type IN ('Outpatient','Emergency','Inpatient','Urgent Care','Observation')),
    admission_source VARCHAR(100),              -- How patient arrived
    discharge_disposition VARCHAR(100),         -- Where patient went after
    primary_physician VARCHAR(255),
    referring_physician VARCHAR(255),
    visit_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_encounters_patient ON encounters(patient_id);
CREATE INDEX idx_encounters_facility ON encounters(facility_id);
CREATE INDEX idx_encounters_date ON encounters(encounter_date);
CREATE INDEX idx_encounters_type ON encounters(encounter_type);
CREATE INDEX idx_encounters_datetime ON encounters(encounter_datetime);

-- Comments
COMMENT ON TABLE encounters IS 'Patient visits and encounters at healthcare facilities';
COMMENT ON COLUMN encounters.encounter_type IS 'Inpatient, Outpatient, or Emergency';
COMMENT ON COLUMN encounters.admission_source IS 'Emergency Department, Physician Referral, Transfer, etc.';